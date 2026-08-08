// ════════════════════════════════════════════════════════════════════
// deposit-engine.js — permanent per-user deposit detection (v6, FREE-ONLY)
//
// v6 CHANGE (important): Etherscan stopped free-tier access to BNB Chain,
// Avalanche, Base and OP in Nov 2025, and BscScan's own free API was fully
// deprecated in Dec 2025. Any code relying on Etherscan/BscScan for BNB /
// USDT-BEP20 / other BEP-20 tokens will silently detect ZERO deposits on
// those chains no matter how correct the API key is.
//
// This version removes that dependency entirely for EVM chains. It talks
// directly to each chain's own free public RPC node (no key, no paywall,
// run by the chain's own foundation — these do not disappear):
//   ETH        -> https://eth.llamarpc.com
//   BSC        -> https://bsc-dataseed.binance.org/   (Binance's own node)
//   POLYGON    -> https://polygon-rpc.com
//   AVALANCHE  -> https://api.avax.network/ext/bc/C/rpc
// Override any of these with ETH_RPC_URL / BSC_RPC_URL / POLYGON_RPC_URL /
// AVALANCHE_RPC_URL env vars if you ever want a private/paid RPC instead.
//
// Native coin deposits (ETH/BNB/POL/AVAX): detected by scanning new blocks
// once per poll cycle (shared scan across ALL users, not per-user calls).
// Token deposits (ERC20/BEP20, e.g. USDT/USDC/...): detected via
// eth_getLogs on the Transfer(address,address,uint256) event, filtered to
// only OUR users' addresses in a single call per token per poll cycle.
//
// TRON / BTC / SOL polling is unchanged — TronGrid, mempool.space and the
// Solana public RPC are still free with no paywall equivalent to Etherscan's.
//
// Every failure path below now logs a warning instead of failing silently.
// A protected debug endpoint (/api/debug/status, /api/debug/poll-now) is
// included so you can see exactly what happened on the last poll cycle.
// ════════════════════════════════════════════════════════════════════

import fetch from 'node-fetch';
import { ethers } from 'ethers';
import { deriveAddresses, WALLETS_ENABLED } from './wallet-gen.js';

const TRONGRID_KEY = process.env.TRONGRID_API_KEY || '';
const SOLANA_RPC   = process.env.SOLANA_RPC || 'https://api.mainnet-beta.solana.com';
const POLL_MS      = Math.max(30_000, Number(process.env.DEPOSIT_POLL_MS || 60_000));
const MIN_CONF     = Math.max(1, Number(process.env.DEPOSIT_MIN_CONFIRMATIONS || 3));
// Safety cap so a long backend downtime doesn't try to scan 500k blocks in one go.
const MAX_BLOCK_SPAN = Math.max(50, Number(process.env.DEPOSIT_MAX_BLOCK_SPAN || 3000));
// Set this in Render env to protect /api/debug/* (pick any random string).
const DEBUG_KEY = process.env.DEBUG_ADMIN_KEY || '';

const TRON_USDT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';

const EVM_CHAINS = {
  1:     { name: 'ETH',       coin: 'ETH',  rpc: process.env.ETH_RPC_URL       || 'https://eth.llamarpc.com' },
  56:    { name: 'BSC',       coin: 'BNB',  rpc: process.env.BSC_RPC_URL       || 'https://bsc-dataseed.binance.org/' },
  137:   { name: 'POLYGON',   coin: 'POL',  rpc: process.env.POLYGON_RPC_URL   || 'https://polygon-rpc.com' },
  43114: { name: 'AVALANCHE', coin: 'AVAX', rpc: process.env.AVALANCHE_RPC_URL || 'https://api.avax.network/ext/bc/C/rpc' },
};

const TOKENS = [
  { chainId: 1,   coin: 'USDT', decimals: 6,  contract: '0xdac17f958d2ee523a2206206994597c13d831ec7' },
  { chainId: 1,   coin: 'USDC', decimals: 6,  contract: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' },
  { chainId: 1,   coin: 'LINK', decimals: 18, contract: '0x514910771af9ca656af840dff83e8264ecf986ca' },
  { chainId: 1,   coin: 'SHIB', decimals: 18, contract: '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce' },
  { chainId: 1,   coin: 'UNI',  decimals: 18, contract: '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984' },
  { chainId: 56,  coin: 'USDT', decimals: 18, contract: '0x55d398326f99059ff775485246999027b3197955' },
  { chainId: 56,  coin: 'USDC', decimals: 18, contract: '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d' },
  { chainId: 56,  coin: 'DOGE', decimals: 8,  contract: '0xba2ae424d960c26247dd6c32edc70b295c744c43' },
  { chainId: 56,  coin: 'XRP',  decimals: 18, contract: '0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe' },
  { chainId: 56,  coin: 'ADA',  decimals: 18, contract: '0x3ee2200efb3400fabb9aacf31297cbdd1d435d47' },
  { chainId: 56,  coin: 'LTC',  decimals: 18, contract: '0x4338665cbb7b2485a8855a139b75d5e34ab0db94' },
  { chainId: 56,  coin: 'TON',  decimals: 9,  contract: '0x76a797a59ba2c17726896976b7b3747bfd1d220f' },
];

const TRANSFER_TOPIC = ethers.id('Transfer(address,address,uint256)');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const safeKey = (value) => String(value).replace(/[.#$/[\]]/g, '_');
const r8 = (value) => Number((Number(value) || 0).toFixed(8));

export function mountDepositEngine(app, ctx) {
  const { db, admin, tgFetch, TG_CHAT } = ctx;
  const log = ctx.log || ((tag, message) => console.log(`[${tag}] ${message}`));
  const warn = (tag, message) => console.warn(`[${tag}][WARN] ${message}`);

  if (!WALLETS_ENABLED || !db || !admin) {
    warn('WALLET', 'deposit engine OFF — MASTER_MNEMONIC missing/invalid, or Firebase admin/db not configured. Per-user addresses and ALL deposit detection are disabled.');
    app.get('/api/wallet/:uid', (_req, res) => res.status(503).json({ error: 'wallets_disabled' }));
    return { ensureWallet: async () => null };
  }

  if (!DEBUG_KEY) {
    warn('WALLET', 'DEBUG_ADMIN_KEY not set — /api/debug/* endpoints are disabled. Set it in Render env vars to enable live debugging.');
  }
  if (!TRONGRID_KEY) {
    warn('WALLET', 'TRONGRID_API_KEY not set — TRON/USDT-TRC20 polling will use the public rate limit, which may be slow or occasionally throttled.');
  }

  // ── EVM providers, created once and reused (with per-chain error isolation) ──
  const providers = {};
  for (const [chainId, meta] of Object.entries(EVM_CHAINS)) {
    try {
      providers[chainId] = new ethers.JsonRpcProvider(meta.rpc, Number(chainId), { staticNetwork: true });
    } catch (error) {
      warn('WALLET', `could not create RPC provider for ${meta.name} (${meta.rpc}): ${error.message}`);
    }
  }

  const inflight = new Map();
  const lastPoll = { at: 0, summary: [] };

  async function allocateIndex() {
    const result = await db.ref('meta/wallet_next_index').transaction((current) =>
      Number.isSafeInteger(current) && current >= 0 ? current + 1 : 1,
    );
    if (!result.committed) throw new Error('wallet index allocation failed');
    return Number(result.snapshot.val()) - 1;
  }

  function publicAddresses(wallet) {
    return { evm: wallet.evm, tron: wallet.tron, btc: wallet.btc, sol: wallet.sol };
  }

  async function ensureWallet(uid) {
    if (!uid) throw new Error('uid required');
    const existing = await db.ref(`user_wallets/${uid}`).once('value');
    if (existing.exists()) return publicAddresses(existing.val());
    if (inflight.has(uid)) return inflight.get(uid);

    const task = (async () => {
      const index = await allocateIndex();
      const addresses = deriveAddresses(index);
      const walletRef = db.ref(`user_wallets/${uid}`);
      const result = await walletRef.transaction((current) => current || {
        ...addresses,
        index,
        uid,
        createdAt: Date.now(),
      });
      if (!result.committed || !result.snapshot.exists()) throw new Error('wallet creation failed');
      const wallet = result.snapshot.val();
      await db.ref('wallet_index').update({
        [safeKey(wallet.evm.toLowerCase())]: uid,
        [safeKey(wallet.tron)]: uid,
        [safeKey(wallet.btc)]: uid,
        [safeKey(wallet.sol)]: uid,
      });
      if (wallet.index === index) log('WALLET', `created permanent addresses for ${uid} at index ${index}`);
      return publicAddresses(wallet);
    })().finally(() => inflight.delete(uid));

    inflight.set(uid, task);
    return task;
  }

  // A wallet is generated automatically when a user record is first created
  // (fires for existing users too, the first time this listener attaches).
  db.ref('users').on('child_added', (snapshot) => {
    ensureWallet(snapshot.key).catch((error) => warn('WALLET', `auto-create for ${snapshot.key} failed: ${error.message}`));
  });

  app.get('/api/wallet/:uid', async (req, res) => {
    try {
      const requestedUid = String(req.params.uid || '').trim();
      const authorization = String(req.headers.authorization || '');
      if (!authorization.startsWith('Bearer ')) return res.status(401).json({ error: 'authentication_required' });
      const decoded = await admin.auth().verifyIdToken(authorization.slice(7));
      if (decoded.uid !== requestedUid) return res.status(403).json({ error: 'forbidden' });
      res.set('Cache-Control', 'private, no-store');
      return res.json(await ensureWallet(decoded.uid));
    } catch (error) {
      const authError = String(error.code || '').startsWith('auth/');
      warn('WALLET', `/api/wallet/${req.params.uid} failed: ${error.message}`);
      return res.status(authError ? 401 : 500).json({ error: authError ? 'invalid_token' : 'wallet_unavailable' });
    }
  });

  // ── Crediting (unchanged: atomic, dedupe-safe transaction) ──
  async function credit(uid, coin, chain, amount, txid, address, eventId = '0') {
    const amt = r8(amount);
    if (!(amt > 0) || !txid) return false;
    const depositKey = safeKey(`${chain}_${txid}_${coin}_${eventId}`);
    const historyKey = safeKey(`auto_${depositKey}`);
    const now = Date.now();
    let credited = false;
    const result = await db.ref().transaction((root) => {
      root = root || {};
      root.deposits_seen = root.deposits_seen || {};
      if (root.deposits_seen[depositKey]) return;
      root.users = root.users || {};
      root.users[uid] = root.users[uid] || {};
      const user = root.users[uid];
      user.balances = user.balances || {};
      user.balances[coin] = r8((Number(user.balances[coin]) || 0) + amt);
      if (coin === 'USDT') user.balance = user.balances[coin];
      user.history = user.history || {};
      user.history[historyKey] = {
        hid: historyKey, ts: now, isoDate: new Date(now).toISOString(),
        type: 'DEPOSIT', coin, amt, status: 'COMPLETED', chain, txid, address, method: 'ONCHAIN_AUTO',
      };
      root.deposits_seen[depositKey] = { uid, coin, amt, chain, txid, eventId, ts: now };
      credited = true;
      return root;
    });
    if (!result.committed || !credited) return false;
    log('DEPOSIT', `${uid} +${amt} ${coin} (${chain}) ${txid}`);
    if (tgFetch && TG_CHAT) {
      tgFetch('sendMessage', {
        chat_id: TG_CHAT,
        text: `💰 <b>Auto Deposit</b>\nUser: <code>${uid}</code>\n${amt} ${coin} on ${chain}\nTx: <code>${txid}</code>`,
        parse_mode: 'HTML',
      }).catch((error) => warn('DEPOSIT', `telegram notify failed (deposit itself was still credited): ${error.message}`));
    }
    return true;
  }

  // ── EVM: one shared scan per chain per cycle covers every user at once ──
  async function pollEvmChain(chainId, meta, addressSet, addressToUid) {
    const provider = providers[chainId];
    if (!provider) { warn('POLL', `${meta.name}: no RPC provider available, skipped`); return { ok: false, newDeposits: 0 }; }
    if (addressSet.size === 0) return { ok: true, newDeposits: 0 };

    const stateRef = db.ref(`meta/evm_scan/${chainId}`);
    let newDeposits = 0;

    let currentBlock;
    try {
      currentBlock = await provider.getBlockNumber();
    } catch (error) {
      warn('POLL', `${meta.name}: RPC unreachable (${meta.rpc}) — ${error.message}`);
      return { ok: false, newDeposits: 0 };
    }

    const safeToBlock = currentBlock - MIN_CONF;
    const stateSnap = await stateRef.once('value');
    let lastBlock = stateSnap.exists() ? Number(stateSnap.val().lastBlock) : safeToBlock - 1;
    if (!Number.isFinite(lastBlock) || lastBlock < 0) lastBlock = safeToBlock - 1;
    if (safeToBlock <= lastBlock) return { ok: true, newDeposits: 0 }; // nothing new yet

    const fromBlock = lastBlock + 1;
    const toBlock = Math.min(safeToBlock, fromBlock + MAX_BLOCK_SPAN - 1);

    // Native coin transfers: scan blocks in this range for txs sent to our addresses.
    try {
      const CONCURRENCY = 8;
      for (let start = fromBlock; start <= toBlock; start += CONCURRENCY) {
        const batch = [];
        for (let b = start; b <= Math.min(start + CONCURRENCY - 1, toBlock); b++) batch.push(b);
        const blocks = await Promise.all(batch.map((b) =>
          provider.getBlock(b, true).catch((error) => { warn('POLL', `${meta.name} block ${b}: ${error.message}`); return null; })
        ));
        for (const block of blocks) {
          if (!block || !block.prefetchedTransactions) continue;
          for (const tx of block.prefetchedTransactions) {
            if (!tx.to) continue;
            const to = tx.to.toLowerCase();
            if (!addressSet.has(to) || tx.value <= 0n) continue;
            const uid = addressToUid.get(to);
            const amount = Number(ethers.formatEther(tx.value));
            const ok = await credit(uid, meta.coin, meta.name, amount, tx.hash, to, `native_${block.number}`);
            if (ok) newDeposits++;
          }
        }
      }
    } catch (error) {
      warn('POLL', `${meta.name} native scan blocks ${fromBlock}-${toBlock}: ${error.message}`);
    }

    // Token (ERC20/BEP20) transfers: one eth_getLogs call per token, filtered to our addresses.
    const paddedAddrs = [...addressSet].map((a) => ethers.zeroPadValue(a, 32));
    for (const token of TOKENS.filter((t) => t.chainId === Number(chainId))) {
      try {
        const logs = await provider.getLogs({
          address: token.contract,
          fromBlock, toBlock,
          topics: [TRANSFER_TOPIC, null, paddedAddrs],
        });
        for (const entry of logs) {
          const to = ('0x' + entry.topics[2].slice(26)).toLowerCase();
          const uid = addressToUid.get(to);
          if (!uid) continue;
          const value = Number(BigInt(entry.data)) / 10 ** token.decimals;
          const ok = await credit(uid, token.coin, meta.name, value, entry.transactionHash, to, `token_${entry.blockNumber}_${entry.logIndex ?? entry.index ?? 0}`);
          if (ok) newDeposits++;
        }
      } catch (error) {
        warn('POLL', `${meta.name} token ${token.coin} logs ${fromBlock}-${toBlock}: ${error.message}`);
      }
    }

    await stateRef.set({ lastBlock: toBlock, updatedAt: Date.now() });
    return { ok: true, newDeposits };
  }

  // ── TRON / BTC / SOL: unchanged, still free with no paywall equivalent ──
  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) throw new Error(`HTTP ${response.status} from ${new URL(url).hostname}`);
    return response.json();
  }

  async function pollTron(uid, address) {
    const headers = TRONGRID_KEY ? { 'TRON-PRO-API-KEY': TRONGRID_KEY } : {};
    const native = await fetchJson(`https://api.trongrid.io/v1/accounts/${address}/transactions?only_to=true&only_confirmed=true&limit=20`, { headers });
    for (const tx of native.data || []) {
      const contract = tx.raw_data?.contract?.[0];
      if (contract?.type !== 'TransferContract' || tx.ret?.[0]?.contractRet !== 'SUCCESS') continue;
      await credit(uid, 'TRX', 'TRON', Number(contract.parameter.value.amount) / 1e6, tx.txID, address, 'native');
    }
    const tokens = await fetchJson(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?only_to=true&only_confirmed=true&limit=20&contract_address=${TRON_USDT}`, { headers });
    for (const tx of tokens.data || []) {
      if (tx.to !== address) continue;
      await credit(uid, 'USDT', 'TRON', Number(tx.value) / 1e6, tx.transaction_id, address, `trc20_${tx.block_timestamp || 0}`);
    }
  }

  async function pollBtc(uid, address) {
    const txs = await fetchJson(`https://mempool.space/api/address/${address}/txs`);
    if (!Array.isArray(txs)) return;
    for (const tx of txs.slice(0, 20)) {
      if (!tx.status?.confirmed) continue;
      const received = (tx.vout || []).filter((out) => out.scriptpubkey_address === address).reduce((sum, out) => sum + out.value, 0);
      await credit(uid, 'BTC', 'BITCOIN', received / 1e8, tx.txid, address, 'outputs');
    }
  }

  async function rpc(method, params) {
    const result = await fetchJson(SOLANA_RPC, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) });
    if (result.error) throw new Error(result.error.message || 'Solana RPC error');
    return result.result;
  }

  async function pollSol(uid, address) {
    const signatures = await rpc('getSignaturesForAddress', [address, { limit: 10 }]);
    for (const item of signatures || []) {
      if (item.err || item.confirmationStatus !== 'finalized') continue;
      const tx = await rpc('getTransaction', [item.signature, { commitment: 'finalized', maxSupportedTransactionVersion: 0 }]);
      if (!tx?.meta) continue;
      const keys = tx.transaction.message.accountKeys.map((key) => typeof key === 'string' ? key : key.pubkey);
      const index = keys.indexOf(address);
      if (index < 0) continue;
      const delta = (tx.meta.postBalances[index] - tx.meta.preBalances[index]) / 1e9;
      if (delta > 0) await credit(uid, 'SOL', 'SOLANA', delta, item.signature, address, 'native');
    }
  }

  // ── main cycle ──
  let busy = false;
  async function runOnce() {
    if (busy) { warn('POLL', 'previous cycle still running, skipped this tick'); return lastPoll.summary; }
    busy = true;
    const summary = [];
    try {
      const wallets = (await db.ref('user_wallets').once('value')).val() || {};
      const uids = Object.keys(wallets);
      if (uids.length === 0) { summary.push('no wallets yet'); return summary; }

      // Shared EVM sweep: one pass per chain covers every user.
      const evmAddressSet = new Set(uids.map((uid) => wallets[uid].evm).filter(Boolean).map((a) => a.toLowerCase()));
      const evmAddressToUid = new Map();
      for (const uid of uids) if (wallets[uid].evm) evmAddressToUid.set(wallets[uid].evm.toLowerCase(), uid);

      for (const [chainId, meta] of Object.entries(EVM_CHAINS)) {
        try {
          const result = await pollEvmChain(chainId, meta, evmAddressSet, evmAddressToUid);
          summary.push(`${meta.name} ${result.ok ? 'ok' : 'ERROR'} (${result.newDeposits} new)`);
        } catch (error) {
          warn('POLL', `${meta.name} cycle failed: ${error.message}`);
          summary.push(`${meta.name} ERROR: ${error.message}`);
        }
        await sleep(200);
      }

      // TRON / BTC / SOL: still per-user (free APIs are already address-scoped).
      let tronErr = 0, btcErr = 0, solErr = 0, tronN = 0, btcN = 0, solN = 0;
      for (const uid of uids) {
        const wallet = wallets[uid];
        try { if (wallet.tron) { tronN++; await pollTron(uid, wallet.tron); } } catch (error) { tronErr++; warn('POLL', `TRON ${uid}: ${error.message}`); }
        try { if (wallet.btc) { btcN++; await pollBtc(uid, wallet.btc); } } catch (error) { btcErr++; warn('POLL', `BTC ${uid}: ${error.message}`); }
        try { if (wallet.sol) { solN++; await pollSol(uid, wallet.sol); } } catch (error) { solErr++; warn('POLL', `SOL ${uid}: ${error.message}`); }
      }
      summary.push(`TRON checked ${tronN} (${tronErr} errors)`);
      summary.push(`BTC checked ${btcN} (${btcErr} errors)`);
      summary.push(`SOL checked ${solN} (${solErr} errors)`);
    } catch (error) {
      warn('POLL', `cycle-level failure: ${error.message}`);
      summary.push(`FATAL: ${error.message}`);
    } finally {
      busy = false;
      lastPoll.at = Date.now();
      lastPoll.summary = summary;
      log('POLL', `cycle done — ${summary.join(' | ')}`);
    }
    return summary;
  }

  // ── debug endpoints (protected by DEBUG_ADMIN_KEY) ──
  function checkDebugAuth(req, res) {
    if (!DEBUG_KEY) { res.status(503).json({ error: 'debug_disabled', hint: 'set DEBUG_ADMIN_KEY env var on Render to enable' }); return false; }
    const key = req.query.key || req.headers['x-debug-key'];
    if (key !== DEBUG_KEY) { res.status(401).json({ error: 'unauthorized' }); return false; }
    return true;
  }

  app.get('/api/debug/status', (req, res) => {
    if (!checkDebugAuth(req, res)) return;
    res.json({
      walletsEnabled: WALLETS_ENABLED,
      chainsConfigured: Object.fromEntries(Object.entries(EVM_CHAINS).map(([id, m]) => [id, { name: m.name, rpc: m.rpc, providerReady: !!providers[id] }])),
      tronKeySet: !!TRONGRID_KEY,
      pollIntervalMs: POLL_MS,
      minConfirmations: MIN_CONF,
      lastPollAt: lastPoll.at ? new Date(lastPoll.at).toISOString() : null,
      lastPollSummary: lastPoll.summary,
    });
  });

  app.post('/api/debug/poll-now', async (req, res) => {
    if (!checkDebugAuth(req, res)) return;
    const summary = await runOnce();
    res.json({ ranAt: new Date().toISOString(), summary });
  });

  const timer = setInterval(() => runOnce().catch((error) => warn('POLL', `unhandled: ${error.message}`)), POLL_MS);
  timer.unref?.();
  setTimeout(() => runOnce().catch((error) => warn('POLL', `unhandled: ${error.message}`)), 10_000).unref?.();
  log('WALLET', `deposit engine on (RPC-only, no Etherscan dependency); polling every ${POLL_MS / 1000}s`);
  return { ensureWallet, runOnce };
}
