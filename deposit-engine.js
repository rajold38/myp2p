import fetch from 'node-fetch';
import { deriveAddresses, WALLETS_ENABLED } from './wallet-gen.js';

const ETHERSCAN_KEY = process.env.ETHERSCAN_API_KEY || '';
const TRONGRID_KEY = process.env.TRONGRID_API_KEY || '';
const SOLANA_RPC = process.env.SOLANA_RPC || 'https://api.mainnet-beta.solana.com';
const POLL_MS = Math.max(30_000, Number(process.env.DEPOSIT_POLL_MS || 60_000));
const MIN_CONF = Math.max(1, Number(process.env.DEPOSIT_MIN_CONFIRMATIONS || 3));
const TRON_USDT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';

const EVM_CHAINS = {
  1: { name: 'ETH', coin: 'ETH' },
  56: { name: 'BSC', coin: 'BNB' },
  137: { name: 'POLYGON', coin: 'POL' },
  43114: { name: 'AVALANCHE', coin: 'AVAX' },
};

const TOKENS = [
  { chainId: 1, coin: 'USDT', decimals: 6, contract: '0xdac17f958d2ee523a2206206994597c13d831ec7' },
  { chainId: 1, coin: 'USDC', decimals: 6, contract: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' },
  { chainId: 1, coin: 'LINK', decimals: 18, contract: '0x514910771af9ca656af840dff83e8264ecf986ca' },
  { chainId: 1, coin: 'SHIB', decimals: 18, contract: '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce' },
  { chainId: 1, coin: 'UNI', decimals: 18, contract: '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984' },
  { chainId: 56, coin: 'USDT', decimals: 18, contract: '0x55d398326f99059ff775485246999027b3197955' },
  { chainId: 56, coin: 'USDC', decimals: 18, contract: '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d' },
  { chainId: 56, coin: 'DOGE', decimals: 8, contract: '0xba2ae424d960c26247dd6c32edc70b295c744c43' },
  { chainId: 56, coin: 'XRP', decimals: 18, contract: '0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe' },
  { chainId: 56, coin: 'ADA', decimals: 18, contract: '0x3ee2200efb3400fabb9aacf31297cbdd1d435d47' },
  { chainId: 56, coin: 'LTC', decimals: 18, contract: '0x4338665cbb7b2485a8855a139b75d5e34ab0db94' },
  { chainId: 56, coin: 'TON', decimals: 9, contract: '0x76a797a59ba2c17726896976b7b3747bfd1d220f' },
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const safeKey = (value) => String(value).replace(/[.#$/[\]]/g, '_');
const r8 = (value) => Number((Number(value) || 0).toFixed(8));

export function mountDepositEngine(app, ctx) {
  const { db, admin, tgFetch, TG_CHAT } = ctx;
  const log = ctx.log || ((tag, message) => console.log(`[${tag}] ${message}`));

  if (!WALLETS_ENABLED || !db || !admin) {
    log('WALLET', 'deposit engine off: MASTER_MNEMONIC or backend configuration missing');
    app.get('/api/wallet/:uid', (_req, res) => res.status(503).json({ error: 'wallets_disabled' }));
    return { ensureWallet: async () => null };
  }

  const inflight = new Map();

  async function allocateIndex() {
    const result = await db.ref('meta/wallet_next_index').transaction((current) =>
      Number.isSafeInteger(current) && current >= 0 ? current + 1 : 1,
    );
    if (!result.committed) throw new Error('wallet index allocation failed');
    return Number(result.snapshot.val()) - 1;
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

  function publicAddresses(wallet) {
    return { evm: wallet.evm, tron: wallet.tron, btc: wallet.btc, sol: wallet.sol };
  }

  // A wallet is generated automatically when a user record is first created.
  db.ref('users').on('child_added', (snapshot) => {
    ensureWallet(snapshot.key).catch((error) => log('WALLET', `${snapshot.key}: ${error.message}`));
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
      log('WALLET', error.message);
      return res.status(authError ? 401 : 500).json({ error: authError ? 'invalid_token' : 'wallet_unavailable' });
    }
  });

  // Balance, history and duplicate marker commit in one database transaction.
  async function credit(uid, coin, chain, amount, txid, address, eventId = '0') {
    const amt = r8(amount);
    if (!(amt > 0) || !txid) return;
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
        hid: historyKey,
        ts: now,
        isoDate: new Date(now).toISOString(),
        type: 'DEPOSIT',
        coin,
        amt,
        status: 'COMPLETED',
        chain,
        txid,
        address,
        method: 'ONCHAIN_AUTO',
      };
      root.deposits_seen[depositKey] = { uid, coin, amt, chain, txid, eventId, ts: now };
      credited = true;
      return root;
    });
    if (!result.committed || !credited) return;
    log('DEPOSIT', `${uid} +${amt} ${coin} (${chain}) ${txid}`);
    if (tgFetch && TG_CHAT) {
      tgFetch('sendMessage', {
        chat_id: TG_CHAT,
        text: `💰 <b>Auto Deposit</b>\nUser: <code>${uid}</code>\n${amt} ${coin} on ${chain}\nTx: <code>${txid}</code>`,
        parse_mode: 'HTML',
      }).catch(() => {});
    }
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) throw new Error(`HTTP ${response.status} from ${new URL(url).hostname}`);
    return response.json();
  }

  async function etherscan(chainId, params) {
    if (!ETHERSCAN_KEY) return [];
    const query = new URLSearchParams({ chainid: String(chainId), apikey: ETHERSCAN_KEY, ...params });
    const json = await fetchJson(`https://api.etherscan.io/v2/api?${query}`);
    return Array.isArray(json.result) ? json.result : [];
  }

  async function pollEvm(uid, rawAddress) {
    const address = rawAddress.toLowerCase();
    for (const [chainId, meta] of Object.entries(EVM_CHAINS)) {
      const nativeTxs = await etherscan(chainId, { module: 'account', action: 'txlist', address, startblock: '0', endblock: '99999999', sort: 'desc', page: '1', offset: '20' });
      for (const tx of nativeTxs) {
        if ((tx.to || '').toLowerCase() !== address || tx.isError === '1' || Number(tx.confirmations || 0) < MIN_CONF) continue;
        await credit(uid, meta.coin, meta.name, Number(tx.value) / 1e18, tx.hash, address, `native_${tx.transactionIndex || 0}`);
      }
      await sleep(250);
      const tokenTxs = await etherscan(chainId, { module: 'account', action: 'tokentx', address, startblock: '0', endblock: '99999999', sort: 'desc', page: '1', offset: '25' });
      for (const tx of tokenTxs) {
        if ((tx.to || '').toLowerCase() !== address || Number(tx.confirmations || 0) < MIN_CONF) continue;
        const token = TOKENS.find((item) => item.chainId === Number(chainId) && item.contract === (tx.contractAddress || '').toLowerCase());
        if (!token) continue;
        await credit(uid, token.coin, meta.name, Number(tx.value) / 10 ** token.decimals, tx.hash, address, `token_${tx.logIndex || tx.transactionIndex || 0}`);
      }
      await sleep(250);
    }
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

  let busy = false;
  async function runOnce() {
    if (busy) return;
    busy = true;
    try {
      const wallets = (await db.ref('user_wallets').once('value')).val() || {};
      for (const [uid, wallet] of Object.entries(wallets)) {
        try { if (wallet.evm && ETHERSCAN_KEY) await pollEvm(uid, wallet.evm); } catch (error) { log('POLL', `EVM ${uid}: ${error.message}`); }
        try { if (wallet.tron) await pollTron(uid, wallet.tron); } catch (error) { log('POLL', `TRON ${uid}: ${error.message}`); }
        try { if (wallet.btc) await pollBtc(uid, wallet.btc); } catch (error) { log('POLL', `BTC ${uid}: ${error.message}`); }
        try { if (wallet.sol) await pollSol(uid, wallet.sol); } catch (error) { log('POLL', `SOL ${uid}: ${error.message}`); }
      }
    } finally {
      busy = false;
    }
  }

  const timer = setInterval(() => runOnce().catch((error) => log('POLL', error.message)), POLL_MS);
  timer.unref?.();
  setTimeout(() => runOnce().catch((error) => log('POLL', error.message)), 10_000).unref?.();
  log('WALLET', `deposit engine on; polling every ${POLL_MS / 1000}s`);
  return { ensureWallet, runOnce };
}