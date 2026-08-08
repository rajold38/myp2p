import fetch from 'node-fetch';
import { deriveAddresses, WALLETS_ENABLED } from './wallet-gen.js';

const ETHERSCAN_KEY = process.env.ETHERSCAN_API_KEY || '';
const TRONGRID_KEY = process.env.TRONGRID_API_KEY || '';
const POLL_MS = Math.max(10_000, Number(process.env.DEPOSIT_POLL_MS || 30_000));
const MIN_CONF = Math.max(1, Number(process.env.DEPOSIT_MIN_CONFIRMATIONS || 1));
const TRON_USDT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';

const EVM_CHAINS = {
  1: { name: 'ETH', coin: 'ETH' },
  56: { name: 'BSC', coin: 'BNB' },
  137: { name: 'POLYGON', coin: 'POL' },
  43114: { name: 'AVALANCHE', coin: 'AVAX' },
};

const TOKENS = [
  { chainId: 1, coin: 'USDT', decimals: 6, contract: '0xdac17f958d2ee523a2206206994597c13d831ec7' },
  { chainId: 56, coin: 'USDT', decimals: 18, contract: '0x55d398326f99059ff775485246999027b3197955' },
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const safeKey = (value) => String(value).replace(/[.#$/[\]]/g, '_');
const r8 = (value) => Number((Number(value) || 0).toFixed(8));

export function mountDepositEngine(app, ctx) {
  const { db, admin, tgFetch, TG_CHAT } = ctx;
  const log = ctx.log || ((tag, message) => console.log(`[${tag}] ${message}`));

  if (!WALLETS_ENABLED || !db || !admin) {
    log('WALLET', 'deposit engine off: MASTER_MNEMONIC missing');
    return { ensureWallet: async () => null };
  }

  const inflight = new Map();

  async function ensureWallet(uid) {
    if (!uid) throw new Error('uid required');
    const existing = await db.ref(`user_wallets/${uid}`).once('value');
    if (existing.exists()) return existing.val();
    if (inflight.has(uid)) return inflight.get(uid);

    const task = (async () => {
      const snap = await db.ref('meta/wallet_next_index').transaction(c => (c || 0) + 1);
      const index = snap.snapshot.val() - 1;
      const addresses = deriveAddresses(index);
      const wallet = { ...addresses, index, uid, createdAt: Date.now() };
      await db.ref(`user_wallets/${uid}`).set(wallet);
      await db.ref('wallet_index').update({
        [safeKey(wallet.evm.toLowerCase())]: uid,
        [safeKey(wallet.tron)]: uid,
        [safeKey(wallet.btc)]: uid,
      });
      return wallet;
    })().finally(() => inflight.delete(uid));

    inflight.set(uid, task);
    return task;
  }

  async function credit(uid, coin, chain, amount, txid, address, eventId = '0') {
    const amt = r8(amount);
    if (amt <= 0 || !txid) return;
    const depositKey = safeKey(`${chain}_${txid}_${coin}_${eventId}`);
    const historyKey = safeKey(`auto_${depositKey}`);
    const now = Date.now();
    
    let alreadySeen = false;
    await db.ref(`deposits_seen/${depositKey}`).transaction(cur => {
      if (cur) { alreadySeen = true; return; }
      return { uid, coin, amt, chain, txid, ts: now };
    });
    if (alreadySeen) return;

    await db.ref().transaction(root => {
      if (!root) return root;
      root.users = root.users || {};
      const user = root.users[uid] || {};
      user.balances = user.balances || {};
      user.balances[coin] = r8((Number(user.balances[coin]) || 0) + amt);
      if (coin === 'USDT') user.balance = user.balances[coin];
      user.history = user.history || {};
      user.history[historyKey] = {
        hid: historyKey, ts: now, isoDate: new Date(now).toISOString(),
        type: 'DEPOSIT', coin, amt, status: 'COMPLETED',
        chain, txid, address, method: 'ONCHAIN_AUTO'
      };
      root.users[uid] = user;
      return root;
    });

    log('DEPOSIT', `✅ ${uid} +${amt} ${coin} (${chain}) Tx: ${txid}`);
    if (tgFetch && TG_CHAT) {
      tgFetch('sendMessage', {
        chat_id: TG_CHAT, parse_mode: 'HTML',
        text: `💰 <b>Deposit Detected</b>\nUser: <code>${uid}</code>\nAmount: <b>${amt} ${coin}</b>\nChain: ${chain}\nTx: <code>${txid}</code>`
      }).catch(() => {});
    }
  }

  async function pollEvm(uid, addr) {
    for (const [chainId, meta] of Object.entries(EVM_CHAINS)) {
      try {
        const url = `https://api.etherscan.io/v2/api?chainid=${chainId}&module=account&action=tokentx&address=${addr}&sort=desc&apikey=${ETHERSCAN_KEY}`;
        const res = await fetch(url).then(r => r.json());
        for (const tx of (res.result || []).slice(0, 5)) {
          if (tx.to?.toLowerCase() === addr.toLowerCase() && Number(tx.confirmations) >= MIN_CONF) {
            const token = TOKENS.find(t => t.chainId == chainId && t.contract.toLowerCase() === tx.contractAddress.toLowerCase());
            if (token) await credit(uid, token.coin, meta.name, Number(tx.value)/Math.pow(10, token.decimals), tx.hash, addr, tx.logIndex);
          }
        }
      } catch (e) {}
      await sleep(200);
    }
  }

  async function runOnce() {
    const wallets = (await db.ref('user_wallets').once('value')).val() || {};
    for (const [uid, w] of Object.entries(wallets)) {
      if (w.evm) await pollEvm(uid, w.evm);
    }
  }

  app.get('/api/wallet/:uid', async (req, res) => {
    const decoded = await admin.auth().verifyIdToken(req.headers.authorization?.split(' ')[1] || '');
    if (decoded.uid !== req.params.uid) return res.status(403).send('forbidden');
    res.json(await ensureWallet(decoded.uid));
  });

  setInterval(() => runOnce().catch(() => {}), POLL_MS);
}
