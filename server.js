// ════════════════════════════════════════════════════════════════════
// PRO P2P Terminal — Telegram Backup Bot (24/7) — v3.0
// Full admin control: users, trades, P2P, balances, broadcasts, stats
// ════════════════════════════════════════════════════════════════════

import express from 'express';
import admin from 'firebase-admin';
import fetch from 'node-fetch';

// ─── ENV VARS ───────────────────────────────────────────────────────
const PORT             = process.env.PORT || 10000;
const TG_TOKEN         = process.env.TG_TOKEN;
const TG_CHAT          = process.env.TG_CHAT;
const FIREBASE_DB_URL  = process.env.FIREBASE_DB_URL;
const FIREBASE_SA_JSON = process.env.FIREBASE_SERVICE_ACCOUNT;
const RENDER_URL       = process.env.RENDER_EXTERNAL_URL || '';

if (!TG_TOKEN || !TG_CHAT || !FIREBASE_DB_URL || !FIREBASE_SA_JSON) {
  console.error('❌ Missing env vars. Need: TG_TOKEN, TG_CHAT, FIREBASE_DB_URL, FIREBASE_SERVICE_ACCOUNT');
  process.exit(1);
}

// ─── LOGGER ─────────────────────────────────────────────────────────
function nowIST() {
  return new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
}
function logIST(tag, msg) {
  console.log(`[${nowIST()} IST] [${tag}] ${msg}`);
}

// ─── FIREBASE ADMIN INIT ────────────────────────────────────────────
let serviceAccount;
try { serviceAccount = JSON.parse(FIREBASE_SA_JSON); }
catch (e) { console.error('❌ FIREBASE_SERVICE_ACCOUNT invalid JSON:', e.message); process.exit(1); }

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: FIREBASE_DB_URL
});

const db = admin.database();
const TG_API = `https://api.telegram.org/bot${TG_TOKEN}`;
const BOT_START_TIME = Date.now();

logIST('INIT', '✅ Firebase Admin initialized');
logIST('INIT', '✅ Bot ready, watching Firebase for pending requests…');

// ─── TELEGRAM HELPERS ───────────────────────────────────────────────
async function tgFetch(endpoint, body) {
  try {
    const r = await fetch(`${TG_API}/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    return await r.json();
  } catch (e) { logIST('TG', `fetch error: ${e.message}`); return { ok: false }; }
}

async function tgSend(text, extra = {}) {
  return tgFetch('sendMessage', { chat_id: TG_CHAT, text, parse_mode: 'Markdown', ...extra });
}
async function tgEdit(msgId, text, extra = {}) {
  return tgFetch('editMessageText', { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: 'Markdown', ...extra });
}
async function tgAnswer(cbQueryId, text) {
  return tgFetch('answerCallbackQuery', { callback_query_id: cbQueryId, text });
}
async function tgSendButtons(text, cbId) {
  const r = await tgFetch('sendMessage', {
    chat_id: TG_CHAT, text, parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [[
      { text: '✅ APPROVE', callback_data: `approve_${cbId}` },
      { text: '❌ REJECT',  callback_data: `reject_${cbId}` }
    ]] }
  });
  return r.ok ? r.result.message_id : null;
}

// ─── HELPERS ────────────────────────────────────────────────────────
const sentByCbId = new Map();

async function findUserByUID(uid) {
  try {
    const snap = await db.ref('users').orderByChild('uid').equalTo(uid).once('value');
    if (snap.exists()) {
      const val = snap.val();
      const firebaseUid = Object.keys(val)[0];
      return { firebaseUid, user: val[firebaseUid] };
    }
  } catch (e) {
    logIST('WARN', `orderByChild failed, scanning: ${e.message}`);
  }
  const allSnap = await db.ref('users').once('value');
  let result = null;
  allSnap.forEach(child => {
    if (child.val()?.uid?.toUpperCase() === uid.toUpperCase()) {
      result = { firebaseUid: child.key, user: child.val() };
    }
  });
  return result;
}

function fmtMsg(type, user, req) {
  const head = type === 'dep' ? '📥 *NEW DEPOSIT REQUEST*' : '💸 *NEW WITHDRAWAL REQUEST*';
  const coin = req.coin || 'USDT';
  const lines = [
    head, '',
    `👤 UID: \`${user.uid || '—'}\``,
    `📛 Name: *${user.name || '—'}*`,
    `📧 Email: ${user.email || '—'}`,
    `💎 Coin: *${coin}*`,
    `💵 Amount: *${req.amt} ${coin}*`,
  ];
  if (req.network || req.chain) lines.push(`🌐 Network: ${req.network || req.chain}`);
  if (req.txid)    lines.push(`🔗 TxID: \`${req.txid}\``);
  if (req.address || req.addr) lines.push(`📬 Address: \`${req.address || req.addr}\``);
  if (req.utr)     lines.push(`🧾 UTR: \`${req.utr}\``);
  const oldBal = parseFloat(user.balance || 0);
  lines.push(`💰 Current Balance: *${oldBal.toFixed(2)} USDT*`);
  lines.push('', `⏰ ${nowIST()} IST`);
  return lines.join('\n');
}

// ─── PENDING REQUEST WATCHER ────────────────────────────────────────
async function maybeSendButtons(firebaseUid, type, req, cbId) {
  if (!req || !cbId) return;
  if (req.botMsgId) {
    sentByCbId.set(cbId, { firebaseUid, type, msgId: req.botMsgId, hid: req.hid, amt: parseFloat(req.amt), coin: req.coin });
    return;
  }
  const ageMs = Date.now() - (req.ts || 0);
  if (req.msgId && ageMs < 90_000) {
    sentByCbId.set(cbId, { firebaseUid, type, msgId: req.msgId, hid: req.hid, amt: parseFloat(req.amt), coin: req.coin });
    return;
  }
  const lockRef = db.ref(`users/${firebaseUid}/pendingReqs/${type}/${cbId}/botLock`);
  const tx = await lockRef.transaction(cur => (cur ? undefined : Date.now()));
  if (!tx.committed) return;

  const userSnap = await db.ref(`users/${firebaseUid}`).once('value');
  const user = userSnap.val() || {};
  const text = fmtMsg(type, user, req);
  const msgId = await tgSendButtons(text, cbId);
  if (msgId) {
    await db.ref(`users/${firebaseUid}/pendingReqs/${type}/${cbId}`).update({ botMsgId: msgId, cbId });
    sentByCbId.set(cbId, { firebaseUid, type, msgId, hid: req.hid, amt: parseFloat(req.amt), coin: req.coin });
    logIST(type.toUpperCase(), `UID=${user.uid} Name=${user.name||'—'} Amt=${req.amt} ${req.coin||'USDT'} Net=${req.network||req.chain||'—'} TXID=${req.txid||'—'} cbId=${cbId} → TG msg sent (msgId: ${msgId})`);
  }
}

async function processPendingMap(firebaseUid, data, cutoff = 0) {
  const reqs = data?.pendingReqs || {};
  for (const type of ['dep', 'wit']) {
    const map = reqs[type] || {};
    for (const [cbId, req] of Object.entries(map)) {
      if (!req || typeof req !== 'object') continue;
      if (cbId === 'botLock') continue;
      if (cutoff && (req.ts || 0) < cutoff) continue;
      await maybeSendButtons(firebaseUid, type, req, cbId).catch(e => logIST('ERR', `${type} ${e.message}`));
    }
  }
}

db.ref('users').on('child_changed', async (snap) => {
  await processPendingMap(snap.key, snap.val() || {});
});
db.ref('users').on('child_added', async (snap) => {
  await processPendingMap(snap.key, snap.val() || {}, BOT_START_TIME - 10_000);
});

// ─── HISTORY UPDATE ─────────────────────────────────────────────────
async function updateHistoryStatus(firebaseUid, hid, status) {
  if (!hid) return;
  const histSnap = await db.ref(`users/${firebaseUid}/history`).once('value');
  if (!histSnap.exists()) return;
  const updates = {};
  histSnap.forEach(child => {
    if (child.val()?.hid === hid) updates[child.key + '/status'] = status;
  });
  if (Object.keys(updates).length) {
    await db.ref(`users/${firebaseUid}/history`).update(updates);
  }
}

// ─── APPROVE / REJECT ───────────────────────────────────────────────
async function handleApprove(firebaseUid, type, ctx) {
  const userRef = db.ref(`users/${firebaseUid}`);
  const userSnap = await userRef.once('value');
  const user = userSnap.val();
  if (!user) return null;
  const oldBal = parseFloat(user.balance || 0);
  let newBal = oldBal;
  if (type === 'dep') {
    newBal = parseFloat((oldBal + ctx.amt).toFixed(8));
    await userRef.update({ balance: newBal, 'balances/USDT': newBal });
  }
  await updateHistoryStatus(firebaseUid, ctx.hid, 'COMPLETED');
  await db.ref(`users/${firebaseUid}/pendingReqs/${type}/${ctx.cbId}`).remove();
  logIST('APPROVE', `UID=${user.uid} Name=${user.name||'—'} ${type.toUpperCase()} ${ctx.amt} USDT | OldBal=${oldBal.toFixed(2)} → NewBal=${newBal.toFixed(2)}`);
  return { oldBal, newBal, user };
}

async function handleReject(firebaseUid, type, ctx) {
  const userRef = db.ref(`users/${firebaseUid}`);
  const userSnap = await userRef.once('value');
  const user = userSnap.val();
  if (!user) return null;
  const oldBal = parseFloat(user.balance || 0);
  let newBal = oldBal;
  if (type === 'wit') {
    newBal = parseFloat((oldBal + ctx.amt).toFixed(8));
    await userRef.update({ balance: newBal, 'balances/USDT': newBal });
  }
  await updateHistoryStatus(firebaseUid, ctx.hid, 'REJECTED');
  await db.ref(`users/${firebaseUid}/pendingReqs/${type}/${ctx.cbId}`).remove();
  logIST('REJECT', `UID=${user.uid} Name=${user.name||'—'} ${type.toUpperCase()} ${ctx.amt} USDT ${type==='wit'?'refunded':''} | OldBal=${oldBal.toFixed(2)} → NewBal=${newBal.toFixed(2)}`);
  return { oldBal, newBal, user };
}

function approveTxt(label, ctx, r, req) {
  const coin = req?.coin || ctx.coin || 'USDT';
  const lines = [`✅ *${label} APPROVED*`, ''];
  if (r?.user) {
    lines.push(`👤 UID: \`${r.user.uid || '—'}\``);
    lines.push(`📛 Name: *${r.user.name || '—'}*`);
  }
  lines.push(`💵 Amount: *${ctx.type==='dep'?'+':'-'}${ctx.amt.toFixed(2)} ${coin}*`);
  if (req?.network || req?.chain) lines.push(`🌐 Network: ${req.network||req.chain}`);
  if (req?.txid) lines.push(`🔗 TxID: \`${req.txid}\``);
  if (req?.address || req?.addr) lines.push(`📬 Address: \`${req.address||req.addr}\``);
  if (r) lines.push(`💰 Balance: ${r.oldBal.toFixed(2)} → *${r.newBal.toFixed(2)} USDT*`);
  lines.push(`⏰ ${nowIST()} IST`);
  lines.push('✅ Approved by Admin');
  return lines.join('\n');
}

function rejectTxt(label, ctx, r, req) {
  const coin = req?.coin || ctx.coin || 'USDT';
  const lines = [`❌ *${label} REJECTED*`, ''];
  if (r?.user) {
    lines.push(`👤 UID: \`${r.user.uid || '—'}\``);
    lines.push(`📛 Name: *${r.user.name || '—'}*`);
  }
  lines.push(`💸 Amount: *${ctx.type==='dep'?'+':'-'}${ctx.amt.toFixed(2)} ${coin}*`);
  if (req?.network || req?.chain) lines.push(`🌐 Network: ${req.network||req.chain}`);
  if (req?.address || req?.addr) lines.push(`📬 Address: \`${req.address||req.addr}\``);
  if (r && ctx.type === 'wit') lines.push(`💰 Refunded: ${r.oldBal.toFixed(2)} → *${r.newBal.toFixed(2)} USDT*`);
  lines.push(`⏰ ${nowIST()} IST`);
  lines.push('❌ Rejected by Admin');
  return lines.join('\n');
}

// ─── POLLING ────────────────────────────────────────────────────────
let lastUpdateId = 0;
async function pollUpdates() {
  const res = await fetch(`${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=25&allowed_updates=${encodeURIComponent('["callback_query","message"]')}`);
  const data = await res.json();
  if (!data.ok) return;
  const updates = data.result || [];
  if (updates.length) logIST('POLL', `${updates.length} update(s) | offset=${lastUpdateId}`);
  for (const upd of updates) {
    lastUpdateId = upd.update_id;
    try { await handleUpdate(upd); } catch (e) { logIST('ERR', `handleUpdate: ${e.message}`); }
  }
}

// ─── CALLBACK ROUTER ────────────────────────────────────────────────
async function handleCallback(cb) {
  const chatId = String(cb.message?.chat?.id || '');
  if (chatId !== String(TG_CHAT)) { await tgAnswer(cb.id, 'Unauthorized'); return; }
  const data = cb.data || '';

  // approve/reject
  let m = data.match(/^(approve|reject)_(.+)$/);
  if (m) return handleApproveRejectCb(cb, m[1], m[2]);

  // userdetail_UID
  m = data.match(/^userdetail_(.+)$/);
  if (m) return sendUserDetailCard(cb, m[1]);

  // ban_/unban_
  m = data.match(/^(ban|unban)_(.+)$/);
  if (m) return handleBanCb(cb, m[1], m[2]);

  // creditprompt_/debitprompt_
  m = data.match(/^(creditprompt|debitprompt)_(.+)$/);
  if (m) {
    await tgAnswer(cb.id, 'Send the command');
    const kind = m[1] === 'creditprompt' ? 'credit' : 'debit';
    await tgSend(`✏️ Reply with:\n\`/${kind} ${m[2]} <amount>\``);
    return;
  }

  // history_UID
  m = data.match(/^history_(.+)$/);
  if (m) { await tgAnswer(cb.id, 'Loading…'); return sendUserHistory(m[1], 10); }

  // closep2p_UID
  m = data.match(/^closep2p_(.+)$/);
  if (m) { await tgAnswer(cb.id, 'Closing…'); return closeAllP2PForUser(m[1]); }

  await tgAnswer(cb.id, '?');
}

async function handleApproveRejectCb(cb, action, cbId) {
  let ctx = sentByCbId.get(cbId);
  let req = null;
  if (!ctx) {
    const snap = await db.ref('users').once('value');
    snap.forEach(child => {
      const reqs = child.val()?.pendingReqs || {};
      for (const type of ['dep', 'wit']) {
        const map = reqs[type] || {};
        for (const [storedCbId, r] of Object.entries(map)) {
          if (storedCbId === cbId || r?.cbId === cbId) {
            ctx = { firebaseUid: child.key, type, hid: r.hid, amt: parseFloat(r.amt), msgId: r.botMsgId, cbId, coin: r.coin };
            req = r;
          }
        }
      }
    });
  } else {
    // fetch req for display fields
    try {
      const rs = await db.ref(`users/${ctx.firebaseUid}/pendingReqs/${ctx.type}/${cbId}`).once('value');
      if (rs.exists()) req = rs.val();
    } catch (e) {}
  }
  if (!ctx) { await tgAnswer(cb.id, '⚠️ Request expired or already handled'); return; }
  ctx.cbId = cbId;

  if (action === 'approve') {
    await tgAnswer(cb.id, '✅ Approved!');
    const r = await handleApprove(ctx.firebaseUid, ctx.type, ctx);
    const label = ctx.type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL';
    if (ctx.msgId) await tgEdit(ctx.msgId, approveTxt(label, ctx, r, req));
  } else {
    await tgAnswer(cb.id, '❌ Rejected!');
    const r = await handleReject(ctx.firebaseUid, ctx.type, ctx);
    const label = ctx.type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL';
    if (ctx.msgId) await tgEdit(ctx.msgId, rejectTxt(label, ctx, r, req));
  }
  sentByCbId.delete(cbId);
}

async function handleBanCb(cb, action, uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgAnswer(cb.id, 'Not found'); return; }
  const banned = action === 'ban';
  await db.ref(`users/${found.firebaseUid}`).update({ banned });
  await tgAnswer(cb.id, banned ? '🚫 Banned' : '✅ Unbanned');
  logIST('ADMIN', `${banned?'BAN':'UNBAN'} UID=${uid} Name=${found.user.name||'—'}`);
  await tgSend(`${banned?'🚫 *USER BANNED*':'✅ *USER UNBANNED*'}\n\n👤 \`${uid}\`\n📛 ${found.user.name || 'Unknown'}`);
}

// ─── USER DETAIL CARD ───────────────────────────────────────────────
async function sendUserDetailCard(cb, uid) {
  await tgAnswer(cb.id, 'Loading…');
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const u = found.user;
  const histSnap = await db.ref(`users/${found.firebaseUid}/history`).once('value');
  const histArr = [];
  histSnap.forEach(c => histArr.push(c.val()));
  histArr.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const last3 = histArr.slice(0, 3).map(h => {
    const sign = (h.type==='WITHDRAW'||h.type==='WITHDRAWAL'||h.type==='ADMIN_DEBIT')?'-':'+';
    const ico = h.status==='COMPLETED'?'✅':h.status==='REJECTED'?'❌':h.status==='CANCELLED'?'🚫':'⏳';
    return `  ${ico} ${h.type} ${sign}${parseFloat(h.amt||0).toFixed(2)} ${h.coin||'USDT'}`;
  }).join('\n') || '  (no transactions)';

  const text = [
    `👤 *USER DETAILS — ${u.uid}*`, '',
    `📛 Name: *${u.name || '—'}*`,
    `📧 Email: ${u.email || '—'}`,
    `📱 Phone: ${u.phone || '—'}`,
    `💰 Balance: *${parseFloat(u.balance || 0).toFixed(2)} USDT*`,
    `🔑 Status: ${u.banned ? '🚫 BANNED' : '✅ Active'}`,
    `📅 Joined: ${u.createdAt ? new Date(u.createdAt).toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata' }) : '—'}`,
    '',
    `📊 *Last 3 Transactions:*`,
    last3
  ].join('\n');

  const buttons = [
    [
      { text: '💰 Credit', callback_data: `creditprompt_${uid}` },
      { text: '➖ Debit',  callback_data: `debitprompt_${uid}` }
    ],
    [
      u.banned
        ? { text: '✅ Unban', callback_data: `unban_${uid}` }
        : { text: '🚫 Ban',   callback_data: `ban_${uid}` },
      { text: '📋 History', callback_data: `history_${uid}` }
    ],
    [
      { text: '🔄 Close P2P', callback_data: `closep2p_${uid}` }
    ]
  ];
  await tgSend(text, { reply_markup: { inline_keyboard: buttons } });
}

async function sendUserHistory(uid, limit = 15) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const snap = await db.ref(`users/${found.firebaseUid}/history`).once('value');
  if (!snap.exists()) { await tgSend(`📭 No history for \`${uid}\``); return; }
  const arr = [];
  snap.forEach(c => arr.push(c.val()));
  arr.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const items = arr.slice(0, limit).map(h => {
    const sign = (h.type==='WITHDRAW'||h.type==='WITHDRAWAL'||h.type==='ADMIN_DEBIT')?'-':'+';
    const ico = h.status==='COMPLETED'?'✅':h.status==='REJECTED'?'❌':h.status==='CANCELLED'?'🚫':'⏳';
    const d = h.date || (h.ts? new Date(h.ts).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}):'');
    return `${ico} ${h.type} ${sign}${parseFloat(h.amt||0).toFixed(2)} ${h.coin||'USDT'} ${h.network?'· '+h.network:''} — ${d}`;
  });
  await tgSend(`📋 *HISTORY — ${uid}* (last ${items.length})\n\n${items.join('\n')}`);
}

async function closeAllP2PForUser(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const orders = found.user.orders || {};
  let closed = 0, refunded = 0;
  for (const [oid, ord] of Object.entries(orders)) {
    if (!ord || !ord.status) continue;
    const st = String(ord.status).toLowerCase();
    if (['completed','cancelled','canceled'].includes(st)) continue;
    if (ord.mode === 'BUY' && ord.usdt) {
      const bal = parseFloat(found.user.balance || 0) + parseFloat(ord.usdt);
      await db.ref(`users/${found.firebaseUid}`).update({ balance: bal, 'balances/USDT': bal });
      refunded += parseFloat(ord.usdt);
      found.user.balance = bal;
    }
    await db.ref(`users/${found.firebaseUid}/orders/${oid}`).update({ status: 'cancelled' });
    closed++;
  }
  logIST('ADMIN', `closeP2P UID=${uid} closed=${closed} refunded=${refunded}`);
  await tgSend(`✅ *P2P CLOSED*\n\n👤 \`${uid}\`\nOrders cancelled: *${closed}*\nRefunded: *${refunded.toFixed(2)} USDT*`);
}

// ─── COMMAND HANDLERS ───────────────────────────────────────────────
const HELP_TEXT = [
  '🤖 *PRO P2P ADMIN BOT*', '',
  '━━━━━━━━━━━━━━━━━━━',
  '👥 *USER MANAGEMENT*',
  '━━━━━━━━━━━━━━━━━━━',
  '`/users` — List all users (inline buttons)',
  '`/user UID` — User details card',
  '`/history UID` — Last 15 transactions',
  '`/credit UID AMT` — Add USDT',
  '`/debit UID AMT` — Deduct USDT',
  '`/balance UID AMT` — Set USDT balance',
  '`/setbalance UID COIN AMT` — Set per-coin balance',
  '`/ban UID` — Ban user',
  '`/unban UID` — Unban user',
  '',
  '━━━━━━━━━━━━━━━━━━━',
  '🔄 *TRADES & ORDERS*',
  '━━━━━━━━━━━━━━━━━━━',
  '`/trades` — All pending deposits/withdrawals/P2P',
  '`/cancel TRADEID` — Cancel deposit/withdrawal',
  '`/closeorder ORDERID` — Force-close a P2P order',
  '`/closep2p UID` — Close all active P2P for user',
  '',
  '━━━━━━━━━━━━━━━━━━━',
  '📊 *PLATFORM*',
  '━━━━━━━━━━━━━━━━━━━',
  '`/stats` — Platform stats',
  '`/broadcast MESSAGE` — Send announcement to all',
  '`/ping` — Health check',
  '`/help` — This menu',
  '',
  '━━━━━━━━━━━━━━━━━━━',
  '⚡ *QUICK*',
  '━━━━━━━━━━━━━━━━━━━',
  '`#UID AMT` — Quick credit (e.g. #ABC123 50)',
].join('\n');

async function handleUsersList() {
  const snap = await db.ref('users').once('value');
  if (!snap.exists()) { await tgSend('No users found.'); return; }
  const users = [];
  snap.forEach(c => { const u = c.val(); if (u && u.uid) users.push(u); });
  if (!users.length) { await tgSend('No users found.'); return; }

  // Batch into chunks of 8 users per message with inline button rows
  await tgSend(`👥 *ALL USERS (${users.length} total)*`);
  const CHUNK = 8;
  for (let i = 0; i < users.length; i += CHUNK) {
    const slice = users.slice(i, i + CHUNK);
    const lines = slice.map(u => {
      const st = u.banned ? '🚫' : '✅';
      return `${st} \`${u.uid}\` | *${u.name || '—'}* | 💰 ${parseFloat(u.balance||0).toFixed(2)} USDT`;
    });
    const keyboard = slice.map(u => [{ text: `👁 ${u.uid} — ${u.name||'—'}`.slice(0,60), callback_data: `userdetail_${u.uid}` }]);
    await tgSend(lines.join('\n'), { reply_markup: { inline_keyboard: keyboard } });
  }
}

async function handleStats() {
  const snap = await db.ref('users').once('value');
  let total = 0, banned = 0, totalBal = 0, pendDep = 0, pendWit = 0, activeP2P = 0;
  snap.forEach(c => {
    const u = c.val() || {}; if (!u.uid) return;
    total++;
    if (u.banned) banned++;
    totalBal += parseFloat(u.balance || 0);
    const reqs = u.pendingReqs || {};
    pendDep += Object.keys(reqs.dep || {}).filter(k => k !== 'botLock').length;
    pendWit += Object.keys(reqs.wit || {}).filter(k => k !== 'botLock').length;
    const orders = u.orders || {};
    for (const o of Object.values(orders)) {
      if (o?.status && !['completed','cancelled','canceled'].includes(String(o.status).toLowerCase())) activeP2P++;
    }
  });
  const upMs = Date.now() - BOT_START_TIME;
  const upH = Math.floor(upMs / 3600000);
  const upM = Math.floor((upMs % 3600000) / 60000);
  await tgSend([
    '📊 *PLATFORM STATS*', '',
    `👥 Total Users: *${total}*`,
    `✅ Active: *${total - banned}* | 🚫 Banned: *${banned}*`,
    `💰 Total Balance: *${totalBal.toFixed(2)} USDT*`,
    `📥 Pending Deposits: *${pendDep}*`,
    `📤 Pending Withdrawals: *${pendWit}*`,
    `🔄 Active P2P Orders: *${activeP2P}*`,
    `⏰ Bot Uptime: *${upH}h ${upM}m*`,
    `⏰ ${nowIST()} IST`,
  ].join('\n'));
}

async function handleTrades() {
  const snap = await db.ref('users').once('value');
  const lines = ['📊 *ALL PENDING / ONGOING TRADES*\n'];
  let count = 0;
  snap.forEach(child => {
    const u = child.val(); if (!u) return;
    const reqs = u.pendingReqs || {};
    for (const type of ['dep', 'wit']) {
      const map = reqs[type] || {};
      for (const [cbId, r] of Object.entries(map)) {
        if (!r || typeof r !== 'object' || cbId === 'botLock') continue;
        const label = type === 'dep' ? '📥 DEP' : '📤 WIT';
        lines.push(`${label} | \`${u.uid || child.key}\` | *${r.amt} ${r.coin||'USDT'}* | cbId: \`${cbId}\``);
        count++;
      }
    }
    const orders = u.orders || {};
    for (const [oid, ord] of Object.entries(orders)) {
      if (ord?.status && !['completed','cancelled','canceled'].includes(String(ord.status).toLowerCase())) {
        lines.push(`🔄 P2P | \`${u.uid || child.key}\` | Order: \`${oid}\` | ${ord.status} | ${ord.usdt || '?'} USDT`);
        count++;
      }
    }
  });
  if (count === 0) lines.push('No pending trades. ✅');
  else lines.push(`\n📊 Total: ${count} pending items`);
  let chunk = '';
  for (const line of lines) {
    if ((chunk + line + '\n').length > 4000) { await tgSend(chunk); chunk = ''; }
    chunk += line + '\n';
  }
  if (chunk) await tgSend(chunk);
}

async function handleCancel(targetId) {
  let found = false;
  const snap = await db.ref('users').once('value');
  const all = snap.val() || {};
  for (const [fuid, u] of Object.entries(all)) {
    const reqs = u?.pendingReqs || {};
    for (const type of ['dep', 'wit']) {
      const map = reqs[type] || {};
      for (const [cbId, req] of Object.entries(map)) {
        if (!req || typeof req !== 'object' || cbId === 'botLock') continue;
        if (cbId === targetId || req.cbId === targetId || req.hid === targetId) {
          if (type === 'wit' && req.amt) {
            const oldBal = parseFloat(u.balance || 0);
            const newBal = parseFloat((oldBal + parseFloat(req.amt)).toFixed(8));
            await db.ref(`users/${fuid}`).update({ balance: newBal, 'balances/USDT': newBal });
          }
          await updateHistoryStatus(fuid, req.hid, 'CANCELLED');
          await db.ref(`users/${fuid}/pendingReqs/${type}/${cbId}`).remove();
          if (req.botMsgId) {
            await tgEdit(req.botMsgId, `🚫 *${type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL'} CANCELLED BY ADMIN*\n\nAmount: *${req.amt} ${req.coin||'USDT'}*${type === 'wit' ? '\n🔴 Funds refunded' : ''}`).catch(() => {});
          }
          sentByCbId.delete(cbId);
          await tgSend(`✅ *CANCELLED*\n\n👤 \`${u.uid || fuid}\`\n📛 ${u.name || 'Unknown'}\nType: ${type.toUpperCase()}\nAmt: *${req.amt} ${req.coin||'USDT'}*`);
          found = true; break;
        }
      }
      if (found) break;
    }
    if (found) break;
  }
  if (!found) await tgSend(`❌ Trade \`${targetId}\` not found or already resolved.`);
}

async function handleCloseOrder(orderId) {
  const snap = await db.ref('users').once('value');
  let done = false;
  snap.forEach(child => {
    if (done) return;
    const u = child.val() || {};
    const orders = u.orders || {};
    for (const [oid, ord] of Object.entries(orders)) {
      if (oid === orderId || String(ord?.id) === orderId) {
        const refund = ord?.mode === 'BUY' && ord?.usdt ? parseFloat(ord.usdt) : 0;
        (async () => {
          if (refund > 0) {
            const oldBal = parseFloat(u.balance || 0);
            const newBal = parseFloat((oldBal + refund).toFixed(8));
            await db.ref(`users/${child.key}`).update({ balance: newBal, 'balances/USDT': newBal });
          }
          await db.ref(`users/${child.key}/orders/${oid}`).update({ status: 'cancelled' });
          await tgSend(`✅ *ORDER CLOSED*\n\nOrder: \`${oid}\`\n👤 UID: \`${u.uid || child.key}\`\n${refund>0?`💰 Refunded: ${refund.toFixed(2)} USDT`:''}`);
          logIST('ADMIN', `closeOrder ${oid} UID=${u.uid} refund=${refund}`);
        })();
        done = true; break;
      }
    }
  });
  if (!done) await tgSend(`❌ Order \`${orderId}\` not found.`);
}

async function handleBroadcast(message) {
  if (!message) { await tgSend('Usage: `/broadcast <message>`'); return; }
  const snap = await db.ref('users').once('value');
  let count = 0;
  snap.forEach(c => { if (c.val()?.uid) count++; });
  const entry = { text: message, ts: Date.now(), from: 'admin', date: nowIST() };
  await db.ref('broadcast').push(entry);
  logIST('BROADCAST', `to ${count} users: "${message}"`);
  await tgSend(`✅ *BROADCAST SENT*\n\nReach: *${count} users*\n💬 "${message}"`);
}

async function handleSetBalancePerCoin(uid, coin, amt) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const updates = { [`balances/${coin}`]: amt };
  if (coin === 'USDT') updates.balance = amt;
  await db.ref(`users/${found.firebaseUid}`).update(updates);
  logIST('ADMIN', `setBalance UID=${uid} ${coin}=${amt}`);
  await tgSend(`✅ *${coin} BALANCE SET*\n\n👤 \`${uid}\`\n📛 ${found.user.name || 'Unknown'}\n💰 ${coin}: *${amt}*`);
}

// ─── UPDATE HANDLER ─────────────────────────────────────────────────
async function handleUpdate(upd) {
  if (upd.callback_query) return handleCallback(upd.callback_query);
  const msg = upd.message;
  if (!msg) return;
  const chatId = String(msg.chat.id);
  if (chatId !== String(TG_CHAT)) return;
  const text = (msg.text || '').trim();

  // Exact commands
  if (text === '/start' || text === '/help') return tgSend(HELP_TEXT);
  if (text === '/ping')   return tgSend(`🟢 Bot online — ${nowIST()} IST\nUptime: ${Math.floor((Date.now()-BOT_START_TIME)/60000)}m`);
  if (text === '/users')  return handleUsersList();
  if (text === '/trades') return handleTrades();
  if (text === '/stats')  return handleStats();

  // Pattern commands
  let m;
  if ((m = text.match(/^\/user\s+([A-Z0-9]{2,15})$/i))) {
    return sendUserDetailCard({ id: 'cmd', message: msg }, m[1].toUpperCase());
  }
  if ((m = text.match(/^\/history\s+([A-Z0-9]{2,15})$/i))) {
    return sendUserHistory(m[1].toUpperCase(), 15);
  }
  if ((m = text.match(/^\/credit\s+([A-Z0-9]{2,15})\s+([\d.]+)$/i))) {
    return adminCreditDebit(m[1].toUpperCase(), parseFloat(m[2]), '+', 'ADMIN_CREDIT');
  }
  if ((m = text.match(/^\/debit\s+([A-Z0-9]{2,15})\s+([\d.]+)$/i))) {
    return adminCreditDebit(m[1].toUpperCase(), parseFloat(m[2]), '-', 'ADMIN_DEBIT');
  }
  if ((m = text.match(/^\/balance\s+([A-Z0-9]{2,15})\s+([\d.]+)$/i))) {
    return adminSetExactBalance(m[1].toUpperCase(), parseFloat(m[2]));
  }
  if ((m = text.match(/^\/setbalance\s+([A-Z0-9]{2,15})\s+([A-Z]{2,8})\s+([\d.]+)$/i))) {
    return handleSetBalancePerCoin(m[1].toUpperCase(), m[2].toUpperCase(), parseFloat(m[3]));
  }
  if ((m = text.match(/^\/ban\s+([A-Z0-9]{2,15})$/i))) {
    return adminBan(m[1].toUpperCase(), true);
  }
  if ((m = text.match(/^\/unban\s+([A-Z0-9]{2,15})$/i))) {
    return adminBan(m[1].toUpperCase(), false);
  }
  if ((m = text.match(/^\/cancel\s+(\S+)$/i))) return handleCancel(m[1]);
  if ((m = text.match(/^\/closeorder\s+(\S+)$/i))) return handleCloseOrder(m[1]);
  if ((m = text.match(/^\/closep2p\s+([A-Z0-9]{2,15})$/i))) return closeAllP2PForUser(m[1].toUpperCase());
  if ((m = text.match(/^\/broadcast\s+([\s\S]+)$/i))) return handleBroadcast(m[1].trim());

  // #UID AMT quick credit
  if ((m = text.match(/^#([A-Z0-9]{2,15})\s+([\d.]+)$/i))) {
    return quickCredit(m[1].toUpperCase(), parseFloat(m[2]));
  }
}

async function adminCreditDebit(uid, amt, sign, type) {
  if (!amt || amt <= 0) { await tgSend('❌ Invalid amount.'); return; }
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const oldBal = parseFloat(found.user.balance || 0);
  const newBal = sign === '+'
    ? parseFloat((oldBal + amt).toFixed(8))
    : Math.max(0, parseFloat((oldBal - amt).toFixed(8)));
  await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal, 'balances/USDT': newBal });
  await db.ref(`users/${found.firebaseUid}/history`).push({
    hid: 'h_admin_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
    type, amt, status: 'COMPLETED', coin: 'USDT',
    uid: found.user.uid || uid,
    date: nowIST(), isoDate: new Date().toISOString(), ts: Date.now(),
    note: `Admin ${sign==='+'?'credited':'debited'} ${amt} USDT`, network: 'INTERNAL',
  });
  logIST('ADMIN', `/${type==='ADMIN_CREDIT'?'credit':'debit'} ${uid} ${amt} → OldBal=${oldBal.toFixed(2)} → NewBal=${newBal.toFixed(2)}`);
  await tgSend(`✅ *${sign==='+'?'CREDITED':'DEBITED'}*\n\n👤 \`${uid}\`\n📛 ${found.user.name || 'Unknown'}\n${sign==='+'?'➕ +':'➖ -'}${amt} USDT\n💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`);
}

async function quickCredit(uid, amt) {
  if (!amt || amt <= 0) { await tgSend('❌ Invalid. Usage: `#UID AMOUNT`'); return; }
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const oldBal = parseFloat(found.user.balance || 0);
  const newBal = parseFloat((oldBal + amt).toFixed(8));
  await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal, 'balances/USDT': newBal });
  await db.ref(`users/${found.firebaseUid}/history`).push({
    hid: 'h_admin_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
    type: 'P2PPRO_CREDIT', amt, status: 'COMPLETED', coin: 'USDT',
    uid: found.user.uid || uid,
    date: nowIST(), isoDate: new Date().toISOString(), ts: Date.now(),
    sender: 'P2PPRO', note: 'P2PPRO sended you ' + amt + ' USDT',
    network: 'INTERNAL', txid: 'p2ppro_' + Date.now()
  });
  logIST('ADMIN', `quickCredit #${uid} ${amt} → ${oldBal.toFixed(2)} → ${newBal.toFixed(2)}`);
  await tgSend(`✅ *CREDITED*\n\n👤 \`${uid}\`\n📛 ${found.user.name || 'Unknown'}\n➕ +${amt} USDT\n💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`);
}

async function adminSetExactBalance(uid, newBal) {
  if (isNaN(newBal) || newBal < 0) { await tgSend('❌ Invalid amount.'); return; }
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const oldBal = parseFloat(found.user.balance || 0);
  await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal, 'balances/USDT': newBal });
  logIST('ADMIN', `setBalance ${uid} → ${newBal}`);
  await tgSend(`✅ *BALANCE SET*\n\n👤 \`${uid}\`\n📛 ${found.user.name || 'Unknown'}\n💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`);
}

async function adminBan(uid, banned) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  await db.ref(`users/${found.firebaseUid}`).update({ banned });
  logIST('ADMIN', `${banned?'BAN':'UNBAN'} ${uid}`);
  await tgSend(`${banned?'🚫 *USER BANNED*':'✅ *USER UNBANNED*'}\n\n👤 \`${uid}\`\n📛 ${found.user.name || 'Unknown'}`);
}

// ─── POLL LOOP w/ EXPONENTIAL BACKOFF ───────────────────────────────
let pollRetryDelay = 2000;
async function pollLoop() {
  while (true) {
    try {
      await pollUpdates();
      pollRetryDelay = 2000;
    } catch (e) {
      logIST('POLL_ERR', e.message);
      await new Promise(r => setTimeout(r, pollRetryDelay));
      pollRetryDelay = Math.min(pollRetryDelay * 2, 30000);
    }
  }
}
pollLoop();

// ─── SELF-PING ──────────────────────────────────────────────────────
if (RENDER_URL) {
  setInterval(async () => {
    try {
      await fetch(`${RENDER_URL}/health`);
      logIST('PING', `Self-ping OK | uptime=${Math.floor(process.uptime())}s`);
    } catch (e) { logIST('PING', `failed: ${e.message}`); }
  }, 10 * 60 * 1000);
}

// ─── EXPRESS HEALTH ─────────────────────────────────────────────────
const app = express();
app.get('/', (req, res) => res.send('PRO P2P Bot v3.0 running ✅'));
app.get('/health', (req, res) => res.json({
  ok: true, uptime: process.uptime(), lastUpdateId, cached: sentByCbId.size, started: new Date(BOT_START_TIME).toISOString()
}));
app.listen(PORT, () => logIST('INIT', `🌐 Health server on :${PORT}`));
