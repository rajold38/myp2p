// ════════════════════════════════════════════════════════════════════
// PRO P2P + Trading Terminal — Unified Backend v4.0
//
// What this server does:
//   1. Serves the static frontend (public/index.html)
//   2. Watches Firebase Realtime DB for pending deposit/withdraw requests
//      and forwards them to the admin Telegram chat with Approve/Reject buttons
//   3. Handles Telegram callbacks (approve/reject) ATOMICALLY against any
//      coin (BTC, ETH, USDT, ...). No more USDT-only hardcoding.
//   4. Provides admin commands: /credit, /debit, /setbalance, /history,
//      /user, /trades, /cancel, /ban, /unban, /broadcast, /convert, etc.
//   5. Single source of truth for balances:
//        users/{fuid}/balances/{COIN}   (a map — never a single number)
//      Migration: on first sight of a user with legacy `balance` field and
//      no `balances/USDT`, copy it across automatically.
//
// All balance mutations go through `mutateBalance(fuid, coin, delta)` which
// is a Firebase transaction — guaranteed atomic, no double-credit races.
//
// All history entries go through `pushHistory(fuid, entry)` and share one
// shape: { hid, ts, type, coin, amt, status, ...meta }
// ════════════════════════════════════════════════════════════════════

import express from 'express';
import admin from 'firebase-admin';
import fetch from 'node-fetch';
import nodemailer from 'nodemailer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── ENV VARS ───────────────────────────────────────────────────────
const PORT             = Number(process.env.PORT) || Number(process.argv[process.argv.indexOf('--port')+1]) || 8080;
const TG_TOKEN         = process.env.TG_TOKEN;
const TG_CHAT          = process.env.TG_CHAT;
const FIREBASE_DB_URL  = process.env.FIREBASE_DB_URL;
const FIREBASE_SA_JSON = process.env.FIREBASE_SERVICE_ACCOUNT;
const RENDER_URL       = process.env.RENDER_EXTERNAL_URL || '';

const MISSING = [];
if (!TG_TOKEN)         MISSING.push('TG_TOKEN');
if (!TG_CHAT)          MISSING.push('TG_CHAT');
if (!FIREBASE_DB_URL)  MISSING.push('FIREBASE_DB_URL');
if (!FIREBASE_SA_JSON) MISSING.push('FIREBASE_SERVICE_ACCOUNT');
const BACKEND_DISABLED = MISSING.length > 0;
if (BACKEND_DISABLED) {
  console.warn(`⚠️  Backend disabled — missing env vars: ${MISSING.join(', ')}`);
  console.warn('   Serving static frontend only. Copy .env.example to .env to enable bot.');
}

// ─── LOGGER ─────────────────────────────────────────────────────────
const nowIST = () => new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
const log = (tag, msg) => console.log(`[${nowIST()} IST] [${tag}] ${msg}`);

// ─── FIREBASE INIT (skipped if backend disabled) ────────────────────
let db = null;
const TG_API          = `https://api.telegram.org/bot${TG_TOKEN}`;
const BOT_START_TIME  = Date.now();
const INSTANCE_ID     = `${BOT_START_TIME}_${Math.random().toString(36).slice(2,8)}`;
const STALE_SENDING_MS = 30_000;

if (!BACKEND_DISABLED) {
  let serviceAccount;
  try { serviceAccount = JSON.parse(FIREBASE_SA_JSON); }
  catch (e) { console.error('❌ FIREBASE_SERVICE_ACCOUNT is not valid JSON:', e.message); process.exit(1); }
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: FIREBASE_DB_URL
  });
  db = admin.database();
  log('INIT', `✅ Firebase Admin ready | instance=${INSTANCE_ID}`);
}


// ════════════════════════════════════════════════════════════════════
// CORE PRIMITIVES — used by every approve/reject/credit/debit path
// ════════════════════════════════════════════════════════════════════

/** Round to 8 decimals to avoid float drift. */
const r8 = (n) => parseFloat(((+n) || 0).toFixed(8));

/** Atomic, idempotent balance mutation for ANY coin.
 *  Returns { oldBal, newBal } or null on failure / insufficient funds. */
async function mutateBalance(fuid, coin, delta) {
  if (!fuid || !coin) return null;
  const COIN = String(coin).toUpperCase();
  const ref = db.ref(`users/${fuid}/balances/${COIN}`);
  let outcome = null;
  const tx = await ref.transaction(cur => {
    const old = parseFloat(cur || 0) || 0;
    const next = r8(old + delta);
    if (next < 0) return; // abort — would overdraw
    outcome = { oldBal: r8(old), newBal: next };
    return next;
  });
  if (!tx.committed) return null;
  return outcome;
}

/** Set a coin balance to an exact value (admin commands). */
async function setBalance(fuid, coin, value) {
  const COIN = String(coin).toUpperCase();
  const v = r8(value);
  const ref = db.ref(`users/${fuid}/balances/${COIN}`);
  const prev = (await ref.once('value')).val() || 0;
  await ref.set(v);
  return { oldBal: r8(prev), newBal: v };
}

/** Push a unified-shape history entry. Returns the generated hid. */
async function pushHistory(fuid, entry) {
  const hid = entry.hid || `h_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
  const clean = {
    hid,
    ts: entry.ts || Date.now(),
    date: nowIST(),
    isoDate: new Date().toISOString(),
    type: entry.type || 'UNKNOWN',
    coin: (entry.coin || 'USDT').toUpperCase(),
    amt: r8(entry.amt || 0),
    status: entry.status || 'COMPLETED',
    ...entry,
  };
  await db.ref(`users/${fuid}/history`).push(clean);
  return hid;
}

/** Update a history row's status by its hid. */
async function updateHistoryStatus(fuid, hid, status) {
  if (!hid) return;
  const snap = await db.ref(`users/${fuid}/history`).once('value');
  if (!snap.exists()) return;
  const updates = {};
  snap.forEach(child => {
    if (child.val()?.hid === hid) updates[`${child.key}/status`] = status;
  });
  if (Object.keys(updates).length) {
    await db.ref(`users/${fuid}/history`).update(updates);
  }
}

/** Keep legacy users/{fuid}/balance synced for old frontend screens. */
async function syncLegacyUsdtBalance(fuid, coin, newBal) {
  if (!fuid || String(coin || '').toUpperCase() !== 'USDT') return;
  await db.ref(`users/${fuid}/balance`).set(r8(newBal || 0));
}

/** Lazy migration: legacy `balance` (USDT number) → `balances.USDT`. */
async function migrateLegacyBalanceOnce(fuid, userVal) {
  try {
    if (!userVal) return;
    const hasMap = userVal.balances && typeof userVal.balances === 'object';
    const hasLegacy = userVal.balance !== undefined && userVal.balance !== null;
    if (hasLegacy && (!hasMap || userVal.balances.USDT === undefined)) {
      const v = r8(userVal.balance);
      await db.ref(`users/${fuid}/balances/USDT`).set(v);
      log('MIGRATE', `${fuid.slice(0,8)} legacy balance ${v} → balances.USDT`);
    }
  } catch (e) { log('MIGRATE', `err ${e.message}`); }
}

/** Look up a user by their short UID. */
async function findUserByUID(uid) {
  if (!uid) return null;
  const U = String(uid).toUpperCase();
  try {
    const snap = await db.ref('users').orderByChild('uid').equalTo(U).once('value');
    if (snap.exists()) {
      const val = snap.val();
      const fuid = Object.keys(val)[0];
      return { fuid, user: val[fuid] };
    }
  } catch (e) {}
  const all = await db.ref('users').once('value');
  let result = null;
  all.forEach(child => {
    if (String(child.val()?.uid || '').toUpperCase() === U) {
      result = { fuid: child.key, user: child.val() };
    }
  });
  return result;
}

// ════════════════════════════════════════════════════════════════════
// SINGLE-INSTANCE GUARD + PERSISTENT OFFSET
// ════════════════════════════════════════════════════════════════════
async function claimInstanceLock() {
  await db.ref('botMeta/activeInstance').set({ id: INSTANCE_ID, ts: BOT_START_TIME });
  log('INIT', `🔒 Claimed instance lock`);
}
async function amIActive() {
  const v = (await db.ref('botMeta/activeInstance').once('value')).val();
  return !v || v.id === INSTANCE_ID;
}
async function loadLastUpdateId() {
  return parseInt((await db.ref('botMeta/lastUpdateId').once('value')).val() || 0, 10);
}
async function saveLastUpdateId(id) {
  await db.ref('botMeta/lastUpdateId').set(id);
}

// ════════════════════════════════════════════════════════════════════
// TELEGRAM HELPERS
// ════════════════════════════════════════════════════════════════════
async function tgFetch(endpoint, body) {
  try {
    const r = await fetch(`${TG_API}/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const data = await r.json();
    if (!data.ok) log('TG', `${endpoint} failed: ${data.description || JSON.stringify(data)}`);
    return data;
  } catch (e) { log('TG', `fetch err ${endpoint}: ${e.message}`); return { ok: false, error: e.message }; }
}
const tgSend   = (text, extra={}) => tgFetch('sendMessage', { chat_id: TG_CHAT, text, parse_mode: 'Markdown', ...extra });
const tgEdit   = (msgId, text, extra={}) => tgFetch('editMessageText', { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: 'Markdown', ...extra });
const tgAnswer = (cbId, text) => tgFetch('answerCallbackQuery', { callback_query_id: cbId, text });

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

// ════════════════════════════════════════════════════════════════════
// PENDING REQUEST WATCHER — sends approve/reject buttons to admin
// ════════════════════════════════════════════════════════════════════
const sentByCbId = new Map();        // cbId -> { fuid, type, msgId, hid, amt, coin }
const locallySentCbIds = new Set();   // in-process dedup
const processedCbIds = new Map();     // tg cb.id -> ts (for retry dedup)

function rememberCb(id) {
  processedCbIds.set(id, Date.now());
  if (processedCbIds.size > 500) {
    const cutoff = Date.now() - 5 * 60_000;
    for (const [k, t] of processedCbIds) if (t < cutoff) processedCbIds.delete(k);
  }
}

function fmtRequestMsg(type, user, req) {
  const head = type === 'dep' ? '📥 *NEW DEPOSIT REQUEST*' : '💸 *NEW WITHDRAWAL REQUEST*';
  const coin = (req.coin || 'USDT').toUpperCase();
  const balances = user.balances || {};
  const lines = [
    head, '',
    `👤 UID: \`${user.uid || '—'}\``,
    `📛 Name: *${user.name || '—'}*`,
    `📧 Email: ${user.email || '—'}`,
    `💎 Coin: *${coin}*`,
    `💵 Amount: *${req.amt} ${coin}*`,
  ];
  if (req.network || req.chain) lines.push(`🌐 Network: ${req.network || req.chain}`);
  if (req.txid)                 lines.push(`🔗 TxID: \`${req.txid}\``);
  if (req.address || req.addr)  lines.push(`📬 Address: \`${req.address || req.addr}\``);
  if (req.utr)                  lines.push(`🧾 UTR: \`${req.utr}\``);
  lines.push(`💰 Current ${coin}: *${r8(balances[coin] || 0)}*`);
  lines.push(`💰 USDT: *${r8(balances.USDT || user.balance || 0).toFixed(2)}*`);
  lines.push('', `⏰ ${nowIST()} IST`);
  return lines.join('\n');
}

async function maybeSendButtons(fuid, type, req, cbId) {
  if (!req || !cbId) return;
  const existingMsgId = req.botMsgId || req.msgId;
  if (existingMsgId) {
    sentByCbId.set(cbId, { fuid, type, msgId: existingMsgId, hid: req.hid, amt: parseFloat(req.amt), coin: req.coin });
    locallySentCbIds.add(cbId);
    return;
  }
  if (locallySentCbIds.has(cbId)) return;

  const reqRef = db.ref(`users/${fuid}/pendingReqs/${type}/${cbId}`);
  const tx = await reqRef.transaction(cur => {
    if (!cur) return;
    if (cur.botMsgId || cur.msgId) return;
    if (cur._sending && cur._sendingTs && (Date.now() - cur._sendingTs) < STALE_SENDING_MS) return;
    cur._sending = INSTANCE_ID;
    cur._sendingTs = Date.now();
    return cur;
  });
  if (!tx.committed || !tx.snapshot.exists()) return;
  const claimed = tx.snapshot.val();
  if (claimed._sending !== INSTANCE_ID) return;
  if (claimed.botMsgId || claimed.msgId) return;

  locallySentCbIds.add(cbId);
  const user = (await db.ref(`users/${fuid}`).once('value')).val() || {};
  await migrateLegacyBalanceOnce(fuid, user);
  const text = fmtRequestMsg(type, user, claimed);
  const msgId = await tgSendButtons(text, cbId);
  if (msgId) {
    await reqRef.update({ botMsgId: msgId, msgId, cbId, _sending: null, _sendingTs: null });
    sentByCbId.set(cbId, { fuid, type, msgId, hid: claimed.hid, amt: parseFloat(claimed.amt), coin: claimed.coin });
    log(type.toUpperCase(), `UID=${user.uid} Amt=${claimed.amt} ${claimed.coin || 'USDT'} cbId=${cbId} → sent msgId=${msgId}`);
  } else {
    locallySentCbIds.delete(cbId);
    await reqRef.update({ _sending: null, _sendingTs: null }).catch(()=>{});
  }
}

async function processPendingMap(fuid, data, cutoff = 0) {
  const reqs = data?.pendingReqs || {};
  for (const type of ['dep', 'wit']) {
    const map = reqs[type] || {};
    for (const [cbId, req] of Object.entries(map)) {
      if (!req || typeof req !== 'object') continue;
      if (cbId === 'botLock') continue;
      if (cutoff && (req.ts || 0) < cutoff) continue;
      await maybeSendButtons(fuid, type, req, cbId).catch(e => log('ERR', `${type} ${e.message}`));
    }
  }
}

if (db) {
  db.ref('users').on('child_changed', async (snap) => {
    if (!(await amIActive())) return;
    const v = snap.val() || {};
    await migrateLegacyBalanceOnce(snap.key, v);
    await processPendingMap(snap.key, v);
  });
  db.ref('users').on('child_added', async (snap) => {
    if (!(await amIActive())) return;
    const v = snap.val() || {};
    await migrateLegacyBalanceOnce(snap.key, v);
    await processPendingMap(snap.key, v, BOT_START_TIME - 10_000);
  });
}

// ════════════════════════════════════════════════════════════════════
// APPROVE / REJECT / CANCEL — idempotent, multi-coin
// ════════════════════════════════════════════════════════════════════
async function claimPendingReq(fuid, type, cbId, action) {
  const ref = db.ref(`users/${fuid}/pendingReqs/${type}/${cbId}`);
  const tx = await ref.transaction(cur => {
    if (!cur) return;
    if (cur._resolved) return;
    cur._resolved = action;
    cur._resolvedBy = INSTANCE_ID;
    cur._resolvedTs = Date.now();
    return cur;
  });
  if (!tx.committed || !tx.snapshot.exists()) return null;
  const v = tx.snapshot.val();
  if (v._resolvedBy !== INSTANCE_ID) return null;
  return v;
}

async function handleApprove(fuid, type, req, cbId) {
  const claimed = await claimPendingReq(fuid, type, cbId, 'approve');
  if (!claimed) { log('IDEMP', `approve skipped cbId=${cbId}`); return null; }

  const coin = (claimed.coin || req.coin || 'USDT').toUpperCase();
  const amt  = parseFloat(claimed.amt || req.amt || 0);
  let result = null;
  if (type === 'dep') {
    // Deposit approved → credit the coin
    result = await mutateBalance(fuid, coin, +amt);
  } else {
    // Withdrawal approved → balance already debited at request time; no change.
    const bal = parseFloat((await db.ref(`users/${fuid}/balances/${coin}`).once('value')).val() || 0);
    result = { oldBal: bal, newBal: bal };
  }
  if (result) await syncLegacyUsdtBalance(fuid, coin, result.newBal);
  await updateHistoryStatus(fuid, claimed.hid, 'COMPLETED');
  await db.ref(`users/${fuid}/pendingReqs/${type}/${cbId}`).remove();
  const user = (await db.ref(`users/${fuid}`).once('value')).val() || {};
  queueResolutionEmail({ type, action: 'approve', user, uid: user.uid, amt, coin, oldBal: result?.oldBal, newBal: result?.newBal });
  log('APPROVE', `UID=${user.uid} ${type.toUpperCase()} ${amt} ${coin} | ${result?.oldBal} → ${result?.newBal}`);
  return { ...result, user, coin, amt };
}

async function handleReject(fuid, type, req, cbId) {
  const claimed = await claimPendingReq(fuid, type, cbId, 'reject');
  if (!claimed) { log('IDEMP', `reject skipped cbId=${cbId}`); return null; }

  const coin = (claimed.coin || req.coin || 'USDT').toUpperCase();
  const amt  = parseFloat(claimed.amt || req.amt || 0);
  let result = null;
  if (type === 'wit') {
    // Withdrawal rejected → refund whatever coin was held
    result = await mutateBalance(fuid, coin, +amt);
  } else {
    const bal = parseFloat((await db.ref(`users/${fuid}/balances/${coin}`).once('value')).val() || 0);
    result = { oldBal: bal, newBal: bal };
  }
  if (result) await syncLegacyUsdtBalance(fuid, coin, result.newBal);
  await updateHistoryStatus(fuid, claimed.hid, 'REJECTED');
  await db.ref(`users/${fuid}/pendingReqs/${type}/${cbId}`).remove();
  const user = (await db.ref(`users/${fuid}`).once('value')).val() || {};
  queueResolutionEmail({ type, action: 'reject', user, uid: user.uid, amt, coin, oldBal: result?.oldBal, newBal: result?.newBal });
  log('REJECT', `UID=${user.uid} ${type.toUpperCase()} ${amt} ${coin} ${type==='wit'?'refunded':''} | ${result?.oldBal} → ${result?.newBal}`);
  return { ...result, user, coin, amt };
}

function fmtResolutionMsg(label, action, ctx, req) {
  const coin = (ctx.coin || req?.coin || 'USDT').toUpperCase();
  const ico = action === 'approve' ? '✅' : '❌';
  const verb = action === 'approve' ? 'APPROVED' : 'REJECTED';
  const sign = ctx.type === 'dep' ? '+' : '-';
  const lines = [`${ico} *${label} ${verb}*`, ''];
  if (ctx.user) {
    lines.push(`👤 UID: \`${ctx.user.uid || '—'}\``);
    lines.push(`📛 Name: *${ctx.user.name || '—'}*`);
  }
  lines.push(`💵 Amount: *${sign}${ctx.amt} ${coin}*`);
  if (req?.network || req?.chain) lines.push(`🌐 Network: ${req.network || req.chain}`);
  if (req?.txid)                  lines.push(`🔗 TxID: \`${req.txid}\``);
  if (req?.address || req?.addr)  lines.push(`📬 Address: \`${req.address || req.addr}\``);
  if (ctx.newBal !== undefined)   lines.push(`💰 ${coin} Balance: ${ctx.oldBal} → *${ctx.newBal}*`);
  lines.push(`⏰ ${nowIST()} IST`);
  lines.push(`${ico} ${verb} by Admin`);
  return lines.join('\n');
}

// ════════════════════════════════════════════════════════════════════
// CALLBACK ROUTER
// ════════════════════════════════════════════════════════════════════
async function handleCallback(cb) {
  if (processedCbIds.has(cb.id)) { await tgAnswer(cb.id, '✓'); return; }
  rememberCb(cb.id);

  const chatId = String(cb.message?.chat?.id || '');
  if (chatId !== String(TG_CHAT)) { await tgAnswer(cb.id, 'Unauthorized'); return; }
  const data = cb.data || '';

  let m;
  if ((m = data.match(/^(approve|reject)_(dep_|wit_)(.+)$/))) return handleApproveRejectCb(cb, m[1], m[2] + m[3]);
  // Trade/P2P callbacks (cbId starts with trade_) are handled by the iframe
  // via the buffered /api/tg/getUpdates feed — leave them alone here.
  if (/^(approve|reject)_trade_/.test(data)) return; // no tg-answer, iframe will
  if ((m = data.match(/^userdetail_(.+)$/)))       return sendUserDetailCard(cb, m[1]);
  if ((m = data.match(/^(ban|unban)_(.+)$/)))      return handleBanCb(cb, m[1], m[2]);
  if ((m = data.match(/^history_(.+)$/)))          { await tgAnswer(cb.id, 'Loading…'); return sendUserHistory(m[1], 15); }
  if ((m = data.match(/^closep2p_(.+)$/)))         { await tgAnswer(cb.id, 'Closing…'); return closeAllP2PForUser(m[1]); }
  if ((m = data.match(/^(creditprompt|debitprompt)_(.+)$/))) {
    await tgAnswer(cb.id, 'Send the command');
    const kind = m[1] === 'creditprompt' ? 'credit' : 'debit';
    await tgSend(`✏️ Reply with:\n\`/${kind} ${m[2]} <amount> [COIN]\``);
    return;
  }
  await tgAnswer(cb.id, '?');
}

async function handleApproveRejectCb(cb, action, cbId) {
  await tgAnswer(cb.id, action === 'approve' ? '⏳ Approving…' : '⏳ Rejecting…');
  let ctx = sentByCbId.get(cbId);
  let req = null;
  if (!ctx) {
    // Scan for the pending request matching this cbId
    const snap = await db.ref('users').once('value');
    snap.forEach(child => {
      const reqs = child.val()?.pendingReqs || {};
      for (const type of ['dep', 'wit']) {
        const map = reqs[type] || {};
        for (const [storedCbId, r] of Object.entries(map)) {
          if (storedCbId === cbId || r?.cbId === cbId) {
            ctx = { fuid: child.key, type, hid: r.hid, amt: parseFloat(r.amt), msgId: r.botMsgId, cbId, coin: r.coin };
            req = r;
          }
        }
      }
    });
  } else {
    try { req = (await db.ref(`users/${ctx.fuid}/pendingReqs/${ctx.type}/${cbId}`).once('value')).val(); } catch (e) {}
  }
  if (!ctx) { return; }

  const handler = action === 'approve' ? handleApprove : handleReject;
  const r = await handler(ctx.fuid, ctx.type, req || ctx, cbId);
  if (!r) { return; }
  const label = ctx.type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL';
  if (ctx.msgId) await tgEdit(ctx.msgId, fmtResolutionMsg(label, action, { ...ctx, ...r }, req));
  sentByCbId.delete(cbId);
}

async function handleBanCb(cb, action, uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgAnswer(cb.id, 'Not found'); return; }
  const banned = action === 'ban';
  await db.ref(`users/${found.fuid}`).update({ banned });
  await tgAnswer(cb.id, banned ? '🚫 Banned' : '✅ Unbanned');
  log('ADMIN', `${banned?'BAN':'UNBAN'} UID=${uid}`);
  await tgSend(`${banned?'🚫 *USER BANNED*':'✅ *USER UNBANNED*'}\n\n👤 \`${uid}\`\n📛 ${found.user.name || 'Unknown'}`);
}

// ════════════════════════════════════════════════════════════════════
// ADMIN COMMANDS
// ════════════════════════════════════════════════════════════════════
const HELP_TEXT = [
  '🤖 *PRO P2P + TRADING ADMIN BOT v4.0*', '',
  '━━━━━━━━━━━━━━━━━━━',
  '👥 *USERS*',
  '━━━━━━━━━━━━━━━━━━━',
  '`/users` — List all users',
  '`/user UID` — User details card',
  '`/history UID` — Last 15 transactions',
  '`/ban UID` · `/unban UID`',
  '',
  '━━━━━━━━━━━━━━━━━━━',
  '💰 *BALANCE (multi-coin)*',
  '━━━━━━━━━━━━━━━━━━━',
  '`/credit UID AMT [COIN]` — Add funds (default USDT)',
  '`/debit  UID AMT [COIN]` — Deduct funds',
  '`/setbalance UID COIN AMT` — Set exact balance',
  '`/balances UID` — Show all coin balances',
  '`/convert UID FROM TO AMT` — Manual swap',
  '',
  '━━━━━━━━━━━━━━━━━━━',
  '🔄 *TRADES & ORDERS*',
  '━━━━━━━━━━━━━━━━━━━',
  '`/trades` — All pending deposits/withdrawals/P2P',
  '`/cancel TRADEID` — Cancel a pending request',
  '`/closeorder ORDERID` — Force-close a P2P order',
  '`/closep2p UID` — Close all active P2P for user',
  '',
  '━━━━━━━━━━━━━━━━━━━',
  '📊 *PLATFORM*',
  '━━━━━━━━━━━━━━━━━━━',
  '`/stats` — Platform stats',
  '`/broadcast MSG` — Announcement to all',
  '`/ping` · `/help`',
  '',
  '⚡ Quick: `#UID AMT` (USDT credit)',
].join('\n');

async function sendUserDetailCard(cb, uid) {
  await tgAnswer(cb.id, 'Loading…');
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const u = found.user;
  await migrateLegacyBalanceOnce(found.fuid, u);
  const balances = u.balances || {};
  const balLines = Object.entries(balances)
    .filter(([,v]) => parseFloat(v) > 0)
    .sort((a,b) => parseFloat(b[1]) - parseFloat(a[1]))
    .slice(0, 8)
    .map(([c,v]) => `   • ${c}: *${r8(v)}*`)
    .join('\n') || '   (no balances)';

  const histSnap = await db.ref(`users/${found.fuid}/history`).once('value');
  const histArr = [];
  histSnap.forEach(c => histArr.push(c.val()));
  histArr.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const last3 = histArr.slice(0, 3).map(h => {
    const sign = /WITHDRAW|DEBIT|CONVERT_OUT|SELL/i.test(h.type) ? '-' : '+';
    const ico = h.status==='COMPLETED'?'✅':h.status==='REJECTED'?'❌':h.status==='CANCELLED'?'🚫':'⏳';
    return `  ${ico} ${h.type} ${sign}${r8(h.amt)} ${(h.coin||'USDT')}`;
  }).join('\n') || '  (no transactions)';

  const text = [
    `👤 *USER DETAILS — ${u.uid}*`, '',
    `📛 Name: *${u.name || '—'}*`,
    `📧 Email: ${u.email || '—'}`,
    `📱 Phone: ${u.phone || '—'}`,
    `🔑 Status: ${u.banned ? '🚫 BANNED' : '✅ Active'}`,
    `📅 Joined: ${u.createdAt ? new Date(u.createdAt).toLocaleDateString('en-IN',{timeZone:'Asia/Kolkata'}) : '—'}`,
    '', `💰 *Balances:*`, balLines,
    '', `📊 *Last 3 Transactions:*`, last3
  ].join('\n');

  const buttons = [
    [{ text: '💰 Credit', callback_data: `creditprompt_${uid}` },
     { text: '➖ Debit',  callback_data: `debitprompt_${uid}` }],
    [ u.banned
        ? { text: '✅ Unban', callback_data: `unban_${uid}` }
        : { text: '🚫 Ban',   callback_data: `ban_${uid}` },
      { text: '📋 History', callback_data: `history_${uid}` }],
    [{ text: '🔄 Close P2P', callback_data: `closep2p_${uid}` }]
  ];
  await tgSend(text, { reply_markup: { inline_keyboard: buttons } });
}

async function sendUserHistory(uid, limit = 15) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const snap = await db.ref(`users/${found.fuid}/history`).once('value');
  if (!snap.exists()) { await tgSend(`📭 No history for \`${uid}\``); return; }
  const arr = [];
  snap.forEach(c => arr.push(c.val()));
  arr.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const items = arr.slice(0, limit).map(h => {
    const sign = /WITHDRAW|DEBIT|CONVERT_OUT|SELL/i.test(h.type) ? '-' : '+';
    const ico = h.status==='COMPLETED'?'✅':h.status==='REJECTED'?'❌':h.status==='CANCELLED'?'🚫':'⏳';
    const d = h.date || (h.ts ? new Date(h.ts).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}) : '');
    const net = h.network ? ' · '+h.network : '';
    return `${ico} ${h.type} ${sign}${r8(h.amt)} ${h.coin||'USDT'}${net} — ${d}`;
  });
  await tgSend(`📋 *HISTORY — ${uid}* (last ${items.length})\n\n${items.join('\n')}`);
}

async function handleUsersList() {
  const snap = await db.ref('users').once('value');
  if (!snap.exists()) { await tgSend('No users found.'); return; }
  const users = [];
  snap.forEach(c => { const u = c.val(); if (u && u.uid) users.push(u); });
  if (!users.length) { await tgSend('No users found.'); return; }
  await tgSend(`👥 *ALL USERS (${users.length} total)*`);
  const CHUNK = 8;
  for (let i = 0; i < users.length; i += CHUNK) {
    const slice = users.slice(i, i + CHUNK);
    const lines = slice.map(u => {
      const st = u.banned ? '🚫' : '✅';
      const usdt = r8((u.balances && u.balances.USDT) || u.balance || 0);
      return `${st} \`${u.uid}\` | *${u.name || '—'}* | 💰 ${usdt} USDT`;
    });
    const keyboard = slice.map(u => [{ text: `👁 ${u.uid} — ${u.name||'—'}`.slice(0,60), callback_data: `userdetail_${u.uid}` }]);
    await tgSend(lines.join('\n'), { reply_markup: { inline_keyboard: keyboard } });
  }
}

async function handleStats() {
  const snap = await db.ref('users').once('value');
  let total = 0, banned = 0, totalUsdt = 0, pendDep = 0, pendWit = 0, activeP2P = 0;
  snap.forEach(c => {
    const u = c.val() || {}; if (!u.uid) return;
    total++; if (u.banned) banned++;
    totalUsdt += parseFloat((u.balances && u.balances.USDT) || u.balance || 0);
    const reqs = u.pendingReqs || {};
    pendDep += Object.keys(reqs.dep || {}).filter(k => k !== 'botLock').length;
    pendWit += Object.keys(reqs.wit || {}).filter(k => k !== 'botLock').length;
    for (const o of Object.values(u.orders || {})) {
      if (o?.status && !['completed','cancelled','canceled'].includes(String(o.status).toLowerCase())) activeP2P++;
    }
  });
  const upMs = Date.now() - BOT_START_TIME;
  const upH = Math.floor(upMs / 3600000);
  const upM = Math.floor((upMs % 3600000) / 60000);
  await tgSend([
    '📊 *PLATFORM STATS*', '',
    `👥 Users: *${total}* (✅ ${total-banned} · 🚫 ${banned})`,
    `💰 Total USDT: *${totalUsdt.toFixed(2)}*`,
    `📥 Pending Dep: *${pendDep}* · 📤 Pending Wit: *${pendWit}*`,
    `🔄 Active P2P: *${activeP2P}*`,
    `⏰ Uptime: *${upH}h ${upM}m*`,
    `🔑 Instance: \`${INSTANCE_ID}\``,
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
    for (const [oid, ord] of Object.entries(u.orders || {})) {
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
          const claimed = await claimPendingReq(fuid, type, cbId, 'cancel');
          if (!claimed) { await tgSend(`⚠️ Trade \`${targetId}\` already resolved.`); return; }
          const coin = (req.coin || 'USDT').toUpperCase();
          if (type === 'wit' && req.amt) {
            await mutateBalance(fuid, coin, +parseFloat(req.amt));
          }
          await updateHistoryStatus(fuid, req.hid, 'CANCELLED');
          await db.ref(`users/${fuid}/pendingReqs/${type}/${cbId}`).remove();
          if (req.botMsgId) {
            await tgEdit(req.botMsgId, `🚫 *${type==='dep'?'DEPOSIT':'WITHDRAWAL'} CANCELLED BY ADMIN*\n\nAmount: *${req.amt} ${coin}*${type==='wit'?'\n🔴 Funds refunded':''}`).catch(()=>{});
          }
          sentByCbId.delete(cbId);
          await tgSend(`✅ *CANCELLED*\n\n👤 \`${u.uid || fuid}\`\nType: ${type.toUpperCase()}\nAmt: *${req.amt} ${coin}*`);
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
  const users = [];
  snap.forEach(c => users.push({ key: c.key, val: c.val() || {} }));
  for (const { key, val: u } of users) {
    for (const [oid, ord] of Object.entries(u.orders || {})) {
      if (oid === orderId || String(ord?.id) === orderId) {
        const coin = (ord?.coin || 'USDT').toUpperCase();
        if (ord?.mode === 'BUY' && ord?.usdt) {
          await mutateBalance(key, 'USDT', +parseFloat(ord.usdt));
        } else if (ord?.mode === 'SELL' && ord?.amt) {
          await mutateBalance(key, coin, +parseFloat(ord.amt));
        }
        await db.ref(`users/${key}/orders/${oid}`).update({ status: 'cancelled' });
        await tgSend(`✅ *ORDER CLOSED*\n\nOrder: \`${oid}\`\n👤 \`${u.uid || key}\``);
        done = true; break;
      }
    }
    if (done) break;
  }
  if (!done) await tgSend(`❌ Order \`${orderId}\` not found.`);
}

async function closeAllP2PForUser(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const orders = found.user.orders || {};
  let closed = 0, refundedUsdt = 0;
  for (const [oid, ord] of Object.entries(orders)) {
    if (!ord || !ord.status) continue;
    const st = String(ord.status).toLowerCase();
    if (['completed','cancelled','canceled'].includes(st)) continue;
    if (ord.mode === 'BUY' && ord.usdt) {
      await mutateBalance(found.fuid, 'USDT', +parseFloat(ord.usdt));
      refundedUsdt += parseFloat(ord.usdt);
    } else if (ord.mode === 'SELL' && ord.amt) {
      await mutateBalance(found.fuid, (ord.coin||'USDT').toUpperCase(), +parseFloat(ord.amt));
    }
    await db.ref(`users/${found.fuid}/orders/${oid}`).update({ status: 'cancelled' });
    closed++;
  }
  log('ADMIN', `closeP2P UID=${uid} closed=${closed} refundedUsdt=${refundedUsdt}`);
  await tgSend(`✅ *P2P CLOSED*\n\n👤 \`${uid}\`\nCancelled: *${closed}*\nUSDT Refunded: *${refundedUsdt.toFixed(2)}*`);
}

async function adminCreditDebit(uid, amt, sign, type, coin = 'USDT') {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const delta = sign === '+' ? +amt : -amt;
  const result = await mutateBalance(found.fuid, coin, delta);
  if (!result) { await tgSend(`❌ Insufficient ${coin} balance for ${uid}.`); return; }
  await pushHistory(found.fuid, {
    type, coin, amt, status: 'COMPLETED', uid: found.user.uid,
    sender: 'ADMIN', note: `${sign === '+' ? 'Credited' : 'Debited'} ${amt} ${coin}`,
  });
  await tgSend([
    `${sign==='+' ? '✅ *CREDITED*' : '✅ *DEBITED*'}`, '',
    `👤 UID: \`${uid}\``,
    `📛 ${found.user.name || '—'}`,
    `${sign==='+'?'➕':'➖'} ${amt} ${coin}`,
    `💰 ${coin}: *${result.oldBal} → ${result.newBal}*`,
  ].join('\n'));
}

async function handleSetBalance(uid, coin, amt) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const r = await setBalance(found.fuid, coin, amt);
  await pushHistory(found.fuid, {
    type: 'ADMIN_SET_BALANCE', coin, amt, status: 'COMPLETED',
    uid: found.user.uid, note: `Set ${coin} = ${amt}`,
  });
  await tgSend(`✅ *${coin.toUpperCase()} BALANCE SET*\n\n👤 \`${uid}\`\n💰 ${r.oldBal} → *${r.newBal}*`);
}

async function handleBalances(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  await migrateLegacyBalanceOnce(found.fuid, found.user);
  const bals = (await db.ref(`users/${found.fuid}/balances`).once('value')).val() || {};
  const lines = Object.entries(bals)
    .map(([c,v]) => ({ c, v: r8(v) }))
    .filter(x => x.v > 0)
    .sort((a,b) => b.v - a.v)
    .map(x => `   • ${x.c}: *${x.v}*`);
  await tgSend(`💰 *${found.user.uid} BALANCES*\n\n${lines.length ? lines.join('\n') : '(empty)'}`);
}

async function handleConvert(uid, from, to, amt) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const FROM = from.toUpperCase(), TO = to.toUpperCase();
  if (FROM === TO) { await tgSend('❌ FROM and TO must differ.'); return; }
  const out = await mutateBalance(found.fuid, FROM, -amt);
  if (!out) { await tgSend(`❌ Insufficient ${FROM} balance.`); return; }
  // No price oracle on server — admin manual conversion is 1:1 by amount.
  // For real-priced conversions the frontend does the rate calc and writes.
  const inn = await mutateBalance(found.fuid, TO, +amt);
  await pushHistory(found.fuid, { type: 'CONVERT_OUT', coin: FROM, amt, status: 'COMPLETED', uid: found.user.uid, fromCoin: FROM, toCoin: TO, rate: 1 });
  await pushHistory(found.fuid, { type: 'CONVERT_IN',  coin: TO,   amt, status: 'COMPLETED', uid: found.user.uid, fromCoin: FROM, toCoin: TO, rate: 1 });
  await tgSend(`🔄 *MANUAL CONVERT*\n\n👤 \`${uid}\`\n${amt} ${FROM} → ${amt} ${TO}\n${FROM}: ${out.oldBal} → ${out.newBal}\n${TO}:   ${inn.oldBal} → ${inn.newBal}`);
}

async function handleBroadcast(message) {
  if (!message) { await tgSend('Usage: `/broadcast <message>`'); return; }
  const snap = await db.ref('users').once('value');
  let count = 0;
  snap.forEach(c => { if (c.val()?.uid) count++; });
  const entry = { text: message, ts: Date.now(), from: 'admin', date: nowIST() };
  await db.ref('broadcast').push(entry);
  log('BROADCAST', `to ${count} users: "${message}"`);
  await tgSend(`✅ *BROADCAST SENT*\n\nReach: *${count} users*\n💬 "${message}"`);
}

// ════════════════════════════════════════════════════════════════════
// UPDATE / MESSAGE HANDLER
// ════════════════════════════════════════════════════════════════════
async function handleUpdate(upd) {
  if (upd.callback_query) return handleCallback(upd.callback_query);
  const msg = upd.message;
  if (!msg) return;
  const chatId = String(msg.chat.id);
  if (chatId !== String(TG_CHAT)) return;
  const text = (msg.text || '').trim();

  if (text === '/start' || text === '/help') return tgSend(HELP_TEXT);
  if (text === '/ping')   return tgSend(`🟢 Bot online — ${nowIST()} IST\nUptime: ${Math.floor((Date.now()-BOT_START_TIME)/60000)}m\nInstance: \`${INSTANCE_ID}\``);
  if (text === '/users')  return handleUsersList();
  if (text === '/trades') return handleTrades();
  if (text === '/stats')  return handleStats();

  let m;
  if ((m = text.match(/^\/user\s+([A-Z0-9]{2,15})$/i)))                   return sendUserDetailCard({ id: 'cmd' }, m[1].toUpperCase());
  if ((m = text.match(/^\/history\s+([A-Z0-9]{2,15})$/i)))                return sendUserHistory(m[1].toUpperCase(), 15);
  if ((m = text.match(/^\/balances\s+([A-Z0-9]{2,15})$/i)))               return handleBalances(m[1].toUpperCase());
  if ((m = text.match(/^\/credit\s+([A-Z0-9]{2,15})\s+([\d.]+)(?:\s+([A-Z]{2,8}))?$/i)))
    return adminCreditDebit(m[1].toUpperCase(), parseFloat(m[2]), '+', 'ADMIN_CREDIT', (m[3] || 'USDT').toUpperCase());
  if ((m = text.match(/^\/debit\s+([A-Z0-9]{2,15})\s+([\d.]+)(?:\s+([A-Z]{2,8}))?$/i)))
    return adminCreditDebit(m[1].toUpperCase(), parseFloat(m[2]), '-', 'ADMIN_DEBIT', (m[3] || 'USDT').toUpperCase());
  if ((m = text.match(/^\/setbalance\s+([A-Z0-9]{2,15})\s+([A-Z]{2,8})\s+([\d.]+)$/i)))
    return handleSetBalance(m[1].toUpperCase(), m[2].toUpperCase(), parseFloat(m[3]));
  if ((m = text.match(/^\/convert\s+([A-Z0-9]{2,15})\s+([A-Z]{2,8})\s+([A-Z]{2,8})\s+([\d.]+)$/i)))
    return handleConvert(m[1].toUpperCase(), m[2], m[3], parseFloat(m[4]));
  if ((m = text.match(/^\/ban\s+([A-Z0-9]{2,15})$/i)))      return db.ref(`users`).once('value').then(s => { s.forEach(c => { if (c.val()?.uid?.toUpperCase()===m[1].toUpperCase()) db.ref(`users/${c.key}`).update({ banned: true }); }); tgSend(`🚫 Banned \`${m[1]}\``); });
  if ((m = text.match(/^\/unban\s+([A-Z0-9]{2,15})$/i)))    return db.ref(`users`).once('value').then(s => { s.forEach(c => { if (c.val()?.uid?.toUpperCase()===m[1].toUpperCase()) db.ref(`users/${c.key}`).update({ banned: false }); }); tgSend(`✅ Unbanned \`${m[1]}\``); });
  if ((m = text.match(/^\/cancel\s+(\S+)$/i)))              return handleCancel(m[1]);
  if ((m = text.match(/^\/closeorder\s+(\S+)$/i)))          return handleCloseOrder(m[1]);
  if ((m = text.match(/^\/closep2p\s+([A-Z0-9]{2,15})$/i))) return closeAllP2PForUser(m[1].toUpperCase());
  if ((m = text.match(/^\/broadcast\s+([\s\S]+)$/i)))       return handleBroadcast(m[1].trim());
  if ((m = text.match(/^#([A-Z0-9]{2,15})\s+([\d.]+)$/i)))  return adminCreditDebit(m[1].toUpperCase(), parseFloat(m[2]), '+', 'ADMIN_CREDIT', 'USDT');
}

// ════════════════════════════════════════════════════════════════════
// POLLING
// ════════════════════════════════════════════════════════════════════
let lastUpdateId = 0;
async function pollUpdates() {
  try {
    if (!(await amIActive())) {
      log('POLL', '⏸️ another instance active — sleeping 30s');
      await new Promise(r => setTimeout(r, 30_000));
      return;
    }
    const res = await fetch(`${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=25&allowed_updates=${encodeURIComponent('["callback_query","message"]')}`);
    const data = await res.json();
    if (!data.ok) return;
    for (const upd of (data.result || [])) {
      lastUpdateId = upd.update_id;
      await saveLastUpdateId(lastUpdateId).catch(()=>{});
      pushRecentUpdate(upd);
      try { await handleUpdate(upd); } catch (e) { log('ERR', `handleUpdate: ${e.message}`); }
    }
  } catch (e) { log('POLL', `err ${e.message}`); }
}

async function pollLoop() {
  while (true) {
    await pollUpdates();
    await new Promise(r => setTimeout(r, 300));
  }
}

// ════════════════════════════════════════════════════════════════════
// RECENT UPDATES BUFFER — exposed via /api/tg/getUpdates so the frontend
// iframe (which can't poll Telegram directly because we own the single
// poll lock) still receives callback_query + message events for P2P
// trade approve/reject and admin chat replies.
// ════════════════════════════════════════════════════════════════════
const RECENT_UPDATES = [];
const RECENT_UPDATES_MAX = 500;
function pushRecentUpdate(upd) {
  if (!upd || typeof upd.update_id !== 'number') return;
  RECENT_UPDATES.push({ ...upd, _ts: Date.now() });
  if (RECENT_UPDATES.length > RECENT_UPDATES_MAX) RECENT_UPDATES.shift();
}

function makeProxyUpdate(kind, result) {
  if (!result) return null;
  const nowId = Date.now() * 1000 + Math.floor(Math.random() * 1000);
  if (kind === 'message') return { update_id: nowId, message: result };
  if (kind === 'callback_query') return { update_id: nowId, callback_query: result };
  return null;
}

// ════════════════════════════════════════════════════════════════════
// EXPRESS — serves frontend + health check + self-ping
// ════════════════════════════════════════════════════════════════════
const app = express();
app.use(express.json({ limit: '1mb' }));

// CORS — allow the Vercel-hosted frontend (or any origin) to reach the proxy
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, Accept, Origin');
  res.setHeader('Access-Control-Max-Age', '86400');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const PUBLIC_DIR = path.join(__dirname, 'public');
const PUBLIC_INDEX = path.join(PUBLIC_DIR, 'index.html');
const ROOT_INDEX = path.join(__dirname, 'index.html');
const INDEX_FILE = fs.existsSync(PUBLIC_INDEX) ? PUBLIC_INDEX : ROOT_INDEX;

if (fs.existsSync(PUBLIC_DIR)) {
  app.use(express.static(PUBLIC_DIR));
}


app.get('/health', (_req, res) => res.json({
  ok: true, instance: INSTANCE_ID, uptimeMs: Date.now() - BOT_START_TIME, ts: nowIST(),
}));

// ─── TELEGRAM PROXY — used by the P2P iframe ────────────────────────
// The iframe can't talk to Telegram directly (we hold the single poll lock
// and the bot token is server-only). These endpoints mimic the small subset
// of the Bot API the iframe uses.
app.get('/api/tg/config', (_req, res) => {
  if (BACKEND_DISABLED) return res.json({ ok: false, error: 'backend_disabled' });
  res.json({ ok: true, chat_id: String(TG_CHAT) });
});

// Telegram-shaped getUpdates: returns buffered updates with update_id > offset.
app.get('/api/tg/getUpdates', (req, res) => {
  const offset = Number(req.query.offset || 0);
  const result = RECENT_UPDATES
    .filter(u => u.update_id >= offset)
    .map(({ _ts, ...rest }) => rest);
  res.json({ ok: true, result });
});

const TG_PROXY_METHODS = new Set(['sendMessage', 'editMessageText', 'answerCallbackQuery']);
app.post('/api/tg/:method', async (req, res) => {
  if (BACKEND_DISABLED) return res.json({ ok: false, error: 'backend_disabled' });
  const m = req.params.method;
  if (!TG_PROXY_METHODS.has(m)) return res.status(403).json({ ok: false, error: 'method_not_allowed' });
  const body = { ...(req.body || {}) };
  // Force chat_id to the configured admin chat — never trust the client.
  if (m === 'sendMessage' || m === 'editMessageText') body.chat_id = TG_CHAT;
  try {
    const r = await tgFetch(m, body);
    if (r.ok && m === 'sendMessage' && body.reply_markup?.inline_keyboard) {
      log('P2P', `proxied button msgId=${r.result?.message_id || '—'} text="${String(body.text || '').slice(0, 45)}"`);
    }
    if (r.ok && m === 'editMessageText') log('P2P', `proxied edit msgId=${body.message_id || '—'}`);
    res.json(r);
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});


// ─── SPACEMAIL SMTP ─────────────────────────────────────────────────
const SMTP_HOST = process.env.SMTP_HOST || 'mail.spacemail.com';
const SMTP_PORT = Number(process.env.SMTP_PORT) || 465;
const SMTP_USER = process.env.SMTP_USER;
const SMTP_PASS = process.env.SMTP_PASS;
const FROM_NAME = process.env.SMTP_FROM_NAME || 'BIEXC';
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const RESEND_FROM = process.env.RESEND_FROM || (SMTP_USER ? `\"${FROM_NAME}\" <${SMTP_USER}>` : `${FROM_NAME} <onboarding@resend.dev>`);
const MAIL_TIMEOUT_MS = Number(process.env.MAIL_TIMEOUT_MS || 8000);

let mailer = null;
let smtpReady = false;
let smtpDisabledReason = '';
let lastMailError = '';
let lastMailSentAt = '';
let lastMailProvider = '';

if (RESEND_API_KEY) {
  log('MAIL', `📧 Resend HTTP mail ready (${RESEND_FROM})`);
}

if (SMTP_USER && SMTP_PASS) {
  mailer = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: SMTP_PORT === 465, // true for 465 (SSL), false for 587 (TLS)
    auth: { user: SMTP_USER, pass: SMTP_PASS },
    connectionTimeout: MAIL_TIMEOUT_MS,
    greetingTimeout: MAIL_TIMEOUT_MS,
    socketTimeout: MAIL_TIMEOUT_MS
  });
  Promise.race([mailer.verify(), mailTimeout('SMTP verify')])
    .then(() => {
      smtpReady = true;
      log('MAIL', `📧 SMTP ready (${SMTP_USER})`);
    })
    .catch(e => {
      smtpReady = false;
      smtpDisabledReason = e.message;
      if (!RESEND_API_KEY) mailer = null;
      log('MAIL', `⚠️  SMTP disabled: ${e.message}. Add RESEND_API_KEY for reliable mail.`);
    });
} else if (!RESEND_API_KEY) {
  log('MAIL', '⚠️  SMTP_USER / SMTP_PASS or RESEND_API_KEY missing — emails disabled');
}

function mailTimeout(label) {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(`${label} timeout after ${MAIL_TIMEOUT_MS}ms`)), MAIL_TIMEOUT_MS);
  });
}

async function sendViaResend({ to, subject, html, text }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), MAIL_TIMEOUT_MS);
  try {
    const response = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${RESEND_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        from: RESEND_FROM,
        to: [String(to).slice(0, 200)],
        subject: String(subject).slice(0, 200),
        html,
        text
      }),
      signal: controller.signal
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data?.message || data?.error || `Resend HTTP ${response.status}`);
    return { messageId: data?.id || 'resend' };
  } finally {
    clearTimeout(timer);
  }
}

async function sendMailAny({ to, subject, html, text }) {
  if (RESEND_API_KEY) {
    try {
      const info = await sendViaResend({ to, subject, html, text });
      lastMailProvider = 'resend';
      lastMailSentAt = new Date().toISOString();
      lastMailError = '';
      return info;
    } catch (e) {
      lastMailError = `Resend: ${e.message}`;
      if (!mailer || !smtpReady) throw e;
      log('MAIL', `⚠️  Resend failed, trying SMTP fallback: ${e.message}`);
    }
  }
  if (!mailer) throw new Error(RESEND_API_KEY ? 'mail not configured' : 'mail not configured: add RESEND_API_KEY');
  if (!smtpReady && smtpDisabledReason) throw new Error(`SMTP unavailable: ${smtpDisabledReason}`);
  const info = await Promise.race([
    mailer.sendMail({
      from: SMTP_USER ? `\"${FROM_NAME}\" <${SMTP_USER}>` : RESEND_FROM,
      to: String(to).slice(0, 200),
      subject: String(subject).slice(0, 200),
      html,
      text
    }),
    mailTimeout('SMTP send')
  ]);
  lastMailProvider = 'smtp';
  lastMailSentAt = new Date().toISOString();
  lastMailError = '';
  return info;
}

function queueResolutionEmail(ctx) {
  setTimeout(() => {
    sendResolutionEmail(ctx).catch(e => log('MAIL', `async send crashed: ${e.message}`));
  }, 0);
}


function buildResolutionEmail({ type, action, amt, coin, oldBal, newBal }) {
  const label = type === 'dep' ? 'Deposit' : 'Withdrawal';
  const amount = `${r8(amt)} ${coin}`;
  const completed = action === 'approve';
  const status = completed ? 'COMPLETED' : 'REJECTED';
  let subject;
  let message;

  if (type === 'dep' && completed) {
    subject = `✅ Deposit of ${amount} Approved`;
    message = `Your deposit of ${amount} has been verified and credited to your Spot Wallet.

New Balance: ${r8(newBal)} ${coin}

Thank you for using BIEXC.`;
  } else if (type === 'dep') {
    subject = `❌ Deposit of ${amount} Rejected`;
    message = `Your deposit request of ${amount} was rejected by admin.

If you believe this is an error, please contact support at t.me/biexc10.`;
  } else if (completed) {
    subject = `✅ Withdrawal of ${amount} Sent`;
    message = `Your withdrawal of ${amount} has been approved and processed.

Thank you for using BIEXC.`;
  } else {
    subject = `❌ Withdrawal of ${amount} Rejected`;
    message = `Your withdrawal request of ${amount} was rejected.

Your funds have been returned to your Spot Wallet.
New Balance: ${r8(newBal)} ${coin}

Contact support at t.me/biexc10 if you need help.`;
  }

  if (oldBal !== undefined && newBal !== undefined) {
    message += `

Balance: ${r8(oldBal)} → ${r8(newBal)} ${coin}`;
  }
  return { subject, message, amount, status };
}

async function sendResolutionEmail(ctx) {
  const to = String(ctx.user?.email || '').trim();
  const uid = ctx.uid || ctx.user?.uid || '—';
  const label = ctx.type === 'dep' ? 'DEP' : 'WIT';
  if (!to || !to.includes('@')) {
    log('MAIL', `skip ${label} UID=${uid} — user email missing`);
    return false;
  }
  if (!RESEND_API_KEY && !mailer) {
    log('MAIL', `skip ${label} UID=${uid} → ${to} — mail not configured`);
    return false;
  }
  const mail = buildResolutionEmail(ctx);
  try {
    const info = await sendMailAny({
      to,
      subject: mail.subject,
      html: mailHtml(mail.subject, mail.message, mail.amount, mail.status, uid),
      text: `${mail.subject}

${mail.message}

Amount: ${mail.amount}

BIEXC — t.me/biexc10`
    });
    log('MAIL', `✅ sent ${label} ${mail.status} UID=${uid} → ${to} msg=${info.messageId || 'ok'}`);
    return true;
  } catch (e) {
    lastMailError = e.message;
    log('MAIL', `❌ failed ${label} UID=${uid} → ${to}: ${e.message}`);
    return false;
  }
}

function mailHtml(subject, message, amount, status, uid) {
  const safe = (s) => String(s ?? '').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\n/g, '<br>');
  const color = status === 'COMPLETED' ? '#10b981'
              : (status === 'REJECTED' || status === 'CANCELLED') ? '#ef4444'
              : '#f0b90b';
  return `<!doctype html><html><body style="margin:0;background:#0b0e11;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#eaecef;">
  <div style="max-width:560px;margin:0 auto;padding:24px;">
    <div style="text-align:center;padding:16px 0 24px;">
      <div style="font-size:24px;font-weight:800;color:#f0b90b;letter-spacing:1px;">BIEXC</div>
      <div style="font-size:12px;color:#848e9c;margin-top:4px;">PRO P2P Terminal</div>
    </div>
    <div style="background:#181a20;border-radius:12px;padding:24px;border:1px solid #2b3139;">
      <h2 style="margin:0 0 16px;color:${color};font-size:18px;">${safe(subject)}</h2>
      <div style="color:#b7bdc6;line-height:1.6;font-size:14px;">${safe(message)}</div>
      ${amount ? `<div style="margin-top:20px;padding:12px;background:#0b0e11;border-radius:8px;border-left:3px solid ${color};">
        <div style="font-size:11px;color:#848e9c;text-transform:uppercase;letter-spacing:.5px;">Amount</div>
        <div style="font-size:18px;font-weight:700;color:#fff;margin-top:4px;">${safe(amount)}</div>
      </div>` : ''}
      ${uid ? `<div style="margin-top:12px;font-size:11px;color:#5e6673;">UID: ${safe(uid)}</div>` : ''}
    </div>
    <div style="text-align:center;font-size:11px;color:#5e6673;padding:20px 0;">
      Need help? <a href="https://t.me/biexc10" style="color:#f0b90b;text-decoration:none;">t.me/biexc10</a><br>
      © BIEXC. This is an automated message — do not reply.
    </div>
  </div></body></html>`;
}

app.get('/api/mail-status', (_req, res) => {
  res.json({
    ok: true,
    configured: Boolean(RESEND_API_KEY || mailer),
    resendConfigured: Boolean(RESEND_API_KEY),
    resendFrom: RESEND_API_KEY ? RESEND_FROM : null,
    smtpConfigured: Boolean(SMTP_USER && SMTP_PASS),
    smtpHost: SMTP_HOST,
    smtpPort: SMTP_PORT,
    smtpReady,
    smtpDisabledReason,
    lastMailProvider,
    lastMailSentAt,
    lastMailError
  });
});

app.post('/api/test-mail', async (req, res) => {
  try {
    if (!RESEND_API_KEY && !mailer) return res.status(503).json({ ok: false, error: 'mail not configured: add RESEND_API_KEY' });
    const to = String(req.body?.to_email || req.query?.to || SMTP_USER || '').trim();
    if (!to || !to.includes('@')) return res.status(400).json({ ok: false, error: 'to_email required' });
    const info = await sendMailAny({
      to,
      subject: 'BIEXC test mail',
      html: mailHtml('BIEXC test mail', 'Mail service is working now.', '', 'COMPLETED', 'TEST'),
      text: `BIEXC test mail\n\nMail service is working now.`
    });
    log('MAIL', `✅ test sent → ${to} msg=${info.messageId || 'ok'}`);
    res.json({ ok: true, provider: lastMailProvider, messageId: info.messageId || 'ok' });
  } catch (e) {
    lastMailError = e.message;
    log('MAIL', `❌ test failed: ${e.message}`);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/send-mail', async (req, res) => {
  try {
    if (!RESEND_API_KEY && !mailer) return res.status(503).json({ ok: false, error: 'mail not configured' });
    const { to_email, to_name, subject, message, amount, status, uid } = req.body || {};
    if (!to_email || !subject) return res.status(400).json({ ok: false, error: 'to_email & subject required' });
    await sendMailAny({
      to: to_email,
      subject,
      html: mailHtml(subject, message || '', amount || '', status || '', uid || ''),
      text: `${subject}

${message || ''}

${amount ? 'Amount: ' + amount : ''}

BIEXC — t.me/biexc10`
    });
    log('MAIL', `sent → ${to_email} | ${String(subject).slice(0, 60)}`);
    res.json({ ok: true });
  } catch (e) {
    log('MAIL', `send error: ${e.message}`);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/*splat', (_req, res) => {
  if (fs.existsSync(INDEX_FILE)) return res.sendFile(INDEX_FILE);
  res.status(200).type('html').send('<!doctype html><title>BIEXC Bot</title><h1>BIEXC Bot is running</h1>');
});

app.listen(PORT, () => log('HTTP', `🌐 Listening on :${PORT}`));

// Self-ping to keep Render free tier awake
if (RENDER_URL) {
  setInterval(() => {
    fetch(`${RENDER_URL}/health`).catch(()=>{});
  }, 10 * 60 * 1000);
  log('INIT', `🏓 Self-ping enabled → ${RENDER_URL}/health`);
}

// ════════════════════════════════════════════════════════════════════
// BOOT
// ════════════════════════════════════════════════════════════════════
(async () => {
  if (BACKEND_DISABLED) {
    log('INIT', `⚠️ Bot not started because env vars are missing: ${MISSING.join(', ')}`);
    return;
  }
  await claimInstanceLock();
  lastUpdateId = await loadLastUpdateId();
  log('INIT', `📍 Resumed from updateId=${lastUpdateId}`);
  await tgSend(`🟢 *Backend v4.0 ONLINE*\n\n⏰ ${nowIST()} IST\n🔑 Instance: \`${INSTANCE_ID}\`\n\nType /help for commands.`).catch(()=>{});
  pollLoop().catch(e => { log('FATAL', e.message); process.exit(1); });
})();

process.on('uncaughtException',  e => log('UNCAUGHT', e.stack || e.message));
process.on('unhandledRejection', e => log('UNHANDLED', e?.stack || e?.message || String(e)));
