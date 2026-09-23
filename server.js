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
import fetch, { FormData } from 'node-fetch';
import { Blob } from 'buffer';
import nodemailer from 'nodemailer';
import * as WA from './whatsapp.js';
import { mountOtp } from './otp.js';
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
 *  Returns { oldBal, newBal } or null on failure / insufficient funds.
 *  For USDT it ALSO mirrors the confirmed value onto the legacy
 *  `users/{fuid}/balance` node itself — immediately after the transaction
 *  commits, with no other await in between — so no caller can ever forget
 *  the legacy sync and the desync window is as small as physically possible. */
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
  // Legacy mirror — single multi-path update, nothing awaited in between.
  if (COIN === 'USDT') await writeLegacyUsdtMirror(fuid, outcome.newBal);
  return outcome;
}

/** Set a coin balance to an exact value (admin commands).
 *  Also mirrors USDT to the legacy node in the same operation. */
async function setBalance(fuid, coin, value) {
  const COIN = String(coin).toUpperCase();
  const v = r8(value);
  const ref = db.ref(`users/${fuid}/balances/${COIN}`);
  const prev = (await ref.once('value')).val() || 0;
  if (COIN === 'USDT') {
    // One atomic multi-path write for both nodes — they can never disagree.
    await db.ref().update({
      [`users/${fuid}/balances/USDT`]: v,
      [`users/${fuid}/balance`]: v,
    });
  } else {
    await ref.set(v);
  }
  return { oldBal: r8(prev), newBal: v };
}

/** Push one history row. Shape: { hid, ts, date, type, coin, amt, status, ... } */
async function pushHistory(fuid, entry) {
  if (!fuid) return null;
  const ref = db.ref(`users/${fuid}/history`).push();
  const hid = entry.hid || ref.key;
  await ref.set({ hid, ts: Date.now(), date: nowIST(), status: 'COMPLETED', coin: 'USDT', ...entry });
  return hid;
}

/** Flip the status of an existing history row (matched by hid or key). */
async function updateHistoryStatus(fuid, hid, status) {
  if (!fuid || !hid) return;
  try {
    const snap = await db.ref(`users/${fuid}/history`).once('value');
    const jobs = [];
    snap.forEach(c => {
      const v = c.val() || {};
      if (c.key === hid || v.hid === hid) {
        jobs.push(db.ref(`users/${fuid}/history/${c.key}`).update({ status, resolvedAt: Date.now() }));
      }
    });
    await Promise.all(jobs);
  } catch (e) { log('HIST', `status err ${e.message}`); }
}

/** In-app notification for one user — read by the app's bell icon
 *  (users/{fuid}/notifs → { title, body, type, ts, read }). */
async function pushNotif(fuid, { title, body, type = 'INFO' }) {
  if (!fuid) return;
  try {
    await db.ref(`users/${fuid}/notifs`).push({ title, body, type, ts: Date.now(), read: false });
  } catch (e) { log('NOTIF', `err ${e.message}`); }
}

// ─── MESSAGE FORMATTING — every bot message uses this one shape ──────
const LINE = '━━━━━━━━━━━━━━━━━━';
const fmtNum = (n) => r8(n).toLocaleString('en-US', { maximumFractionDigits: 8 });

/** card('📥 TITLE', [['UID','ABC'], 'plain line'], 'footer') */
function card(title, rows = [], foot = '') {
  const body = rows.map(r => Array.isArray(r) ? `${r[0]}: *${r[1]}*` : String(r));
  return [`*${title}*`, LINE, ...body, LINE, foot || `⏰ ${nowIST()} IST`].join('\n');
}
const ok  = (title, rows) => card(`✅ ${title}`, rows);
const bad = (msg) => `❌ ${msg}`;

/** IST midnight for a moment, and its YYYY-MM-DD key. */
const IST_OFF = 5.5 * 3600_000;
function istDayStart(ts = Date.now()) {
  const d = new Date(ts + IST_OFF);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - IST_OFF;
}
const istDayKey = (ts = Date.now()) => new Date(ts + IST_OFF).toISOString().slice(0, 10);

/** Write the legacy `users/{fuid}/balance` mirror via a single multi-path
 *  update. Idempotent — safe to call twice with the same value. */
async function writeLegacyUsdtMirror(fuid, newBal) {
  if (!fuid) return;
  try {
    await db.ref().update({ [`users/${fuid}/balance`]: r8(newBal || 0) });
  } catch (e) { log('LEGACY', `mirror err ${e.message}`); }
}

/** Keep legacy users/{fuid}/balance synced for old frontend screens.
 *  Kept for backwards-compatibility: idempotent and safe to call twice,
 *  since mutateBalance/setBalance already sync USDT themselves. */
async function syncLegacyUsdtBalance(fuid, coin, newBal) {
  if (!fuid || String(coin || '').toUpperCase() !== 'USDT') return;
  await writeLegacyUsdtMirror(fuid, newBal);
}

/** Warn (once per read) when the two balance nodes disagree. */
const LEGACY_EPS = 1e-6;
function warnIfLegacyMismatch(uid, userVal) {
  try {
    if (!userVal) return;
    const legacy = parseFloat(userVal.balance);
    const usdt = parseFloat(userVal.balances?.USDT);
    if (!Number.isFinite(legacy) || !Number.isFinite(usdt)) return;
    if (Math.abs(legacy - usdt) > LEGACY_EPS) {
      log('WARN', `legacy/balances mismatch for ${uid}: legacy=${legacy} vs balances.USDT=${usdt}`);
    }
  } catch (e) {}
}

/** fuids already known to be migrated — avoids re-checking on every event. */
const MIGRATED_FUIDS = new Set();

/** Lazy migration: legacy `balance` (USDT number) → `balances.USDT`. */
async function migrateLegacyBalanceOnce(fuid, userVal) {
  try {
    if (!userVal || !fuid) return;
    if (MIGRATED_FUIDS.has(fuid)) return; // already confirmed — skip entirely
    const hasMap = userVal.balances && typeof userVal.balances === 'object';
    const hasLegacy = userVal.balance !== undefined && userVal.balance !== null;
    if (hasMap && userVal.balances.USDT !== undefined) {
      MIGRATED_FUIDS.add(fuid);
      return;
    }
    if (hasLegacy && (!hasMap || userVal.balances.USDT === undefined)) {
      const v = r8(userVal.balance);
      // Both nodes written together so they start out in sync.
      await db.ref().update({
        [`users/${fuid}/balances/USDT`]: v,
        [`users/${fuid}/balance`]: v,
      });
      MIGRATED_FUIDS.add(fuid);
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
// KYC — block user with un-closeable notice until admin approves
// ════════════════════════════════════════════════════════════════════
const kycSentFor = new Set(); // fuid+ts already forwarded to TG

async function requireKycForUid(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  await db.ref(`users/${found.fuid}`).update({
    kycRequired: true,
    kycStatus: 'REQUIRED',
    kycRequestedAt: Date.now(),
  });
  log('KYC', `required for UID=${uid}`);
  await tgSend(`📝 *KYC REQUIRED*\n\n👤 \`${uid}\`\n📛 ${found.user.name || '—'}\n\nUser will see an un-closeable notice on next app load.`);
}

async function maybeForwardKycSubmission(fuid, user) {
  const sub = user?.kycSubmission;
  if (!sub || typeof sub !== 'object') return;
  if (sub.status !== 'PENDING') return;
  const key = `${fuid}_${sub.ts || 0}`;
  if (kycSentFor.has(key)) return;
  kycSentFor.add(key);
  const uid = user.uid || fuid.slice(0,8);
  const text = [
    '📝 *KYC SUBMISSION — REVIEW*', '',
    `👤 UID: \`${uid}\``,
    `📛 Name: *${sub.name || '—'}*`,
    `🆔 Aadhar: \`${sub.aadhar || '—'}\``,
    `📱 Mobile: ${sub.mobile || '—'}`,
    `📧 Email: ${sub.email || '—'}`,
    `🏠 Address: ${sub.address || '—'}`,
    `🕒 ${sub.ts ? new Date(sub.ts).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}) : ''}`,
  ].join('\n');
  await tgSend(text, { reply_markup: { inline_keyboard: [[
    { text: '✅ Approve KYC', callback_data: `kycapprove_${uid}` },
    { text: '❌ Reject KYC',  callback_data: `kycreject_${uid}` },
  ]] } });
  log('KYC', `submission forwarded UID=${uid}`);
}

async function handleKycDecision(cb, action, uid) {
  await tgAnswer(cb.id, action === 'approve' ? '✅ Approving…' : '❌ Rejecting…');
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  if (action === 'approve') {
    await db.ref(`users/${found.fuid}`).update({
      kycStatus: 'APPROVED',
      kycRequired: false,
      kycApprovedAt: Date.now(),
    });
    await db.ref(`users/${found.fuid}/kycSubmission/status`).set('APPROVED').catch(()=>{});
    await tgSend(`✅ *KYC APPROVED*\n\n👤 \`${uid}\`\n📛 ${found.user.name || '—'}`);
  } else {
    await db.ref(`users/${found.fuid}`).update({
      kycStatus: 'REJECTED',
      kycRejectedAt: Date.now(),
    });
    await db.ref(`users/${found.fuid}/kycSubmission/status`).set('REJECTED').catch(()=>{});
    // clear sent marker so a fresh submission re-notifies admin
    for (const k of Array.from(kycSentFor)) if (k.startsWith(found.fuid+'_')) kycSentFor.delete(k);
    await tgSend(`❌ *KYC REJECTED*\n\n👤 \`${uid}\`\n(Notice remains visible to user.)`);
  }
}


// ════════════════════════════════════════════════════════════════════
// SINGLE-INSTANCE GUARD + PERSISTENT OFFSET
// ════════════════════════════════════════════════════════════════════
const LOCK_STALE_MS = 90_000;   // an instance that stopped beating loses the lock

async function claimInstanceLock() {
  await db.ref('botMeta/activeInstance').set({ id: INSTANCE_ID, ts: Date.now() });
  log('INIT', `🔒 Claimed instance lock`);
}
/** Refresh our heartbeat so no other instance steals the lock while we poll. */
async function beatInstanceLock() {
  try { await db.ref('botMeta/activeInstance').update({ id: INSTANCE_ID, ts: Date.now() }); } catch (e) {}
}
/**
 * We are the active poller when the lock is empty, ours, or abandoned
 * (previous instance died without releasing it — the old code slept forever
 * in that case, which is why the bot answered once and then went silent).
 */
async function amIActive() {
  let v = null;
  try { v = (await db.ref('botMeta/activeInstance').once('value')).val(); }
  catch (e) { return true; }                       // DB hiccup: keep polling
  if (!v || v.id === INSTANCE_ID) return true;
  const age = Date.now() - Number(v.ts || 0);
  if (age > LOCK_STALE_MS) {
    log('POLL', `🔓 stale lock from ${v.id} (${Math.round(age / 1000)}s old) — taking over`);
    await claimInstanceLock();
    return true;
  }
  return false;
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
  const coin = (req.coin || 'USDT').toUpperCase();
  const amt  = parseFloat(req.amt) || 0;
  const bals = user.balances || {};
  const rows = [
    ['👤 UID', `\`${user.uid || '—'}\``],
    ['📛 Name', user.name || '—'],
    ['📧 Email', user.email || '—'],
    ['💵 Amount', `${fmtNum(amt)} ${coin}`],
  ];
  if (req.network || req.chain) rows.push(['🌐 Network', req.network || req.chain]);
  if (req.txid)                 rows.push(['🔗 TxID', `\`${req.txid}\``]);
  if (req.address || req.addr)  rows.push(['📬 Address', `\`${req.address || req.addr}\``]);
  if (req.utr)                  rows.push(['🧾 UTR', `\`${req.utr}\``]);
  rows.push([`💰 Wallet ${coin}`, fmtNum(bals[coin] || (coin === 'USDT' ? user.balance : 0) || 0)]);
  if (coin !== 'USDT') rows.push(['💰 Wallet USDT', fmtNum(bals.USDT || user.balance || 0)]);
  if (coin === 'USDT' && amt >= BIG_AMOUNT) {
    rows.push('', `🚨 *LARGE ${type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL'}* — verify before approving`);
  }
  return card(type === 'dep' ? '📥 DEPOSIT REQUEST' : '📤 WITHDRAWAL REQUEST', rows);
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
    await maybeForwardKycSubmission(snap.key, v).catch(e => log('ERR', `kyc ${e.message}`));

  });
  db.ref('users').on('child_added', async (snap) => {
    if (!(await amIActive())) return;
    const v = snap.val() || {};
    await migrateLegacyBalanceOnce(snap.key, v);
    await processPendingMap(snap.key, v, BOT_START_TIME - 10_000);
    await maybeForwardKycSubmission(snap.key, v).catch(e => log('ERR', `kyc ${e.message}`));
    await alertNewUser(snap.key, v).catch(e => log('ERR', `newuser ${e.message}`));
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
  queueResolutionEmail({ type, action: 'approve', user, uid: user.uid, amt, coin, oldBal: result?.oldBal, newBal: result?.newBal, network: claimed.network || claimed.chain || req?.network || req?.chain, txid: claimed.txid || req?.txid, address: claimed.address || claimed.addr || req?.address || req?.addr, hid: claimed.hid });
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
  queueResolutionEmail({ type, action: 'reject', user, uid: user.uid, amt, coin, oldBal: result?.oldBal, newBal: result?.newBal, network: claimed.network || claimed.chain || req?.network || req?.chain, txid: claimed.txid || req?.txid, address: claimed.address || claimed.addr || req?.address || req?.addr, hid: claimed.hid });
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
  if (chatId !== String(TG_CHAT)) {
    log('CB', `⛔ ignored tap from chat ${chatId} (TG_CHAT=${TG_CHAT})`);
    await tgAnswer(cb.id, 'Unauthorized');
    return;
  }
  const data = cb.data || '';

  let m;
  if ((m = data.match(/^(approve|reject)_(dep_|wit_)(.+)$/))) return handleApproveRejectCb(cb, m[1], m[2] + m[3]);
  // Trade/P2P callbacks (cbId starts with trade_) are handled by the iframe
  // via the buffered /api/tg/getUpdates feed — leave them alone here.
  if (/^(approve|reject)_trade_/.test(data)) return; // no tg-answer, iframe will
  if (/^(join|ignore)_support_/.test(data)) return; // handled by iframe support overlay
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
  if ((m = data.match(/^bcdel_(.+)$/))) {
    await tgAnswer(cb.id, '🗑 Deleting…');
    await handleBroadDelete(m[1]);
    await tgSend(`✅ Broadcast deleted — removed from all user notifications.`);
    return;
  }
  if (data === 'bcclearall') {
    await tgAnswer(cb.id, '🧹 Clearing all…');
    await handleBroadClearAll();
    await tgSend(`✅ *ALL broadcasts cleared* — wiped from every user's notifications.`);
    return;
  }
  if ((m = data.match(/^kyc(approve|reject)_(.+)$/))) return handleKycDecision(cb, m[1], m[2]);
  if ((m = data.match(/^kycreq_(.+)$/)))    { await tgAnswer(cb.id, '📝 KYC required'); return requireKycForUid(m[1]); }
  if ((m = data.match(/^bal_(.+)$/)))       { await tgAnswer(cb.id, '💰 Loading…');     return handleBalances(m[1]); }
  if ((m = data.match(/^msgprompt_(.+)$/))) {
    await tgAnswer(cb.id, 'Send the text');
    await tgSend(`✏️ Reply with:\n\`/msg ${m[1]} your message\``);
    return;
  }
  // ── button dashboard ──────────────────────────────────────────────
  if (data === 'menu_home')    { await tgAnswer(cb.id, '🏠'); return sendMenu(); }
  if (data === 'menu_users')   { await tgAnswer(cb.id, '👥'); return handleUsersList(); }
  if (data === 'menu_stats')   { await tgAnswer(cb.id, '📊'); return handleStats(); }
  if (data === 'menu_pending') { await tgAnswer(cb.id, '⏳'); return handleTrades(); }
  if (data === 'menu_today')   { await tgAnswer(cb.id, '📈'); return sendDailySummary(); }
  if (data === 'menu_broad')   { await tgAnswer(cb.id, '📢'); return handleBroadList(); }
  if (data === 'menu_help')    { await tgAnswer(cb.id, '🧾'); return tgSend(HELP_TEXT); }
  if (data === 'menu_find')    { await tgAnswer(cb.id, '🔍'); return tgSend('🔍 Send `/user UID` — e.g. `/user AB12CD`'); }
  log('CB', `❓ no handler for callback_data="${data}"`);
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
  await tgAnswer(cb.id, action === 'ban' ? '🚫 Banning…' : '✅ Unbanning…');
  await setBanned(uid, action === 'ban');
}

/** Ban / unban a user + tell them in-app. */
async function setBanned(uid, banned) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  await db.ref(`users/${found.fuid}`).update({ banned });
  await pushNotif(found.fuid, banned
    ? { title: 'Account restricted', body: 'Your account has been restricted. Please contact support.', type: 'ALERT' }
    : { title: 'Account restored',   body: 'Your account is active again. Happy trading!', type: 'INFO' });
  log('ADMIN', `${banned ? 'BAN' : 'UNBAN'} UID=${uid}`);
  await tgSend(card(banned ? '🚫 USER BANNED' : '✅ USER UNBANNED', [
    ['👤 UID', `\`${uid}\``],
    ['📛 Name', found.user.name || '—'],
  ]));
}

/** Send an in-app message to one user. */
async function handleAdminMessage(uid, body) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  await pushNotif(found.fuid, { title: 'Message from Support', body, type: 'SUPPORT' });
  log('ADMIN', `MSG → UID=${uid}`);
  await tgSend(ok('MESSAGE DELIVERED', [
    ['👤 UID', `\`${uid}\``],
    ['💬 Text', body.slice(0, 300)],
  ]));
}

// ════════════════════════════════════════════════════════════════════
// ADMIN COMMANDS
// ════════════════════════════════════════════════════════════════════
const HELP_TEXT = [
  '*🧾 BIEXC ADMIN — COMMANDS*', LINE,
  '👥 *Users*',
  '`/users`  ·  `/user UID`  ·  `/history UID`',
  '`/ban UID`  ·  `/unban UID`  ·  `/kyc UID`',
  '`/msg UID <text>` — in-app message to the user',
  '',
  '💰 *Balance*',
  '`/credit UID AMT [COIN]`',
  '`/debit UID AMT [COIN]`',
  '`/setbalance UID COIN AMT`',
  '`/balances UID`  ·  `/convert UID FROM TO AMT`',
  '⚡ shortcut: `#UID AMT` → USDT credit',
  '',
  '🔄 *Trades*',
  '`/trades`  ·  `/cancel ID`',
  '`/closeorder ID`  ·  `/closep2p UID`',
  '',
  '📊 *Platform*',
  '`/stats`  ·  `/today`  ·  `/ping`',
  '`/broadcast MSG`  ·  `/broad`  ·  `/broaddel KEY`  ·  `/broadclear`',
  LINE,
  '🏠 `/menu` — button dashboard',
].join('\n');

const MENU_KB = { inline_keyboard: [
  [{ text: '👥 Users',     callback_data: 'menu_users'   }, { text: '📊 Stats',       callback_data: 'menu_stats' }],
  [{ text: '⏳ Pending',   callback_data: 'menu_pending' }, { text: '📈 Today',       callback_data: 'menu_today' }],
  [{ text: '🔍 Find user', callback_data: 'menu_find'    }, { text: '📢 Broadcasts',  callback_data: 'menu_broad' }],
  [{ text: '🧾 Commands',  callback_data: 'menu_help'    }],
] };

/** Home dashboard — live numbers + buttons, no commands to remember. */
async function sendMenu() {
  const s = await collectStats();
  await tgSend(card('🏠 BIEXC ADMIN DASHBOARD', [
    ['👥 Users', `${s.total}  (🚫 ${s.banned})`],
    ['💰 Total USDT', fmtNum(s.totalUsdt)],
    ['⏳ Pending', `📥 ${s.pendDep}  ·  📤 ${s.pendWit}`],
    ['🔄 Active P2P', s.activeP2P],
  ]), { reply_markup: MENU_KB });
}

async function sendUserDetailCard(cb, uid) {
  if (cb?.id) await tgAnswer(cb.id, 'Loading…');
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  const u = found.user;
  await migrateLegacyBalanceOnce(found.fuid, u);
  warnIfLegacyMismatch(uid, u);

  const balances = u.balances || {};
  const balLines = Object.entries(balances)
    .map(([c, v]) => [c, r8(v)])
    .filter(([, v]) => v > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([c, v]) => `   • ${c}: *${fmtNum(v)}*`);

  const histSnap = await db.ref(`users/${found.fuid}/history`).once('value');
  const histArr = [];
  histSnap.forEach(c => histArr.push(c.val() || {}));
  histArr.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const last3 = histArr.slice(0, 3).map(h => {
    const sign = /WITHDRAW|DEBIT|CONVERT_OUT|SELL/i.test(h.type || '') ? '−' : '+';
    const ico = h.status === 'COMPLETED' ? '✅' : h.status === 'REJECTED' ? '❌' : h.status === 'CANCELLED' ? '🚫' : '⏳';
    return `   ${ico} ${h.type || '—'}  ${sign}${fmtNum(h.amt)} ${h.coin || 'USDT'}`;
  });

  const rows = [
    ['👤 UID', `\`${u.uid || '—'}\``],
    ['📛 Name', u.name || '—'],
    ['📧 Email', u.email || '—'],
    ['📱 Phone', u.phone || '—'],
    ['🔑 Status', u.banned ? '🚫 BANNED' : '✅ Active'],
    ['📝 KYC', u.kycStatus || (u.kycSubmission?.status ? `${u.kycSubmission.status} (submitted)` : '—')],
    ['📅 Joined', u.createdAt ? new Date(u.createdAt).toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata' }) : '—'],
    '', '💰 *Balances*', ...(balLines.length ? balLines : ['   (empty)']),
    '', '📊 *Last 3 transactions*', ...(last3.length ? last3 : ['   (none)']),
  ];

  const buttons = [
    [{ text: '➕ Credit', callback_data: `creditprompt_${uid}` },
     { text: '➖ Debit',  callback_data: `debitprompt_${uid}` }],
    [{ text: '💰 Balances', callback_data: `bal_${uid}` },
     { text: '📋 History',  callback_data: `history_${uid}` }],
    [{ text: '💬 Message', callback_data: `msgprompt_${uid}` },
     { text: '📝 Ask KYC',  callback_data: `kycreq_${uid}` }],
    [ u.banned
        ? { text: '✅ Unban', callback_data: `unban_${uid}` }
        : { text: '🚫 Ban',   callback_data: `ban_${uid}` },
      { text: '🔄 Close P2P', callback_data: `closep2p_${uid}` }],
    [{ text: '↻ Refresh', callback_data: `userdetail_${uid}` },
     { text: '🏠 Menu',   callback_data: 'menu_home' }],
  ];
  await tgSend(card(`👤 USER — ${u.uid || uid}`, rows), { reply_markup: { inline_keyboard: buttons } });
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
    const sign = /WITHDRAW|DEBIT|CONVERT_OUT|SELL/i.test(h.type || '') ? '−' : '+';
    const ico = h.status === 'COMPLETED' ? '✅' : h.status === 'REJECTED' ? '❌' : h.status === 'CANCELLED' ? '🚫' : '⏳';
    const d = h.date || (h.ts ? new Date(h.ts).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) : '');
    const net = h.network ? ` · ${h.network}` : '';
    return `${ico} *${h.type || '—'}*  ${sign}${fmtNum(h.amt)} ${h.coin || 'USDT'}${net}\n    ${d}`;
  });
  await tgSend(card(`📋 HISTORY — ${uid}`, [[`Showing`, `last ${items.length}`], '', ...items]),
    { reply_markup: { inline_keyboard: [[{ text: '👤 User card', callback_data: `userdetail_${uid}` }, { text: '🏠 Menu', callback_data: 'menu_home' }]] } });
}

async function handleUsersList() {
  const snap = await db.ref('users').once('value');
  const users = [];
  snap.forEach(c => { const u = c.val(); if (u && u.uid) users.push(u); });
  if (!users.length) { await tgSend(bad('No users yet.')); return; }
  users.sort((a, b) => r8((b.balances || {}).USDT || b.balance || 0) - r8((a.balances || {}).USDT || a.balance || 0));
  await tgSend(card('👥 ALL USERS', [['Total', users.length], ['Sorted by', 'USDT balance']]));
  const CHUNK = 8;
  for (let i = 0; i < users.length; i += CHUNK) {
    const slice = users.slice(i, i + CHUNK);
    const lines = slice.map(u => {
      const st = u.banned ? '🚫' : '✅';
      const usdt = fmtNum((u.balances || {}).USDT || u.balance || 0);
      return `${st} \`${u.uid}\`  *${u.name || '—'}*\n     💰 ${usdt} USDT`;
    });
    const keyboard = slice.map(u => [{ text: `👁 ${u.uid} — ${u.name || '—'}`.slice(0, 60), callback_data: `userdetail_${u.uid}` }]);
    keyboard.push([{ text: '🏠 Menu', callback_data: 'menu_home' }]);
    await tgSend(lines.join('\n'), { reply_markup: { inline_keyboard: keyboard } });
  }
}

/** One pass over `users` → every number the dashboard / stats / summary need. */
async function collectStats() {
  const snap = await db.ref('users').once('value');
  const s = {
    total: 0, banned: 0, totalUsdt: 0, pendDep: 0, pendWit: 0, pendDepAmt: 0, pendWitAmt: 0,
    activeP2P: 0, newToday: 0, depToday: 0, depTodayAmt: 0, witToday: 0, witTodayAmt: 0, kycPending: 0,
  };
  const dayStart = istDayStart();
  snap.forEach(c => {
    const u = c.val() || {}; if (!u.uid) return;
    s.total++; if (u.banned) s.banned++;
    s.totalUsdt += r8((u.balances || {}).USDT || u.balance || 0);
    if ((u.createdAt || 0) >= dayStart) s.newToday++;
    if ((u.kycSubmission && !u.kycSubmission.status) || u.kycStatus === 'PENDING') s.kycPending++;
    const reqs = u.pendingReqs || {};
    for (const [kind, key] of [['dep', 'Dep'], ['wit', 'Wit']]) {
      for (const [k, v] of Object.entries(reqs[kind] || {})) {
        if (k === 'botLock' || !v) continue;
        s[`pend${key}`]++;
        s[`pend${key}Amt`] += r8(v.amt);
      }
    }
    for (const o of Object.values(u.orders || {})) {
      if (o?.status && !['completed', 'cancelled', 'canceled'].includes(String(o.status).toLowerCase())) s.activeP2P++;
    }
    for (const h of Object.values(u.history || {})) {
      if (!h || (h.ts || 0) < dayStart || h.status !== 'COMPLETED') continue;
      if (/DEPOSIT/i.test(h.type || ''))  { s.depToday++; s.depTodayAmt += r8(h.amt); }
      if (/WITHDRAW/i.test(h.type || '')) { s.witToday++; s.witTodayAmt += r8(h.amt); }
    }
  });
  return s;
}

async function handleStats() {
  const s = await collectStats();
  const upMs = Date.now() - BOT_START_TIME;
  await tgSend(card('📊 PLATFORM STATS', [
    ['👥 Users', `${s.total}  (✅ ${s.total - s.banned} · 🚫 ${s.banned})`],
    ['🆕 New today', s.newToday],
    ['💰 Total USDT', fmtNum(s.totalUsdt)],
    ['📥 Pending deposits', `${s.pendDep}  (${fmtNum(s.pendDepAmt)} USDT)`],
    ['📤 Pending withdrawals', `${s.pendWit}  (${fmtNum(s.pendWitAmt)} USDT)`],
    ['📝 KYC waiting', s.kycPending],
    ['🔄 Active P2P', s.activeP2P],
    ['⏰ Uptime', `${Math.floor(upMs / 3600000)}h ${Math.floor((upMs % 3600000) / 60000)}m`],
    ['🔑 Instance', `\`${INSTANCE_ID}\``],
  ]), { reply_markup: { inline_keyboard: [[
    { text: '↻ Refresh', callback_data: 'menu_stats' }, { text: '🏠 Menu', callback_data: 'menu_home' },
  ]] } });
}

// ── Daily summary + live alerts ──────────────────────────────────────
const BIG_AMOUNT = parseFloat(process.env.BIG_AMOUNT || '1000');
let lastSummaryDay = null;
const alertedUsers = new Set();

/** Today's activity at a glance (also sent automatically at 9am IST). */
async function sendDailySummary() {
  const s = await collectStats();
  await tgSend(card(`📈 TODAY — ${istDayKey()}`, [
    ['🆕 New users', s.newToday],
    ['📥 Deposits', `${s.depToday}  (${fmtNum(s.depTodayAmt)} USDT)`],
    ['📤 Withdrawals', `${s.witToday}  (${fmtNum(s.witTodayAmt)} USDT)`],
    ['📊 Net flow', `${fmtNum(s.depTodayAmt - s.witTodayAmt)} USDT`],
    ['⏳ Still pending', `📥 ${s.pendDep} · 📤 ${s.pendWit}`],
    ['💰 Total USDT held', fmtNum(s.totalUsdt)],
    ['👥 Users', s.total],
  ]), { reply_markup: { inline_keyboard: [[
    { text: '⏳ Pending', callback_data: 'menu_pending' }, { text: '🏠 Menu', callback_data: 'menu_home' },
  ]] } });
}

/** Fire the summary once a day, shortly after 9am IST. */
async function dailySummaryTick() {
  try {
    const now = Date.now();
    const key = istDayKey(now);
    const hourIST = new Date(now + IST_OFF).getUTCHours();
    if (hourIST < 9 || lastSummaryDay === key) return;
    lastSummaryDay = key;
    if (!(await amIActive())) return;
    await sendDailySummary();
  } catch (e) { log('ERR', `summary ${e.message}`); }
}

/** Ping the admin when a brand-new account signs up. */
async function alertNewUser(fuid, u) {
  if (!u?.uid || !u.createdAt) return;
  if (u.createdAt < BOT_START_TIME || alertedUsers.has(fuid)) return;
  alertedUsers.add(fuid);
  await tgSend(card('🆕 NEW USER SIGNED UP', [
    ['👤 UID', `\`${u.uid}\``],
    ['📛 Name', u.name || '—'],
    ['📧 Email', u.email || '—'],
    ['📱 Phone', u.phone || '—'],
  ]), { reply_markup: { inline_keyboard: [[{ text: '👁 Open user', callback_data: `userdetail_${u.uid}` }]] } });
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
  warnIfLegacyMismatch(uid, found.user);
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
  if (!message) { await tgSend(bad('Usage: `/broadcast <message>`')); return; }
  const snap = await db.ref('users').once('value');
  let count = 0;
  snap.forEach(c => { if (c.val()?.uid) count++; });
  const entry = { text: message, ts: Date.now(), from: 'admin', date: nowIST() };
  // Write both node names — the app reads `broadcasts`, older builds read `broadcast`.
  const key = db.ref('broadcasts').push().key;
  await db.ref().update({ [`broadcasts/${key}`]: entry, [`broadcast/${key}`]: entry });
  log('BROADCAST', `to ${count} users: "${message}"`);
  await tgSend(ok('BROADCAST SENT', [
    ['👥 Reach', `${count} users`],
    ['💬 Message', message.slice(0, 300)],
  ]));
}

// ── /broad — list all broadcasts with delete buttons ───────────────
async function handleBroadList() {
  const snap = await db.ref('broadcast').once('value');
  if (!snap.exists()) { await tgSend('📭 No broadcasts yet.'); return; }
  const items = [];
  snap.forEach(c => { items.push({ key: c.key, ...c.val() }); });
  items.sort((a,b) => (b.ts||0) - (a.ts||0));
  const top = items.slice(0, 20);
  const lines = [`📢 *BROADCASTS* (${items.length} total, showing ${top.length})`, ''];
  const keyboard = [];
  top.forEach((b, i) => {
    const when = b.date || new Date(b.ts||0).toLocaleString('en-IN');
    const txt = (b.text || '').slice(0, 80);
    lines.push(`*${i+1}.* _${when}_\n💬 ${txt}`);
    keyboard.push([{ text: `🗑 Delete #${i+1}`, callback_data: `bcdel_${b.key}` }]);
  });
  keyboard.push([{ text: '🧹 Clear ALL broadcasts', callback_data: 'bcclearall' }]);
  await tgSend(lines.join('\n\n'), { reply_markup: { inline_keyboard: keyboard } });
}

async function handleBroadDelete(key) {
  await db.ref().update({ [`broadcast/${key}`]: null, [`broadcasts/${key}`]: null });
  log('BROADCAST', `deleted ${key}`);
}

async function handleBroadClearAll() {
  await db.ref().update({ broadcast: null, broadcasts: null });
  log('BROADCAST', 'cleared all');
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

  if (text === '/start' || text === '/menu')  return sendMenu();
  if (text === '/help')   return tgSend(HELP_TEXT);
  if (text === '/ping')   return tgSend(card('🟢 BOT ONLINE', [
    ['⏱ Uptime', `${Math.floor((Date.now() - BOT_START_TIME) / 60000)}m`],
    ['🔑 Instance', `\`${INSTANCE_ID}\``],
  ]));
  if (text === '/users')  return handleUsersList();
  if (text === '/trades' || text === '/pending') return handleTrades();
  if (text === '/stats')  return handleStats();
  if (text === '/today' || text === '/summary')  return sendDailySummary();

  let m;
  if ((m = text.match(/^\/(?:kyc|uid)\s+([A-Z0-9]{2,15})$/i)))             return requireKycForUid(m[1].toUpperCase());
  if ((m = text.match(/^\/user\s+([A-Z0-9]{2,15})$/i)))                   return sendUserDetailCard(null, m[1].toUpperCase());
  if ((m = text.match(/^\/msg\s+([A-Z0-9]{2,15})\s+([\s\S]+)$/i)))        return handleAdminMessage(m[1].toUpperCase(), m[2].trim());
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
  if ((m = text.match(/^\/ban\s+([A-Z0-9]{2,15})$/i)))      return setBanned(m[1].toUpperCase(), true);
  if ((m = text.match(/^\/unban\s+([A-Z0-9]{2,15})$/i)))    return setBanned(m[1].toUpperCase(), false);
  if ((m = text.match(/^\/cancel\s+(\S+)$/i)))              return handleCancel(m[1]);
  if ((m = text.match(/^\/closeorder\s+(\S+)$/i)))          return handleCloseOrder(m[1]);
  if ((m = text.match(/^\/closep2p\s+([A-Z0-9]{2,15})$/i))) return closeAllP2PForUser(m[1].toUpperCase());
  if ((m = text.match(/^\/broadcast\s+([\s\S]+)$/i)))       return handleBroadcast(m[1].trim());
  if (text === '/broad' || text === '/broadlist')           return handleBroadList();
  if (text === '/broadclear')                               return handleBroadClearAll().then(() => tgSend(ok('ALL BROADCASTS CLEARED', [])));
  if ((m = text.match(/^\/broaddel\s+(\S+)$/i)))            return handleBroadDelete(m[1]).then(() => tgSend(ok('BROADCAST DELETED', [['🔑 Key', `\`${m[1]}\``]])));
  if ((m = text.match(/^#([A-Z0-9]{2,15})\s+([\d.]+)$/i)))  return adminCreditDebit(m[1].toUpperCase(), parseFloat(m[2]), '+', 'ADMIN_CREDIT', 'USDT');
  if (text.startsWith('/')) return tgSend(bad('Unknown command — tap a button below or send /help.'), { reply_markup: MENU_KB });
}

// ════════════════════════════════════════════════════════════════════
// POLLING
// ════════════════════════════════════════════════════════════════════
let lastUpdateId = 0;
let lastPollOkAt = Date.now();
let pollFailStreak = 0;

/** A webhook and getUpdates cannot coexist: Telegram then answers every
 *  getUpdates with 409 and the bot goes permanently silent. */
async function dropWebhook(reason) {
  try {
    const r = await fetch(`${TG_API}/deleteWebhook?drop_pending_updates=false`).then(x => x.json());
    log('POLL', `🔌 deleteWebhook (${reason}) → ${r && r.ok ? 'ok' : JSON.stringify(r)}`);
  } catch (e) { log('POLL', `deleteWebhook failed: ${e.message}`); }
}

async function pollUpdates() {
  try {
    if (!(await amIActive())) {
      log('POLL', '⏸️ another instance active — sleeping 15s');
      await new Promise(r => setTimeout(r, 15_000));
      return;
    }
    await beatInstanceLock();
    const res = await fetch(`${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=25&allowed_updates=${encodeURIComponent('["callback_query","message"]')}`);
    const data = await res.json().catch(() => null);

    if (!data || !data.ok) {
      pollFailStreak++;
      const code = data && data.error_code;
      const desc = (data && data.description) || 'no response';
      log('POLL', `⚠️ getUpdates failed (${code || '—'}): ${desc}`);
      // 409 = webhook set or a second getUpdates consumer. Both are fatal to
      // button taps, so clear the webhook and retake the lock, then retry.
      if (code === 409 || /conflict|webhook/i.test(desc)) {
        await dropWebhook('409 conflict');
        await claimInstanceLock();
      }
      if (code === 401 || code === 404) {
        log('POLL', '🛑 bot token rejected by Telegram — check TG_BOT_TOKEN');
      }
      await new Promise(r => setTimeout(r, Math.min(15_000, 1000 * pollFailStreak)));
      return;
    }

    pollFailStreak = 0;
    lastPollOkAt = Date.now();
    for (const upd of (data.result || [])) {
      lastUpdateId = upd.update_id;
      await saveLastUpdateId(lastUpdateId).catch(()=>{});
      pushRecentUpdate(upd);
      try { await handleUpdate(upd); } catch (e) { log('ERR', `handleUpdate: ${e.message}`); }
    }
  } catch (e) {
    pollFailStreak++;
    log('POLL', `err ${e.message}`);
    await new Promise(r => setTimeout(r, Math.min(10_000, 1000 * pollFailStreak)));
  }
}

async function pollLoop() {
  while (true) {
    await pollUpdates();
    await new Promise(r => setTimeout(r, 300));
  }
}

/** Watchdog: if no successful poll for 3 minutes, force the bot back online. */
function startPollWatchdog() {
  setInterval(async () => {
    if (Date.now() - lastPollOkAt < 180_000) return;
    log('POLL', '🚑 no successful poll for 3m — reclaiming lock + clearing webhook');
    lastPollOkAt = Date.now();
    await dropWebhook('watchdog');
    await claimInstanceLock().catch(() => {});
  }, 60_000);
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
app.use(express.json({ limit: '15mb' }));

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
app.post('/api/tg/:method', async (req, res, next) => {
  if (BACKEND_DISABLED) return res.json({ ok: false, error: 'backend_disabled' });
  const m = req.params.method;
  // Let the dedicated multipart photo route below handle image uploads.
  // Without this, /api/tg/:method catches /api/tg/sendPhotoBase64 first
  // and returns method_not_allowed before Telegram receives the photo.
  if (m === 'sendPhotoBase64') return next();
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


// ─── PHOTO: user -> admin ───────────────────────────────────────────
// Client sends base64 image; we forward to Telegram as multipart sendPhoto.
app.post('/api/tg/sendPhotoBase64', async (req, res) => {
  if (BACKEND_DISABLED) return res.json({ ok: false, error: 'backend_disabled' });
  try {
    const { imageBase64, caption, reply_to_message_id } = req.body || {};
    if (!imageBase64 || typeof imageBase64 !== 'string') {
      return res.status(400).json({ ok: false, error: 'missing_image' });
    }
    const b64 = imageBase64.replace(/^data:image\/\w+;base64,/, '');
    const buf = Buffer.from(b64, 'base64');
    if (buf.length > 9 * 1024 * 1024) {
      return res.status(413).json({ ok: false, error: 'image_too_large' });
    }
    const fd = new FormData();
    fd.append('chat_id', String(TG_CHAT));
    if (caption) fd.append('caption', String(caption).slice(0, 1024));
    if (reply_to_message_id) fd.append('reply_to_message_id', String(reply_to_message_id));
    const blob = new Blob([buf], { type: 'image/jpeg' });
    fd.append('photo', blob, 'photo.jpg');
    const r = await fetch(`${TG_API}/sendPhoto`, { method: 'POST', body: fd });
    const j = await r.json();
    if (j.ok) log('P2P', `photo sent msgId=${j.result?.message_id || '—'} size=${buf.length}b`);
    res.json(j);
  } catch (e) {
    log('P2P', 'sendPhotoBase64 error: ' + e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── PHOTO: admin -> user ───────────────────────────────────────────
// Client polls updates and, on photo msg, calls this to pull the file as base64.
app.get('/api/tg/fetchFile', async (req, res) => {
  if (BACKEND_DISABLED) return res.json({ ok: false, error: 'backend_disabled' });
  try {
    const fileId = String(req.query.file_id || '');
    if (!fileId) return res.status(400).json({ ok: false, error: 'missing_file_id' });
    const gf = await fetch(`${TG_API}/getFile?file_id=${encodeURIComponent(fileId)}`);
    const gj = await gf.json();
    if (!gj.ok) return res.status(502).json(gj);
    const path = gj.result.file_path;
    const dl = await fetch(`https://api.telegram.org/file/bot${TG_TOKEN}/${path}`);
    if (!dl.ok) return res.status(502).json({ ok: false, error: 'download_failed' });
    const ab = await dl.arrayBuffer();
    const b64 = Buffer.from(ab).toString('base64');
    res.json({ ok: true, imageBase64: 'data:image/jpeg;base64,' + b64 });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
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


function buildResolutionEmail(ctx) {
  const { type, action, amt, coin, oldBal, newBal, txid, network, address, hid, uid } = ctx;
  const label = type === 'dep' ? 'Deposit' : 'Withdrawal';
  const amount = `${r8(amt)} ${coin}`;
  const completed = action === 'approve';
  const status = completed ? 'COMPLETED' : 'CANCELLED';
  let subject;
  let message;

  if (type === 'dep' && completed) {
    subject = `Deposit Completed — ${amount}`;
    message = `Your deposit has been verified on-chain and credited to your Spot Wallet. Funds are available for trading immediately.`;
  } else if (type === 'dep') {
    subject = `Deposit Cancelled — ${amount}`;
    message = `Your deposit request could not be processed and has been cancelled. No funds have been credited. If you believe this is an error, please contact our support team.`;
  } else if (completed) {
    subject = `Withdrawal Completed — ${amount}`;
    message = `Your withdrawal has been broadcast to the network and is on its way to the destination address. Confirmation time depends on network congestion.`;
  } else {
    subject = `Withdrawal Cancelled — ${amount}`;
    message = `Your withdrawal request has been cancelled and the held funds have been returned to your Spot Wallet.`;
  }
  return { subject, message, amount, status, label, oldBal, newBal, coin, txid, network, address, hid, uid, type, action };
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
      html: mailHtml({ ...mail, uid }),
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

function mailHtml(opts) {
  const o = opts || {};
  const subject = o.subject || '';
  const message = o.message || '';
  const amount  = o.amount  || '';
  const status  = o.status  || '';
  const uid     = o.uid     || '';
  const label   = o.label   || '';
  const coin    = o.coin    || '';
  const network = o.network || '';
  const address = o.address || '';
  const txid    = o.txid    || o.txnId || '';
  const hid     = o.hid     || '';
  const type    = o.type    || '';
  const oldBal  = o.oldBal;
  const newBal  = o.newBal;

  const esc = (v) => String(v == null ? '' : v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const nl2br = (v) => esc(v).replace(/\n/g, '<br>');

  const isOk  = status === 'COMPLETED';
  const isBad = status === 'CANCELLED' || status === 'REJECTED';
  const accent   = isOk ? '#0ECB81' : isBad ? '#F6465D' : '#F0B90B';
  const accentBg = isOk ? 'rgba(14,203,129,.10)' : isBad ? 'rgba(246,70,93,.10)' : 'rgba(240,185,11,.10)';
  const badge    = isOk ? 'COMPLETED' : isBad ? 'CANCELLED' : (status || 'PENDING');
  const verb     = isOk ? 'Completed' : isBad ? 'Cancelled' : 'Update';
  const headline = label ? (label + ' ' + verb) : (subject || 'Transaction Update');
  const sign     = type === 'dep' ? '+' : '−';
  const direction = type === 'dep' ? 'Incoming' : 'Outgoing';

  const now = new Date();
  const istStr = now.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', day:'2-digit', month:'short', year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true });
  const utcStr = now.toISOString().replace('T',' ').slice(0,16) + ' UTC';
  const refNo  = 'BIEXC-' + (hid ? String(hid).slice(-8).toUpperCase() : (txid ? String(txid).slice(-8).toUpperCase() : Math.random().toString(36).slice(2,10).toUpperCase()));

  const det = (k, v, mono) => '<tr><td class="det-k" style="padding:11px 0;color:#8B95A7;font-size:12px;line-height:1.4;width:38%;vertical-align:top;letter-spacing:.2px;">' + esc(k) + '</td><td class="det-v" style="padding:11px 0;color:#EAECEF;font-size:13px;line-height:1.4;text-align:right;font-weight:600;' + (mono ? "font-family:'SF Mono',Menlo,Consolas,monospace;letter-spacing:-.2px;" : '') + 'word-break:break-all;">' + v + '</td></tr>';

  const sec = (t) => '<tr><td colspan="2" style="padding:18px 0 6px;border-top:1px solid #1c222b;"><div style="font-size:10px;color:#5E6673;letter-spacing:2.5px;font-weight:700;text-transform:uppercase;">' + esc(t) + '</div></td></tr>';

  const node = (n, txt, active) => '<td align="center" width="33.33%" style="padding:0 4px;"><div style="width:28px;height:28px;line-height:28px;margin:0 auto;border-radius:50%;background:' + (active?accent:'#1c222b') + ';color:' + (active?'#0a0d12':'#5E6673') + ';font-size:12px;font-weight:800;border:2px solid ' + (active?accent:'#1c222b') + ';">' + n + '</div><div style="margin-top:8px;font-size:10px;letter-spacing:1.2px;color:' + (active?'#EAECEF':'#5E6673') + ';text-transform:uppercase;font-weight:600;">' + esc(txt) + '</div></td>';

  const logo = '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;"><tr><td align="center" style="width:64px;height:64px;border-radius:50%;background:radial-gradient(circle at 35% 30%, #2a2a2a 0%, #0a0a0a 75%);border:2px solid #F0B90B;box-shadow:0 0 0 4px rgba(240,185,11,.06), 0 6px 18px rgba(240,185,11,.18);font:800 20px/64px -apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#F0B90B;letter-spacing:1.5px;text-align:center;">BN</td></tr></table>';

  const stage3Active = isOk;
  const stage2Label = type === 'wit' ? 'Broadcast' : 'Verified';
  const stage3Label = isBad ? 'Cancelled' : (type === 'wit' ? 'Sent' : 'Credited');

  const addrBlock = address ? '<tr><td style="padding:14px 16px;background:#06080b;border:1px solid #1c222b;border-radius:10px;"><div style="font-size:10px;color:#5E6673;letter-spacing:2px;font-weight:700;text-transform:uppercase;margin-bottom:6px;">' + (type==='wit'?'Destination Address':'Sender Address') + '</div><div style="font-family:\'SF Mono\',Menlo,Consolas,monospace;font-size:12px;color:#EAECEF;word-break:break-all;line-height:1.5;">' + esc(address) + '</div></td></tr><tr><td style="height:10px;line-height:10px;font-size:0;">&nbsp;</td></tr>' : '';

  const txidBlock = txid ? '<tr><td style="padding:14px 16px;background:#06080b;border:1px solid #1c222b;border-radius:10px;"><div style="font-size:10px;color:#5E6673;letter-spacing:2px;font-weight:700;text-transform:uppercase;margin-bottom:6px;">Transaction Hash</div><div style="font-family:\'SF Mono\',Menlo,Consolas,monospace;font-size:12px;color:#EAECEF;word-break:break-all;line-height:1.5;">' + esc(txid) + '</div></td></tr><tr><td style="height:10px;line-height:10px;font-size:0;">&nbsp;</td></tr>' : '';

  const balDelta = (oldBal !== undefined && newBal !== undefined) ? (Number(newBal) - Number(oldBal)) : null;
  const deltaStr = balDelta === null ? '' : '<span style="color:' + (balDelta>=0?'#0ECB81':'#F6465D') + ';">' + (balDelta>=0?'+':'') + r8(balDelta) + ' ' + esc(coin) + '</span>';

  const stepperFill = stage3Active ? 100 : (isBad ? 100 : 66);

  let html = '';
  html += '<!doctype html><html lang="en"><head>';
  html += '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
  html += '<meta name="color-scheme" content="dark only"><meta name="supported-color-schemes" content="dark only">';
  html += '<title>' + esc(subject) + '</title>';
  html += '<style>@media (max-width:520px){.wrap{padding:14px 6px !important;}.card{padding:22px 14px !important;border-radius:14px !important;}.hero-amt{font-size:22px !important;letter-spacing:-.4px !important;}.h1{font-size:20px !important;}.det-k{width:44% !important;font-size:11px !important;}.det-v{font-size:12px !important;}.stage-lbl{font-size:9px !important;}}a{color:#F0B90B;text-decoration:none;}</style>';
  html += '</head><body style="margin:0;padding:0;background:#06080b;font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,\'Helvetica Neue\',Arial,sans-serif;color:#EAECEF;-webkit-font-smoothing:antialiased;">';
  html += '<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:#06080b;">' + esc(headline) + ' · ' + esc(amount) + ' · Ref ' + refNo + '</div>';
  html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#06080b;"><tr><td align="center" class="wrap" style="padding:30px 16px;">';
  html += '<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px;width:100%;">';

  // Brand
  html += '<tr><td align="center" style="padding:4px 0 22px;">' + logo;
  html += '<div style="margin-top:12px;font-size:18px;font-weight:800;color:#F0B90B;letter-spacing:4px;">BIEXC</div>';
  html += '<div style="margin-top:3px;font-size:10px;color:#5E6673;letter-spacing:2.5px;text-transform:uppercase;font-weight:600;">Pro Trading Terminal</div></td></tr>';

  // Card
  html += '<tr><td class="card" style="background:linear-gradient(180deg,#10151d 0%,#0c1117 100%);border:1px solid #1c222b;border-radius:16px;padding:32px 30px;">';

  // Ribbon
  html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:22px;"><tr>';
  html += '<td align="left" style="vertical-align:middle;"><span style="display:inline-block;background:' + accentBg + ';color:' + accent + ';padding:5px 12px;border-radius:999px;font-size:10px;font-weight:800;letter-spacing:1.8px;border:1px solid ' + accent + ';">● ' + badge + '</span></td>';
  html += '<td align="right" style="vertical-align:middle;"><span style="font-family:\'SF Mono\',Menlo,Consolas,monospace;font-size:11px;color:#5E6673;letter-spacing:.5px;">Ref · ' + refNo + '</span></td>';
  html += '</tr></table>';

  // Headline
  html += '<h1 class="h1" style="margin:0 0 8px;font-size:23px;font-weight:700;color:#FFFFFF;letter-spacing:-.3px;line-height:1.25;">' + esc(headline) + '</h1>';
  html += '<p style="margin:0 0 26px;font-size:13.5px;line-height:1.6;color:#B7BDC6;">' + nl2br(message) + '</p>';

  // Amount hero
  if (amount) {
    html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 24px;"><tr><td style="background:#06080b;border:1px solid #1c222b;border-left:3px solid ' + accent + ';border-radius:12px;padding:22px 22px;">';
    html += '<div style="font-size:10px;color:#5E6673;letter-spacing:2.2px;text-transform:uppercase;font-weight:700;">' + esc(direction) + ' ' + esc(label) + '</div>';
    html += '<div class="hero-amt" style="margin-top:8px;font-size:34px;font-weight:700;color:#FFFFFF;letter-spacing:-.8px;font-family:\'SF Mono\',Menlo,Consolas,monospace;line-height:1.1;">';
    html += '<span style="color:' + accent + ';">' + sign + '</span> ' + esc(amount);
    if (network) html += ' <span style="font-size:13px;color:#5E6673;font-weight:600;letter-spacing:1px;text-transform:uppercase;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;">· ' + esc(network) + '</span>';
    html += '</div></td></tr></table>';
  }

  // Stepper
  html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 22px;"><tr><td>';
  html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>';
  html += node('1','Requested',true);
  html += node('2',stage2Label,true);
  html += node('3',stage3Label,stage3Active || isBad);
  html += '</tr><tr><td colspan="3" style="padding:10px 14px 0;">';
  html += '<div style="height:2px;background:linear-gradient(to right, ' + accent + ' 0%, ' + accent + ' ' + stepperFill + '%, #1c222b ' + stepperFill + '%, #1c222b 100%);border-radius:2px;"></div>';
  html += '</td></tr></table></td></tr></table>';

  // Address & TXID blocks
  if (address || txid) {
    html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">';
    html += addrBlock + txidBlock;
    html += '</table>';
  }

  // Details
  html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">';
  html += sec('Transaction');
  if (label) html += det('Type', esc(direction) + ' ' + esc(label));
  if (coin) html += det('Asset', esc(coin));
  if (network) html += det('Network', esc(network));
  if (amount) html += det('Amount', '<span style="color:' + accent + ';">' + sign + '</span> ' + esc(amount), true);

  if (oldBal !== undefined && newBal !== undefined) {
    html += sec('Wallet Balance');
    html += det('Before', r8(oldBal) + ' ' + esc(coin), true);
    html += det('After',  r8(newBal) + ' ' + esc(coin), true);
    if (balDelta !== null) html += det('Net Change', deltaStr, true);
  }

  html += sec('System');
  if (uid) html += det('Account UID', esc(uid), true);
  html += det('Reference', refNo, true);
  html += det('Status', '<span style="color:' + accent + ';">● ' + badge + '</span>');
  html += det('Time (IST)', esc(istStr));
  html += det('Time (UTC)', esc(utcStr));
  html += '</table>';

  // Security
  html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:26px;"><tr><td style="background:rgba(240,185,11,.04);border:1px solid rgba(240,185,11,.20);border-radius:10px;padding:14px 16px;">';
  html += '<div style="font-size:12px;color:#B7BDC6;line-height:1.6;"><span style="color:#F0B90B;font-weight:700;">🔒 Security · </span>BIEXC will never ask for your password, 2FA code, recovery phrase, or private keys. All transactions are final and recorded on-chain.</div>';
  html += '</td></tr></table>';

  // CTAs
  html += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:24px 0 0;"><tr><td align="center" style="padding:0 4px 8px;">';
  html += '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;"><tr>';
  html += '<td style="background:linear-gradient(135deg,#F0B90B 0%,#F8D12F 100%);border-radius:10px;"><a href="https://t.me/biexc10" style="display:inline-block;padding:13px 24px;font-size:12.5px;font-weight:800;color:#0a0d12;text-decoration:none;letter-spacing:.4px;">Contact Support →</a></td>';
  html += '<td style="width:10px;font-size:0;line-height:0;">&nbsp;</td>';
  html += '<td style="background:transparent;border:1px solid #2b3139;border-radius:10px;"><a href="https://t.me/biexc10" style="display:inline-block;padding:12px 22px;font-size:12.5px;font-weight:700;color:#EAECEF;text-decoration:none;letter-spacing:.4px;">View History</a></td>';
  html += '</tr></table></td></tr></table>';

  html += '</td></tr>';

  // Footer
  html += '<tr><td align="center" style="padding:24px 16px 8px;"><div style="font-size:11px;color:#5E6673;line-height:1.8;">';
  html += 'This is an automated transaction notification — please do not reply.<br>';
  html += 'Need help? <a href="https://t.me/biexc10">t.me/biexc10</a> · Telegram: <a href="https://t.me/biexc10">@biexc10</a><br>';
  html += '© ' + now.getFullYear() + ' BIEXC · Pro Trading Terminal · All rights reserved.';
  html += '</div></td></tr>';

  html += '</table></td></tr></table></body></html>';
  return html;
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
      html: mailHtml({ subject: 'BIEXC test mail', message: 'Mail service is working now.', amount: '0 USDT', status: 'COMPLETED', uid: 'TEST', label: 'Deposit', coin: 'USDT', type: 'dep', action: 'approve' }),
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
      html: mailHtml({ subject, message: message || '', amount: amount || '', status: status || '', uid: uid || '', label: '', coin: '' }),
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

// ─── WHATSAPP OTP LOGIN (QR page, /api/otp/send, /api/otp/verify) ───
mountOtp(app, { admin, db, log, adminKey: process.env.WA_ADMIN_KEY || '' });

app.get('/*splat', (_req, res) => {
  if (fs.existsSync(INDEX_FILE)) return res.sendFile(INDEX_FILE);
  res.status(200).type('html').send('<!doctype html><title>BIEXC Bot</title><h1>BIEXC Bot is running</h1>');
});

app.listen(PORT, () => log('HTTP', `🌐 Listening on :${PORT}`));

// Start the WhatsApp session (persisted in Firebase → survives restarts)
if (String(process.env.WA_ENABLED || 'true') !== 'false') {
  WA.start(db).then(s => log('WA', `engine started (state=${s.state}) → open /api/wa/qr to link`))
              .catch(e => log('WA', `engine failed: ${e.message}`));
}

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
  await dropWebhook('boot');            // guarantees getUpdates can run
  lastUpdateId = await loadLastUpdateId();
  log('INIT', `📍 Resumed from updateId=${lastUpdateId}`);
  // If we boot after 9am IST, skip today's summary so restarts don't spam it.
  if (new Date(Date.now() + IST_OFF).getUTCHours() >= 9) lastSummaryDay = istDayKey();
  await sendMenu().catch(() => {});
  setInterval(() => { dailySummaryTick(); }, 5 * 60_000);
  setInterval(() => { beatInstanceLock(); }, 20_000);
  startPollWatchdog();
  pollLoop().catch(e => {
    log('FATAL', `pollLoop died: ${e.message} — restarting in 2s`);
    setTimeout(() => pollLoop().catch(() => process.exit(1)), 2000);
  });
})();

process.on('uncaughtException',  e => log('UNCAUGHT', e.stack || e.message));
process.on('unhandledRejection', e => log('UNHANDLED', e?.stack || e?.message || String(e)));
