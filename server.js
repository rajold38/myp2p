// ════════════════════════════════════════════════════════════════════
// BIEXC — P2P + Trading Terminal Backend v5.0 (one-tap safe, KYC-locked P2P, tested)
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
import { mountEmailOtp } from './emailotp.js';
import { createReferral } from './referral.js';
import fs from 'fs';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── ENV VARS ───────────────────────────────────────────────────────
const PORT             = Number(process.env.PORT) || Number(process.argv[process.argv.indexOf('--port')+1]) || 8080;
const TG_TOKEN         = process.env.TG_TOKEN;
const TG_CHAT          = process.env.TG_CHAT;
const TG_TOKEN_SUPPORT = process.env.TG_TOKEN_SUPPORT;
const TG_CHAT_SUPPORT = process.env.TG_CHAT_SUPPORT;
const SUPPORT_ENABLED = !!(TG_TOKEN_SUPPORT && TG_CHAT_SUPPORT && TG_TOKEN_SUPPORT !== TG_TOKEN);
const TG_API_SUP = `https://api.telegram.org/bot${TG_TOKEN_SUPPORT}`;
const SUP_SCOPE = 'care_' + crypto.createHash('sha256').update(TG_TOKEN_SUPPORT || 'disabled').digest('hex').slice(0,16);
const SUP_STATE = `botState/${SUP_SCOPE}`;
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
let REF = null; // Refer & Earn engine (set up after Express starts)
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
  const COIN=String(coin||'').toUpperCase(); delta=Number(delta);
  if(!fuid||!/^[A-Z0-9]{1,20}$/.test(COIN)||!Number.isFinite(delta))return null;
  let outcome=null;
  const tx=await db.ref(`users/${fuid}`).transaction(cur=>{
    outcome=null;if(!cur)return;
    const u={...cur},b={...(cur.balances||{})},old=Number(b[COIN]??(COIN==='USDT'?cur.balance:0)??0),next=r8(old+delta);
    if(!Number.isFinite(old)||next<0)return;
    b[COIN]=next;u.balances=b;if(COIN==='USDT')u.balance=next;
    outcome={oldBal:r8(old),newBal:next};return u;
  });
  return tx.committed?outcome:null;
}

/** Set a coin balance to an exact value (admin commands).
 *  Also mirrors USDT to the legacy node in the same operation. */
async function setBalance(fuid, coin, value) {
 const COIN=String(coin||'').toUpperCase(),v=Number(value);if(!fuid||!/^[A-Z0-9]{1,20}$/.test(COIN)||!Number.isFinite(v)||v<0)throw new Error('Invalid balance');
 let result;const tx=await db.ref(`users/${fuid}`).transaction(cur=>{if(!cur)return;const u={...cur},b={...(cur.balances||{})};result={oldBal:r8(b[COIN]||0),newBal:r8(v)};b[COIN]=r8(v);u.balances=b;if(COIN==='USDT')u.balance=b[COIN];return u});
 if(!tx.committed)throw new Error('Balance update failed');return result;
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

/** Store the admin-entered withdrawal TxID on the matching history row. */
async function setHistoryHash(fuid, hid, hash) {
  const snap = await db.ref(`users/${fuid}/history`).once('value');
  const jobs = [];
  snap.forEach(c => {
    const v = c.val() || {};
    if (c.key === hid || v.hid === hid) jobs.push(db.ref(`users/${fuid}/history/${c.key}`).update({ hash, txid: hash, hashAt: Date.now() }));
  });
  await Promise.all(jobs);
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
const good = (msg) => `✅ ${msg}`;

// <V43_HELPERS> ── clean HTML bot messages (addresses/UIDs tap-to-copy) ──
const hEsc = (v) => String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const hCode = (v) => `<code>${hEsc(v)}</code>`;
/** hcard('📤 TITLE', ['line', '', 'line']) — every value must already be escaped. */
function hcard(title, rows = [], foot) {
  const out = [`<b>${hEsc(title)}</b>`, ''];
  for (const r of rows) { if (r == null || r === false) continue; out.push(Array.isArray(r) ? `${r[0]} ${r[1]}` : String(r)); }
  out.push('', foot === undefined ? `<i>⏰ ${hEsc(nowIST())} IST</i>` : foot);
  return out.join('\n').replace(/\n{3,}/g, '\n\n').trim();
}
/** Telegram refused the formatting → plain text without markup. */
function deFmt(t, mode) {
  if (mode === 'HTML') return String(t).replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
  return String(t).replace(/[*`]/g, '');
}
const isBscTx = (h) => /^0x[0-9a-fA-F]{64}$/.test(String(h || '').trim());
const bscTxUrl = (h) => `https://bscscan.com/tx/${String(h || '').trim()}`;
const bscBtn = (h, label = '🔎 View on BscScan') => isBscTx(h) ? [{ text: label, url: bscTxUrl(h) }] : null;
// </V43_HELPERS>

/** IST midnight for a moment, and its YYYY-MM-DD key. */
const IST_OFF = 5.5 * 3600_000;
function istDayStart(ts = Date.now()) {
  const d = new Date(ts + IST_OFF);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - IST_OFF;
}
const istDayKey = (ts = Date.now()) => new Date(ts + IST_OFF).toISOString().slice(0, 10);

/** Write the legacy `users/{fuid}/balance` mirror via a single multi-path
 *  update. Idempotent — safe to call twice with the same value. */
async function writeLegacyUsdtMirror(fuid) {
 if(!fuid)return;
 await db.ref(`users/${fuid}`).transaction(cur=>{if(!cur||cur.balances?.USDT==null)return;if(cur.balance===cur.balances.USDT)return;return {...cur,balance:cur.balances.USDT}});
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
      await db.ref(`users/${fuid}`).transaction(cur=>{
        if(!cur||cur.balances?.USDT!==undefined)return;
        const confirmed=Number(cur.balance);if(!Number.isFinite(confirmed)||confirmed<0)return;
        return {...cur,balances:{...(cur.balances||{}),USDT:r8(confirmed)},balance:r8(confirmed)};
      });
      MIGRATED_FUIDS.add(fuid);
      log('MIGRATE', `${fuid.slice(0,8)} legacy balance ${v} → balances.USDT`);
    }
  } catch (e) { log('MIGRATE', `err ${e.message}`); }
}

/** Shown UID for any user (old accounts may have no `uid` field). */
const shownUID = (fuid, u) => String(u?.uid || String(fuid).slice(0, 8)).toUpperCase();
/** Find a user by short UID, full Firebase id, its first 8 chars, email or phone. */
async function findUserByUID(uid) {
  if (uid == null) return null;
  const raw = String(uid).trim().replace(/^`+|`+$/g, '');
  if (!raw) return null;
  const U = raw.toUpperCase();
  try {
    for (const v of [U, raw]) {
      const snap = await db.ref('users').orderByChild('uid').equalTo(v).once('value');
      if (snap.exists()) { const val = snap.val(); const fuid = Object.keys(val)[0]; return { fuid, user: val[fuid] }; }
    }
  } catch (e) {}
  try {
    if (/^[A-Za-z0-9_-]{20,40}$/.test(raw)) {
      const d = await db.ref(`users/${raw}`).once('value');
      if (d.exists()) return { fuid: raw, user: d.val() };
    }
  } catch (e) {}
  const all = await db.ref('users').once('value');
  const digits = raw.replace(/\D/g, '');
  let exact = null, loose = null;
  all.forEach(child => {
    const u = child.val() || {}; const k = child.key;
    if (String(u.uid || '').toUpperCase() === U || k === raw) { exact = exact || { fuid: k, user: u }; return; }
    if (!loose && (
      shownUID(k, u) === U ||
      (raw.includes('@') && String(u.email || '').toLowerCase() === raw.toLowerCase()) ||
      (digits.length >= 10 && [u.phone, u.kycLevels?.basic?.phone].some(p => String(p || '').replace(/\D/g, '').endsWith(digits.slice(-10))))
    )) loose = { fuid: k, user: u };
  });
  return exact || loose;
}

// ════════════════════════════════════════════════════════════════════
// KYC — block user with un-closeable notice until admin approves
// ════════════════════════════════════════════════════════════════════
const kycSentFor = new Set(); // fuid+l2At already notified

function kycInfo(u) {
  const k = u?.kycLevels || {};
  const lv = k.level || 0;
  if (lv >= 2 || u?.kycStatus === 'APPROVED') return { done: true, label: k.approvedBy === 'ADMIN' && !k.docs ? '✅ Verified (by admin)' : '✅ Verified · Level 2' };
  if (lv >= 1 && k.l2status === 'PENDING') return { done: false, pending: true, label: '🟡 Level 2 submitted — review' };
  if (lv >= 1) return { done: false, label: '🔵 Level 1 done · Level 2 pending' };
  return { done: false, label: '⚪ Not started' };
}

/** New Level-2 submission → info card with an Open-user button (no old approve/reject). */
async function maybeForwardKycSubmission(fuid, user) {
  const k = user?.kycLevels;
  if (!k || k.l2status !== 'PENDING') return;
  const key = `${fuid}_${k.l2At || 0}`;
  if (kycSentFor.has(key)) return;
  kycSentFor.add(key);
  // Auto-approve: documents already passed the name check in the app.
  const now = Date.now();
  await db.ref(`users/${fuid}`).update({
    'kycLevels/level': 2, 'kycLevels/l2status': 'APPROVED', 'kycLevels/approvedBy': 'AUTO', 'kycLevels/approvedAt': now,
    kycStatus: 'APPROVED', kycRequired: false, kycApprovedAt: now,
  });
  if (REF) REF.evaluate(fuid).catch(() => {});
  if (k.l2At && k.l2At < BOT_START_TIME - 60_000) return; // old submission → approve quietly
  const bb = k.basic || {};
  const uid = user.uid || fuid.slice(0, 8);
  await tgSend(card('✅ KYC AUTO-APPROVED', [
    ['👤 UID', `\`${uid}\``],
    ['📛 Name', bb.name || user.name || '—'],
    ['📱 Mobile', bb.phone || '—'],
    ['🎂 DOB', bb.dob || '—'],
    ['💼 Profession', bb.profession || '—'],
    ['🏠 Address', [bb.address, bb.city, bb.state, bb.pin].filter(Boolean).join(', ') || '—'],
  ]), { reply_markup: { inline_keyboard: [[
    { text: '🪪 KYC details', callback_data: `kycview_${uid}` },
    { text: '👁 Open user', callback_data: `userdetail_${uid}` },
  ]] } });
}

/** Admin one-tap: mark the user fully verified (works even with no KYC submitted). */
async function approveKyc(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  const now = Date.now();
  const k = found.user.kycLevels || {};
  await db.ref(`users/${found.fuid}`).update({
    'kycLevels/level': 2,
    'kycLevels/l2status': 'APPROVED',
    'kycLevels/approvedBy': 'ADMIN',
    'kycLevels/approvedAt': now,
    'kycLevels/l1At': k.l1At || now,
    'kycLevels/l2At': k.l2At || now,
    kycStatus: 'APPROVED', kycRequired: false, kycApprovedAt: now,
  });
  await pushNotif(found.fuid, { title: 'KYC approved 🎉', body: 'Your identity is verified. All features are unlocked.', type: 'INFO' });
  log('KYC', `approved by admin UID=${uid}`);
  if (REF) REF.evaluate(found.fuid).catch(() => {});
  await tgSend(ok('KYC APPROVED', [['👤 UID', `\`${uid}\``], ['📛 Name', found.user.name || '—']]),
    { reply_markup: { inline_keyboard: [[{ text: '👁 Open user', callback_data: `userdetail_${uid}` }, { text: '🏠 Menu', callback_data: 'menu_home' }]] } });
}

/** Undo verification (user must complete KYC again). */
async function revokeKyc(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  await db.ref(`users/${found.fuid}`).update({ kycLevels: null, kycStatus: null, kycRequired: false });
  log('KYC', `revoked UID=${uid}`);
  await tgSend(ok('KYC RESET', [['👤 UID', `\`${uid}\``], ['ℹ️', 'User must verify again']]),
    { reply_markup: { inline_keyboard: [[{ text: '👁 Open user', callback_data: `userdetail_${uid}` }]] } });
}

// ════════════════════════════════════════════════════════════════════
// BUTTON FLOWS — the bot asks, the admin just types the value
// ════════════════════════════════════════════════════════════════════
let ASK = null;               // { kind, uid?, coin?, ts }
const CONFIRM = new Map();    // id -> action
const ASK_TTL = 10 * 60_000;
const COINS = ['USDT', 'BTC', 'ETH', 'BNB', 'SOL', 'TRX', 'XRP', 'DOGE'];
const cancelKb = { inline_keyboard: [[{ text: '✖ Cancel', callback_data: 'askcancel' }]] };

function ask(kind, extra = {}) { ASK = { kind, ...extra, ts: Date.now() }; }
function newConfirm(action) {
  const id = Math.random().toString(36).slice(2, 9);
  CONFIRM.set(id, { ...action, ts: Date.now() });
  for (const [k, v] of CONFIRM) if (Date.now() - v.ts > ASK_TTL) CONFIRM.delete(k);
  return id;
}

async function coinPicker(kind, uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  const held = Object.entries(found.user.balances || {}).filter(([, v]) => r8(v) > 0).map(([c]) => c.toUpperCase());
  const list = [...new Set([...COINS, ...held])].slice(0, 15);
  const rows = [];
  for (let i = 0; i < list.length; i += 4) rows.push(list.slice(i, i + 4).map(c => ({ text: c, callback_data: `pc_${kind}_${uid}_${c}` })));
  rows.push([{ text: '⌨️ Other coin', callback_data: `pc_${kind}_${uid}_*` }, { text: '✖ Cancel', callback_data: 'askcancel' }]);
  const title = { credit: '➕ CREDIT', debit: '➖ DEBIT', setbal: '🎯 SET BALANCE' }[kind];
  await tgSend(card(`${title} — ${uid}`, [['📛 Name', found.user.name || '—'], 'Pick the coin 👇']), { reply_markup: { inline_keyboard: rows } });
}

async function askAmount(kind, uid, coin) {
  ask('amount', { op: kind, uid, coin });
  const verb = { credit: 'credit', debit: 'debit', setbal: 'set as the new balance' }[kind];
  await tgSend(`✏️ Type the *${coin}* amount to ${verb} for \`${uid}\`\n_e.g._ \`250\``, { reply_markup: cancelKb });
}

async function sendSearchResults(q) {
  const Q = q.toLowerCase();
  const snap = await db.ref('users').once('value');
  const hits = [];
  snap.forEach(c => {
    const u = c.val(); if (!u || typeof u !== 'object') return;
    const id = shownUID(c.key, u);
    const hay = `${u.name || ''} ${u.email || ''} ${u.phone || ''} ${u.kycLevels?.basic?.name || ''} ${u.kycLevels?.basic?.phone || ''} ${id} ${c.key}`.toLowerCase();
    if (hay.includes(Q)) hits.push({ ...u, uid: id });
  });
  if (!hits.length) {
    await tgSend(bad(`No user matches “${q}”.`), { reply_markup: { inline_keyboard: [[{ text: '🔎 Search again', callback_data: 'menu_search' }, { text: '🏠 Menu', callback_data: 'menu_home' }]] } });
    return;
  }
  const top = hits.slice(0, 12);
  const kb = top.map(u => [{ text: `👤 ${u.name || '—'} · ${u.uid}`.slice(0, 60), callback_data: `userdetail_${u.uid}` }]);
  kb.push([{ text: '🔎 New search', callback_data: 'menu_search' }, { text: '🏠 Menu', callback_data: 'menu_home' }]);
  await tgSend(card(`🔎 RESULTS — “${q}”`, [['Found', hits.length > 12 ? `${hits.length} (showing 12)` : hits.length], 'Tap a user 👇']), { reply_markup: { inline_keyboard: kb } });
}

/** Plain text typed by the admin while the bot is waiting for an answer. */
async function handleAskReply(text) {
  const a = ASK; ASK = null;
  if (a.kind === 'mwd' || a.kind === 'plim') {
    const n = Number(String(text).replace(/[^0-9.]/g, ''));
    if (!Number.isFinite(n) || n < 0 || String(text).trim() === '') { ask(a.kind, { uid: a.uid }); return tgSend(bad('Send a number, e.g. `3`'), { reply_markup: cancelKb }); }
    const found = await findUserByUID(a.uid);
    if (!found) return tgSend(bad(`UID \`${a.uid}\` not found.`));
    const key = a.kind === 'mwd' ? 'minWithdraw' : 'p2pDaily';
    const val = a.kind === 'plim' ? Math.floor(n) : r8(n);
    await db.ref(`users/${found.fuid}/limits/${key}`).set(val > 0 ? val : null);
    log('ADMIN', `${key} UID=${a.uid} → ${val || 'default'}`);
    await tgSend(good(a.kind === 'mwd'
      ? `Min withdrawal for \`${a.uid}\` set to *$${val > 0 ? val : 10}*`
      : `P2P daily limit for \`${a.uid}\` set to *${val > 0 ? val + ' / day' : 'Unlimited'}*`));
    return sendUserDetailCard(null, a.uid);
  }
  if (a.kind === 'p2prate') {
    const n = Number(String(text).replace(/[^0-9.]/g, ''));
    const isLim = /Lim/.test(a.field);
    if (!Number.isFinite(n) || n <= 0 || n > (isLim ? 1000000 : 10000)) { ask('p2prate', { field: a.field }); return tgSend(bad('Send a valid rate, e.g. `97.50`'), { reply_markup: cancelKb }); }
    await db.ref(`settings/p2p/${a.field}`).set(Math.round(n * 100) / 100);
    P2P_CACHE.at = 0;
    return sendP2PRates();
  }
  if (a.kind === 'uid') {
    const t = text.trim();
    const uid = t.includes('@') ? t : t.replace(/[^A-Za-z0-9_-]/g, '');
    if (!uid) { ask('uid'); return tgSend(bad('Send a valid UID, e.g. `AB12CD`'), { reply_markup: cancelKb }); }
    return sendUserDetailCard(null, uid);
  }
  if (a.kind === 'withash') {
    const h = text.trim().replace(/\s+/g, '');
    if (!/^[A-Za-z0-9:_-]{10,200}$/.test(h)) { ASK = { ...a, ts: Date.now() }; return tgSend(bad('Send a valid transaction hash (TxID).'), { reply_markup: cancelKb }); }
    await setHistoryHash(a.fuid, a.hid, h);
    log('WIT', `hash saved UID=${a.uid} hid=${a.hid}`);
    return tgSend(card('✅ HASH SAVED', [['👤 UID', `\`${a.uid || '—'}\``], ['💸 Amount', `${a.amt} ${a.coin}`], ['🔗 TxID', `\`${h}\``]]));
  }
  if (a.kind === 'search') return sendSearchResults(text.slice(0, 60));
  if (a.kind === 'coin') {
    const c = text.replace(/[^A-Z0-9]/gi, '').toUpperCase().slice(0, 10);
    if (!c) { ASK = a; return tgSend(bad('Send a coin symbol, e.g. `MATIC`'), { reply_markup: cancelKb }); }
    return askAmount(a.op, a.uid, c);
  }
  if (a.kind === 'amount') {
    const amt = parseFloat(text.replace(/,/g, ''));
    if (!Number.isFinite(amt) || amt < 0 || (a.op !== 'setbal' && amt === 0)) { ASK = a; return tgSend(bad('Send a number, e.g. `250`'), { reply_markup: cancelKb }); }
    const found = await findUserByUID(a.uid);
    if (!found) return tgSend(bad(`UID \`${a.uid}\` not found.`));
    const cur = r8((found.user.balances || {})[a.coin] || 0);
    const after = a.op === 'credit' ? cur + amt : a.op === 'debit' ? cur - amt : amt;
    const id = newConfirm({ op: a.op, uid: a.uid, coin: a.coin, amt });
    const title = { credit: '➕ CONFIRM CREDIT', debit: '➖ CONFIRM DEBIT', setbal: '🎯 CONFIRM SET BALANCE' }[a.op];
    return tgSend(card(title, [
      ['👤 UID', `\`${a.uid}\``], ['📛 Name', found.user.name || '—'],
      ['🪙 Coin', a.coin], ['💵 Amount', fmtNum(amt)],
      ['💰 Balance', `${fmtNum(cur)} → ${fmtNum(after)}`],
    ]) + (a.op === 'setbal' ? '' : '\n\n📜 *Show this in user\'s history?*'), { reply_markup: { inline_keyboard: a.op === 'setbal'
      ? [[{ text: '✅ Confirm', callback_data: `cfm_${id}` }, { text: '✖ Cancel', callback_data: 'askcancel' }]]
      : [[{ text: '✅ Yes, show in history', callback_data: `cfm_${id}` }], [{ text: '🙈 No, hide from history', callback_data: `cfmh_${id}` }], [{ text: '✖ Cancel', callback_data: 'askcancel' }]] } });
  }
  if (a.kind === 'msg') {
    const id = newConfirm({ op: 'msg', uid: a.uid, text });
    return tgSend(card('💬 SEND MESSAGE?', [['👤 To', `\`${a.uid}\``], ['💬 Text', text.slice(0, 300)]]),
      { reply_markup: { inline_keyboard: [[{ text: '✅ Send', callback_data: `cfm_${id}` }, { text: '✖ Cancel', callback_data: 'askcancel' }]] } });
  }
  if (a.kind === 'broadcast') {
    const id = newConfirm({ op: 'broadcast', text });
    return tgSend(card('📢 SEND TO ALL USERS?', [['💬 Text', text.slice(0, 300)]]),
      { reply_markup: { inline_keyboard: [[{ text: '✅ Send to all', callback_data: `cfm_${id}` }, { text: '✖ Cancel', callback_data: 'askcancel' }]] } });
  }
}

async function runConfirm(id, hideHist = false) {
  const c = CONFIRM.get(id); CONFIRM.delete(id);
  if (!c) return tgSend(bad('This confirmation expired — start again.'), { reply_markup: MENU_KB });
  if (c.op === 'credit') { await adminCreditDebit(c.uid, c.amt, '+', 'ADMIN_CREDIT', c.coin, !hideHist); return sendUserDetailCard(null, c.uid); }
  if (c.op === 'debit')  { await adminCreditDebit(c.uid, c.amt, '-', 'ADMIN_DEBIT', c.coin, !hideHist);  return sendUserDetailCard(null, c.uid); }
  if (c.op === 'setbal') { await handleSetBalance(c.uid, c.coin, c.amt); return sendUserDetailCard(null, c.uid); }
  if (c.op === 'msg')    { await handleAdminMessage(c.uid, c.text); return; }
  if (c.op === 'broadcast') return handleBroadcast(c.text);
  if (c.op === 'allmail') return allMailRun(c.text);
  if (c.op === 'kycrevoke') return revokeKyc(c.uid);
  if (c.op === 'ban') return setBanned(c.uid, true);
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
let _activeCache = { ok: true, at: 0 };
async function amIActive() {
  // Cached for 8s — this was a Firebase read on EVERY poll and EVERY user
  // change, which added a full round-trip of latency to each button tap.
  if (Date.now() - _activeCache.at < 8_000) return _activeCache.ok;
  const ok = await amIActiveRaw();
  _activeCache = { ok, at: Date.now() };
  return ok;
}
async function amIActiveRaw() {
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
    let data = await r.json();
    // A name/email with _ * ` [ breaks Markdown → Telegram refuses the whole
    // message (that is why some user cards never appeared). Retry as plain text.
    if (!data.ok && body && body.parse_mode && /parse entities|can't find end/i.test(data.description || '')) {
      const plain = { ...body }; delete plain.parse_mode;
      if (typeof plain.text === 'string') plain.text = deFmt(plain.text, body.parse_mode);
      if (typeof plain.caption === 'string') plain.caption = deFmt(plain.caption, body.parse_mode);
      const r2 = await fetch(`${TG_API}/${endpoint}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(plain) });
      data = await r2.json();
    }
    if (!data.ok) log('TG', `${endpoint} failed: ${data.description || JSON.stringify(data)}`);
    return data;
  } catch (e) { log('TG', `fetch err ${endpoint}: ${e.message}`); return { ok: false, error: e.message }; }
}
const tgSend   = (text, extra={}) => tgFetch('sendMessage', { chat_id: TG_CHAT, text, parse_mode: 'Markdown', ...extra });
const tgEdit   = (msgId, text, extra={}) => tgFetch('editMessageText', { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: 'Markdown', ...extra });
const tgAnswer = (cbId, text, alert = false) => tgFetch('answerCallbackQuery', { callback_query_id: cbId, text, show_alert: !!alert });
const tgSendH  = (text, extra={}) => tgFetch('sendMessage', { chat_id: TG_CHAT, text, parse_mode: 'HTML', disable_web_page_preview: true, ...extra });
const tgEditH  = (msgId, text, extra={}) => tgFetch('editMessageText', { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: 'HTML', disable_web_page_preview: true, ...extra });
/** Small 👍 on the admin's own message = "delivered" (no extra chat noise). */
const tgReact  = (msgId, emoji = '👍') => tgFetch('setMessageReaction', { chat_id: TG_CHAT, message_id: msgId, reaction: [{ type: 'emoji', emoji }] });

async function tgFetchSup(endpoint, body) {
  if(!SUPPORT_ENABLED)return {ok:false,description:'Customer-care bot is not configured'};
  try {
    const r = await fetch(`${TG_API_SUP}/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    let data = await r.json();
    // A name/email with _ * ` [ breaks Markdown → Telegram refuses the whole
    // message (that is why some user cards never appeared). Retry as plain text.
    if (!data.ok && body && body.parse_mode && /parse entities|can't find end/i.test(data.description || '')) {
      const plain = { ...body }; delete plain.parse_mode;
      if (typeof plain.text === 'string') plain.text = deFmt(plain.text, body.parse_mode);
      if (typeof plain.caption === 'string') plain.caption = deFmt(plain.caption, body.parse_mode);
      const r2 = await fetch(`${TG_API_SUP}/${endpoint}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(plain) });
      data = await r2.json();
    }
    if (!data.ok) log('TG', `${endpoint} failed: ${data.description || JSON.stringify(data)}`);
    return data;
  } catch (e) { log('TG', `fetch err ${endpoint}: ${e.message}`); return { ok: false, error: e.message }; }
}
const tgSendSup   = (text, extra={}) => tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text, parse_mode: 'Markdown', ...extra });
const tgEditSup   = (msgId, text, extra={}) => tgFetchSup('editMessageText', { chat_id: TG_CHAT_SUPPORT, message_id: msgId, text, parse_mode: 'Markdown', ...extra });
const tgAnswerSup = (cbId, text, alert = false) => tgFetchSup('answerCallbackQuery', { callback_query_id: cbId, text, show_alert: !!alert });
const tgReactSup  = (msgId, emoji = '👍') => tgFetchSup('setMessageReaction', { chat_id: TG_CHAT_SUPPORT, message_id: msgId, reaction: [{ type: 'emoji', emoji }] });

// ════════════════════════════════════════════════════════════════════
// ONE-TAP GUARD — an Approve / Reject button can work ONLY ONCE.
// Layer 1: in-memory Map, set synchronously → a double tap that arrives
//          in the same instant is blocked with zero delay.
// Layer 2: Firebase transaction at botLocks/taps/{key} → survives restarts,
//          redeploys and a second server instance.
// The key is the request id (not the action), so Approve-then-Reject or
// Reject-then-Approve on the same request is also blocked.
// ════════════════════════════════════════════════════════════════════
const TAP_DONE = new Map(); // key -> { action, ts }
const tapKey = (s) => String(s).replace(/[.#$\[\]\/]/g, '_').slice(0, 200);
async function claimTapOnce(rawKey, action) {
  const k = tapKey(rawKey);
  if (TAP_DONE.has(k)) return { ok: false, prev: TAP_DONE.get(k).action };
  TAP_DONE.set(k, { action, ts: Date.now() });
  if (TAP_DONE.size > 3000) {
    const cutoff = Date.now() - 6 * 3600_000;
    for (const [kk, v] of TAP_DONE) if (v.ts < cutoff) TAP_DONE.delete(kk);
  }
  if (!db) return { ok: true };
  try {
    let prev = null;
    const tx = await db.ref(`botLocks/taps/${k}`).transaction(cur => {
      if (cur) { prev = cur.action || 'done'; return; }   // already used → abort
      return { action, ts: Date.now(), by: INSTANCE_ID };
    });
    if (!tx.committed) {
      TAP_DONE.set(k, { action: prev || tx.snapshot?.val()?.action || 'done', ts: Date.now() });
      return { ok: false, prev: prev || tx.snapshot?.val()?.action || 'done' };
    }
    return { ok: true };
  } catch (e) {
    log('TAP', `lock err ${e.message} — memory lock still active`);
    return { ok: true };
  }
}
/** Undo a tap lock — only used when processing failed BEFORE anything changed. */
async function releaseTap(rawKey) {
  const k = tapKey(rawKey);
  TAP_DONE.delete(k);
  try { if (db) await db.ref(`botLocks/taps/${k}`).remove(); } catch (e) {}
}
/** Swap the Approve/Reject buttons for a single status label — instantly. */
function setCbButtons(cb, label) {
  const msg = cb?.message; if (!msg?.message_id) return Promise.resolve();
  return tgFetch('editMessageReplyMarkup', {
    chat_id: msg.chat?.id || TG_CHAT, message_id: msg.message_id,
    reply_markup: { inline_keyboard: [[{ text: label, callback_data: 'noop_done' }]] },
  }).catch(() => {});
}
const doneWord = (a) => a === 'reject' ? 'REJECTED' : a === 'approve' ? 'APPROVED' : 'PROCESSED';
/** Short memory-only debounce for buttons that are safe but noisy when double-tapped. */
const TAP_DEBOUNCE = new Map();
function debounceTap(key, ms = 8000) {
  const t = TAP_DEBOUNCE.get(key);
  if (t && Date.now() - t < ms) return false;
  TAP_DEBOUNCE.set(key, Date.now());
  if (TAP_DEBOUNCE.size > 500) for (const [k, v] of TAP_DEBOUNCE) if (Date.now() - v > 60_000) TAP_DEBOUNCE.delete(k);
  return true;
}

async function tgSendButtons(text, cbId, extraRows = []) {
  const r = await tgFetch('sendMessage', {
    chat_id: TG_CHAT, text, parse_mode: 'HTML', disable_web_page_preview: true,
    reply_markup: { inline_keyboard: [[
      { text: '✅ APPROVE', callback_data: `approve_${cbId}` },
      { text: '❌ REJECT',  callback_data: `reject_${cbId}` }
    ], ...extraRows.filter(Boolean)] }
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
  const net = req.network || req.chain, addr = req.address || req.addr;
  const wallet = bals[coin] ?? (coin === 'USDT' ? user.balance : 0) ?? 0;
  const rows = [
    `👤 <b>${hEsc(user.name || 'User')}</b> · ${hCode(user.uid || '—')}`,
    user.email ? `📧 ${hEsc(user.email)}` : null,
    '',
    `💵 <b>${hEsc(fmtNum(amt))} ${hEsc(coin)}</b>${net ? ` · ${hEsc(net)}` : ''}`,
  ];
  if (addr)     rows.push('', `📬 ${type === 'wit' ? 'Send to' : 'Address'} <i>(tap to copy)</i>`, hCode(addr));
  if (req.txid) rows.push('', '🔗 TxID <i>(tap to copy)</i>', hCode(req.txid));
  if (req.utr)  rows.push(`🧾 UTR ${hCode(req.utr)}`);
  rows.push('', `💼 Wallet ${hEsc(fmtNum(wallet || 0))} ${hEsc(coin)}${coin !== 'USDT' ? ` · USDT ${hEsc(fmtNum(bals.USDT || user.balance || 0))}` : ''}`);
  if (coin === 'USDT' && amt >= BIG_AMOUNT) rows.push('', `🚨 <b>LARGE ${type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL'}</b> — verify before approving`);
  return hcard(type === 'dep' ? '📥 DEPOSIT REQUEST' : '📤 WITHDRAWAL REQUEST', rows);
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
  const msgId = await tgSendButtons(text, cbId, [type === 'dep' ? bscBtn(claimed.txid, '🔎 Check TxID on BscScan') : null]);
  if (msgId) {
    await reqRef.update({ botMsgId: msgId, msgId, cbId, _sending: null, _sendingTs: null });
    sentByCbId.set(cbId, { fuid, type, msgId, hid: claimed.hid, amt: parseFloat(claimed.amt), coin: claimed.coin });
    log(type.toUpperCase(), `UID=${user.uid} Amt=${claimed.amt} ${claimed.coin || 'USDT'} cbId=${cbId} → sent msgId=${msgId}`);
    queueRequestMail(type, user, claimed, cbId);
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

// ════════════════════════════════════════════════════════════════════
// BONUS — $10 KYC bonus (main balance) + $10 welcome bonus unlock after 3 trades.
// Same single claim point as the app (users/{fuid}/bonus transaction) → paid once.
// ════════════════════════════════════════════════════════════════════
const BONUS_KYC = parseFloat(process.env.BONUS_KYC || '10');     // advertised maximum ("up to")
const BONUS_MIN = parseFloat(process.env.BONUS_MIN || '1');
const BONUS_MAX = Math.min(BONUS_KYC, parseFloat(process.env.BONUS_MAX || '2'));
/** Actual KYC bonus: random between BONUS_MIN and BONUS_MAX, 2 decimals. */
const pickKycBonus = () => Math.round((BONUS_MIN + Math.random() * Math.max(0, BONUS_MAX - BONUS_MIN)) * 100) / 100;
const BONUS_TRADES = parseInt(process.env.BONUS_TRADES || '3', 10);
const bonusBusy = new Set();
async function bonusCredit(fuid, amt, type, note) {
  const tr = await db.ref(`users/${fuid}/balances/USDT`).transaction(c => Math.round(((parseFloat(c) || 0) + amt) * 1e8) / 1e8);
  if (tr.committed) await db.ref(`users/${fuid}/balance`).set(parseFloat(tr.snapshot.val()) || 0).catch(() => {});
  const hid = `h_bonus_${type.toLowerCase()}_${Date.now()}`;
  await db.ref(`users/${fuid}/history/${hid}`).set({ hid, ts: Date.now(), type, coin: 'USDT', amt, status: 'COMPLETED', note }).catch(() => {});
}
async function bonusClaim(fuid, fn) {
  let res = null;
  const tx = await db.ref(`users/${fuid}/bonus`).transaction(cur => { res = null; const b = cur || {}; const o = fn(b); if (!o) return; res = o.res; return o.b; });
  return tx.committed ? res : null;
}
// REFERRAL BONUS (v38) — every referred user (old + new) gets a locked $REF_BONUS.
// It is NOT in balances.USDT, so it can never be withdrawn or sold in P2P.
// Unlocks into main balance after REF_BUYS completed P2P BUY orders made
// after the grant. Counted from history, so every settlement path counts.
// The old KYC/welcome locked bonus is removed (already-paid bonuses stay).
const REF_BONUS = parseFloat(process.env.REF_BONUS || '20');
const REF_BUYS = parseInt(process.env.REF_BUYS || '2', 10);
const isReferred = v => !!(v && (v.referral?.by || v.referredBy));
function countRefBuys(v, since) {
  const ids = new Set();
  for (const h of Object.values(v?.history || {})) {
    if (!h || !h.orderId) continue;
    if (String(h.type || '').toUpperCase() !== 'BUY') continue;
    if (String(h.status || '').toUpperCase() !== 'COMPLETED') continue;
    if ((Number(h.ts) || 0) < since) continue;
    ids.add(String(h.orderId));
  }
  return ids.size;
}
const bonusNoteQ = [];
const bonusNote = line => { bonusNoteQ.push(line); };
setInterval(() => {
  if (!bonusNoteQ.length) return;
  const lines = bonusNoteQ.splice(0, 40);
  tgSend(`🎁 REFERRAL BONUS\n\n${lines.join('\n')}${bonusNoteQ.length ? `\n\n…${bonusNoteQ.length} more in next message` : ''}`, { parse_mode: undefined }).catch(() => {});
}, 20000);
async function bonusCountAction() { /* v38: counting is read from history in evaluateBonus */ }
async function evaluateBonus(fuid, v) {
  if (!v || bonusBusy.has(fuid)) return;
  const b = v.bonus || {};
  const ref = isReferred(v), isRef = b.kind === 'REFERRAL';
  const who = `${v.uid || fuid}${v.name ? ' ' + v.name : ''}`;
  const oldLocked = !isRef && b.welcomeGranted && b.welcomeStatus === 'LOCKED';
  const wantGrant = ref && !isRef;
  const wantRemove = !ref && oldLocked;
  const locked = isRef && b.welcomeStatus === 'LOCKED';
  const buys = locked ? countRefBuys(v, b.welcomeAt || 0) : 0;
  const wantUnlock = locked && buys >= REF_BUYS;
  const wantProgress = locked && !wantUnlock && buys > (b.tradesCompleted || 0);
  if (!wantGrant && !wantRemove && !wantUnlock && !wantProgress) return;
  bonusBusy.add(fuid);
  try {
    if (wantGrant || wantRemove) {
      let removedAmt = 0;
      const r = await bonusClaim(fuid, x => {
        if (x.kind === 'REFERRAL') return null;
        const now = Date.now(), log = { ...(x.log || {}) };
        const prevLocked = x.welcomeGranted && x.welcomeStatus === 'LOCKED';
        removedAmt = prevLocked ? (Number(x.welcomeAmt) || 0) : 0;
        if (prevLocked) log[`l${now}r`] = { type: 'REMOVED', text: `Old $${removedAmt} welcome bonus removed`, ts: now };
        if (!ref) {
          if (!prevLocked) return null;
          return { b: { ...x, welcomeStatus: 'REMOVED', removedAt: now, log }, res: 'removed' };
        }
        log[`l${now}g`] = { type: 'GRANTED', text: `$${REF_BONUS} referral bonus received (locked)`, ts: now + 1 };
        const legacyWelcome = x.welcomeGranted ? { amt: Number(x.welcomeAmt) || 0, status: prevLocked ? 'REMOVED' : (x.welcomeStatus || '') } : null;
        return { b: { kycGranted: x.kycGranted || null, kycAmt: x.kycAmt || null, kycAt: x.kycAt || null, legacyWelcome,
          kind: 'REFERRAL', welcomeGranted: true, welcomeAmt: REF_BONUS, welcomeStatus: 'LOCKED', welcomeAt: now, need: REF_BUYS, tradesCompleted: 0, log }, res: 'granted' };
      });
      if (r === 'granted') { log('BONUS', `referral bonus locked ${who}`); bonusNote(`🔒 $${REF_BONUS} locked bonus → ${who}${removedAmt ? ` (old $${removedAmt} removed)` : ''}`); }
      if (r === 'removed') { log('BONUS', `old bonus removed ${who}`); bonusNote(`🗑 Old $${removedAmt} locked bonus removed → ${who}`); }
    }
    if (wantProgress) {
      const n = await bonusClaim(fuid, x => {
        if (x.kind !== 'REFERRAL' || x.welcomeStatus !== 'LOCKED' || (x.tradesCompleted || 0) >= buys) return null;
        const now = Date.now();
        return { b: { ...x, tradesCompleted: buys, log: { ...(x.log || {}), [`l${now}t`]: { type: 'TRADE', text: `P2P Buy completed (${buys}/${REF_BUYS})`, ts: now } } }, res: buys };
      });
      if (n) bonusNote(`📈 ${who}: ${n}/${REF_BUYS} P2P buys done`);
    }
    if (wantUnlock) {
      // v39: never auto-credit. Mark CLAIMABLE; user taps Claim in Assets.
      const ok = await bonusClaim(fuid, x => {
        if (x.kind !== 'REFERRAL' || x.welcomeStatus !== 'LOCKED') return null;
        const now = Date.now();
        return { b: { ...x, welcomeStatus: 'CLAIMABLE', tradesCompleted: buys, claimableAt: now,
          log: { ...(x.log || {}), [`l${now}c`]: { type: 'CLAIMABLE', text: `${REF_BUYS} P2P buys done — bonus ready to claim`, ts: now } } }, res: true };
      });
      if (ok) {
        bonusNote(`🟢 ${who}: ${REF_BUYS}/${REF_BUYS} buys — $${Number(b.welcomeAmt) || REF_BONUS} ready to claim`);
        pushNotif(fuid, { title: 'Bonus ready to claim', body: `Open Assets and tap Claim to get your $${Number(b.welcomeAmt) || REF_BONUS} bonus.`, type: 'INFO' }).catch(() => {});
      }
    }
  } catch (e) { log('BONUS', `err ${e.message}`); }
  finally { bonusBusy.delete(fuid); }
}


// ════════════════════════════════════════════════════════════════════
// V48 FESTIVE LINK BONUS (DIWALI20) — opened from the email link.
// One per account, KYC required. Kept OUTSIDE balances.USDT until claimed.
// Claimable after `need` completed P2P BUY orders made between activation
// and expiry (activation + `days`). Claim credits main balance once.
// Authoritative activation time lives in promoIndex/{code}/{fuid}.
// ════════════════════════════════════════════════════════════════════
const PROMO_DEFAULTS = {
  DIWALI20: { code: 'DIWALI20', title: 'Diwali Bonus', amt: parseFloat(process.env.DIWALI_BONUS || '20'), days: parseInt(process.env.DIWALI_DAYS || '7', 10), need: parseInt(process.env.DIWALI_BUYS || '2', 10), active: true },
};
const PROMO_APP_URL = String(process.env.PROMO_APP_URL || 'https://biexc.in').replace(/\/$/, '');
const promoLink = (code) => `${PROMO_APP_URL}/?promo=${code}`;
const PROMO_CFG = new Map();
const normPromo = (c) => String(c || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 20);
async function promoCfg(code, fresh = false) {
  code = normPromo(code);
  const base = PROMO_DEFAULTS[code];
  if (!base) return null;
  const hit = PROMO_CFG.get(code);
  if (!fresh && hit && Date.now() - hit.at < 15000) return hit.v;
  let o = {};
  try { o = (await db.ref(`settings/promos/${code}`).once('value')).val() || {}; } catch {}
  const v = { ...base, ...(typeof o.active === 'boolean' ? { active: o.active } : {}) };
  PROMO_CFG.set(code, { at: Date.now(), v });
  return v;
}
function countBuysWindow(v, since, until) {
  const ids = new Set();
  for (const h of Object.values(v?.history || {})) {
    if (!h || !h.orderId) continue;
    if (String(h.type || '').toUpperCase() !== 'BUY') continue;
    if (String(h.status || '').toUpperCase() !== 'COMPLETED') continue;
    const t = Number(h.ts) || 0;
    if (t < since || (until && t > until)) continue;
    ids.add(String(h.orderId));
  }
  return ids.size;
}
const promoNoteQ = [];
const promoNote = (line) => { promoNoteQ.push(line); };
setInterval(() => {
  if (!promoNoteQ.length) return;
  const lines = promoNoteQ.splice(0, 40);
  tgSend(`DIWALI BONUS\n\n${lines.join('\n')}${promoNoteQ.length ? `\n\n…${promoNoteQ.length} more in next message` : ''}`, { parse_mode: undefined }).catch(() => {});
}, 20000);
const promoBusy = new Set();
async function evaluatePromos(fuid, v) {
  const ps = v && v.promos;
  if (!ps || typeof ps !== 'object' || promoBusy.has(fuid)) return;
  for (const [code, p] of Object.entries(ps)) {
    if (!p || p.status !== 'LOCKED' || !PROMO_DEFAULTS[code]) continue;
    const need = Number(p.need) || PROMO_DEFAULTS[code].need;
    const n = countBuysWindow(v, Number(p.at) || 0, Number(p.exp) || 0);
    const now = Date.now();
    const next = n >= need ? 'CLAIMABLE' : (p.exp && now > Number(p.exp) ? 'EXPIRED' : null);
    if (!next && n <= (Number(p.done) || 0)) continue;
    promoBusy.add(fuid);
    try {
      let res = null;
      const tx = await db.ref(`users/${fuid}/promos/${code}`).transaction(cur => {
        res = null;
        if (!cur) return cur;
        if (cur.status !== 'LOCKED') return;
        res = next || 'progress';
        return { ...cur, done: Math.min(n, need), ...(next ? { status: next, [next === 'CLAIMABLE' ? 'readyAt' : 'expiredAt']: now } : {}) };
      });
      if (!tx.committed || !res) continue;
      db.ref(`promoIndex/${code}/${fuid}`).update({ done: Math.min(n, need), ...(next ? { status: next } : {}) }).catch(() => {});
      const who = `${v.uid || fuid}${v.name ? ' ' + v.name : ''}`;
      if (res === 'CLAIMABLE') {
        promoNote(`🟢 ${who}: ${need}/${need} P2P buys — $${p.amt} ready to claim`);
        pushNotif(fuid, { title: 'Diwali bonus ready', body: `Open Assets and tap Claim to get your $${p.amt} Diwali bonus.`, type: 'INFO' }).catch(() => {});
      } else if (res === 'EXPIRED') promoNote(`⌛ ${who}: Diwali bonus expired (${n}/${need} buys)`);
      else promoNote(`📈 ${who}: ${n}/${need} P2P buys`);
    } catch (e) { log('PROMO', `eval ${e.message}`); }
    finally { promoBusy.delete(fuid); }
  }
}
async function promoStats(code) {
  const snap = await db.ref(`promoIndex/${code}`).once('value');
  const st = { total: 0, LOCKED: 0, CLAIMABLE: 0, CLAIMED: 0, EXPIRED: 0, paid: 0, recent: [] };
  const now = Date.now();
  snap.forEach(c => {
    const x = c.val() || {}; st.total++;
    let k = x.status || 'LOCKED';
    if (k === 'LOCKED' && x.exp && now > x.exp) k = 'EXPIRED';
    st[k] = (st[k] || 0) + 1;
    if (k === 'CLAIMED') st.paid += Number(x.amt) || 0;
    st.recent.push(x);
  });
  st.recent.sort((a, b) => (b.claimedAt || b.at || 0) - (a.claimedAt || a.at || 0));
  return st;
}
async function sendPromoPanel(code = 'DIWALI20', editId = null) {
  const cfg = await promoCfg(code, true);
  const st = await promoStats(code);
  const rows = [
    `${cfg.active ? '🟢 <b>Link is ON</b> — new users can activate' : '🔴 <b>Link is OFF</b> — no new activations'}`,
    '',
    `🎁 <b>$${hEsc(cfg.amt)}</b> · ${hEsc(cfg.need)} P2P Buy trades · ${hEsc(cfg.days)} days`,
    `🔗 ${hCode(promoLink(code))}`,
    '',
    `👥 Activated: <b>${st.total}</b>`,
    `⏳ In progress: <b>${st.LOCKED}</b>`,
    `🟢 Ready to claim: <b>${st.CLAIMABLE}</b>`,
    `✅ Claimed: <b>${st.CLAIMED}</b> · paid <b>$${hEsc(r8(st.paid))}</b>`,
    `⌛ Expired: <b>${st.EXPIRED}</b>`,
  ];
  const recent = st.recent.slice(0, 6);
  if (recent.length) {
    rows.push('', '<b>Latest</b>');
    for (const x of recent) rows.push(`${x.status === 'CLAIMED' ? '✅' : x.status === 'CLAIMABLE' ? '🟢' : x.status === 'EXPIRED' ? '⌛' : '⏳'} ${hCode(x.uid || '—')} · ${hEsc(x.done || 0)}/${hEsc(x.need || cfg.need)}`);
  }
  const kb = { inline_keyboard: [
    [cfg.active ? { text: '🔴 Turn link OFF', callback_data: `promo_off_${code}` } : { text: '🟢 Turn link ON', callback_data: `promo_on_${code}` }],
    [{ text: '🔄 Refresh', callback_data: `promo_ref_${code}` }, { text: '🏠 Menu', callback_data: 'menu_home' }],
  ] };
  const text = hcard(`🪔 ${hEsc(cfg.title).toUpperCase()}`, rows);
  if (editId) return tgFetch('editMessageText', { chat_id: TG_CHAT, message_id: editId, text, parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: kb }).catch(() => tgSendH(text, { reply_markup: kb }));
  return tgSendH(text, { reply_markup: kb });
}


// ════════════════════════════════════════════════════════════════════
// V49 DIWALI BMW LUCKY DRAW
// draw/cfg { startAt, endAt, active, min, prize }   (timer starts on first use)
// draw/entries/{fuid} { id, fuid, uid, name, at, bal }  — one entry per account
// The minimum balance is checked only when the user taps Participate.
// ════════════════════════════════════════════════════════════════════
const DRAW_MIN = parseFloat(process.env.DRAW_MIN || '50');
const DRAW_DAYS = parseInt(process.env.DRAW_DAYS || '30', 10);
const DRAW_PRIZE = process.env.DRAW_PRIZE || 'BMW 5 Series';
const DRAW_C = { at: 0, v: null };
async function drawCfg(fresh = false) {
  if (!fresh && DRAW_C.v && Date.now() - DRAW_C.at < 15000) return DRAW_C.v;
  const ref = db.ref('draw/cfg');
  let v = (await ref.once('value')).val();
  if (!v || !v.endAt) {
    const now = Date.now();
    const tx = await ref.transaction(c => (c && c.endAt ? undefined : { startAt: now, endAt: now + DRAW_DAYS * 86400000, active: true, min: DRAW_MIN, prize: DRAW_PRIZE }));
    v = tx.snapshot.val();
  }
  v = { min: DRAW_MIN, prize: DRAW_PRIZE, active: true, ...(v || {}) };
  DRAW_C.v = v; DRAW_C.at = Date.now();
  return v;
}
const drawOpen = (c) => !!c && c.active !== false && Date.now() < Number(c.endAt || 0);
const drawUsdt = (u) => { const n = Number(u?.balances?.USDT ?? u?.balance ?? 0); return Number.isFinite(n) ? Math.max(0, n) : 0; };
function drawLeft(ms) {
  if (ms <= 0) return 'ended';
  const d = Math.floor(ms / 86400000), h = Math.floor(ms % 86400000 / 3600000), m = Math.floor(ms % 3600000 / 60000);
  return d ? `${d}d ${h}h left` : h ? `${h}h ${m}m left` : `${m}m left`;
}
const drawDate = (t) => new Date(Number(t) || 0).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });
const drawNoteQ = [];
setInterval(() => {
  if (!drawNoteQ.length) return;
  const lines = drawNoteQ.splice(0, 40);
  tgSend(`🚗 BMW LUCKY DRAW\n\n${lines.join('\n')}${drawNoteQ.length ? `\n\n…${drawNoteQ.length} more in next message` : ''}`, { parse_mode: undefined }).catch(() => {});
}, 20000);
async function drawEntries() {
  const snap = await db.ref('draw/entries').once('value');
  const all = [];
  snap.forEach(c => { const x = c.val(); if (x && x.id) all.push({ ...x, fuid: x.fuid || c.key }); });
  all.sort((a, b) => (b.at || 0) - (a.at || 0));
  return all;
}
async function sendDrawPanel(editId = null, extra = []) {
  const cfg = await drawCfg(true);
  const all = await drawEntries();
  const open = drawOpen(cfg);
  const rows = [
    open ? '🟢 <b>Draw is OPEN</b>' : (cfg.active === false ? '🔴 <b>Draw is OFF</b>' : '⌛ <b>Draw has ENDED</b>'),
    '',
    `🏆 Prize: <b>${hEsc(cfg.prize)}</b>`,
    `💵 Minimum balance to join: <b>$${hEsc(cfg.min)}</b> (checked at Participate)`,
    `🕒 Started: ${hEsc(drawDate(cfg.startAt))}`,
    `⏳ Ends: <b>${hEsc(drawDate(cfg.endAt))}</b> · ${hEsc(drawLeft(Number(cfg.endAt) - Date.now()))}`,
    '',
    `👥 Entries: <b>${all.length}</b>`,
  ];
  const recent = all.slice(0, 8);
  if (recent.length) {
    rows.push('', '<b>Latest entries</b>');
    for (const x of recent) rows.push(`${hCode(x.id)} · ${hCode(x.uid || '—')} ${hEsc(x.name || '')} · $${hEsc(r8(x.bal || 0))}`);
  }
  if (extra.length) rows.push('', ...extra);
  const kb = { inline_keyboard: [
    [cfg.active === false ? { text: '🟢 Turn draw ON', callback_data: 'draw_on' } : { text: '🔴 Turn draw OFF', callback_data: 'draw_off' }],
    [{ text: `🔁 Restart ${DRAW_DAYS}-day timer`, callback_data: 'draw_restart' }, { text: '🎲 Pick random winner', callback_data: 'draw_pick' }],
    [{ text: '🔄 Refresh', callback_data: 'draw_ref' }, { text: '🏠 Menu', callback_data: 'menu_home' }],
  ] };
  const text = hcard('🚗 BMW LUCKY DRAW', rows);
  if (editId) return tgFetch('editMessageText', { chat_id: TG_CHAT, message_id: editId, text, parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: kb }).catch(() => tgSendH(text, { reply_markup: kb }));
  return tgSendH(text, { reply_markup: kb });
}
async function drawPickWinner() {
  const all = await drawEntries();
  if (!all.length) return ['🎲 No entries yet.'];
  const x = all[crypto.randomInt(all.length)];
  let email = '—', phone = '—', banned = false;
  try { const u = (await db.ref(`users/${x.fuid}`).once('value')).val() || {}; email = u.email || '—'; phone = u.phone || '—'; banned = u.banned === true || u.frozen === true; } catch {}
  await db.ref('draw/picks').push({ at: Date.now(), id: x.id, fuid: x.fuid, uid: x.uid || null, of: all.length }).catch(() => {});
  return [
    `🎲 <b>Random pick</b> (out of ${all.length} entries)`,
    `Entry ${hCode(x.id)} · UID ${hCode(x.uid || '—')} ${hEsc(x.name || '')}`,
    `Email: ${hCode(email)} · Phone: ${hCode(phone)}`,
    `Joined ${hEsc(drawDate(x.at))} with $${hEsc(r8(x.bal || 0))}${banned ? '\n⚠️ This account is banned/frozen' : ''}`,
    'Nothing is announced automatically. Pick again if needed.',
  ];
}


// ════════════════════════════════════════════════════════════════════
// UID — every account gets a short UID (KYC or not). Runs for every
// existing user at boot (child_added) and for every new sign-up.
// ════════════════════════════════════════════════════════════════════
const UID_CH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const UID_DONE = new Set();
async function ensureUid(fuid, v) {
  if (!db || !fuid || UID_DONE.has(fuid)) return;
  if (v && v.uid) { UID_DONE.add(fuid); return; }
  for (let i = 0; i < 6; i++) {
    let id = ''; for (let j = 0; j < 6; j++) id += UID_CH[crypto.randomInt(32)];
    const t = await db.ref(`uidIndex/${id}`).transaction(c => (c ? undefined : fuid));
    if (!t.committed) continue;
    const r = await db.ref(`users/${fuid}/uid`).transaction(c => c || id);
    const final = r.snapshot.val();
    if (final !== id) await db.ref(`uidIndex/${id}`).remove().catch(() => {});
    UID_DONE.add(fuid);
    log('UID', `assigned ${final} → ${fuid}`);
    return;
  }
}

// ════════════════════════════════════════════════════════════════════
// LIVE SUPPORT — in-app chat  ⇄  this Telegram bot
// users/{fuid}/support/meta  { status: bot|waiting|live|closed, ... }
// users/{fuid}/support/msgs  { from: user|agent|bot|sys, text?, img?, ts, fwd? }
// ════════════════════════════════════════════════════════════════════
const SUP = { active: null };
const SUP_CHAIN = new Map();
const supRef = (f, p = '') => db.ref(`users/${f}/support${p ? '/' + p : ''}`);
const onceFlag = async (ref) => (await ref.transaction(c => (c ? undefined : Date.now()))).committed;
async function supLoadActive() { try { SUP.active = (await db.ref(`${SUP_STATE}/active`).once('value')).val() || null; } catch {} }
async function supSetActive(f) { SUP.active = f || null; try { await db.ref(`${SUP_STATE}/active`).set(f || null); } catch {} }
async function supLite(fuid) {
  const g = async (k) => (await db.ref(`users/${fuid}/${k}`).once('value')).val();
  const [uid, name, email, phone, nick] = await Promise.all([g('uid'), g('name'), g('email'), g('phone'), g('profile/nickname')]);
  return { uid: String(uid || String(fuid).slice(0, 8)).toUpperCase(), name: nick || name || 'User', email: email || '—', phone: phone || '—' };
}
const supPush = (fuid, m) => supRef(fuid, 'msgs').push({ ts: Date.now(), ...m });
/** Remember which user a support-bot message belongs to (for reply-tags). */
async function supMap(mid, fuid) { if (!mid || !fuid) return; try { await db.ref(`${SUP_STATE}/messages/${mid}`).set(fuid); } catch {} }
/** Which user is a reply-tag aimed at? Saved map first, then "UID XXXXXX" in the tagged text. */
async function supFuidFromReply(rtMsg) {
  if (!rtMsg?.message_id) return null;
  const mapped = (await db.ref(`${SUP_STATE}/messages/${rtMsg.message_id}`).once('value')).val();
  if (mapped) return mapped;
  const m = String(rtMsg.text || rtMsg.caption || '').match(/UID[:\s]*([A-Z0-9]{4,12})\b/);
  if (!m) return null;
  const f = await findUserByUID(m[1]).catch(() => null);
  return f && String(shownUID(f.fuid, f.user)).toUpperCase() === m[1].toUpperCase() ? f.fuid : null;
}
function supQueue(fuid) {
  const prev = SUP_CHAIN.get(fuid) || Promise.resolve();
  const next = prev.then(() => supProcess(fuid)).catch(e => log('SUP', `err ${e.message}`));
  SUP_CHAIN.set(fuid, next);
  next.finally(() => { if (SUP_CHAIN.get(fuid) === next) SUP_CHAIN.delete(fuid); });
  return next;
}
function supDataUrlToBuf(u) { const m = String(u || '').match(/^data:(image\/[\w+.-]+);base64,(.+)$/); return m ? { type: m[1], buf: Buffer.from(m[2], 'base64') } : null; }
async function supSendPhoto(buf, type, caption, extra = {}) {
  const fd = new FormData();
  fd.append('chat_id', String(TG_CHAT_SUPPORT));
  if (caption) fd.append('caption', String(caption).slice(0, 1000));
  if (extra.reply_markup) fd.append('reply_markup', JSON.stringify(extra.reply_markup));
  fd.append('photo', new Blob([buf], { type: type || 'image/jpeg' }), 'photo.jpg');
  try { return await (await fetch(`${TG_API_SUP}/sendPhoto`, { method: 'POST', body: fd })).json(); } catch (e) { return { ok: false, error: e.message }; }
}
async function supForward(fuid, lite, m) {
  const head = `💬 ${lite.name} · UID ${lite.uid}`;
  const kb = SUP.active === fuid ? undefined : { inline_keyboard: [[{ text: '↩️ Reply to this user', callback_data: `sup_sw_${fuid}` }, { text: '🔚 End', callback_data: `sup_end_${fuid}` }]] };
  let r;
  const ph = m.img && supDataUrlToBuf(m.img);
  if (ph) r = await supSendPhoto(ph.buf, ph.type, `${head}${m.text ? '\n\n' + m.text : ''}`, kb ? { reply_markup: kb } : {});
  else r = await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text: `${head}\n\n${String(m.text || '').slice(0, 3500)}`, ...(kb ? { reply_markup: kb } : {}) });
  const mid = r?.result?.message_id;
  if (mid) await db.ref(`${SUP_STATE}/messages/${mid}`).set(fuid);
  return !!mid;
}
async function supProcess(fuid) {
  if(!SUPPORT_ENABLED)return;
  const notifiedKey=`notified_${SUP_SCOPE}`,cardKey=`card_${SUP_SCOPE}`,cancelKey=`cancel_${SUP_SCOPE}`,endKey=`end_${SUP_SCOPE}`,fwdKey=`fwd_${SUP_SCOPE}`;
  const s = (await supRef(fuid).once('value')).val();
  if (!s || !s.meta) return;
  const meta = s.meta, msgs = s.msgs || {};
  // V31: scoped flags from an EARLIER session must not block a new request.
  // The app clears only unscoped flags, so without this a user's 2nd+ live
  // request never reached the support bot and stayed on "Connecting…" forever.
  // Each flag remembers which request (reqAt / closedAt) it belongs to, so phone
  // clock differences cannot cause duplicate cards.
  {
    const reqAt = Number(meta.reqAt || 0), closedAt = Number(meta.closedAt || 0), reset = {};
    const stale = (k, mark) => meta[k] && (meta[k + '_r'] != null ? Number(meta[k + '_r']) !== mark : Number(meta[k]) < mark);
    if (['waiting','live'].includes(meta.status) && stale(notifiedKey, reqAt)) { reset[notifiedKey] = null; reset[notifiedKey + '_r'] = null; reset[cardKey] = null; }
    if (stale(cancelKey, reqAt)) { reset[cancelKey] = null; reset[cancelKey + '_r'] = null; }
    if (meta.status === 'closed' && stale(endKey, closedAt)) { reset[endKey] = null; reset[endKey + '_r'] = null; }
    if (Object.keys(reset).length) { await supRef(fuid, 'meta').update(reset); Object.keys(reset).forEach(k => { meta[k] = null; }); log('SUP', `new session for ${fuid} — cleared old flags`); }
  }
  // 1) new request → Accept / Busy card for the admin
  if (['waiting','live'].includes(meta.status) && !meta[notifiedKey] && await onceFlag(supRef(fuid, `meta/${notifiedKey}`))) {
    const lite = await supLite(fuid);
    const ctx = Object.values(msgs).filter(x => x && x.ts >= (meta.sessionAt || 0)).sort((a, b) => a.ts - b.ts).slice(-6)
      .map(x => `${x.from === 'user' ? '👤' : x.from === 'agent' ? '🎧' : '🤖'} ${x.img ? '[photo] ' : ''}${String(x.text || '').slice(0, 140)}`).join('\n');
    const r = await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT,
      text: `🎧 NEW SUPPORT REQUEST\n\n👤 ${lite.name} · UID ${lite.uid}\n📧 ${lite.email}\n📱 ${lite.phone}\n📝 ${meta.topic || 'General'}\n\nRecent messages\n${ctx || '—'}\n\n↩️ After accepting, reply (swipe) on this user's message to answer only them.`,
      reply_markup: { inline_keyboard: [meta.status==='live' ? [{text:'Reply to user',callback_data:`sup_sw_${fuid}`},{text:'End chat',callback_data:`sup_end_${fuid}`}] : [{ text: '✅ Accept chat', callback_data: `sup_acc_${fuid}` }, { text: '⏳ Busy', callback_data: `sup_rej_${fuid}` }]] } });
    if (r?.result?.message_id) { await supRef(fuid, 'meta').update({ [cardKey]: r.result.message_id, [notifiedKey + '_r']: Number(meta.reqAt || 0) }); await supMap(r.result.message_id, fuid); }
    else {await supRef(fuid, `meta/${notifiedKey}`).remove();return;}
    log('SUP', `request from ${lite.uid}`);
  }
  // 2) user cancelled while waiting
  if (meta.cancelled && !meta[cancelKey] && await onceFlag(supRef(fuid, `meta/${cancelKey}`))) {
    await supRef(fuid, 'meta').update({ [cancelKey + '_r']: Number(meta.reqAt || 0) });
    const lite = await supLite(fuid);
    if (meta[cardKey]) await tgFetchSup('editMessageText', { chat_id: TG_CHAT_SUPPORT, message_id: meta[cardKey], text: `🎧 Support request from ${lite.name} · UID ${lite.uid}\n\n❌ Cancelled by user` });
  }
  // 3) live → forward every unsent user message (photos too)
  if (meta.status === 'live') {
    let lite = null;
    const keys = Object.keys(msgs).filter(k => msgs[k] && msgs[k].from === 'user' && !msgs[k][fwdKey] && (msgs[k].ts || 0) >= (meta.reqAt || 0) - 1000)
      .sort((a, b) => (msgs[a].ts || 0) - (msgs[b].ts || 0));
    for (const k of keys) {
      if (!(await onceFlag(supRef(fuid, `msgs/${k}/${fwdKey}`)))) continue;
      lite = lite || await supLite(fuid);
      const sent=await supForward(fuid, lite, msgs[k]);
      if(!sent){await supRef(fuid, `msgs/${k}/${fwdKey}`).remove();break;}
    }
  }
  // 4) user ended the chat
  if (meta.status === 'closed' && meta.closedBy === 'user' && !meta[endKey] && await onceFlag(supRef(fuid, `meta/${endKey}`))) {
    await supRef(fuid, 'meta').update({ [endKey + '_r']: Number(meta.closedAt || 0) });
    const lite = await supLite(fuid);
    if (SUP.active === fuid) await supSetActive(null);
    await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text: `🔚 ${lite.name} · UID ${lite.uid} ended the support chat.` });
  }
}
async function supAgentMsg(fuid, m) {
  await supPush(fuid, { from: 'agent', ...m });
  await supRef(fuid, 'meta').update({ lastAgentAt: Date.now() });
}
async function handleSupCb(cb, act, fuid) {
  if (!db) return tgAnswerSup(cb.id, 'Backend offline');
  const lite = await supLite(fuid);
  if (act === 'acc') {
    const t = await supRef(fuid, 'meta').transaction(c => { if (!c || c.status !== 'waiting') return; c.status = 'live'; c.acceptedAt = Date.now(); c.agent = 'Elina · BIEXC Support'; c.agentAvatar = 'elina'; return c; });
    if (!t.committed) return tgAnswerSup(cb.id, 'Already handled ✔', true);
    await supPush(fuid, { from: 'sys', text: 'A support agent has joined the chat ✅' });
    await supSetActive(fuid);
    await tgAnswerSup(cb.id, '✅ Connected');
    if (cb.message?.message_id) await tgFetchSup('editMessageReplyMarkup', { chat_id: TG_CHAT_SUPPORT, message_id: cb.message.message_id, reply_markup: { inline_keyboard: [[{ text: '✅ Accepted — chatting now', callback_data: 'noop_done' }]] } });
    const started = await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text: `🟢 Live chat started — ${lite.name} · UID ${lite.uid}\n\n↩️ Reply (swipe) on any message of a user → goes ONLY to that user.\n✍️ Normal message → goes to the active chat (${lite.uid}).\n👍 on your message = delivered.\n\n/chats — open chats · /end — finish active chat`,
      reply_markup: { inline_keyboard: [[{ text: '🔚 End chat', callback_data: `sup_end_${fuid}` }]] } });
    await supMap(started?.result?.message_id, fuid);
    return supQueue(fuid);
  }
  if (act === 'rej') {
    const t = await supRef(fuid, 'meta').transaction(c => { if (!c || c.status !== 'waiting') return; c.status = 'bot'; c.busyAt = Date.now(); return c; });
    if (!t.committed) return tgAnswerSup(cb.id, 'Already handled ✔', true);
    await supPush(fuid, { from: 'sys', text: 'All our agents are busy right now. Please try again in a few minutes — our assistant can still help you meanwhile.' });
    await tgAnswerSup(cb.id, 'Marked busy');
    if (cb.message?.message_id) await tgFetchSup('editMessageReplyMarkup', { chat_id: TG_CHAT_SUPPORT, message_id: cb.message.message_id, reply_markup: { inline_keyboard: [[{ text: '⏳ Marked busy', callback_data: 'noop_done' }]] } });
    return;
  }
  if (act === 'end') return supEnd(fuid, cb.id);
  if (act === 'sw') {
    const st = (await supRef(fuid, 'meta/status').once('value')).val();
    if (st !== 'live') return tgAnswerSup(cb.id, 'This chat is not live any more', true);
    await supSetActive(fuid);
    await tgAnswerSup(cb.id, `Now replying to ${lite.uid}`);
    const sw = await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text: `↩️ Active chat: ${lite.name} · UID ${lite.uid}. Type your message.` });
    return supMap(sw?.result?.message_id, fuid);
  }
}
async function supEnd(fuid, cbId) {
  const lite = await supLite(fuid);
  const t = await supRef(fuid, 'meta').transaction(c => { if (!c || c.status === 'closed') return; c.status = 'closed'; c.closedBy = 'agent'; c.closedAt = Date.now(); return c; });
  if (SUP.active === fuid) await supSetActive(null);
  if (!t.committed) { if (cbId) await tgAnswerSup(cbId, 'Already ended ✔'); return; }
  await supPush(fuid, { from: 'sys', text: 'The agent has ended this chat. Thank you for contacting BIEXC Support 💛' });
  if (cbId) await tgAnswerSup(cbId, 'Chat ended');
  await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text: `🔚 Chat with ${lite.name} · UID ${lite.uid} ended.` });
}
async function supList() {
  const snap = await db.ref('users').once('value');
  const rows = [];
  snap.forEach(c => { const m = c.val()?.support?.meta; if (m && (m.status === 'live' || m.status === 'waiting')) rows.push({ fuid: c.key, m, u: c.val() }); });
  if (!rows.length) return tgSendSup('🎧 No open support chats right now.');
  const kb = rows.slice(0, 20).map(r => [{ text: `${r.m.status === 'live' ? '🟢' : '🟡'} ${(r.u.profile?.nickname || r.u.name || 'User').slice(0, 20)} · ${shownUID(r.fuid, r.u)}${SUP.active === r.fuid ? ' (active)' : ''}`, callback_data: r.m.status === 'live' ? `sup_sw_${r.fuid}` : `sup_acc_${r.fuid}` }]);
  return tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text: `🎧 Open support chats (${rows.length})\n🟢 live — tap to reply  ·  🟡 waiting — tap to accept`, reply_markup: { inline_keyboard: kb } });
}
async function supTgPhotoToDataUrl(photos) {
  const list = [...photos].sort((a, b) => (a.file_size || 0) - (b.file_size || 0));
  const pick = list.filter(p => (p.file_size || 0) <= 700_000).pop() || list[0];
  const j = await (await fetch(`${TG_API_SUP}/getFile?file_id=${encodeURIComponent(pick.file_id)}`)).json();
  if (!j.ok) return null;
  const r = await fetch(`https://api.telegram.org/file/bot${TG_TOKEN_SUPPORT}/${j.result.file_path}`);
  const buf = Buffer.from(await r.arrayBuffer());
  return `data:image/jpeg;base64,${buf.toString('base64')}`;
}
/** Admin message → user's in-app chat. Returns true when consumed. */
async function supRouteAdmin(msg, text) {
  if (!db || msg.from?.is_bot || String(msg.chat?.id)!==String(TG_CHAT_SUPPORT)) return false;
  if (text.startsWith('/')) {
    if (/^\/chats?\b/i.test(text)) { await supList(); return true; }
    if (/^\/end\b/i.test(text)) { if (SUP.active) await supEnd(SUP.active); else await tgSendSup('No active support chat.'); return true; }
    return false;
  }
  let fuid = null;
  const rtMsg = msg.reply_to_message, rt = rtMsg?.message_id;
  // V43: a reply-tag ALWAYS wins and goes only to the tagged user — even when
  // several chats are open and a different chat is "active".
  if (rt) {
    fuid = await supFuidFromReply(rtMsg);
    if (!fuid) { await tgSendSup('⚠️ I could not tell which user this reply is for. Reply directly on the user\'s message, or open /chats.', { parse_mode: undefined, reply_to_message_id: msg.message_id }); return true; }
  } else { fuid = SUP.active; }
  if (!fuid) return false;
  const st = (await supRef(fuid, 'meta/status').once('value')).val();
  if (st !== 'live') { if (!rt) { await supSetActive(null); return false; } await tgSendSup('⚠️ That chat is closed — the user did not get this message.', { parse_mode: undefined, reply_to_message_id: msg.message_id }); return true; }
  const delivered = async (m) => { await supAgentMsg(fuid, m); await supMap(msg.message_id, fuid); tgReactSup(msg.message_id).catch(() => {}); return true; };
  const caption = msg.caption || '';
  if (Array.isArray(msg.photo) && msg.photo.length) {
    const img = await supTgPhotoToDataUrl(msg.photo).catch(() => null);
    if (!img) { await tgSendSup('⚠️ Could not deliver that photo, please try again.'); return true; }
    return delivered({ img, ...(caption ? { text: caption } : {}) });
  }
  if (msg.document && /^image\//.test(msg.document.mime_type || '')) {
    const img = await supTgPhotoToDataUrl([{ file_id: msg.document.file_id, file_size: msg.document.file_size }]).catch(() => null);
    if (img) return delivered({ img, ...(caption ? { text: caption } : {}) });
  }
  if (text) return delivered({ text: text.slice(0, 4000) });
  if (msg.sticker || msg.document || msg.video || msg.voice) { await tgSendSup('ℹ️ Only text and photos can be sent to the user.'); return true; }
  return false;
}

// ─── KYC MOBILE OTP (WhatsApp) ──────────────────────────────────────
const KYC_OTP = new Map(); // fuid -> { phone, code, exp, tries, last, sent[] }
async function fuidFromIdToken(req) {
  const t = String(req.body?.idToken || '');
  if (!t) return null;
  try { return (await admin.auth().verifyIdToken(t)).uid; } catch { return null; }
}

// ════════════════════════════════════════════════════════════════════
// AUTO DEPOSIT — USDT BEP-20 only, one shared admin wallet, 60s window
// ════════════════════════════════════════════════════════════════════
const AD = {
  USDT: '0x55d398326f99059ff775485246999027b3197955',
  TOPIC: '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
  RPCS: (process.env.BSC_RPC_URLS || 'https://bsc-dataseed.binance.org,https://bsc-rpc.publicnode.com').split(',').map(s => s.trim()).filter(Boolean),
  CONF: 3, WINDOW: 60_000, MIN: parseFloat(process.env.AUTO_DEPOSIT_MIN || '1'),
  DEFAULT_WALLET: (process.env.DEPOSIT_WALLET || '0x34df4807cafcc8ab0b251f8d9e5abd3b15f12172').toLowerCase(),
  rlStart: new Map(), rlCheck: new Map(), scanning: false,
};
async function adRpc(method, params) {
  let lastErr;
  for (const url of AD.RPCS) {
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), timeout: 12_000 });
      const j = await r.json();
      if (j.error) throw new Error(j.error.message || 'rpc error');
      return j.result;
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error('all RPCs failed');
}
const adBlock = async () => parseInt(await adRpc('eth_blockNumber', []), 16);
const AD_TS = new Map();
async function adBlockTs(n) {
  if (AD_TS.has(n)) return AD_TS.get(n);
  const b = await adRpc('eth_getBlockByNumber', ['0x' + n.toString(16), false]);
  const ts = parseInt(b.timestamp, 16) * 1000;
  if (AD_TS.size > 5000) AD_TS.clear();
  AD_TS.set(n, ts); return ts;
}
/** Average BSC block time in ms (measured, cached for 10 min). */
let AD_BT = { v: 3000, at: 0 };
async function adBlockMs(latest) {
  if (Date.now() - AD_BT.at < 600_000) return AD_BT.v;
  try { const a = await adBlockTs(latest), b = await adBlockTs(latest - 200); AD_BT = { v: Math.max(200, (a - b) / 200), at: Date.now() }; } catch {}
  return AD_BT.v;
}
const AD_GRACE = 60_000;   // V43: a transfer up to 1 min before "I Have Paid" belongs to that user
const AD_HOLD = 90_000;    // the background scan leaves fresh transfers for a click first
async function adWallet() {
  const v = (await db.ref('settings/depositAddress').once('value')).val();
  return (/^0x[0-9a-fA-F]{40}$/.test(v || '') ? v : AD.DEFAULT_WALLET).toLowerCase();
}
function adParse(l) {
  return {
    txHash: l.transactionHash.toLowerCase(), logIndex: parseInt(l.logIndex, 16), block: parseInt(l.blockNumber, 16),
    from: '0x' + l.topics[1].slice(26).toLowerCase(),
    amt: Math.floor(Number(BigInt(l.data) / 10n ** 10n)) / 1e8, // 18 decimals → 8 dp
  };
}
async function adLogs(wallet, fromBlock, toBlock) {
  if (toBlock < fromBlock) return [];
  const out = [];
  for (let a = fromBlock; a <= toBlock; a += 2000) {
    const b = Math.min(toBlock, a + 1999);
    const logs = await adRpc('eth_getLogs', [{ address: AD.USDT, fromBlock: '0x' + a.toString(16), toBlock: '0x' + b.toString(16),
      topics: [AD.TOPIC, null, '0x' + wallet.slice(2).padStart(64, '0')] }]);
    for (const l of logs || []) if (!l.removed) out.push(adParse(l));
  }
  return out;
}
/** Credit one on-chain transfer exactly once (deposits/{tx}_{idx} claimed in a transaction). */
async function adCredit(fuid,t,source){
 const amt=Number(t.amt),key=`${t.txHash}_${t.logIndex}`,ts=Date.now();
 if(!Number.isFinite(amt)||amt<=0||!/^[A-Za-z0-9_-]+$/.test(key))throw new Error('Invalid transfer');
 const claim=await db.ref(`deposits/${key}`).transaction(cur=>cur?undefined:{...t,fuid,source,ts,status:'crediting',atomicCreditVersion:1});
 const existing=claim.snapshot.val();if(!existing||existing.fuid!==fuid||existing.status==='credited')return null;
 // Never re-credit a pre-upgrade claim: its old balance-write outcome is unknown.
 if(!claim.committed&&!existing.atomicCreditVersion)throw new Error('Legacy deposit claim requires review');
 const hid='h_auto_'+key;let res=null;
 const credit=await db.ref(`users/${fuid}`).transaction(cur=>{
  res=null;if(!cur)return;
  const receipt=cur.depositCredits?.[key];if(receipt){res={oldBal:receipt.oldBal,newBal:receipt.newBal,dup:true};return cur;}
  const old=Number(cur.balances?.USDT??cur.balance??0);if(!Number.isFinite(old))return;
  const next=r8(old+amt);res={oldBal:old,newBal:next};
  return {...cur,balances:{...(cur.balances||{}),USDT:next},balance:next,
   depositCredits:{...(cur.depositCredits||{}),[key]:{...res,hid,amt,ts}},
   history:{...(cur.history||{}),[hid]:{hid,ts,date:nowIST(),type:'DEPOSIT',coin:'USDT',amt,network:'BEP-20',txid:t.txHash,hash:t.txHash,hashAt:ts,from:t.from,auto:true,status:'COMPLETED'}}};
 });
 if(!credit.committed||!res)throw new Error('Deposit not credited; retry safely');
 await db.ref(`deposits/${key}`).update({status:'credited',hid});
 await db.ref(`unmatchedDeposits/${key}`).remove().catch(()=>{});
 if(REF)REF.onDeposit(fuid,'USDT',amt).catch(()=>{});bonusCountAction(fuid,`dep_auto_${key}`).catch(()=>{});
 pushNotif(fuid,{title:'Deposit received',body:`${amt} USDT credited.`,type:'INFO'}).catch(()=>{});
 if(!res.dup)adNotifyCredited(fuid,t,source,res,key,hid).catch(e=>log('AUTODEP',`notify ${e.message}`));
 return {...t,hid,newBal:res.newBal};
}
async function adUnmatched(t, reason) {
  const key = `${t.txHash}_${t.logIndex}`;
  if ((await db.ref(`deposits/${key}`).once('value')).exists()) return;
  const tx = await db.ref(`unmatchedDeposits/${key}`).transaction(cur => cur ? undefined : { ...t, reason, status: 'pending_review', ts: Date.now() });
  if (!tx.committed) return;
  log('AUTODEP', `UNMATCHED ${t.amt} USDT tx=${t.txHash} (${reason})`);
  const why = /no open deposit window/.test(reason) ? 'Nobody tapped “I have paid” within 1 minute' : reason;
  const b = bscBtn(t.txHash);
  const r = await tgSendH(hcard('⚠️ UNASSIGNED DEPOSIT', [
    `💰 <b>${hEsc(fmtNum(t.amt))} USDT</b> · BEP-20`,
    `📤 From ${hCode(t.from)}`,
    `ℹ️ ${hEsc(why)}`,
    '',
    '↩️ <b>Reply to this message with the user\'s UID</b> to assign it.',
  ]), b ? { reply_markup: { inline_keyboard: [b] } } : {}).catch(() => null);
  const mid = r?.result?.message_id;
  if (mid) { await db.ref(`botState/depAssignMsg/${mid}`).set(key).catch(() => {}); await db.ref(`unmatchedDeposits/${key}/tgMsgId`).set(mid).catch(() => {}); }
}
/** Bot notice + user email after a deposit is credited (auto-detected or assigned). */
async function adNotifyCredited(fuid, t, source, res, key, hid) {
  const g = async (k) => (await db.ref(`users/${fuid}/${k}`).once('value')).val();
  const [uid, name, email, nick] = await Promise.all([g('uid'), g('name'), g('email'), g('profile/nickname')]);
  const how = source === 'admin' ? 'Assigned by admin' : source === 'window' ? 'Auto-detected · user tapped “I have paid”' : 'Auto-detected';
  const b = bscBtn(t.txHash, '🔎 View transaction');
  await tgSendH(hcard('✅ DEPOSIT CREDITED', [
    `👤 <b>${hEsc(nick || name || 'User')}</b> · ${hCode(uid || '—')}`,
    '',
    `💰 <b>+${hEsc(fmtNum(t.amt))} USDT</b> · BEP-20`,
    `💼 ${hEsc(fmtNum(res.oldBal))} → <b>${hEsc(fmtNum(res.newBal))}</b> USDT`,
    `📤 From ${hCode(t.from)}`,
    `⚙️ ${hEsc(how)}`,
  ]), b ? { reply_markup: { inline_keyboard: [b] } } : {}).catch(() => {});
  queueUserTxMail(`dep_auto_${key}`, { uid, name, email }, {
    subject: `Deposit Completed — ${r8(t.amt)} USDT`, headline: 'Deposit Completed',
    message: source === 'admin'
      ? 'Your USDT (BEP-20) deposit has been verified by our team and credited to your Spot Wallet. Funds are available immediately.'
      : 'Your USDT (BEP-20) deposit was detected on-chain and credited to your Spot Wallet automatically. Funds are available immediately.',
    amount: `${r8(t.amt)} USDT`, status: 'COMPLETED', label: 'Deposit', type: 'dep', coin: 'USDT', network: 'BEP-20',
    address: t.from, txid: t.txHash, hid, oldBal: res.oldBal, newBal: res.newBal,
  });
}
/** Admin replied to an UNASSIGNED DEPOSIT card with a UID → ask Confirm / Reject. */
async function depAssignReply(msg, text) {
  const rt = msg.reply_to_message?.message_id; if (!rt) return false;
  const key = (await db.ref(`botState/depAssignMsg/${rt}`).once('value')).val();
  if (!key) return false;
  const opt = { reply_to_message_id: msg.message_id };
  const t = (await db.ref(`unmatchedDeposits/${key}`).once('value')).val();
  if (!t || t.status !== 'pending_review') { await tgSendH('ℹ️ This deposit is already assigned or closed.', opt); return true; }
  const want = String(text || '').trim().replace(/^#/, '').split(/\s+/)[0].toUpperCase();
  if (!/^[A-Z0-9]{4,12}$/.test(want)) { await tgSendH('✍️ Reply with only the user\'s UID, e.g. <code>AB12CD</code>', opt); return true; }
  const f = await findUserByUID(want).catch(() => null);
  if (!f || String(shownUID(f.fuid, f.user)).toUpperCase() !== want) { await tgSendH(`❌ UID ${hCode(want)} not found. Reply again with the correct UID.`, opt); return true; }
  const rid = crypto.randomBytes(5).toString('hex');
  await db.ref(`botState/depAssignReq/${rid}`).set({ key, fuid: f.fuid, uid: want, cardId: rt, ts: Date.now() });
  const u = f.user || {}, bal = Number(u.balances?.USDT ?? u.balance ?? 0);
  const b = bscBtn(t.txHash);
  await tgSendH(hcard('🧾 CONFIRM DEPOSIT ASSIGNMENT', [
    `💰 <b>${hEsc(fmtNum(t.amt))} USDT</b> · BEP-20`,
    `👤 <b>${hEsc(u.profile?.nickname || u.name || 'User')}</b> · ${hCode(want)}`,
    u.email ? `📧 ${hEsc(u.email)}` : null,
    `💼 Balance now ${hEsc(fmtNum(bal))} USDT → <b>${hEsc(fmtNum(r8(bal + Number(t.amt))))}</b>`,
  ]), { ...opt, reply_markup: { inline_keyboard: [[{ text: '✅ Confirm', callback_data: `dasg_ok_${rid}` }, { text: '✖ Reject', callback_data: `dasg_no_${rid}` }], ...(b ? [b] : [])] } });
  return true;
}
async function handleDepAssignCb(cb, act, rid) {
  const ref = db.ref(`botState/depAssignReq/${rid}`);
  const r = (await ref.once('value')).val();
  const mid = cb.message?.message_id;
  if (!r) { await tgAnswer(cb.id, 'This request has expired.', true); return; }
  const lock = await claimTapOnce(`dasg_${rid}`, act === 'ok' ? 'approve' : 'reject');
  if (!lock.ok) { await tgAnswer(cb.id, `Already ${doneWord(lock.prev)}`, true); return; }
  if (act === 'no') {
    await tgAnswer(cb.id, 'Rejected');
    await ref.remove().catch(() => {});
    if (mid) await tgEditH(mid, '✖ <b>Assignment rejected.</b> The deposit is still unassigned — reply to the deposit card with another UID.').catch(() => {});
    return;
  }
  const t = (await db.ref(`unmatchedDeposits/${r.key}`).once('value')).val();
  if (!t || t.status !== 'pending_review') { await tgAnswer(cb.id, 'Already assigned or closed.', true); await ref.remove().catch(() => {}); return; }
  await tgAnswer(cb.id, 'Crediting…');
  let c = null;
  try { c = await adCredit(r.fuid, t, 'admin'); }
  catch (e) { await releaseTap(`dasg_${rid}`); await tgSendH(`❌ Could not credit: ${hEsc(e.message)}`, mid ? { reply_to_message_id: mid } : {}); return; }
  await ref.remove().catch(() => {});
  if (!c) { if (mid) await tgEditH(mid, 'ℹ️ This deposit was already credited.').catch(() => {}); return; }
  if (mid) tgFetch('deleteMessage', { chat_id: TG_CHAT, message_id: mid }).catch(() => {});
  const b = bscBtn(t.txHash);
  if (r.cardId) tgEditH(r.cardId, hcard('✅ DEPOSIT ASSIGNED', [`💰 <b>${hEsc(fmtNum(t.amt))} USDT</b> · BEP-20`, `👤 Credited to ${hCode(r.uid)}`, `📤 From ${hCode(t.from)}`]), b ? { reply_markup: { inline_keyboard: [b] } } : {}).catch(() => {});
}
/** Who owns a transfer: saved sender wallet, else a deposit window covering its block. */
async function adOwner(t) {
  const ts = await adBlockTs(t.block).catch(() => 0);
  const lock = (await db.ref('settings/depositLock').once('value')).val();
  if (lock && lock.status === 'open' && t.block >= lock.startBlock && ts >= (lock.since || lock.startedAt)) return lock.fuid;
  const past = (await db.ref('depositWindows').orderByChild('endBlock').startAt(t.block).once('value')).val() || {};
  for (const w of Object.values(past)) if (t.block >= w.startBlock && t.block <= w.endBlock && ts >= (w.since || w.startedAt || 0)) return w.fuid;
  const byWallet = (await db.ref('depositWalletIndex/' + t.from).once('value')).val();
  return byWallet || null;
}
async function adHandle(t) {
  const prior=(await db.ref(`deposits/${t.txHash}_${t.logIndex}`).once('value')).val();
  if(prior){if(prior.status==='crediting'&&prior.atomicCreditVersion===1)return adCredit(prior.fuid,prior,prior.source||'recovery');return null;}
  const owner = await adOwner(t);
  if (!owner) return adUnmatched(t, 'no open deposit window'), null;
  if (t.amt < AD.MIN) return adUnmatched(t, `below minimum ${AD.MIN} USDT`), null;
  return adCredit(owner, t, 'scan');
}
async function adCloseLock(lock, status) {
  if (!lock) return;
  let endBlock = lock.startBlock + 60;
  try { const l = await adBlock(); endBlock = l + Math.ceil(20_000 / await adBlockMs(l)); } catch {}
  await db.ref(`depositWindows/${lock.id}`).set({ fuid: lock.fuid, startBlock: lock.startBlock, endBlock, since: lock.since || lock.startedAt, startedAt: lock.startedAt, closedAt: Date.now(), status });
  await db.ref('settings/depositLock').transaction(cur => (cur && cur.id === lock.id) ? null : cur);
}
async function adCleanup() {
  const lock = (await db.ref('settings/depositLock').once('value')).val();
  if (lock && Date.now() > lock.expiresAt) await adCloseLock(lock, 'expired');
  const old = (await db.ref('depositWindows').orderByChild('closedAt').endAt(Date.now() - 24 * 3600_000).once('value')).val() || {};
  for (const k of Object.keys(old)) db.ref(`depositWindows/${k}`).remove().catch(() => {});
}
async function adScan() {
  if (AD.scanning) return; AD.scanning = true;
  try {
    const pending=(await db.ref('deposits').orderByChild('status').equalTo('crediting').limitToFirst(25).once('value')).val()||{};
    for(const t of Object.values(pending)){if(t.atomicCreditVersion===1)await adCredit(t.fuid,t,t.source||'recovery').catch(e=>log('AUTODEP',`retry ${e.message}`));}
    const wallet = await adWallet();
    const latest = await adBlock();
    const safe = latest - Math.max(AD.CONF, Math.ceil(AD_HOLD / await adBlockMs(latest)));
    let from = (await db.ref('settings/lastScannedBlock').once('value')).val();
    if (!from) from = safe - 200;
    from = Math.max(from, safe - 20_000);
    if (safe <= from) return;
    for (const t of await adLogs(wallet, from + 1, safe)) await adHandle(t).catch(e => log('AUTODEP', `handle ${e.message}`));
    await db.ref('settings/lastScannedBlock').set(safe);
  } catch (e) { log('AUTODEP', `scan ${e.message}`); } finally { AD.scanning = false; }
}
function adLimited(map, fuid, ms) { const n = Date.now(), l = map.get(fuid) || 0; if (n - l < ms) return Math.ceil((ms - (n - l)) / 1000); map.set(fuid, n); return 0; }

function mountAutoDeposit(app) {
  app.post('/api/bonus/claim', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      let out = null, why = 'not_eligible';
      const tx = await db.ref(`users/${fuid}`).transaction(cur => {
        out = null; why = 'not_eligible'; if (!cur) return cur;
        const x = cur.bonus || {};
        if (x.kind !== 'REFERRAL' || !isReferred(cur)) return;
        if (x.welcomeStatus === 'UNLOCKED') { why = 'already_claimed'; return; }
        if (x.welcomeStatus !== 'LOCKED' && x.welcomeStatus !== 'CLAIMABLE') return;
        const n = countRefBuys(cur, x.welcomeAt || 0);
        if (n < REF_BUYS) { why = 'need_trades'; out = { n }; return; }
        const amt = Number(x.welcomeAmt) || REF_BONUS;
        const bal = { ...(cur.balances || {}) }, old = Number(bal.USDT ?? cur.balance ?? 0);
        if (!Number.isFinite(old)) return;
        bal.USDT = r8(old + amt);
        const now = Date.now(), hid = `h_bonus_referral_${now}`;
        out = { amt, old, nb: bal.USDT };
        return { ...cur, balances: bal, balance: bal.USDT,
          bonus: { ...x, welcomeStatus: 'UNLOCKED', tradesCompleted: n, unlockedAt: now, claimedAt: now, unlockedBy: 'claim',
            log: { ...(x.log || {}), [`l${now}u`]: { type: 'UNLOCKED', text: `Claimed — $${amt} added to main balance`, ts: now } } },
          history: { ...(cur.history || {}), [hid]: { hid, ts: now, date: nowIST(), type: 'REFERRAL_BONUS', coin: 'USDT', amt, status: 'COMPLETED', note: `Referral bonus claimed after ${REF_BUYS} P2P buys` } } };
      });
      if (!tx.committed || !out || out.amt == null) return res.json({ ok: false, error: why, done: out?.n || 0, need: REF_BUYS });
      const v = tx.snapshot.val() || {};
      log('BONUS', `referral claim +${out.amt} ${fuid}`);
      bonusNote(`✅ $${out.amt} claimed → ${v.uid || fuid}${v.name ? ' ' + v.name : ''} · balance ${out.old} → ${out.nb}`);
      res.json({ ok: true, amt: out.amt, balance: out.nb });
    } catch (e) { log('BONUS', `claim err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });

  // ─── V48 festive link bonus ───
  const PROMO_RL = new Map();
  const promoPub = (p) => p ? { code: p.code, amt: p.amt, need: p.need, done: p.done || 0, at: p.at, exp: p.exp, status: p.status } : null;
  app.get('/api/promo/info', async (req, res) => {
    try {
      const cfg = await promoCfg(req.query?.code);
      if (!cfg) return res.json({ ok: false, error: 'invalid' });
      res.json({ ok: true, code: cfg.code, title: cfg.title, amt: cfg.amt, days: cfg.days, need: cfg.need, active: cfg.active });
    } catch { res.json({ ok: false, error: 'server' }); }
  });
  app.post('/api/promo/activate', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      if (adLimited(PROMO_RL, fuid, 3000)) return res.json({ ok: false, error: 'slow_down' });
      const cfg = await promoCfg(req.body?.code);
      if (!cfg) return res.json({ ok: false, error: 'invalid' });
      const code = cfg.code;
      const u = (await db.ref(`users/${fuid}`).once('value')).val();
      if (!u) return res.json({ ok: false, error: 'no_user' });
      const have = u.promos && u.promos[code];
      if (have) return res.json({ ok: true, already: true, promo: promoPub(have) });
      if (u.banned === true || u.frozen === true) return res.json({ ok: false, error: 'blocked' });
      if (!cfg.active) return res.json({ ok: false, error: 'ended' });
      const k = kycInfo(u);
      if (!k.done) return res.json({ ok: false, error: 'kyc', pending: !!k.pending });
      const now = Date.now();
      const rec = { code, at: now, exp: now + cfg.days * 86400000, amt: cfg.amt, need: cfg.need, days: cfg.days, status: 'LOCKED', done: 0, uid: u.uid || null };
      let created = false;
      const ix = await db.ref(`promoIndex/${code}/${fuid}`).transaction(cur => { created = false; if (cur) return; created = true; return rec; });
      const finalRec = (ix.snapshot && ix.snapshot.val()) || rec;
      await db.ref(`users/${fuid}/promos/${code}`).transaction(cur => cur || { ...finalRec, status: finalRec.status || 'LOCKED' });
      if (created) {
        log('PROMO', `activated ${code} ${fuid}`);
        promoNote(`🪔 Activated → ${u.uid || fuid}${u.name ? ' ' + u.name : ''} ($${cfg.amt}, ${cfg.days}d)`);
      }
      res.json({ ok: true, already: !created, promo: promoPub(finalRec) });
    } catch (e) { log('PROMO', `activate err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });
  app.post('/api/promo/claim', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      if (adLimited(PROMO_RL, fuid, 2500)) return res.json({ ok: false, error: 'slow_down' });
      const code = normPromo(req.body?.code);
      if (!PROMO_DEFAULTS[code]) return res.json({ ok: false, error: 'invalid' });
      const idx = (await db.ref(`promoIndex/${code}/${fuid}`).once('value')).val();
      if (!idx || !idx.at) return res.json({ ok: false, error: 'not_eligible' });
      const at = Number(idx.at), exp = Math.min(Number(idx.exp) || 0, at + (Number(idx.days) || PROMO_DEFAULTS[code].days) * 86400000);
      const need = Number(idx.need) || PROMO_DEFAULTS[code].need, amt = Number(idx.amt) || PROMO_DEFAULTS[code].amt;
      let out = null, why = 'not_eligible', done = 0;
      const tx = await db.ref(`users/${fuid}`).transaction(cur => {
        out = null; why = 'not_eligible'; done = 0;
        if (!cur) return cur;
        const p = (cur.promos || {})[code];
        if (!p) return;
        if (p.status === 'CLAIMED') { why = 'already_claimed'; return; }
        const n = countBuysWindow(cur, at, exp); done = n;
        if (n < need) { why = Date.now() > exp ? 'expired' : 'need_trades'; return; }
        const bal = { ...(cur.balances || {}) }, old = Number(bal.USDT ?? cur.balance ?? 0);
        if (!Number.isFinite(old)) return;
        bal.USDT = r8(old + amt);
        const now = Date.now(), hid = `h_bonus_${code.toLowerCase()}_${now}`;
        out = { amt, old, nb: bal.USDT };
        return { ...cur, balances: bal, balance: bal.USDT,
          promos: { ...(cur.promos || {}), [code]: { ...p, status: 'CLAIMED', done: need, claimedAt: now } },
          history: { ...(cur.history || {}), [hid]: { hid, ts: now, date: nowIST(), type: 'DIWALI_BONUS', coin: 'USDT', amt, status: 'COMPLETED', note: `Diwali bonus claimed after ${need} P2P buys` } } };
      });
      if (!tx.committed || !out) return res.json({ ok: false, error: why, done: Math.min(done, need), need });
      db.ref(`promoIndex/${code}/${fuid}`).update({ status: 'CLAIMED', done: need, claimedAt: Date.now() }).catch(() => {});
      const v = tx.snapshot.val() || {};
      log('PROMO', `claim ${code} +${out.amt} ${fuid}`);
      promoNote(`✅ $${out.amt} claimed → ${v.uid || fuid}${v.name ? ' ' + v.name : ''} · balance ${out.old} → ${out.nb}`);
      res.json({ ok: true, amt: out.amt, balance: out.nb });
    } catch (e) { log('PROMO', `claim err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });

  // ─── V49 BMW LUCKY DRAW ───
  const DRAW_RL = new Map();
  app.get('/api/draw/info', async (_req, res) => {
    try {
      const c = await drawCfg();
      res.json({ ok: true, active: c.active !== false, open: drawOpen(c), startAt: c.startAt, endAt: c.endAt, min: c.min, prize: c.prize, now: Date.now() });
    } catch { res.json({ ok: false, error: 'server' }); }
  });
  app.post('/api/draw/status', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      const e = (await db.ref(`draw/entries/${fuid}`).once('value')).val();
      res.json({ ok: true, entry: e && e.id ? e.id : null });
    } catch { res.json({ ok: false, error: 'server' }); }
  });
  app.post('/api/draw/join', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      if (adLimited(DRAW_RL, fuid, 2500)) return res.json({ ok: false, error: 'slow_down' });
      const have = (await db.ref(`draw/entries/${fuid}`).once('value')).val();
      if (have && have.id) return res.json({ ok: true, already: true, entry: have.id });
      const cfg = await drawCfg();
      if (!drawOpen(cfg)) return res.json({ ok: false, error: 'ended' });
      const u = (await db.ref(`users/${fuid}`).once('value')).val();
      if (!u) return res.json({ ok: false, error: 'no_user' });
      if (u.banned === true || u.frozen === true) return res.json({ ok: false, error: 'blocked' });
      const bal = drawUsdt(u), min = Number(cfg.min) || DRAW_MIN;
      if (!(bal >= min)) return res.json({ ok: false, error: 'balance', balance: r8(bal), min });
      let id = 'DW-'; for (let j = 0; j < 6; j++) id += UID_CH[crypto.randomInt(32)];
      const name = (u.profile && u.profile.nickname) || u.name || null;
      const rec = { id, fuid, uid: u.uid || null, name, at: Date.now(), bal: r8(bal) };
      let created = false;
      const tx = await db.ref(`draw/entries/${fuid}`).transaction(c => { created = false; if (c) return; created = true; return rec; });
      const fin = (tx.snapshot && tx.snapshot.val()) || rec;
      if (created) {
        log('DRAW', `entry ${fin.id} ${fuid} bal ${fin.bal}`);
        drawNoteQ.push(`🎟 ${fin.id} → ${u.uid || fuid}${name ? ' ' + name : ''} · balance $${fin.bal}`);
      }
      res.json({ ok: true, already: !created, entry: fin.id });
    } catch (e) { log('DRAW', `join err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });

  // ─── QR LOGIN v2: server creates session, laptop polls with secret, phone approves ───
  const QR_TTL = 3 * 60 * 1000 + 30_000;
  const QR_TOK = new Map();   // qrId -> { token, exp }  (memory only; never written to the database)
  const QR_RL = new Map();    // ip -> [timestamps]
  const qrHash = (x) => crypto.createHash('sha256').update(String(x)).digest('hex');
  const qrIdOk = (id) => /^qr_[0-9]+_[a-z0-9]{6,20}$/i.test(id);
  const qrIp = (req) => String(req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '').split(',')[0].trim();
  setInterval(() => { const n = Date.now(); for (const [k, v] of QR_TOK) if (v.exp < n) QR_TOK.delete(k); for (const [k, v] of QR_RL) { const f = v.filter(t => n - t < 60_000); f.length ? QR_RL.set(k, f) : QR_RL.delete(k); } }, 60_000).unref?.();
  async function qrSecretOk(v, secret) {
    if (!v || !v.sh || !secret) return false;
    const a = Buffer.from(String(v.sh)), b = Buffer.from(qrHash(secret));
    return a.length === b.length && crypto.timingSafeEqual(a, b);
  }
  app.post('/api/qr/new', async (req, res) => {
    try {
      const ip = qrIp(req), n = Date.now(), hits = (QR_RL.get(ip) || []).filter(t => n - t < 60_000);
      if (hits.length >= 12) return res.json({ ok: false, error: 'too_many' });
      hits.push(n); QR_RL.set(ip, hits);
      let rnd = ''; for (let j = 0; j < 14; j++) rnd += 'abcdefghijklmnopqrstuvwxyz0123456789'[crypto.randomInt(36)];
      const qrId = `qr_${n}_${rnd}`, secret = crypto.randomBytes(24).toString('hex');
      await db.ref(`qrLogin/${qrId}`).set({ status: 'pending', ts: n, sh: qrHash(secret), v: 2 });
      res.json({ ok: true, qrId, secret, ttl: 180_000 });
    } catch (e) { log('QR', `new err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });
  app.post('/api/qr/poll', async (req, res) => {
    try {
      const qrId = String(req.body?.qrId || ''), secret = String(req.body?.secret || '');
      if (!qrIdOk(qrId)) return res.json({ ok: false, error: 'bad_id' });
      const ref = db.ref(`qrLogin/${qrId}`), v = (await ref.get()).val();
      if (!v) return res.json({ ok: true, status: 'expired' });
      if (!(await qrSecretOk(v, secret))) return res.json({ ok: false, error: 'bad_secret' });
      if (Date.now() - (Number(v.ts) || 0) > QR_TTL) { await ref.remove().catch(() => {}); QR_TOK.delete(qrId); return res.json({ ok: true, status: 'expired' }); }
      if (v.status === 'confirmed') {
        const t = QR_TOK.get(qrId); QR_TOK.delete(qrId); await ref.remove().catch(() => {});
        if (!t) return res.json({ ok: true, status: 'expired' });
        return res.json({ ok: true, status: 'confirmed', token: t.token });
      }
      if (v.status === 'cancelled') { await ref.remove().catch(() => {}); return res.json({ ok: true, status: 'cancelled' }); }
      res.json({ ok: true, status: v.status === 'scanned' ? 'scanned' : 'pending' });
    } catch (e) { log('QR', `poll err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });
  app.post('/api/qr/scan', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      const qrId = String(req.body?.qrId || '');
      if (!qrIdOk(qrId)) return res.json({ ok: false, error: 'bad_id' });
      const cancel = req.body?.cancel === true;
      const tx = await db.ref(`qrLogin/${qrId}`).transaction(cur => {
        if (!cur || (cur.status !== 'pending' && cur.status !== 'scanned')) return;
        if (Date.now() - (Number(cur.ts) || 0) > QR_TTL) return;
        if (cur.by && cur.by !== fuid) return;
        return { ...cur, status: cancel ? 'cancelled' : 'scanned', by: fuid, scannedAt: Date.now() };
      });
      res.json(tx.committed ? { ok: true } : { ok: false, error: 'expired' });
    } catch (e) { log('QR', `scan err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });
  // Phone approves → laptop gets a custom token (v2 via /api/qr/poll, legacy nodes via database listener)
  app.post('/api/qr/approve', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      const qrId = String(req.body?.qrId || '');
      if (!qrIdOk(qrId)) return res.json({ ok: false, error: 'bad_id' });
      const ref = db.ref(`qrLogin/${qrId}`);
      const v = (await ref.get()).val();
      if (!v || (v.status !== 'pending' && v.status !== 'scanned')) return res.json({ ok: false, error: 'expired' });
      if (v.by && v.by !== fuid) return res.json({ ok: false, error: 'expired' });
      if (Date.now() - (Number(v.ts) || 0) > (v.v === 2 ? QR_TTL : 5 * 60 * 1000)) { await ref.remove().catch(() => {}); return res.json({ ok: false, error: 'expired' }); }
      const token = await admin.auth().createCustomToken(fuid);
      if (v.v === 2) {
        QR_TOK.set(qrId, { token, exp: Date.now() + 120_000 });
        await ref.update({ status: 'confirmed', by: fuid, confirmedAt: Date.now() });
      } else {
        await ref.update({ status: 'confirmed', token, uid: fuid, confirmedAt: Date.now() });
      }
      log('QR', `login approved for ${fuid}`);
      res.json({ ok: true });
    } catch (e) { log('QR', `approve err ${e.message}`); res.json({ ok: false, error: 'server' }); }
  });
  app.post('/api/deposit/config', async (_req, res) => {
    try { res.json({ ok: true, address: await adWallet(), min: AD.MIN, windowSec: AD.WINDOW / 1000 }); } catch { res.json({ ok: false }); }
  });
  app.post('/api/deposit/start', async (req, res) => {
    try {
      if (BACKEND_DISABLED) return res.json({ ok: false, error: 'backend_disabled' });
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      let lock = (await db.ref('settings/depositLock').once('value')).val();
      if (lock && lock.fuid === fuid && lock.status === 'open' && Date.now() < lock.expiresAt)
        return res.json({ ok: true, expiresAt: lock.expiresAt, now: Date.now(), address: await adWallet() });
      const wait = adLimited(AD.rlStart, fuid, 30_000);
      if (wait) return res.json({ ok: false, error: 'too_soon', waitSec: wait });
      if (lock && Date.now() > lock.expiresAt) { await adCloseLock(lock, 'expired'); lock = null; }
      const latest = await adBlock();
      const now = Date.now();
      const startBlock = Math.max(0, latest - Math.ceil((AD_GRACE + 15_000) / await adBlockMs(latest)));
      const mine = { id: `w${now}_${fuid.slice(0, 6)}`, fuid, startedAt: now, since: now - AD_GRACE, expiresAt: now + AD.WINDOW, startBlock, status: 'open' };
      const tx = await db.ref('settings/depositLock').transaction(cur => (cur && cur.status === 'open' && Date.now() < cur.expiresAt) ? undefined : mine);
      if (!tx.committed) {
        AD.rlStart.delete(fuid);
        const cur = tx.snapshot.val() || {};
        return res.json({ ok: false, error: 'busy', waitSec: Math.max(1, Math.ceil(((cur.expiresAt || now) - Date.now()) / 1000)) });
      }
      log('AUTODEP', `window open fuid=${fuid} block=${startBlock}`);
      res.json({ ok: true, expiresAt: mine.expiresAt, now, address: await adWallet() });
    } catch (e) { log('AUTODEP', `start ${e.message}`); res.json({ ok: false, error: 'network' }); }
  });
  app.post('/api/deposit/check', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      if (adLimited(AD.rlCheck, fuid, 3_000)) return res.json({ ok: true, status: 'waiting' });
      const lock = (await db.ref('settings/depositLock').once('value')).val();
      if (!lock || lock.fuid !== fuid) return res.json({ ok: true, status: 'closed' });
      if (Date.now() > lock.expiresAt) { await adCloseLock(lock, 'expired'); return res.json({ ok: true, status: 'closed' }); }
      const safe = (await adBlock()) - AD.CONF;
      const logs = await adLogs(await adWallet(), lock.startBlock, safe);
      const credited = [];
      for (const t of logs) {
        if ((await adBlockTs(t.block).catch(() => Date.now())) < (lock.since || lock.startedAt)) continue; // older than 30s before the click
        if (t.amt < AD.MIN) { await adUnmatched(t, `below minimum ${AD.MIN} USDT`); continue; }
        const c = await adCredit(fuid, t, 'window'); if (c) credited.push(c);
      }
      if (!credited.length) return res.json({ ok: true, status: 'waiting', expiresAt: lock.expiresAt, now: Date.now() });
      await adCloseLock(lock, 'completed');
      const total = r8(credited.reduce((s, c) => s + c.amt, 0));
      res.json({ ok: true, status: 'credited', amount: total, txHash: credited[0].txHash, newBal: credited[credited.length - 1].newBal });
    } catch (e) { log('AUTODEP', `check ${e.message}`); res.json({ ok: true, status: 'waiting' }); }
  });
  app.post('/api/deposit/cancel', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      const lock = (await db.ref('settings/depositLock').once('value')).val();
      if (lock && lock.fuid === fuid) await adCloseLock(lock, 'cancelled'); // owner only
      res.json({ ok: true });
    } catch { res.json({ ok: false }); }
  });
}
function startAutoDepositJobs() {
  setInterval(() => { amIActive().then(a => a && adCleanup()).catch(e => log('AUTODEP', `cleanup ${e.message}`)); }, 15_000);
  setInterval(() => { amIActive().then(a => a && adScan()).catch(() => {}); }, 30_000);
  log('INIT', `🪙 Auto deposit (USDT BEP-20) min=${AD.MIN} rpcs=${AD.RPCS.length}`);
}
/** Admin commands: /deplock, /deprelease, /setwallet <addr>, /unmatched, /assign <key> <UID> */
async function adAdminCmd(text) {
  let m;
  if (/^\/deplock\b/i.test(text)) {
    const l = (await db.ref('settings/depositLock').once('value')).val();
    return tgSend(l ? card('🔒 DEPOSIT WINDOW', [['fuid', `\`${l.fuid}\``], ['Ends in', `${Math.max(0, Math.ceil((l.expiresAt - Date.now()) / 1000))}s`], ['Release', '/deprelease']]) : '🔓 No deposit window open.'), true;
  }
  if (/^\/deprelease\b/i.test(text)) {
    const l = (await db.ref('settings/depositLock').once('value')).val();
    if (l) await adCloseLock(l, 'released_by_admin');
    return tgSend(l ? '🔓 Deposit window released.' : '🔓 Nothing to release.'), true;
  }
  if ((m = text.match(/^\/setwallet\s+(0x[0-9a-fA-F]{40})\s*$/i))) {
    await db.ref('settings/depositAddress').set(m[1]);
    return tgSend(`✅ Deposit wallet set to \`${m[1]}\``), true;
  }
  if (/^\/setwallet\b/i.test(text)) return tgSend(`Current wallet: \`${await adWallet()}\`\nUse: \`/setwallet 0x...\``), true;
  if (/^\/unmatched\b/i.test(text)) {
    const v = (await db.ref('unmatchedDeposits').orderByChild('status').equalTo('pending_review').limitToLast(15).once('value')).val() || {};
    const keys = Object.keys(v);
    if (!keys.length) return tgSend('✅ No unmatched deposits.'), true;
    return tgSendH(hcard(`⚠️ UNASSIGNED DEPOSITS (${keys.length})`, [...keys.map(k => `• <b>${hEsc(fmtNum(v[k].amt))} USDT</b> · from ${hCode(String(v[k].from || '').slice(0, 10) + '…')}\n   ${v[k].tgMsgId ? '↩️ reply to its card with the UID' : hCode(`/assign ${k} UID`)}`)], '<i>Reply to a deposit card with a UID to assign it.</i>')), true;
  }
  if ((m = text.match(/^\/assign\s+(\S+)\s+(\S+)/i))) {
    const t = (await db.ref(`unmatchedDeposits/${m[1]}`).once('value')).val();
    if (!t) return tgSend(bad('Unmatched deposit not found.')), true;
    const f = await findUserByUID(m[2]);
    if (!f) return tgSend(bad(`UID \`${m[2]}\` not found.`)), true;
    const c = await adCredit(f.fuid, t, 'admin');
    if (c && t.tgMsgId) tgEditH(t.tgMsgId, hcard('✅ DEPOSIT ASSIGNED', [`💰 <b>${hEsc(fmtNum(t.amt))} USDT</b> · BEP-20`, `👤 Credited to ${hCode(m[2].toUpperCase())}`]), bscBtn(t.txHash) ? { reply_markup: { inline_keyboard: [bscBtn(t.txHash)] } } : {}).catch(() => {});
    return (c ? true : tgSend(bad('Already credited.'))), true;
  }
  return false;
}

function mountKycOtp(app) {
  app.post('/api/kyc-otp/send', async (req, res) => {
    try {
      if (BACKEND_DISABLED) return res.json({ ok: false, error: 'backend_disabled' });
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      const p = String(req.body?.phone || '').replace(/\D/g, '').slice(-10);
      if (!/^[6-9]\d{9}$/.test(p)) return res.json({ ok: false, error: 'bad_phone' });
      const now = Date.now();
      const st = KYC_OTP.get(fuid) || { sent: [] };
      if (st.last && now - st.last < 45_000) return res.json({ ok: false, error: 'too_soon', waitSec: Math.ceil((45_000 - (now - st.last)) / 1000) });
      st.sent = (st.sent || []).filter(t => now - t < 3600_000);
      if (st.sent.length >= 6) return res.json({ ok: false, error: 'rate_limited' });
      if (!WA.status().linked) return res.json({ ok: false, error: 'whatsapp_not_linked' });
      const phone = '91' + p;
      if (!(await WA.isOnWhatsApp(phone))) return res.json({ ok: false, error: 'not_on_whatsapp' });
      const code = String(crypto.randomInt(100000, 1000000));
      await WA.sendText(phone, `🔐 *BIEXC* — your mobile verification code is *${code}*\n\nUse it to confirm this number for KYC. Valid for 10 minutes. Never share this code with anyone, not even BIEXC staff.`);
      Object.assign(st, { phone: p, code, exp: now + 10 * 60_000, tries: 0, last: now }); st.sent.push(now);
      KYC_OTP.set(fuid, st);
      log('KYCOTP', `code sent → +91${p.slice(0, 2)}******${p.slice(-2)}`);
      res.json({ ok: true, waitSec: 45 });
    } catch (e) { log('KYCOTP', `send err ${e.message}`); res.json({ ok: false, error: 'send_failed' }); }
  });
  app.post('/api/kyc-otp/verify', async (req, res) => {
    try {
      const fuid = await fuidFromIdToken(req);
      if (!fuid) return res.json({ ok: false, error: 'auth' });
      const p = String(req.body?.phone || '').replace(/\D/g, '').slice(-10);
      const code = String(req.body?.code || '').replace(/\D/g, '');
      const st = KYC_OTP.get(fuid);
      if (!st || !st.code || st.phone !== p) return res.json({ ok: false, error: 'no_code' });
      if (Date.now() > st.exp) { KYC_OTP.delete(fuid); return res.json({ ok: false, error: 'expired' }); }
      if (st.tries >= 5) { KYC_OTP.delete(fuid); return res.json({ ok: false, error: 'too_many_tries' }); }
      if (code !== st.code) { st.tries++; return res.json({ ok: false, error: 'invalid_code', left: 5 - st.tries }); }
      KYC_OTP.delete(fuid);
      await db.ref(`users/${fuid}/kycPhone`).set({ phone: '+91' + p, verified: true, ts: Date.now() });
      log('KYCOTP', `verified ${fuid}`);
      res.json({ ok: true, phone: '+91' + p });
    } catch (e) { log('KYCOTP', `verify err ${e.message}`); res.json({ ok: false, error: 'verify_failed' }); }
  });
}

if (db) {
  db.ref('users').on('child_changed', async (snap) => {
    if (!(await amIActive())) return;
    const v = snap.val() || {};
    ensureUid(snap.key, v).catch(e => log('ERR', `uid ${e.message}`));
    if (v.support) supQueue(snap.key);
    await migrateLegacyBalanceOnce(snap.key, v);
    await processPendingMap(snap.key, v);
    await maybeForwardKycSubmission(snap.key, v).catch(e => log('ERR', `kyc ${e.message}`));
    if (REF && v.referral?.by && !v.referral.rewarded) await REF.evaluate(snap.key).catch(e => log('ERR', `refer ${e.message}`));
    await evaluateBonus(snap.key, v);
    await evaluatePromos(snap.key, v).catch(e => log('PROMO', e.message));
  });
  db.ref('users').on('child_added', async (snap) => {
    if (!(await amIActive())) return;
    const v = snap.val() || {};
    ensureUid(snap.key, v).catch(e => log('ERR', `uid ${e.message}`));
    if (v.support) supQueue(snap.key);
    await migrateLegacyBalanceOnce(snap.key, v);
    await processPendingMap(snap.key, v, BOT_START_TIME - 10_000);
    await maybeForwardKycSubmission(snap.key, v).catch(e => log('ERR', `kyc ${e.message}`));
    await alertNewUser(snap.key, v).catch(e => log('ERR', `newuser ${e.message}`));
    await evaluateBonus(snap.key, v);
    await evaluatePromos(snap.key, v).catch(e => log('PROMO', e.message));
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

async function resolvePendingAtomic(fuid,type,cbId,action){
 let result=null;const ts=Date.now();
 const tx=await db.ref(`users/${fuid}`).transaction(cur=>{
  result=null;if(!cur)return;const q=cur.pendingReqs?.[type]?.[cbId];if(!['dep','wit'].includes(type)||!['approve','reject'].includes(action)||!q)return;
   // A previous version could claim then fail before moving money. Never guess its outcome.
   if(q._resolved)return;
  const coin=String(q.coin||'USDT').toUpperCase(),amt=Number(q.amt);
  if(!Number.isFinite(amt)||amt<=0||!/^[A-Z0-9]{1,20}$/.test(coin))return;
  const u={...cur},balances={...(cur.balances||{})},old=Number(balances[coin]??(coin==='USDT'?cur.balance:0)??0);
  const delta=type==='dep'&&action==='approve'||type==='wit'&&action==='reject'?amt:0;
  const next=r8(old+delta);if(!Number.isFinite(old)||next<0)return;
  balances[coin]=next;u.balances=balances;if(coin==='USDT')u.balance=next;
  const pending={...(cur.pendingReqs||{})};pending[type]={...(pending[type]||{})};delete pending[type][cbId];u.pendingReqs=pending;
  const history={...(cur.history||{})};for(const [k,h] of Object.entries(history))if(k===q.hid||h?.hid===q.hid)history[k]={...h,status:action==='approve'?'COMPLETED':'REJECTED',resolvedAt:ts};u.history=history;
  u.walletReceipts={...(cur.walletReceipts||{}),[cbId]:{action,coin,amt,ts}};
  result={oldBal:r8(old),newBal:next,user:u,coin,amt,request:q};return u;
 });return tx.committed?result:null;
}
async function handleApprove(fuid,type,req,cbId){
 const result=await resolvePendingAtomic(fuid,type,cbId,'approve');if(!result)return null;
 if(type==='dep'&&REF)REF.onDeposit(fuid,result.coin,result.amt).catch(()=>{});
 if(type==='dep')bonusCountAction(fuid,`dep_${cbId}`).catch(()=>{});
 queueResolutionEmail({type,action:'approve',...result,uid:result.user.uid,network:result.request.network||result.request.chain,txid:result.request.txid,address:result.request.address||result.request.addr,hid:result.request.hid});return result;
}
async function handleReject(fuid,type,req,cbId){
 const result=await resolvePendingAtomic(fuid,type,cbId,'reject');if(!result)return null;
 queueResolutionEmail({type,action:'reject',...result,uid:result.user.uid,network:result.request.network||result.request.chain,txid:result.request.txid,address:result.request.address||result.request.addr,hid:result.request.hid});return result;
}

function fmtResolutionMsg(label, action, ctx, req) {
  const coin = (ctx.coin || req?.coin || 'USDT').toUpperCase();
  const ico = action === 'approve' ? '✅' : '❌';
  const verb = action === 'approve' ? 'APPROVED' : 'REJECTED';
  const sign = ctx.type === 'dep' ? '+' : '−';
  const net = req?.network || req?.chain, addr = req?.address || req?.addr;
  const rows = [];
  if (ctx.user) rows.push(`👤 <b>${hEsc(ctx.user.name || 'User')}</b> · ${hCode(ctx.user.uid || '—')}`, '');
  rows.push(`💵 <b>${sign}${hEsc(fmtNum(ctx.amt))} ${hEsc(coin)}</b>${net ? ` · ${hEsc(net)}` : ''}`);
  if (addr)     rows.push('', '📬 Address', hCode(addr));
  if (req?.txid) rows.push('', '🔗 TxID', hCode(req.txid));
  if (ctx.newBal !== undefined) rows.push('', `💼 ${hEsc(fmtNum(ctx.oldBal))} → <b>${hEsc(fmtNum(ctx.newBal))}</b> ${hEsc(coin)}`);
  if (action !== 'approve' && ctx.type === 'wit') rows.push('🔄 Funds returned to the user');
  return hcard(`${ico} ${label} ${verb}`, rows);
}

// ════════════════════════════════════════════════════════════════════
// CALLBACK ROUTER
// ════════════════════════════════════════════════════════════════════
async function handleCallback(cb, upd = null) {
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
  if (data.startsWith('sup_')) return tgAnswer(cb.id, 'Customer care has moved to the separate support bot.', true);
  if (data === 'rhb_no')  { await tgAnswer(cb.id, 'Cancelled'); return tgEdit(cb.message?.message_id, '✖ Cancelled.').catch(() => {}); }
  if (data === 'rnk_yes') {
    if (!(await claimTapOnce(`rnk_${cb.message?.message_id}`, 'yes').catch(() => ({ ok: true }))).ok) return tgAnswer(cb.id, 'Already done');
    await tgAnswer(cb.id, 'Deleting…');
    tgEdit(cb.message?.message_id, '⏳ Deleting non-KYC accounts…').catch(() => {});
    return resetNoKycUsers().catch(e => tgSend(bad(e.message)));
  }
  if (data === 'rhb_yes') {
    if (!(await claimTapOnce(`rhb_${cb.message?.message_id}`, 'yes').catch(() => ({ ok: true }))).ok) return tgAnswer(cb.id, 'Already done');
    await tgAnswer(cb.id, 'Resetting…');
    tgEdit(cb.message?.message_id, '⏳ Resetting all balances…').catch(() => {});
    return resetAllBalances().catch(e => tgSend(bad(e.message)));
  }
  if (data === 'noop_done') { await tgAnswer(cb.id, '✔ Already done — nothing more to do.'); return; }
  if ((m = data.match(/^dasg_(ok|no)_([a-f0-9]{6,20})$/))) return handleDepAssignCb(cb, m[1], m[2]);
  if ((m = data.match(/^(approve|reject)_(dep_|wit_)(.+)$/))) return handleApproveRejectCb(cb, m[1], m[2] + m[3], upd);
  // Trade/P2P callbacks (cbId starts with trade_) are completed by the app
  // via the buffered /api/tg/getUpdates feed. The server now makes sure only
  // the FIRST tap ever reaches the app — every later tap is refused here.
  if ((m = data.match(/^(approve|reject)_(trade_.+)$/))) {
    const action = m[1], tradeCb = m[2];
    if (action === 'approve') {
      const owner = await findTradeOwner(tradeCb);
      if (owner && !kycAllowsTrade(owner.user)) {
        await tgAnswer(cb.id, `⛔ BLOCKED — UID ${owner.user.uid || '?'} has NOT completed KYC. You can only Reject this order.`, true);
        log('TRADE', `approve blocked — no KYC UID=${owner.user.uid}`);
        return;
      }
    }
    const owner=await findTradeOwner(tradeCb);
    const oid=owner&&Object.keys(owner.user.chats||{}).find(k=>owner.user.chats[k]?.cbId===tradeCb);
    const guess=String(tradeCb).split('_')[1],orderId=oid||(owner?.user.orders?.[guess]?guess:null);
    if(!owner||!orderId){await tgAnswer(cb.id,'Order unavailable — review before retrying.',true);return;}
    // New protocol is declared by each atomically-created order, not account-wide.
    // Existing installed clients retain their original hand-off and fallback.
    if(owner.user.orders?.[orderId]?.walletProtocol!==5){
    const lock = await claimTapOnce(`cb_${tradeCb}`, action);
    if (!lock.ok) {
      await tgAnswer(cb.id, `⚠️ Already ${doneWord(lock.prev)} — this button works only once.`, true);
      setCbButtons(cb, `${lock.prev === 'reject' ? '❌' : '✅'} ${doneWord(lock.prev)}`);
      log('TAP', `blocked duplicate ${action} for ${tradeCb}`);
      return;
    }
    // Instant feedback + kill the buttons before anything else.
    tgAnswer(cb.id, action === 'approve' ? '✅ Approved — order completing' : '❌ Rejected');
    setCbButtons(cb, action === 'approve' ? '✅ APPROVED' : '❌ REJECTED');
    if (upd) pushRecentUpdate(upd);                  // hand it to the app — exactly once
    log('TRADE', `${action} ${tradeCb} → forwarded once`);
    queueP2PMail(owner.user, orderId, owner.user.orders?.[orderId], action === 'approve' ? 'complete' : 'reject');
    p2pGroupLogOrder(owner.user, orderId, owner.user.orders?.[orderId], action);
    // Safety net: if the user's app is closed / offline the order would never
    // settle and the balance would never update. Settle it server-side after 30s.
    setTimeout(async () => {
      try {
        const owner = await findTradeOwner(tradeCb);
        if (!owner) return;
        const orders = owner.user.orders || {};
        const chats = owner.user.chats || {};
        let oid = Object.keys(chats).find(k => chats[k]?.cbId === tradeCb);
        if (!oid) { const guess = String(tradeCb).split('_')[1]; if (orders[guess]) oid = guess; }
        if (!oid || !orders[oid]) return;           // app already settled it
        const r = await adminSettleOrder(oid, action === 'approve' ? 'complete' : 'reject', true);
        log('TRADE', `fallback settle #${oid} ${action} → ${r && r.ok ? 'done' : 'skipped'}`);
      } catch (e) { log('TRADE', `fallback settle err ${e.message}`); }
    }, 30000);
      return;
    }
    const result=await settleP2PAtomic(owner.fuid,orderId,action==='approve'?'complete':'reject');
    if(!result?.ok){await tgAnswer(cb.id,result?.error||'Settlement needs review.',true);return;}
    tgAnswer(cb.id,action==='approve'?'✅ Completed — funds settled':'❌ Rejected — funds settled');
    setCbButtons(cb,action==='approve'?'✅ COMPLETED':'❌ REJECTED');
    if(upd)pushRecentUpdate(upd);
    if(action==='approve')bonusCountAction(owner.fuid,`trade_${orderId}`).catch(()=>{});
    pushNotif(owner.fuid,{title:action==='approve'?'P2P completed':'P2P cancelled',body:`Order #${orderId} settled.`,type:'INFO'}).catch(()=>{});

    return;
  }
  if (/^(join|ignore)_support_/.test(data)) return; // handled by iframe support overlay
  if ((m = data.match(/^userdetail_(.+)$/)))       return sendUserDetailCard(cb, m[1]);
  if ((m = data.match(/^(ban|unban)_(.+)$/)))      return handleBanCb(cb, m[1], m[2]);
  if ((m = data.match(/^refs_(.+)$/)))             { await tgAnswer(cb.id, 'Loading…'); return sendUserReferrals(m[1]); }
  if ((m = data.match(/^history_(.+)$/)))          { await tgAnswer(cb.id, 'Loading…'); return sendUserHistory(m[1], 15); }
  if ((m = data.match(/^closep2p_(.+)$/)))         { await tgAnswer(cb.id, 'Closing…'); return closeAllP2PForUser(m[1]); }
  if ((m = data.match(/^(creditprompt|debitprompt)_(.+)$/))) { await tgAnswer(cb.id, '🪙'); return coinPicker(m[1] === 'creditprompt' ? 'credit' : 'debit', m[2]); }
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
  if ((m = data.match(/^kycview_(.+)$/)))   { await tgAnswer(cb.id, '🪪 Loading KYC…'); return sendKycDetails(m[1]); }
  if ((m = data.match(/^kycok_(.+)$/)))     {
    if (!debounceTap(`kycok_${m[1]}`, 15000)) { await tgAnswer(cb.id, '✔ Already approving — please wait'); return; }
    await tgAnswer(cb.id, '✅ Approving KYC…'); setCbButtons(cb, '✅ KYC APPROVED'); return approveKyc(m[1]);
  }
  if ((m = data.match(/^kycrevoke_(.+)$/))) {
    await tgAnswer(cb.id, 'Confirm');
    const id = newConfirm({ op: 'kycrevoke', uid: m[1] });
    return tgSend(`⚠️ Reset KYC for \`${m[1]}\`? The user must verify again.`, { reply_markup: { inline_keyboard: [[{ text: '✅ Yes, reset', callback_data: `cfm_${id}` }, { text: '✖ Cancel', callback_data: 'askcancel' }]] } });
  }
  if ((m = data.match(/^(credit|debit|setbal)go_(.+)$/))) { await tgAnswer(cb.id, '🪙'); ASK = null; return coinPicker(m[1], m[2]); }
  if ((m = data.match(/^pc_(credit|debit|setbal)_([A-Z0-9]+)_(.+)$/i))) {
    await tgAnswer(cb.id, m[3]);
    if (m[3] === '*') { ask('coin', { op: m[1], uid: m[2] }); return tgSend('⌨️ Type the coin symbol, e.g. `MATIC`', { reply_markup: cancelKb }); }
    return askAmount(m[1], m[2], m[3].toUpperCase());
  }
  if ((m = data.match(/^cfm(h?)_(.+)$/)))   {
    if (!CONFIRM.has(m[2])) { await tgAnswer(cb.id, '✔ Already done / expired'); setCbButtons(cb, '✔ DONE'); return; }
    await tgAnswer(cb.id, '⏳ Working…'); setCbButtons(cb, m[1] ? '✔ DONE · hidden from history' : '✔ DONE'); return runConfirm(m[2], !!m[1]);
  }
  if (data === 'askcancel')                 { ASK = null; await tgAnswer(cb.id, 'Cancelled'); return tgSend('✖ Cancelled.', { reply_markup: MENU_KB }); }
  if ((m = data.match(/^banask_(.+)$/))) {
    await tgAnswer(cb.id, 'Confirm');
    const id = newConfirm({ op: 'ban', uid: m[1] });
    return tgSend(`⚠️ Ban \`${m[1]}\`?`, { reply_markup: { inline_keyboard: [[{ text: '🚫 Yes, ban', callback_data: `cfm_${id}` }, { text: '✖ Cancel', callback_data: 'askcancel' }]] } });
  }
  if ((m = data.match(/^bal_(.+)$/)))       { await tgAnswer(cb.id, '💰 Loading…');     return handleBalances(m[1]); }
  if ((m = data.match(/^mwd_(.+)$/))) {
    await tgAnswer(cb.id, '💸');
    ask('mwd', { uid: m[1] });
    return tgSend(`💸 Send the *minimum withdrawal* (USD) for \`${m[1]}\`, e.g. \`25\`.\nSend \`0\` to reset to default ($10).`, { reply_markup: cancelKb });
  }
  if ((m = data.match(/^p2pblk([bs]?)_(.+)$/))) {
    const side = m[1]; // '' = both, 'b' = buy only, 's' = sell only
    const uid = m[2];
    const found = await findUserByUID(uid);
    if (!found) { await tgAnswer(cb.id, 'Not found'); return tgSend(bad(`UID \`${uid}\` not found.`)); }
    const key = side === 'b' ? 'p2pBlockBuy' : side === 's' ? 'p2pBlockSell' : 'p2pBlocked';
    const r = db.ref(`users/${found.fuid}/limits/${key}`);
    const next = !((await r.once('value')).val() === true);
    await r.set(next ? true : null);
    const lbl = side === 'b' ? 'P2P BUY' : side === 's' ? 'P2P SELL' : 'P2P (Buy+Sell)';
    log('ADMIN', `${key} UID=${uid} → ${next}`);
    await tgAnswer(cb.id, next ? `${lbl} blocked` : `${lbl} unblocked`);
    await tgSend(good(`${lbl} for \`${uid}\` is now *${next ? 'BLOCKED' : 'ALLOWED'}*`));
    return sendUserDetailCard(null, uid);
  }
  if ((m = data.match(/^plim_(.+)$/))) {
    await tgAnswer(cb.id, '🔢');
    ask('plim', { uid: m[1] });
    return tgSend(`🔢 Send the *P2P orders per day* limit for \`${m[1]}\`, e.g. \`3\`.\nSend \`0\` for unlimited.`, { reply_markup: cancelKb });
  }
  if (data === 'menu_p2prates') { await tgAnswer(cb.id, '💱'); return sendP2PRates(); }
  if ((m = data.match(/^promo_(on|off|ref)_([A-Z0-9]{3,20})$/))) {
    if (!PROMO_DEFAULTS[m[2]]) return tgAnswer(cb.id, 'Unknown offer');
    if (m[1] !== 'ref') { await db.ref(`settings/promos/${m[2]}`).update({ active: m[1] === 'on', updatedAt: Date.now() }); PROMO_CFG.delete(m[2]); }
    await tgAnswer(cb.id, m[1] === 'on' ? '🟢 Link ON' : m[1] === 'off' ? '🔴 Link OFF' : '🔄');
    return sendPromoPanel(m[2], cb.message?.message_id);
  }
  if ((m = data.match(/^draw_(on|off|ref|restart|restartyes|pick)$/))) {
    const k = m[1];
    if (k === 'on' || k === 'off') { await db.ref('draw/cfg').update({ active: k === 'on', updatedAt: Date.now() }); DRAW_C.at = 0; await tgAnswer(cb.id, k === 'on' ? '🟢 Draw ON' : '🔴 Draw OFF'); return sendDrawPanel(cb.message?.message_id); }
    if (k === 'restart') { await tgAnswer(cb.id, '🔁'); return tgSend(`🔁 Restart the BMW draw timer for ${DRAW_DAYS} days from now?\nExisting entries are kept.`, { parse_mode: undefined, reply_markup: { inline_keyboard: [[{ text: '✅ Yes, restart', callback_data: 'draw_restartyes' }, { text: '✖️ Cancel', callback_data: 'draw_ref' }]] } }); }
    if (k === 'restartyes') { const now = Date.now(); await db.ref('draw/cfg').update({ startAt: now, endAt: now + DRAW_DAYS * 86400000, active: true, updatedAt: now }); DRAW_C.at = 0; await tgAnswer(cb.id, '🔁 Timer restarted'); return sendDrawPanel(); }
    if (k === 'pick') { await tgAnswer(cb.id, '🎲'); return sendDrawPanel(null, await drawPickWinner()); }
    await tgAnswer(cb.id, '🔄');
    return sendDrawPanel(cb.message?.message_id);
  }
  if (data === 'menu_promo') { await tgAnswer(cb.id, '🪔'); return sendPromoPanel('DIWALI20'); }
  if ((m = data.match(/^p2pr_(buyMin|buyMax|sellMin|sellMax|buyLimMin|buyLimMax|sellLimMin|sellLimMax)$/))) {
    await tgAnswer(cb.id, '✏️');
    ask('p2prate', { field: m[1] });
    const lbl = { buyMin: 'BUY rate start', buyMax: 'BUY rate end', sellMin: 'SELL rate start', sellMax: 'SELL rate end', buyLimMin: 'BUY minimum-limit start', buyLimMax: 'BUY minimum-limit end', sellLimMin: 'SELL minimum-limit start', sellLimMax: 'SELL minimum-limit end' }[m[1]];
    return tgSend(`✏️ Send the new *${lbl}* in ₹, e.g. \`${/Lim/.test(m[1]) ? '2500' : '97.50'}\``, { reply_markup: cancelKb });
  }
  if (data === 'p2pr_reset') {
    await db.ref('settings/p2p').set({ ...P2P_DEFAULTS });
    P2P_CACHE.at = 0;
    await tgAnswer(cb.id, '♻️ Reset done');
    return sendP2PRates();
  }
  if (data === 'p2pr_night') {
    const cur = await getP2PSettings(true);
    await db.ref('settings/p2p/nightOn').set(!(cur.nightOn !== false));
    P2P_CACHE.at = 0;
    await tgAnswer(cb.id, 'Updated');
    return sendP2PRates();
  }
  if ((m = data.match(/^msgprompt_(.+)$/))) {
    await tgAnswer(cb.id, '💬');
    ask('msg', { uid: m[1] });
    return tgSend(`💬 Type the message for \`${m[1]}\` — it shows in their app notifications.`, { reply_markup: cancelKb });
  }
  // ── button dashboard ──────────────────────────────────────────────
  if (data === 'menu_home')    { await tgAnswer(cb.id, '🏠'); return sendMenu(); }
  if (data === 'wa_panel')     { await tgAnswer(cb.id, '📱'); return sendWaPanel(); }
  if (data === 'wa_qr')        { await tgAnswer(cb.id, '🔄 New QR…'); return sendWaQR(true); }
  if (data === 'wa_test') {
    const st = WA.status();
    if (!st.linked) { await tgAnswer(cb.id, 'Not linked'); return sendWaPanel(); }
    try { await WA.sendText(st.number, `✅ BIEXC test message — WhatsApp OTP is working (${new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' })})`); await tgAnswer(cb.id, '✅ Sent'); return tgSend('🧪 Test message sent to your own WhatsApp (check "Message yourself").'); }
    catch (e) { await tgAnswer(cb.id, 'Failed'); return tgSend(`⚠️ Test failed: ${e.message}`); }
  }
  if (data === 'wa_unlinkask') {
    await tgAnswer(cb.id, 'Confirm');
    return tgSend('⚠️ Unlink WhatsApp? OTPs WhatsApp par jana band ho jayenge jab tak dobara link na karo.', { reply_markup: { inline_keyboard: [[{ text: '🔌 Yes, unlink', callback_data: 'wa_unlink' }, { text: '✖ Cancel', callback_data: 'askcancel' }]] } });
  }
  if (data === 'wa_unlink') {
    await tgAnswer(cb.id, 'Unlinking…');
    await WA.logout();
    return tgSend('🔌 WhatsApp unlinked.', { reply_markup: { inline_keyboard: [[{ text: '📱 Link again (QR)', callback_data: 'wa_qr' }, { text: '🏠 Menu', callback_data: 'menu_home' }]] } });
  }
  if (data === 'menu_users')   { await tgAnswer(cb.id, '👥'); return handleUsersList(); }
  if (data === 'menu_stats')   { await tgAnswer(cb.id, '📊'); return handleStats(); }
  if (data === 'menu_today')   { await tgAnswer(cb.id, '📅'); return handleToday(); }
  if (data === 'menu_pending') { await tgAnswer(cb.id, '⏳'); return handleTrades(); }
  if (data === 'menu_today')   { await tgAnswer(cb.id, '📈'); return sendDailySummary(); }
  if (data === 'menu_broad')   { await tgAnswer(cb.id, '📢'); return handleBroadList(); }
  if (data === 'menu_find')    { await tgAnswer(cb.id, '🆔'); ask('uid'); return tgSend('🆔 Type the user *UID* — e.g. `AB12CD`', { reply_markup: cancelKb }); }
  if (data === 'menu_search')  { await tgAnswer(cb.id, '🔎'); ask('search'); return tgSend('🔎 Type a *name* (or email / phone) to search', { reply_markup: cancelKb }); }
  if (data === 'menu_bcnew')   { await tgAnswer(cb.id, '📢'); ask('broadcast'); return tgSend('📢 Type the message to send to *all users*', { reply_markup: cancelKb }); }
  log('CB', `❓ no handler for callback_data="${data}"`);
  await tgAnswer(cb.id, '?');

}

async function handleApproveRejectCb(cb, action, cbId, upd = null) {
  const lock = await claimTapOnce(`cb_${cbId}`, action);
  if (!lock.ok) {
    await tgAnswer(cb.id, `⚠️ Already ${doneWord(lock.prev)} — this button works only once.`, true);
    setCbButtons(cb, `${lock.prev === 'reject' ? '❌' : '✅'} ${doneWord(lock.prev)}`);
    log('TAP', `blocked duplicate ${action} for ${cbId}`);
    return;
  }
  // Instant reply + buttons removed straight away (no waiting for the DB).
  tgAnswer(cb.id, action === 'approve' ? '✅ Approving…' : '❌ Rejecting…');
  setCbButtons(cb, action === 'approve' ? '⏳ APPROVING…' : '⏳ REJECTING…');
  let witInfo = null;
  if (action === 'approve' && cbId.startsWith('wit_')) witInfo = await findWitReq(cbId).catch(() => null);
  try {
    const done = await processApproveReject(cb, action, cbId);
    if (witInfo) askWithdrawHash(witInfo).catch(e => log('WIT', `hash prompt ${e.message}`));
    if (upd) pushRecentUpdate(upd); // app updates its screen — once
    if (!done) setCbButtons(cb, '✔ ALREADY HANDLED');
  } catch (e) {
    // Nothing is half-done here: claimPendingReq() is itself atomic, so it is
    // safe to let the admin tap again after a crash/network error.
    log('ERR', `approve/reject ${cbId}: ${e.message}`);
    await releaseTap(`cb_${cbId}`);
    await tgFetch('editMessageReplyMarkup', {
      chat_id: cb.message?.chat?.id || TG_CHAT, message_id: cb.message?.message_id,
      reply_markup: { inline_keyboard: [[
        { text: '✅ APPROVE', callback_data: `approve_${cbId}` },
        { text: '❌ REJECT',  callback_data: `reject_${cbId}` }]] },
    });
    await tgSend(bad(`Could not finish that — please tap again. (${e.message})`));
  }
}


// ════════════════════════════════════════════════════════════════════
// ADMIN P2P SETTLEMENT — /complete <orderId>, /reject <orderId>
// Works even when the user never taps Release/Paid or is offline.
// ════════════════════════════════════════════════════════════════════
async function findOrderById(orderId) {
  const snap = await db.ref('users').once('value');
  let hit = null;
  snap.forEach(c => {
    if (hit) return;
    const o = c.val()?.orders?.[orderId];
    if (o) hit = { fuid: c.key, user: c.val(), order: o };
  });
  return hit;
}
async function settleP2PAtomic(fuid,orderId,action){
 if(!['complete','reject'].includes(action))return {ok:false,error:'Invalid decision'};orderId=String(orderId);let result=null,error='Order already finished';const ts=Date.now();
 const tx=await db.ref(`users/${fuid}`).transaction(cur=>{
  result=null;if(!cur)return;const o=cur.orders?.[orderId];if(!o)return;
  if(action==='complete'&&!kycAllowsTrade(cur)){error='KYC required';return;}
  const chat=cur.chats?.[orderId]||{},key=String(chat.cbId||`admin_${orderId}`).replace(/[./]/g,'_');
  if(cur.tradeDone?.[key]||cur.tradeDone?.[orderId]){error='Legacy settlement lock needs review';return;}
  const mode=String(o.mode||'BUY').toUpperCase(),amt=Number(o.usdt);
  if(!['BUY','SELL'].includes(mode)||!Number.isFinite(amt)||amt<=0){error='Invalid order';return;}
  const u={...cur},b={...(cur.balances||{})},old=Number(b.USDT??cur.balance??0);
  const credit=mode==='BUY'&&action==='complete'||mode==='SELL'&&action!=='complete'?amt:0;
  if(!Number.isFinite(old)){error='Invalid balance';return;}b.USDT=r8(old+credit);u.balances=b;u.balance=b.USDT;
  u.orders={...cur.orders};delete u.orders[orderId];u.chats={...(cur.chats||{})};delete u.chats[orderId];
  u.tradeDone={...(cur.tradeDone||{}),[key]:{action:action==='complete'?'approve':'reject',by:'server',ts},[orderId]:{action:action==='complete'?'approve':'reject',by:'server',ts}};
  const hid='h_p2p_'+orderId;u.history={...(cur.history||{}),[hid]:{hid,ts,date:nowIST(),type:action==='complete'?mode:mode+'_CANCEL',status:action==='complete'?'COMPLETED':'CANCELLED',coin:'USDT',amt,orderId,merchant:o.merchant||'',rate:o.rate||0,inr:o.inr||0,paymentMethod:o.payMethod||'UPI',network:'BEP20',reason:action==='complete'?'':'Order cancelled; held SELL funds returned.'}};
  result={ok:true,order:o,chat,user:u,oldBal:old,newBal:b.USDT};return u;
 });
 if(tx.committed&&result){queueP2PMail(result.user,orderId,result.order,action);p2pGroupLogOrder(result.user,orderId,result.order,action);}
 return tx.committed?result:{ok:false,error};
}
async function adminSettleOrder(orderId,action,quiet=false){
 orderId=String(orderId).replace(/^#/,'');const f=await findOrderById(orderId);if(!f)return quiet?{ok:false,error:'Order not found'}:tgSend(bad('Order not found or already finished'));
 const r=await settleP2PAtomic(f.fuid,orderId,action);if(!r.ok)return quiet?r:tgSend(bad(r.error));
 if(action==='complete')bonusCountAction(f.fuid,`trade_${orderId}`).catch(()=>{});
 pushNotif(f.fuid,{title:action==='complete'?'P2P order completed':'P2P order cancelled',body:`Order #${orderId}: ${r.order.usdt} USDT.`,type:'INFO'}).catch(()=>{});
 const p2pLine=`🧾 Order ${hCode('#'+orderId)} · ${hCode(r.user.uid||'—')} · <b>${hEsc(fmtNum(r.order.usdt))} USDT</b>`;
 if(r.chat.tgMsgId)tgEditH(r.chat.tgMsgId,[`${action==='complete'?'✅ <b>P2P TRADE COMPLETED</b>':'❌ <b>P2P TRADE CANCELLED</b>'}`,p2pLine].join('\n')).catch(()=>{});
 return quiet?r:tgSendH(hcard(action==='complete'?'✅ P2P SETTLED — COMPLETED':'❌ P2P SETTLED — CANCELLED',[p2pLine,`💼 ${hEsc(fmtNum(r.oldBal))} → <b>${hEsc(fmtNum(r.newBal))}</b> USDT`]));
}

/** /cancelalltrades — reject every pending deposit/withdrawal and every open P2P order (SELL USDT refunded once) and clear stuck chats. */
async function cancelAllTrades() {
  const snap = await db.ref('users').once('value');
  const jobs = [];
  snap.forEach(c => {
    const u = c.val() || {};
    for (const id of Object.keys(u.orders || {})) jobs.push({ fuid: c.key, id });
    for (const id of Object.keys(u.chats || {})) if (!(u.orders || {})[id]) jobs.push({ fuid: c.key, id, chatOnly: true });
  });
  // Pending deposit / withdrawal requests (dep/wit) — rejected the same way as the ❌ button.
  const reqs = [];
  snap.forEach(c => {
    const pr = (c.val() || {}).pendingReqs || {};
    for (const type of ['dep', 'wit']) for (const [cbId, r] of Object.entries(pr[type] || {})) {
      if (cbId === 'botLock') continue;
      reqs.push({ fuid: c.key, type, cbId, r: r && typeof r === 'object' ? r : {} });
    }
  });
  let rDone = 0, rRefund = 0, rForced = 0;
  for (const q of reqs) {
    try {
      const res = await handleReject(q.fuid, q.type, q.r, q.cbId);
      if (res) { rDone++; if (q.type === 'wit') rRefund++; if (q.r.botMsgId) tgEditH(q.r.botMsgId, fmtResolutionMsg(q.type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL', 'reject', { type: q.type, ...res }, q.r)).catch(() => {}); }
      else {
        // Preserve unresolved money-bearing requests for manual reconciliation.
        rForced++;log('REVIEW',`pending request ${q.fuid}/${q.type}/${q.cbId} retained; outcome unknown`);
      }
      sentByCbId.delete(q.cbId);
    } catch (e) { log('ERR', `cancelall req ${q.cbId}: ${e.message}`); }
  }
  if (reqs.length) await tgSend(card('🧹 DEPOSITS / WITHDRAWALS CLOSED', [['Rejected', rDone], ['Withdraw refunds', rRefund], ['Needs review (retained)', rForced]]));
  if (!jobs.length) return tgSend('✅ No pending P2P trades.');
  await tgSend(`⏳ Cancelling ${jobs.length} trade(s)…`);
  let done = 0, refunded = 0, forced = 0;
  for (const j of jobs) {
    try {
      if (!j.chatOnly) {
        const r = await adminSettleOrder(j.id, 'reject', true);
        if (r && r.ok) { done++; if (r.refunded) refunded++; continue; }
        forced++;log('REVIEW',`order ${j.fuid}/${j.id} retained; settlement not committed`);continue;
      }
      await db.ref(`users/${j.fuid}/chats/${j.id}`).remove();
      forced++;
    } catch (e) { log('ERR', `cancelall ${j.id}: ${e.message}`); }
  }
  log('TRADE', `cancelalltrades: ${done} rejected, ${refunded} refunded, ${forced} force-cleared`);
  return tgSend(card('🧹 ALL TRADES CANCELLED', [['Rejected', done], ['SELL refunds', refunded], ['Needs review (retained)', forced]]));
}

/** /resethousebalance — after confirmation, set every coin balance of every user to 0. */
async function resetAllBalances() {
  const snap = await db.ref('users').once('value');
  const upd = {}; let n = 0;
  snap.forEach(c => {
    const u = c.val(); if (!u || typeof u !== 'object') return;
    n++;
    for (const k of Object.keys(u.balances && typeof u.balances === 'object' ? u.balances : {})) upd[`users/${c.key}/balances/${k}`] = 0;
    upd[`users/${c.key}/balances/USDT`] = 0;
    if (u.balance !== undefined) upd[`users/${c.key}/balance`] = 0;
  });
  const keys = Object.keys(upd);
  for (let i = 0; i < keys.length; i += 500) await db.ref().update(Object.fromEntries(keys.slice(i, i + 500).map(k => [k, upd[k]])));
  log('ADMIN', `resethousebalance: ${n} users zeroed`);
  return tgSend(card('🧹 BALANCES RESET', [['Users', n], ['All balances', '0']]));
}

/** Locate a withdrawal request (owner + history id) before it is resolved. */
async function findWitReq(cbId) {
  const c = sentByCbId.get(cbId);
  if (c && c.hid) return { fuid: c.fuid, hid: c.hid, amt: c.amt, coin: c.coin || 'USDT' };
  const snap = await db.ref('users').once('value');
  let out = null;
  snap.forEach(child => {
    const map = child.val()?.pendingReqs?.wit || {};
    for (const [k, r] of Object.entries(map)) {
      if ((k === cbId || r?.cbId === cbId) && r?.hid) out = { fuid: child.key, hid: r.hid, amt: r.amt, coin: r.coin || 'USDT' };
    }
  });
  return out;
}

/** After a withdrawal is approved, ask the admin for the on-chain TxID. */
async function askWithdrawHash(w) {
  const u = (await db.ref(`users/${w.fuid}`).once('value')).val() || {};
  const uid = u.uid || '—';
  ask('withash', { fuid: w.fuid, hid: w.hid, amt: w.amt, coin: w.coin, uid });
  log('WIT', `asking hash UID=${uid} hid=${w.hid}`);
  await tgSendH(hcard('🔗 WITHDRAWAL APPROVED — SEND TxID', [`👤 ${hCode(uid)}`, `💵 <b>${hEsc(fmtNum(w.amt))} ${hEsc(w.coin)}</b>`, '', 'Send the <b>transaction hash (TxID)</b> as your next message. It will show in the user\'s history.']), { reply_markup: cancelKb });
}

async function processApproveReject(cb, action, cbId) {
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
  if (!ctx) { return false; }

  const handler = action === 'approve' ? handleApprove : handleReject;
  const r = await handler(ctx.fuid, ctx.type, req || ctx, cbId);
  if (!r) { return false; }
  const label = ctx.type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL';
  const msgId = ctx.msgId || cb.message?.message_id;
  if (msgId) await tgEditH(msgId, fmtResolutionMsg(label, action, { ...ctx, ...r }, req), (ctx.type === 'dep' && isBscTx(req?.txid)) ? { reply_markup: { inline_keyboard: [bscBtn(req.txid)] } } : {});
  sentByCbId.delete(cbId);
  return true;
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
const MENU_KB = { inline_keyboard: [
  [{ text: '🆔 Open user by UID', callback_data: 'menu_find' }],
  [{ text: '🔎 Search by name', callback_data: 'menu_search' }, { text: '👥 All users', callback_data: 'menu_users' }],
  [{ text: '⏳ Pending', callback_data: 'menu_pending' }, { text: '📊 Stats', callback_data: 'menu_stats' }],
  [{ text: '📈 Today', callback_data: 'menu_today' }, { text: '📢 Broadcasts', callback_data: 'menu_broad' }],
  [{ text: '✉️ New broadcast', callback_data: 'menu_bcnew' }, { text: '↻ Refresh', callback_data: 'menu_home' }],
  [{ text: '📱 WhatsApp', callback_data: 'wa_panel' }, { text: '💱 P2P rates', callback_data: 'menu_p2prates' }],
  [{ text: '🪔 Diwali bonus', callback_data: 'menu_promo' }, { text: '🚗 BMW draw', callback_data: 'draw_ref' }],
] };

// ════════════════════════════════════════════════════════════════════
// P2P RATE SETTINGS + per-user limits (read by the app via /api/p2p/config)
// ════════════════════════════════════════════════════════════════════
const P2P_DEFAULTS = { buyMin: 97, buyMax: 99, sellMin: 101, sellMax: 108, buyLimMin: 500, buyLimMax: 1500, sellLimMin: 1000, sellLimMax: 4000, nightOn: true };
const P2P_CACHE = { at: 0, v: { ...P2P_DEFAULTS } };
async function getP2PSettings(fresh = false) {
  if (!fresh && Date.now() - P2P_CACHE.at < 30000) return P2P_CACHE.v;
  try {
    const v = (await db.ref('settings/p2p').once('value')).val() || {};
    P2P_CACHE.v = { ...P2P_DEFAULTS, ...v }; P2P_CACHE.at = Date.now();
  } catch (e) { log('P2P', `settings read err ${e.message}`); }
  return P2P_CACHE.v;
}
/** Hour in India (0-23). */
function istHour(ts = Date.now()) { return new Date(ts + 5.5 * 3600e3).getUTCHours(); }
async function sendP2PRates() {
  const s = await getP2PSettings(true);
  return tgSend(card('💱 P2P RATES', [
    ['🟢 BUY start', `₹${s.buyMin}`], ['🟢 BUY end', `₹${s.buyMax}`],
    ['🔴 SELL start', `₹${s.sellMin}`], ['🔴 SELL end', `₹${s.sellMax}`],
    ['🟢 BUY min limit', `₹${s.buyLimMin} – ₹${s.buyLimMax}`], ['🔴 SELL min limit', `₹${s.sellLimMin} – ₹${s.sellLimMax}`],
    ['🌙 Night mode (1–5 AM IST)', s.nightOn !== false ? 'ON · only 2–3 ads' : 'OFF'],
    'Tap a value to change it 👇',
  ]), { reply_markup: { inline_keyboard: [
    [{ text: 'BUY start', callback_data: 'p2pr_buyMin' }, { text: 'BUY end', callback_data: 'p2pr_buyMax' }],
    [{ text: 'SELL start', callback_data: 'p2pr_sellMin' }, { text: 'SELL end', callback_data: 'p2pr_sellMax' }],
    [{ text: 'BUY min-limit start', callback_data: 'p2pr_buyLimMin' }, { text: 'BUY min-limit end', callback_data: 'p2pr_buyLimMax' }],
    [{ text: 'SELL min-limit start', callback_data: 'p2pr_sellLimMin' }, { text: 'SELL min-limit end', callback_data: 'p2pr_sellLimMax' }],
    [{ text: s.nightOn !== false ? '🌙 Night mode: ON' : '🌙 Night mode: OFF', callback_data: 'p2pr_night' }],
    [{ text: '♻️ Reset all to default', callback_data: 'p2pr_reset' }],
    [{ text: '🏠 Menu', callback_data: 'menu_home' }],
  ] } });
}
async function userLimitsInfo(fuid) {
  const u = (await db.ref(`users/${fuid}`).once('value')).val() || {};
  const p2pDaily = Math.floor(r8(u.limits?.p2pDaily)) || 0;
  const minWithdraw = r8(u.limits?.minWithdraw) || (isReferred(u) ? 20 : 10);
  const from = istDayStart();
  let used = 0;
  for (const h of Object.values(u.history || {})) {
    const t = String(h?.type || '').toUpperCase();
    if ((t === 'BUY' || t === 'SELL') && (h.ts || 0) >= from) used++;
  }
  for (const o of Object.values(u.orders || {})) {
    const st = String(o?.status || '').toLowerCase();
    if (o && !['completed', 'cancelled', 'canceled'].includes(st)) used++;
  }
  return { p2pDaily, minWithdraw, usedToday: used, p2pBlocked: u.limits?.p2pBlocked === true, p2pBlockBuy: u.limits?.p2pBlockBuy === true, p2pBlockSell: u.limits?.p2pBlockSell === true };
}

// ════════════════════════════════════════════════════════════════════
// WHATSAPP PANEL — /whatsapp : linked account, or a QR to link it
// ════════════════════════════════════════════════════════════════════
let WA_QR_MSG = null;         // message_id of the last QR photo we sent
let WA_QR_SENT_AT = 0;
const waAgo = (ts) => { if (!ts) return '—'; const m = Math.round((Date.now() - ts) / 60000); return m < 1 ? 'just now' : m < 60 ? `${m}m ago` : `${Math.floor(m / 60)}h ${m % 60}m ago`; };
const WA_LINKED_KB = { inline_keyboard: [
  [{ text: '🧪 Send test message', callback_data: 'wa_test' }, { text: '↻ Refresh', callback_data: 'wa_panel' }],
  [{ text: '🔌 Unlink WhatsApp', callback_data: 'wa_unlinkask' }],
  [{ text: '🏠 Menu', callback_data: 'menu_home' }],
] };
const WA_QR_KB = { inline_keyboard: [
  [{ text: '🔄 New QR', callback_data: 'wa_qr' }, { text: '↻ Check status', callback_data: 'wa_panel' }],
  [{ text: '🏠 Menu', callback_data: 'menu_home' }],
] };

async function tgDeleteMsg(id) { if (id) await tgFetch('deleteMessage', { chat_id: TG_CHAT, message_id: id }); }

async function tgSendPhotoBuf(buf, caption, reply_markup) {
  try {
    const fd = new FormData();
    fd.append('chat_id', String(TG_CHAT));
    fd.append('caption', caption.slice(0, 1024));
    fd.append('parse_mode', 'Markdown');
    if (reply_markup) fd.append('reply_markup', JSON.stringify(reply_markup));
    fd.append('photo', new Blob([buf], { type: 'image/png' }), 'whatsapp-qr.png');
    const r = await fetch(`${TG_API}/sendPhoto`, { method: 'POST', body: fd });
    const j = await r.json();
    if (!j.ok) log('WA', `sendPhoto failed: ${j.description}`);
    return j;
  } catch (e) { log('WA', `sendPhoto err: ${e.message}`); return { ok: false }; }
}

async function sendWaLinkedCard() {
  const s = WA.status();
  return tgSend(card('📱 WHATSAPP — LINKED ✅', [
    ['📞 Number', s.number ? `+${s.number}` : '—'],
    ['👤 Name', s.name || '—'],
    ['🟢 Status', 'Connected'],
    ['⏱ Connected', waAgo(s.linkedAt)],
  ], 'OTP messages go out from this number. Session is saved — stays linked after restarts.'), { reply_markup: WA_LINKED_KB });
}

async function sendWaQR(force = false) {
  if (!force && WA_QR_MSG && Date.now() - WA_QR_SENT_AT < 15_000) return;  // debounce
  const wait = await tgSend('⏳ Preparing WhatsApp QR…');
  const r = (await WA.requestQR().catch(e => { log("WA", `requestQR err ${e.message}`); return null; })) || {};
  if (wait?.ok) await tgDeleteMsg(wait.result.message_id);
  if (r.linked) return sendWaLinkedCard();
  const buf = await WA.getQRBuffer();
  if (!buf) return tgSend('⚠️ QR could not be created right now. Try again in a few seconds.', { reply_markup: WA_QR_KB });
  await tgDeleteMsg(WA_QR_MSG);
  const caption = '*📱 Link WhatsApp*\n\n' +
    '1. Phone me WhatsApp kholo\n' +
    '2. ⋮ (Settings) → *Linked devices* → *Link a device*\n' +
    '3. Ye QR scan karo\n\n' +
    '_QR ~20 sec me badalta hai — expire ho to_ 🔄 *New QR* _dabao._\n' +
    'Link hote hi yahan ✅ message aayega.';
  const j = await tgSendPhotoBuf(buf, caption, WA_QR_KB);
  if (j.ok) { WA_QR_MSG = j.result.message_id; WA_QR_SENT_AT = Date.now(); }
}

async function sendWaPanel() {
  const s = WA.status();
  if (s.linked) return sendWaLinkedCard();
  if (s.registered && (s.state === 'connecting' || s.state === 'offline')) {
    return tgSend(card('📱 WHATSAPP — RECONNECTING ⏳', [
      ['📞 Number', s.number ? `+${s.number}` : 'saved session'],
      ['🔁 Status', 'Reconnecting (session kept, no QR needed)'],
    ], 'Wait 10–30 sec, then tap Refresh.'), { reply_markup: { inline_keyboard: [[{ text: '↻ Refresh', callback_data: 'wa_panel' }, { text: '🏠 Menu', callback_data: 'menu_home' }]] } });
  }
  return sendWaQR(true);
}

// live events from the engine → bot messages
WA.onEvent(async (type, info) => {
  if (type === 'linked') {
    await tgDeleteMsg(WA_QR_MSG); WA_QR_MSG = null;
    await tgSend(`✅ *WhatsApp linked!*\n📞 +${info.number || '—'}${info.name ? `  (${info.name})` : ''}\nAb ye logout nahi hoga — session saved hai.`, { reply_markup: WA_LINKED_KB });
  } else if (type === 'loggedout') {
    await tgSend('❌ *WhatsApp logged out* (phone se device hataya gaya). Dobara link karne ke liye QR lo 👇', { reply_markup: { inline_keyboard: [[{ text: '📱 Get QR', callback_data: 'wa_qr' }]] } });
  } else if (type === 'replaced') {
    log('WA', 'connection replaced by another instance — retrying');
  }
});

/** Home dashboard — live numbers + buttons, no commands to remember. */
async function sendMenu() {
  const s = await collectStats();
  await tgSend(card('🏠 BIEXC ADMIN DASHBOARD', [
    ['👥 Users', `${s.total}  (🚫 ${s.banned})`],
    ['💰 Total USDT', fmtNum(s.totalUsdt)],
    ['⏳ Pending', `📥 ${s.pendDep}  ·  📤 ${s.pendWit}`],
    ['🔄 Active P2P', s.activeP2P],
    ['📝 KYC to review', s.kycPending],
  ], 'Tap a button 👇 — no commands needed'), { reply_markup: MENU_KB });
}

/** KYC rule for P2P: Level 2 approved, or Level 2 documents submitted. */
function kycAllowsTrade(u) {
  const k = u?.kycLevels || {};
  return (k.level || 0) >= 2 || u?.kycStatus === 'APPROVED' || ((k.level || 0) >= 1 && k.l2status === 'PENDING');
}
/** cbId looks like trade_<orderId>_<ts> → find which user owns that order. */
async function findTradeOwner(tradeCb) {
  try {
    const orderId = String(tradeCb).split('_')[1];
    const snap = await db.ref('users').once('value');
    let hit = null;
    snap.forEach(c => {
      if (hit) return;
      const u = c.val() || {};
      const chats = u.chats || {};
      for (const [cid, st] of Object.entries(chats)) if (st?.cbId === tradeCb || cid === orderId) { hit = { fuid: c.key, user: u }; return; }
      for (const [oid, o] of Object.entries(u.orders || {})) if (oid === orderId || String(o?.id) === orderId) { hit = { fuid: c.key, user: u }; return; }
    });
    return hit;
  } catch (e) { log('TRADE', `owner lookup err ${e.message}`); return null; }
}

/** Full KYC + contact details for one user, with the Aadhaar/PAN photos. */
async function sendKycDetails(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  const u = found.user, k = u.kycLevels || {}, b = k.basic || {}, d = k.docs || {};
  const when = (t) => t ? new Date(t).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) : '—';
  const st = k.l2status === 'PENDING' ? '🟡 Submitted (review)' : (k.level || 0) >= 2 || u.kycStatus === 'APPROVED' ? '✅ Approved' : (k.level || 0) >= 1 ? '🔵 Only Level 1' : '⚪ Not started';
  const mask = (x, keep = 4) => x ? String(x) : '—';
  await tgSend(card(`🪪 KYC — ${u.uid || uid}`, [
    ['📝 Status', st],
    ['🛂 P2P allowed', kycAllowsTrade(u) ? '✅ Yes' : '⛔ No'],
    '', '*Level 1 — personal*',
    ['📛 Full name', b.name || '—'],
    ['📱 Mobile', b.phone ? `+91 ${String(b.phone).replace(/^\+?91/, '')}` : (u.phone || '—')],
    ['📧 Email', u.email || '—'],
    ['🎂 DOB', b.dob || '—'],
    ['💼 Profession', b.profession || '—'],
    ['🏠 Address', b.address || '—'],
    ['🏙 City / PIN', [b.city, b.pin].filter(Boolean).join(' · ') || '—'],
    ['🗺 State', b.state || '—'],
    ['🕐 L1 at', when(k.l1At || b.ts)],
    '', '*Level 2 — documents*',
    ['🆔 Aadhaar', d.aadhaar?.id ? mask(d.aadhaar.id) : (d.aadhaar ? 'photo only' : '—')],
    ['🧾 PAN', d.pan?.id ? mask(d.pan.id) : (d.pan ? 'photo only' : '—')],
    ['✔ Name match', [d.aadhaar?.nameMatch, d.pan?.nameMatch].filter(Boolean).join(' / ') || '—'],
    ['🕐 L2 at', when(k.l2At)],
    ['👮 Approved by', k.approvedBy ? `${k.approvedBy} · ${when(k.approvedAt)}` : '—'],
  ]), { reply_markup: { inline_keyboard: [[
    (k.level || 0) >= 2 || u.kycStatus === 'APPROVED'
      ? { text: '♻️ Reset KYC', callback_data: `kycrevoke_${uid}` }
      : { text: '✅ Approve KYC', callback_data: `kycok_${uid}` },
    { text: '👤 Back to user', callback_data: `userdetail_${uid}` }]] } });
  for (const [label, doc] of [['Aadhaar', d.aadhaar], ['PAN', d.pan]]) {
    if (!doc || typeof doc !== 'object') continue;
    // Send front, then back (older submissions may have front only).
    const sides = [['front', doc.img], ['back', doc.backImg]];
    for (const [side, img] of sides) {
      if (typeof img !== 'string' || !img.startsWith('data:image')) continue;
      try {
        const buf = Buffer.from(img.replace(/^data:image\/\w+;base64,/, ''), 'base64');
        await tgSendPhotoBuf(buf, `🪪 ${label} (${side === 'back' ? 'Back' : 'Front'}) — ${b.name || u.name || ''} (${u.uid || uid})`);
      } catch (e) { log('KYC', `photo err ${e.message}`); }
    }
  }
}

async function sendUserDetailCard(cb, uid) {
  if (cb?.id) await tgAnswer(cb.id, 'Loading…');
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`), { reply_markup: { inline_keyboard: [[{ text: '🔎 Search by name', callback_data: 'menu_search' }, { text: '🏠 Menu', callback_data: 'menu_home' }]] } }); return; }
  uid = shownUID(found.fuid, found.user);
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
    ['📝 KYC', kycInfo(u).label],
    ['🎁 Refer code', u.refer?.code ? `\`${u.refer.code}\`` : '—'],
    ['🤝 Referred by', u.referral?.by ? `\`${u.referral.byUid || '—'}\` ${u.referral.byName || ''}${u.referral.rewarded ? ' · paid ✅' : ''}` : '—'],
    ['👥 Invited', `${Object.keys(u.refer?.invites || {}).length} · earned ${fmtNum(u.refer?.earned || 0)} USDT`],
    ['💸 Min withdraw', `$${r8(u.limits?.minWithdraw) > 0 ? r8(u.limits.minWithdraw) : (isReferred(u) ? '20 (referral default)' : 10)}`],
    ['⛔ P2P status', u.limits?.p2pBlocked ? 'BLOCKED (Buy+Sell)' : (u.limits?.p2pBlockBuy || u.limits?.p2pBlockSell) ? `Partial: ${[u.limits?.p2pBlockBuy && 'Buy', u.limits?.p2pBlockSell && 'Sell'].filter(Boolean).join(' + ')} blocked` : 'Allowed'],
    ['🔢 P2P daily limit', r8(u.limits?.p2pDaily) > 0 ? `${r8(u.limits.p2pDaily)} / day` : 'Unlimited'],
    ['📅 Joined', u.createdAt ? new Date(u.createdAt).toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata' }) : '—'],
    '', '💰 *Balances*', ...(balLines.length ? balLines : ['   (empty)']),
    '', '📊 *Last 3 transactions*', ...(last3.length ? last3 : ['   (none)']),
  ];

  const kd = kycInfo(u).done;
  const buttons = [
    [{ text: '➕ Credit', callback_data: `creditgo_${uid}` },
     { text: '➖ Debit',  callback_data: `debitgo_${uid}` },
     { text: '🎯 Set bal', callback_data: `setbalgo_${uid}` }],
    [{ text: '💰 Balances', callback_data: `bal_${uid}` },
     { text: '📋 History',  callback_data: `history_${uid}` },
     { text: '🎁 Referrals', callback_data: `refs_${uid}` }],
    [ kd ? { text: '♻️ Reset KYC', callback_data: `kycrevoke_${uid}` }
         : { text: '✅ Approve KYC', callback_data: `kycok_${uid}` },
      { text: '💬 Message', callback_data: `msgprompt_${uid}` }],
    [{ text: '🪪 KYC details', callback_data: `kycview_${uid}` },
     { text: '📞 Contact info', callback_data: `kycview_${uid}` }],
    [ u.banned
        ? { text: '✅ Unban', callback_data: `unban_${uid}` }
        : { text: '🚫 Ban',   callback_data: `banask_${uid}` },
      { text: '🔄 Close P2P', callback_data: `closep2p_${uid}` }],
    [{ text: '💸 MWithdraw', callback_data: `mwd_${uid}` },
     { text: '🔢 PLimit', callback_data: `plim_${uid}` }],
    [ u.limits?.p2pBlockBuy
        ? { text: '✅ Buy allowed', callback_data: `p2pblkb_${uid}` }
        : { text: '⛔ Block Buy', callback_data: `p2pblkb_${uid}` },
      u.limits?.p2pBlockSell
        ? { text: '✅ Sell allowed', callback_data: `p2pblks_${uid}` }
        : { text: '⛔ Block Sell', callback_data: `p2pblks_${uid}` },
      u.limits?.p2pBlocked
        ? { text: '✅ Both allowed', callback_data: `p2pblk_${uid}` }
        : { text: '⛔ Block Both', callback_data: `p2pblk_${uid}` }],
    [{ text: '↻ Refresh', callback_data: `userdetail_${uid}` },
     { text: '🆔 Other UID', callback_data: 'menu_find' },
     { text: '🏠 Menu',   callback_data: 'menu_home' }],
  ];
  await tgSend(card(`👤 USER — ${u.uid || uid}`, rows), { reply_markup: { inline_keyboard: buttons } });
}

async function sendUserReferrals(uid) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(bad(`UID \`${uid}\` not found.`)); return; }
  const u = found.user;
  const inv = Object.values(u.refer?.invites || {}).sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const ico = { registered: '🆕', kyc: '🪪', deposited: '💵', rewarded: '✅', expired: '⌛' };
  const lines = [`🎁 *REFERRALS — ${u.uid || uid}*`, '',
    `🏷 Code: \`${u.refer?.code || '—'}\``,
    `👥 Invited: *${inv.length}*  ·  ✅ Paid: *${inv.filter(i => i.status === 'rewarded').length}*`,
    `💰 Earned: *${fmtNum(u.refer?.earned || 0)} USDT*`,
    u.referral?.by ? `🤝 Referred by: \`${u.referral.byUid || '—'}\` ${u.referral.byName || ''} · deposit $${fmtNum(u.referral.depositUsd || 0)}${u.referral.rewarded ? ' · paid ✅' : ''}` : '🤝 Referred by: —',
    '', ...(inv.length ? inv.slice(0, 25).map(i => `${ico[i.status] || '•'} \`${i.uid || '—'}\` ${i.name || ''} · ${i.status} · $${fmtNum(i.deposit || 0)}`) : ['(no invites yet)'])];
  await tgSend(lines.join('\n'), { reply_markup: { inline_keyboard: [[{ text: '👁 Open user', callback_data: `userdetail_${uid}` }, { text: '🏠 Menu', callback_data: 'menu_home' }]] } });
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

/** Users that have not finished KYC (Level 2 / approved). */
async function listNoKycUsers() {
  const snap = await db.ref('users').once('value');
  const out = [];
  snap.forEach(c => { const u = c.val(); if (u && typeof u === 'object' && !kycInfo(u).done) out.push({ fuid: c.key, uid: u.uid, user: u }); });
  return out;
}

/** /resetnokyc — delete every non-KYC account (data + login) so the same email can sign up fresh. */
async function resetNoKycUsers() {
  const list = await listNoKycUsers();
  if (!list.length) return tgSend('✅ No non-KYC users.');
  const set = new Set(list.map(x => x.fuid));
  const [refs, wallets] = await Promise.all([db.ref('referralCodes').once('value'), db.ref('depositWalletIndex').once('value')]);
  const upd = {};
  for (const x of list) { upd[`users/${x.fuid}`] = null; if (x.uid) upd[`uidIndex/${x.uid}`] = null; }
  refs.forEach(c => { if (set.has(c.val())) upd[`referralCodes/${c.key}`] = null; });
  wallets.forEach(c => { const v = c.val(); if (set.has(typeof v === 'object' && v ? v.fuid : v)) upd[`depositWalletIndex/${c.key}`] = null; });
  const keys = Object.keys(upd);
  for (let i = 0; i < keys.length; i += 500) await db.ref().update(Object.fromEntries(keys.slice(i, i + 500).map(k => [k, null])));
  let authDel = 0, authFail = 0;
  const ids = [...set];
  for (let i = 0; i < ids.length; i += 1000) {
    try { const r = await admin.auth().deleteUsers(ids.slice(i, i + 1000)); authDel += r.successCount; authFail += r.failureCount; }
    catch (e) { authFail += Math.min(1000, ids.length - i); log('ERR', `resetnokyc auth: ${e.message}`); }
  }
  log('ADMIN', `resetnokyc: ${list.length} accounts deleted (auth ok ${authDel}, fail ${authFail})`);
  return tgSend(card('🧹 NON-KYC ACCOUNTS RESET', [['Accounts deleted', list.length], ['Logins removed', authDel], ['Login errors', authFail]]));
}

async function handleActiveUsers() {
  const snap = await db.ref('users').once('value');
  const users = [];
  snap.forEach(c => {
    const u = c.val(); if (!u || typeof u !== 'object') return;
    const bals = {};
    for (const [k, v] of Object.entries(u.balances && typeof u.balances === 'object' ? u.balances : {})) { const n = r8(v); if (n > 0) bals[k.toUpperCase()] = n; }
    if (bals.USDT === undefined && r8(u.balance) > 0) bals.USDT = r8(u.balance);
    if (Object.keys(bals).length) users.push({ ...u, uid: shownUID(c.key, u), bals });
  });
  if (!users.length) { await tgSend('No users with balance.'); return; }
  users.sort((a, b) => (b.bals.USDT || 0) - (a.bals.USDT || 0));
  await tgSend(card('💰 ACTIVE USERS', [['With balance', users.length], ['Sorted by', 'USDT balance']]));
  const CHUNK = 8;
  for (let i = 0; i < users.length; i += CHUNK) {
    const slice = users.slice(i, i + CHUNK);
    const lines = slice.map(u => `${u.banned ? '🚫' : '✅'} \`${u.uid}\`  *${u.name || '—'}*\n     💰 ${Object.entries(u.bals).map(([c, v]) => `${fmtNum(v)} ${c}`).join(' · ')}`);
    const keyboard = slice.map(u => [{ text: `👁 ${u.uid} — ${u.name || '—'}`.slice(0, 60), callback_data: `userdetail_${u.uid}` }]);
    keyboard.push([{ text: '🏠 Menu', callback_data: 'menu_home' }]);
    await tgSend(lines.join('\n'), { reply_markup: { inline_keyboard: keyboard } });
  }
}

async function handleUsersList() {
  const snap = await db.ref('users').once('value');
  const users = [];
  snap.forEach(c => { const u = c.val(); if (u && typeof u === 'object') users.push({ ...u, uid: shownUID(c.key, u) }); });
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
    if (kycInfo(u).pending) s.kycPending++;
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

// ── /today — detailed report for the current IST day (12am–12am) ─────
async function handleToday() {
  const wait = await tgSend('⏳ Counting today\'s activity…');
  try {
    const dayStart = istDayStart();
    const snap = await db.ref('users').once('value');
    const t = {
      newUsers: 0,
      buyN: 0, buyUsdt: 0, buyInr: 0,
      sellN: 0, sellUsdt: 0, sellInr: 0,
      p2pCancel: 0, p2pActive: 0,
      depN: 0, depUsdt: 0, depPendN: 0, depPendUsdt: 0,
      witN: 0, witUsdt: 0, witPendN: 0, witPendUsdt: 0, witRejN: 0,
      creditN: 0, creditUsdt: 0, debitN: 0, debitUsdt: 0,
    };
    snap.forEach(c => {
      const u = c.val() || {}; if (!u.uid) return;
      if ((u.createdAt || 0) >= dayStart) t.newUsers++;
      for (const o of Object.values(u.orders || {})) {
        const st = String(o?.status || '').toLowerCase();
        if (st && !['completed', 'cancelled', 'canceled'].includes(st) && (o.ts || o.createdAt || 0) >= dayStart) t.p2pActive++;
      }
      for (const [kind, pre] of [['dep', 'depPend'], ['wit', 'witPend']]) {
        for (const [k, v] of Object.entries((u.pendingReqs || {})[kind] || {})) {
          if (k === 'botLock' || !v) continue;
          if ((v.ts || v.createdAt || 0) < dayStart) continue;
          t[`${pre}N`]++; t[`${pre}Usdt`] += r8(v.amt);
        }
      }
      for (const h of Object.values(u.history || {})) {
        if (!h || (h.ts || 0) < dayStart) continue;
        const ty = String(h.type || '').toUpperCase();
        const amt = r8(h.amt);
        const done = h.status === 'COMPLETED';
        if (ty === 'BUY' && done) { t.buyN++; t.buyUsdt += amt; t.buyInr += r8(h.inr); }
        else if (ty === 'SELL' && done) { t.sellN++; t.sellUsdt += amt; t.sellInr += r8(h.inr); }
        else if (ty === 'BUY_CANCEL' || ty === 'SELL_CANCEL') t.p2pCancel++;
        else if (/DEPOSIT/.test(ty) && done) { t.depN++; t.depUsdt += amt; }
        else if (/WITHDRAW/.test(ty)) {
          if (done) { t.witN++; t.witUsdt += amt; }
          else if (h.status === 'REJECTED') t.witRejN++;
        }
        else if (ty === 'ADMIN_CREDIT' && done) { t.creditN++; t.creditUsdt += amt; }
        else if (ty === 'ADMIN_DEBIT' && done) { t.debitN++; t.debitUsdt += amt; }
      }
    });
    const p2pN = t.buyN + t.sellN, p2pUsdt = t.buyUsdt + t.sellUsdt, p2pInr = t.buyInr + t.sellInr;
    const istNow = new Date(Date.now() + IST_OFF).toISOString().slice(11, 16);
    if (wait?.ok) await tgDeleteMsg(wait.result.message_id);
    await tgSend(card(`📅 TODAY'S REPORT — ${istDayKey()} (IST)`, [
      ['🕛 Period', `12:00 AM → ${istNow} IST (resets at midnight)`],
      '────────────────',
      ['🟢 P2P BUY', `${t.buyN} orders · ${fmtNum(t.buyUsdt)} USDT · ₹${fmtNum(t.buyInr)}`],
      ['🔴 P2P SELL', `${t.sellN} orders · ${fmtNum(t.sellUsdt)} USDT · ₹${fmtNum(t.sellInr)}`],
      ['🔁 P2P total', `${p2pN} orders · ${fmtNum(p2pUsdt)} USDT · ₹${fmtNum(p2pInr)}`],
      ['⏳ P2P active now', t.p2pActive],
      ['✖ P2P cancelled', t.p2pCancel],
      '────────────────',
      ['📥 Deposits done', `${t.depN} · ${fmtNum(t.depUsdt)} USDT`],
      ['📥 Deposits pending', `${t.depPendN} · ${fmtNum(t.depPendUsdt)} USDT`],
      ['📤 Withdrawals done', `${t.witN} · ${fmtNum(t.witUsdt)} USDT`],
      ['📤 Withdrawals pending', `${t.witPendN} · ${fmtNum(t.witPendUsdt)} USDT`],
      ['📤 Withdrawals rejected', t.witRejN],
      '────────────────',
      ['👑 Admin credit', `${t.creditN} · ${fmtNum(t.creditUsdt)} USDT`],
      ['👑 Admin debit', `${t.debitN} · ${fmtNum(t.debitUsdt)} USDT`],
      ['🆕 New users', t.newUsers],
    ]), { reply_markup: { inline_keyboard: [[
      { text: '↻ Refresh', callback_data: 'menu_today' }, { text: '🏠 Menu', callback_data: 'menu_home' },
    ]] } });
  } catch (e) {
    if (wait?.ok) await tgDeleteMsg(wait.result.message_id);
    await tgSend(bad(`Report failed: ${e.message}`));
  }
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

async function adminCreditDebit(uid, amt, sign, type, coin = 'USDT', showHist = true) {
  const found = await findUserByUID(uid);
  if (!found) { await tgSend(`❌ UID \`${uid}\` not found.`); return; }
  const delta = sign === '+' ? +amt : -amt;
  const result = await mutateBalance(found.fuid, coin, delta);
  if (!result) { await tgSend(`❌ Insufficient ${coin} balance for ${uid}.`); return; }
  if (showHist) await pushHistory(found.fuid, {
    type, coin, amt, status: 'COMPLETED', uid: found.user.uid,
    sender: 'ADMIN', note: `${sign === '+' ? 'Credited' : 'Debited'} ${amt} ${coin}`,
  });
  await tgSend([
    `${sign==='+' ? '✅ *CREDITED*' : '✅ *DEBITED*'}`, '',
    `👤 UID: \`${uid}\``,
    `📛 ${found.user.name || '—'}`,
    `${sign==='+'?'➕':'➖'} ${amt} ${coin}`,
    `💰 ${coin}: *${result.oldBal} → ${result.newBal}*`,
    showHist ? '📜 Shown in history' : '🙈 Hidden from history',
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

// ── /allmail <text> — email every registered user (one by one, throttled) ──
const ALLMAIL = { job: null };
const ALLMAIL_GAP_MS = Number(process.env.ALLMAIL_GAP_MS || 700);
async function allMailRecipients() {
  const snap = await db.ref('users').once('value');
  const seen = new Set(), list = [];
  snap.forEach(c => {
    const u = c.val() || {}, e = String(u.email || u.profile?.email || '').trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e) || seen.has(e) || u.banned === true || u.frozen === true) return;
    seen.add(e); list.push({ fuid: c.key, email: e, name: u.profile?.nickname || u.name || '' });
  });
  return list;
}
async function allMailPrompt(text) {
  if (!text) return tgSend(bad('Usage: `/allmail your message here`\nFirst line becomes the subject if you write `Subject | message`.'));
  if (ALLMAIL.job) return tgSend(bad(`Already sending (${ALLMAIL.job.sent}/${ALLMAIL.job.total}). Use /allmailstop first.`));
  if (!RESEND_API_KEY && !mailer) return tgSend(bad('Mail is not configured on the server (RESEND_API_KEY or SMTP).'));
  const list = await allMailRecipients();
  const id = newConfirm({ op: 'allmail', text });
  const mins = Math.ceil(list.length * ALLMAIL_GAP_MS / 60000);
  const md = (x) => String(x || '').replace(/[_*`\[\]]/g, ' ');
  const { style } = amSplit(text.split('|').slice(1).join('|') || text);
  const btn = [...text.matchAll(/\[button:\s*([^|\]]+)\|\s*([^\]\s]+)\s*\]/gi)].map(x => `${x[1].trim()} → ${x[2]}`);
  return tgSend(card('📧 EMAIL ALL USERS?', [['👥 Recipients', `${list.length} emails`], ['⏱ Approx time', `${mins} min`], ...(style ? [['🎨 Design', md(AM_STYLES[style].name)]] : []), ...btn.map(b => ['🔘 Button', md(b)]), ['💬 Text', md(amPlain(text)).slice(0, 300)]], '🧪 Tip: /allmailtest your@email.com <same text> sends only to you first.'),
    { reply_markup: { inline_keyboard: [[{ text: '✅ Send email to all', callback_data: `cfm_${id}` }, { text: '✖ Cancel', callback_data: 'askcancel' }]] } });
}
async function allMailRun(text) {
  if (ALLMAIL.job) return tgSend(bad('Already sending — use /allmailstop first.'));
  const parts = text.split('|');
  const subject = (parts.length > 1 ? parts.shift() : 'Update from BIEXC').trim().slice(0, 120) || 'Update from BIEXC';
  const body = parts.join('|').trim();
  const list = await allMailRecipients();
  const job = ALLMAIL.job = { total: list.length, sent: 0, failed: 0, stop: false };
  log('ALLMAIL', `start ${list.length} recipients subject="${subject}"`);
  await tgSend(`📧 Sending to ${list.length} users… I will report progress. /allmailstop to stop.`, { parse_mode: undefined });
  (async () => {
    let lastReport = Date.now(), lastErr = '';
    for (const r of list) {
      if (job.stop) break;
      try { await sendMailAny({ to: r.email, subject, html: allMailHtml(subject, body, r.name), text: (r.name ? `Hi ${r.name},\n\n` : '') + amPlain(body) }); job.sent++; }
      catch (e) { job.failed++; lastErr = e.message; if (/rate|429|quota|limit/i.test(e.message)) await new Promise(x => setTimeout(x, 5000)); }
      if (Date.now() - lastReport > 60000) { lastReport = Date.now(); tgSend(`📧 Progress: ${job.sent} sent · ${job.failed} failed · ${job.total} total`, { parse_mode: undefined }).catch(() => {}); }
      await new Promise(x => setTimeout(x, ALLMAIL_GAP_MS));
    }
    ALLMAIL.job = null;
    log('ALLMAIL', `done sent=${job.sent} failed=${job.failed}`);
    tgSend(`${job.stop ? '⏹ Stopped' : '✅ Done'} — ${job.sent} sent · ${job.failed} failed · ${job.total} total${lastErr ? '\nLast error: ' + lastErr.slice(0, 200) : ''}`, { parse_mode: undefined }).catch(() => {});
  })().catch(e => { ALLMAIL.job = null; tgSend('Email job crashed: ' + e.message, { parse_mode: undefined }).catch(() => {}); });
}

async function allMailTest(to, text) {
  if (!RESEND_API_KEY && !mailer) return tgSend(bad('Mail is not configured on the server (RESEND_API_KEY or SMTP).'));
  const parts = text.split('|');
  const subject = (parts.length > 1 ? parts.shift() : 'Update from BIEXC').trim().slice(0, 120) || 'Update from BIEXC';
  const body = parts.join('|').trim();
  try {
    await sendMailAny({ to, subject, html: allMailHtml(subject, body, 'Test'), text: 'Hi Test,\n\n' + amPlain(body) });
    return tgSend(`🧪 Test email sent to ${to}. Check Inbox / Promotions / Spam.`, { parse_mode: undefined });
  } catch (e) { return tgSend(`❌ Test email failed: ${e.message.slice(0, 200)}`, { parse_mode: undefined }); }
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
  if (upd.callback_query) return handleCallback(upd.callback_query, upd);
  const msg = upd.message;
  if (!msg) return;
  const chatId = String(msg.chat.id);
  if (chatId !== String(TG_CHAT)) return;
  const text = (msg.text || '').trim();

  // V43: reply-tags first — deposit assignment and P2P chat go to exactly the tagged user/order.
  if (db && !msg.from?.is_bot) {
    try {
      if (msg.reply_to_message && await depAssignReply(msg, text)) { SERVER_HANDLED_UPD.add(upd.update_id); return; }
      if (await p2pAdminReply(msg, text)) { SERVER_HANDLED_UPD.add(upd.update_id); return; }
    } catch (e) { log('ROUTE', `reply routing: ${e.message}`); }
  }

  // A pending withdrawal TxID request takes priority over live support routing.
  if (ASK && ASK.kind === 'withash' && text && !text.startsWith('/') && Date.now() - ASK.ts < ASK_TTL) return handleAskReply(text);
  // Customer-care messages are exclusively handled by the separate bot.
  if (!text) return;
  if (/^\/(start|menu|help)\b/i.test(text)) { ASK = null; return sendMenu(); }
  if (/^\/(whatsapp|wa)\b/i.test(text)) { ASK = null; return sendWaPanel(); }
  { const pm = text.match(/^\/(complete|reject)(?:@\S+)?\s+#?(\S+)/i);
    if (pm) { ASK = null; return adminSettleOrder(pm[2], pm[1].toLowerCase()).catch(e => tgSend(bad(e.message))); }
    if (/^\/(complete|reject)\b/i.test(text)) return tgSend('Use: `/complete ORDERID` or `/reject ORDERID`'); }
  if (/^\/(deplock|deprelease|setwallet|unmatched|assign)\b/i.test(text)) { ASK = null; if (await adAdminCmd(text).catch(e => (tgSend(bad(e.message)), true))) return; }
  if (/^\/cancelalltrades?(?:@\S+)?\s*$/i.test(text)) { ASK = null; return cancelAllTrades().catch(e => tgSend(bad(e.message))); }
  if (/^\/resetnokyc(?:@\S+)?\s*$/i.test(text)) { ASK = null; return listNoKycUsers().then(l => l.length ? tgSend(`⚠️ *Delete ${l.length} non-KYC account(s)?*\nTheir data, balance and login will be removed. They can sign up again with the same email.\nThis cannot be undone.`, { reply_markup: { inline_keyboard: [[{ text: '✅ Yes, delete', callback_data: 'rnk_yes' }, { text: '✖ Cancel', callback_data: 'rhb_no' }]] } }) : tgSend('✅ No non-KYC users.')).catch(e => tgSend(bad(e.message))); }
  if (/^\/resethousebalance(?:@\S+)?\s*$/i.test(text)) { ASK = null; return tgSend('⚠️ *Reset ALL user balances to 0?*\nThis cannot be undone.', { reply_markup: { inline_keyboard: [[{ text: '✅ Yes, reset all', callback_data: 'rhb_yes' }, { text: '✖ Cancel', callback_data: 'rhb_no' }]] } }); }
  { const am = text.match(/^\/allmail(?:@\S+)?(?:\s+([\s\S]+))?$/i);
    if (am) { ASK = null; return allMailPrompt((am[1] || '').trim()).catch(e => tgSend(bad(e.message))); } }
  if (/^\/allmailstop(?:@\S+)?\s*$/i.test(text)) { ASK = null; if (!ALLMAIL.job) return tgSend('No email sending in progress.'); ALLMAIL.job.stop = true; return tgSend('⏹ Stopping — no more emails will be sent.'); }
  if (/^\/activeusers?(?:@\S+)?\s*$/i.test(text)) { ASK = null; return handleActiveUsers().catch(e => tgSend(bad(e.message))); }
  if (/^\/today(?:@\S+)?\s*$/i.test(text)) { ASK = null; return handleToday().catch(e => tgSend(bad(e.message))); }
  if (/^\/(draw|bmw)(?:@\S+)?\s*$/i.test(text)) { ASK = null; return sendDrawPanel().catch(e => tgSend(bad(e.message))); }
  if (/^\/(diwali|promo)(?:@\S+)?\s*$/i.test(text)) { ASK = null; return sendPromoPanel('DIWALI20').catch(e => tgSend(bad(e.message))); }
  { const mt = text.match(/^\/allmailtest(?:@\S+)?\s+(\S+@\S+)\s+([\s\S]+)$/i);
    if (mt) { ASK = null; return allMailTest(mt[1], mt[2].trim()).catch(e => tgSend(bad(e.message))); }
    if (/^\/allmailtest\b/i.test(text)) return tgSend('Use: /allmailtest your@email.com Subject | message', { parse_mode: undefined }); }
  { const mt = text.match(/^\/mailtest(?:@\S+)?(?:\s+(\S+))?\s*$/i);
    if (mt) { ASK = null; return mailTestCmd(mt[1]).catch(e => tgSend(bad(e.message))); } }
  if (/^\/mailstatus(?:@\S+)?\s*$/i.test(text)) { ASK = null; return tgSendH(mailStatusCard()); }
  if (text === '/ping') return tgSend(card('🟢 BOT ONLINE', [
    ['⏱ Uptime', `${Math.floor((Date.now() - BOT_START_TIME) / 60000)}m`],
    ['🔑 Instance', `\`${INSTANCE_ID}\``],
  ]));
  if (ASK && Date.now() - ASK.ts < ASK_TTL && !text.startsWith('/')) return handleAskReply(text);
  ASK = null;
  // Typed a UID on its own → open that user straight away
  if (/^[A-Za-z0-9_@.+-]{4,40}$/.test(text)) {
    const f = await findUserByUID(text);
    if (f) return sendUserDetailCard(null, shownUID(f.fuid, f.user));
  }
  // Anything else → treat as a name search, then show the dashboard buttons
  if (!text.startsWith('/') && text.length >= 2) return sendSearchResults(text.slice(0, 60));
  return sendMenu();
}

// ════════════════════════════════════════════════════════════════════
// POLLING
// ════════════════════════════════════════════════════════════════════
let lastUpdateId = 0;
let lastPollOkAt = Date.now();
let lastBeatAt = 0;
/** Approve/Reject taps are handed to the app only AFTER the one-tap guard
 *  accepts them (see handleCallback) — never straight from the poll loop. */
const isGuardedTap = (upd) => /^(approve|reject)_(dep_|wit_|trade_)/.test(upd?.callback_query?.data || '');
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
    if (Date.now() - lastBeatAt > 20_000) { lastBeatAt = Date.now(); beatInstanceLock(); }
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
    const batch = data.result || [];
    if (batch.length) {
      // One offset write per batch (was one awaited write per update).
      lastUpdateId = batch[batch.length - 1].update_id;
      saveLastUpdateId(lastUpdateId).catch(() => {});
    }
    for (const upd of batch) {
      // Messages reach the app only when the server did not deliver them itself
      // (stops double chat bubbles and stray "#UID" text in old app versions).
      const isMsg = !!upd.message;
      if (!isGuardedTap(upd) && !isMsg) pushRecentUpdate(upd);
      try { await handleUpdate(upd); } catch (e) { log('ERR', `handleUpdate: ${e.message}`); }
      if (isMsg && !SERVER_HANDLED_UPD.delete(upd.update_id)) pushRecentUpdate(upd);
    }
  } catch (e) {
    pollFailStreak++;
    log('POLL', `err ${e.message}`);
    await new Promise(r => setTimeout(r, Math.min(10_000, 1000 * pollFailStreak)));
  }
}



// ════════════════════════════════════════════════════════════════════
// V47 P2P LOG GROUP — the customer-care bot posts a short log line for every
// P2P event (new BUY / new SELL, payment claimed, release requested,
// completed, cancelled) into ONE Telegram group chosen by the admin.
// Setup: add the support bot to the group (admin) → approve in support chat.
// A group can never connect itself: only a tap in TG_CHAT_SUPPORT turns it on.
// Emails, UPI ids and payment proofs are never posted to the group.
// ════════════════════════════════════════════════════════════════════
const LOG_GROUP_REF = 'botState/logGroup';
const LOG_EVENTS_REF = 'botState/p2pLog';
let LOG_GROUP = null;                 // { chatId, title, setAt }
let LOG_GROUP_WATCHING = false;
const LOG_REQ_COOLDOWN = new Map();   // chatId → last request ts (anti-spam)

function logGroupWatch() {
  if (LOG_GROUP_WATCHING || !db) return;
  LOG_GROUP_WATCHING = true;
  db.ref(LOG_GROUP_REF).on('value', s => { const v = s.val(); LOG_GROUP = v && v.chatId ? v : null; },
    e => { LOG_GROUP_WATCHING = false; log('LOGS', `watch cancelled: ${e.message}`); });
}
const logIsGroupChat = (chat) => chat && (chat.type === 'group' || chat.type === 'supergroup');
const logIsAdminChat = (id) => String(id) === String(TG_CHAT_SUPPORT) || String(id) === String(TG_CHAT);

async function logGroupSet(chatId, title) {
  const v = { chatId: String(chatId), title: String(title || 'Group').slice(0, 80), setAt: Date.now() };
  await db.ref(LOG_GROUP_REF).set(v); LOG_GROUP = v; return v;
}
async function logGroupClear() { await db.ref(LOG_GROUP_REF).remove(); LOG_GROUP = null; }

/** Send one HTML line to the log group. Follows a group → supergroup upgrade once. */
async function logGroupSend(html, chatId = LOG_GROUP?.chatId) {
  if (!SUPPORT_ENABLED || !chatId) return { ok: false };
  let r = await tgFetchSup('sendMessage', { chat_id: chatId, text: html, parse_mode: 'HTML', disable_web_page_preview: true, disable_notification: false });
  const moved = r?.parameters?.migrate_to_chat_id;
  if (!r.ok && moved && LOG_GROUP && String(LOG_GROUP.chatId) === String(chatId)) {
    await logGroupSet(moved, LOG_GROUP.title).catch(() => {});
    r = await tgFetchSup('sendMessage', { chat_id: moved, text: html, parse_mode: 'HTML', disable_web_page_preview: true });
  }
  if (!r.ok && LOG_GROUP && String(LOG_GROUP.chatId) === String(chatId) && /kicked|not a member|chat not found|have no rights/i.test(r.description || '')) {
    log('LOGS', `log group unreachable (${r.description}) — paused`);
  }
  return r;
}

const logInr = (v) => { if (v === '' || v == null) return ''; const n = Number(String(v).replace(/[₹,\s]/g, '')); return Number.isFinite(n) && /^[₹\d.,\s]+$/.test(String(v)) ? `₹${n.toLocaleString('en-IN', { maximumFractionDigits: 2 })}` : String(v); };
const logUsdt = (v) => { if (v === '' || v == null) return ''; const n = Number(String(v).replace(/[,\s]|USDT/gi, '')); return Number.isFinite(n) ? `${fmtNum(n)} USDT` : String(v); };

/** Card text for one P2P event. info = { mode, name, uid, merchant, inr, usdt, rate }. */
function p2pLogCard(kind, orderId, info) {
  const mode = String(info.mode || '').toUpperCase();
  const side = mode === 'SELL' ? 'SELL' : mode === 'BUY' ? 'BUY' : '';
  const dot = side === 'SELL' ? '🔴' : '🟢';
  const head = {
    new: `${dot} <b>NEW ${side ? side + ' ' : ''}ORDER</b>`,
    claim: `💸 <b>${side || 'BUY'} · PAYMENT CLAIMED</b>`,
    release: `🔓 <b>${side || 'SELL'} · RELEASE REQUESTED</b>`,
    done: `✅ <b>${side ? side + ' ' : ''}ORDER COMPLETED</b>`,
    cancel: `❌ <b>${side ? side + ' ' : ''}ORDER CANCELLED</b>`,
  }[kind];
  if (!head) return null;
  const amt = [logInr(info.inr), logUsdt(info.usdt)].filter(Boolean).join('  ⇄  ');
  const rate = info.rate ? ` @ ${hEsc(String(info.rate).replace(/^₹?/, '₹'))}` : '';
  return [
    `${head} · P2P`,
    `👤 ${hEsc(info.name || 'User')} · ${hCode(info.uid || '—')}`,
    `🧾 ${hCode('#' + orderId)}${info.merchant ? ` · ${hEsc(info.merchant)}` : ''}`,
    amt ? `💵 <b>${hEsc(amt)}</b>${rate}` : null,
    `<i>⏰ ${hEsc(nowIST())} IST</i>`,
  ].filter(Boolean).join('\n');
}

/** Post one event exactly once per order+kind, across restarts and instances. */
async function p2pGroupLog(kind, orderId, info = {}) {
  try {
    if (!db || !SUPPORT_ENABLED || !LOG_GROUP?.chatId || !orderId) return;
    if (!['new', 'claim', 'release', 'done', 'cancel'].includes(kind)) return;
    orderId = String(orderId).replace(/^#/, '');
    const ref = db.ref(`${LOG_EVENTS_REF}/${orderId.replace(/[.#$\[\]\/]/g, '_')}`);
    // Completed and cancelled are final — only the first of the two is logged.
    const finalKind = kind === 'done' || kind === 'cancel';
    const tx = await ref.transaction(cur => {
      const c = cur || {};
      if (c.sent?.[kind] || (finalKind && (c.sent?.done || c.sent?.cancel))) return;   // abort: already logged
      const clean = {};
      for (const k of ['mode', 'name', 'uid', 'merchant', 'inr', 'usdt', 'rate']) {
        const v = info[k]; if (v !== undefined && v !== null && v !== '' && v !== '—') clean[k] = String(v).slice(0, 60);
      }
      return { ...c, ...Object.fromEntries(Object.entries(clean).filter(([k]) => !c[k])), ts: c.ts || Date.now(), sent: { ...(c.sent || {}), [kind]: Date.now() } };
    }, undefined, false);
    if (!tx.committed) return;
    const rec = tx.snapshot.val() || {};
    const html = p2pLogCard(kind, orderId, { ...rec, ...info, mode: info.mode || rec.mode, name: info.name || rec.name, uid: info.uid || rec.uid });
    if (html) await logGroupSend(html);
    if (Math.random() < 0.02) {   // occasional clean-up of week-old records
      const old = (await db.ref(LOG_EVENTS_REF).orderByChild('ts').endAt(Date.now() - 7 * 86400_000).limitToFirst(200).once('value')).val() || {};
      for (const k of Object.keys(old)) db.ref(`${LOG_EVENTS_REF}/${k}`).remove().catch(() => {});
    }
  } catch (e) { log('LOGS', `p2p log ${kind} #${orderId}: ${e.message}`); }
}
/** From a P2P message the app sent to the admin bot. */
function p2pGroupLogFromText(raw, kind) {
  const f = p2pFields(raw);
  if (!f.order) return;
  p2pGroupLog(kind, f.order, { mode: f.mode, name: f.user, uid: f.uid ? f.uid.toUpperCase() : '', merchant: f.merchant, inr: f.inr, usdt: f.usdt, rate: f.rate });
}
/** From a settled / decided order object on the server. */
function p2pGroupLogOrder(user, orderId, order, action) {
  if (!order) return;
  p2pGroupLog(action === 'complete' || action === 'approve' ? 'done' : 'cancel', orderId, {
    mode: order.mode, name: user?.profile?.nickname || user?.name, uid: user?.uid, merchant: order.merchant,
    inr: order.inr, usdt: order.usdt, rate: order.rate,
  });
}

async function logGroupAskApproval(chat, by) {
  const id = String(chat.id), now = Date.now();
  if (logIsAdminChat(id)) return;
  if (now - (LOG_REQ_COOLDOWN.get(id) || 0) < 60_000) return;
  LOG_REQ_COOLDOWN.set(id, now);
  const already = LOG_GROUP && String(LOG_GROUP.chatId) === id;
  await tgFetchSup('sendMessage', {
    chat_id: TG_CHAT_SUPPORT, parse_mode: 'HTML', disable_web_page_preview: true,
    text: [already ? '📋 <b>P2P LOG GROUP</b> — already connected' : '📋 <b>CONNECT P2P LOG GROUP?</b>', '',
      `👥 ${hEsc(chat.title || 'Group')}`, `🆔 ${hCode(id)}`, by ? `🙋 Asked by ${hEsc(by)}` : null, '',
      'New BUY / SELL orders, payment claims, completed and cancelled orders will be posted there.',
      LOG_GROUP && !already ? `\n⚠️ This replaces the current log group: ${hEsc(LOG_GROUP.title)}` : null].filter(v => v != null).join('\n'),
    reply_markup: { inline_keyboard: [[{ text: '✅ Use this group', callback_data: `logs_on_${id}` }, { text: '✖️ Ignore', callback_data: `logs_no_${id}` }]] },
  });
}

async function logGroupStatus() {
  const g = LOG_GROUP;
  return tgFetchSup('sendMessage', {
    chat_id: TG_CHAT_SUPPORT, parse_mode: 'HTML',
    text: g ? `📋 <b>P2P LOG GROUP</b>\n\n👥 ${hEsc(g.title)}\n🆔 ${hCode(g.chatId)}\n🟢 Logging ON` :
      '📋 <b>P2P LOG GROUP</b>\n\n⚪ Not connected.\n\n1. Add this support bot to your group and make it admin.\n2. A "Connect" message appears here — tap ✅.\n(Or send /setlogs inside the group.)',
    reply_markup: g ? { inline_keyboard: [[{ text: '🧪 Send test', callback_data: 'logs_test' }, { text: '⛔ Turn off', callback_data: 'logs_off' }]] } : undefined,
  });
}

/** Returns true when the update belonged to the log-group feature. */
async function logGroupHandleUpdate(upd) {
  if (!db) return false;
  // Bot added to / removed from a group.
  const mc = upd.my_chat_member;
  if (mc) {
    if (!logIsGroupChat(mc.chat)) return true;
    const st = mc.new_chat_member?.status;
    if (st === 'administrator' || st === 'member') await logGroupAskApproval(mc.chat, mc.from?.username ? '@' + mc.from.username : mc.from?.first_name);
    else if ((st === 'left' || st === 'kicked') && LOG_GROUP && String(LOG_GROUP.chatId) === String(mc.chat.id)) {
      await logGroupClear();
      await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, parse_mode: 'HTML', text: `⛔ Bot removed from log group <b>${hEsc(mc.chat.title || '')}</b> — P2P logging is OFF.` });
    }
    return true;
  }
  // Buttons in the support chat.
  const cb = upd.callback_query;
  if (cb && /^logs_/.test(String(cb.data || ''))) {
    if (String(cb.message?.chat?.id) !== String(TG_CHAT_SUPPORT)) { await tgFetchSup('answerCallbackQuery', { callback_query_id: cb.id, text: 'Unauthorized' }); return true; }
    const d = String(cb.data), mid = cb.message?.message_id;
    const done = (label) => mid && tgFetchSup('editMessageReplyMarkup', { chat_id: TG_CHAT_SUPPORT, message_id: mid, reply_markup: { inline_keyboard: [[{ text: label, callback_data: 'noop_done' }]] } });
    let m;
    if ((m = d.match(/^logs_on_(-?\d{3,20})$/))) {
      const info = await tgFetchSup('getChat', { chat_id: m[1] });
      if (!info.ok) { await tgFetchSup('answerCallbackQuery', { callback_query_id: cb.id, text: 'Bot is not in that group any more.', show_alert: true }); return true; }
      const g = await logGroupSet(info.result.id, info.result.title);
      const t = await logGroupSend('🟢 <b>BIEXC P2P logs connected</b>\nNew BUY / SELL orders will appear here.', g.chatId);
      await tgFetchSup('answerCallbackQuery', { callback_query_id: cb.id, text: t.ok ? '✅ Log group connected' : '⚠️ Saved, but the bot cannot post there yet — give it permission to send messages.', show_alert: !t.ok });
      await done(`✅ Connected — ${String(g.title).slice(0, 30)}`);
      log('LOGS', `log group set → ${g.chatId}`);
      return true;
    }
    if ((m = d.match(/^logs_no_/))) { await tgFetchSup('answerCallbackQuery', { callback_query_id: cb.id, text: 'Ignored' }); await done('✖️ Ignored'); return true; }
    if (d === 'logs_off') {
      const g = LOG_GROUP; await logGroupClear();
      if (g) logGroupSend('⛔ <b>BIEXC P2P logs turned off</b> by admin.', g.chatId).catch(() => {});
      await tgFetchSup('answerCallbackQuery', { callback_query_id: cb.id, text: '⛔ Logging off' }); await done('⛔ Logging OFF'); return true;
    }
    if (d === 'logs_test') {
      const r = LOG_GROUP ? await logGroupSend(p2pLogCard('new', '1234567890', { mode: 'BUY', name: 'Test User', uid: 'TEST01', merchant: 'Test merchant', inr: 1000, usdt: 10.2, rate: 98 }).replace('· P2P', '· P2P · 🧪 TEST')) : { ok: false };
      await tgFetchSup('answerCallbackQuery', { callback_query_id: cb.id, text: r.ok ? '🧪 Test sent to group' : '⚠️ Could not post — check the bot is still admin in the group.', show_alert: !r.ok });
      return true;
    }
    await tgFetchSup('answerCallbackQuery', { callback_query_id: cb.id, text: 'Unknown' });
    return true;
  }
  // Messages.
  const msg = upd.message;
  if (!msg || msg.from?.is_bot) return false;
  const text = String(msg.text || '').trim();
  if (logIsGroupChat(msg.chat) && !logIsAdminChat(msg.chat.id)) {
    if (msg.migrate_to_chat_id && LOG_GROUP && String(LOG_GROUP.chatId) === String(msg.chat.id)) await logGroupSet(msg.migrate_to_chat_id, LOG_GROUP.title);
    else if (/^\/(setlogs|logs)(?:@\S+)?\b/i.test(text)) {
      await logGroupAskApproval(msg.chat, msg.from?.username ? '@' + msg.from.username : msg.from?.first_name);
      await tgFetchSup('sendMessage', { chat_id: msg.chat.id, reply_to_message_id: msg.message_id, text: LOG_GROUP && String(LOG_GROUP.chatId) === String(msg.chat.id) ? '🟢 P2P logs are already ON for this group.' : '📨 Request sent to BIEXC admin. Logs start after approval.' });
    }
    return true;   // the bot never chats in the log group
  }
  if (String(msg.chat?.id) === String(TG_CHAT_SUPPORT)) {
    if (/^\/logs(?:@\S+)?\s*$/i.test(text)) { await logGroupStatus(); return true; }
    if (/^\/logsoff(?:@\S+)?\b/i.test(text)) {
      const g = LOG_GROUP; await logGroupClear();
      if (g) logGroupSend('⛔ <b>BIEXC P2P logs turned off</b> by admin.', g.chatId).catch(() => {});
      await tgFetchSup('sendMessage', { chat_id: TG_CHAT_SUPPORT, text: '⛔ P2P logging OFF.' }); return true;
    }
  }
  return false;
}

// Independent customer-care update stream. Never enters the main update buffer.
async function handleSupportUpdate(upd){
  if(await logGroupHandleUpdate(upd))return;
  if(upd.callback_query){
    const cb=upd.callback_query;
    if(String(cb.message?.chat?.id)!==String(TG_CHAT_SUPPORT))return tgAnswerSup(cb.id,'Unauthorized');
    const m=String(cb.data||'').match(/^sup_(acc|rej|end|sw)_(.+)$/);
    if(m)return handleSupCb(cb,m[1],m[2]);
    if(cb.data==='sup_list'){await tgAnswerSup(cb.id,'Open chats');return supList();}
    return tgAnswerSup(cb.id,'Customer-care bot only');
  }
  const msg=upd.message;if(!msg||String(msg.chat?.id)!==String(TG_CHAT_SUPPORT)||msg.from?.is_bot)return;
  const text=String(msg.text||'').trim();
  if(/^\/(start|menu|help)(?:@\S+)?\b/i.test(text))return tgSendSup('BIEXC Customer Care\n\n/chats — waiting and live chats\n/end — close the active chat\n↩️ Reply (swipe) on a user\'s message → goes only to that user\n👍 on your message = delivered\n\n/logs — P2P log group status · /logsoff — stop logs\n\nTrading, withdrawal and deposit commands remain on the main bot.',{parse_mode:undefined});
  if(await supRouteAdmin(msg,text))return;
  return tgSendSup(text.startsWith('/')?'Use the main bot for financial commands. /chats opens customer-care requests.':'Select a live chat with /chats before sending a reply.',{parse_mode:undefined});
}
async function supportPollLoop(){
  if(!SUPPORT_ENABLED){log('SUP','Customer-care disabled: set different TG_TOKEN_SUPPORT and TG_CHAT_SUPPORT. Main bot remains active.');return;}
  const clear=await tgFetchSup('deleteWebhook',{drop_pending_updates:false});if(!clear.ok){log('SUP','Cannot start customer-care polling. Check support bot configuration.');return;}
  logGroupWatch();
  let offset=Number((await db.ref(`${SUP_STATE}/offset`).once('value')).val()||0),failures=0;
  while(true){
    if(!(await amIActive())){await new Promise(r=>setTimeout(r,15000));continue;}
    const data=await tgFetchSup('getUpdates',{offset:offset+1,timeout:25,allowed_updates:['callback_query','message','my_chat_member']});
    if(!data.ok){
      if([401,404,409].includes(data.error_code)){log('SUP','Support bot polling stopped: invalid token or another consumer. Main trading bot is unaffected.');return;}
      failures++;await new Promise(r=>setTimeout(r,Math.min(30000,1000*2**Math.min(failures,5))));continue;
    }
    failures=0;
    for(const upd of data.result||[]){
      try{await handleSupportUpdate(upd);offset=upd.update_id;await db.ref(`${SUP_STATE}/offset`).set(offset);}
      catch(e){log('SUP',`Support update failed: ${e.message}`);await new Promise(r=>setTimeout(r,2000));break;}
    }
    await new Promise(r=>setTimeout(r,40));
  }
}

async function pollLoop() {
  while (true) {
    await pollUpdates();
    await new Promise(r => setTimeout(r, 40));
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
const SERVER_HANDLED_UPD = new Set();
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

// ════════════════════════════════════════════════════════════════════
// V43 P2P — clean bot messages + reply-tag chat routing
// Every P2P message the app sends is re-drawn as one tidy card. Each card is
// remembered (botState/p2pMsgs/{message_id} → order) so the admin can simply
// reply (swipe) on it and the text/photo lands in that order's chat — no UID
// needed. "#UID message" / "#ORDERID message" keeps working too.
// ════════════════════════════════════════════════════════════════════
// <P2P_PRETTY>
function p2pFields(raw) {
  const s = String(raw || '').replace(/[*`]/g, '');
  const get = (re) => { const m = s.match(re); return m ? m[1].trim() : ''; };
  const msg = s.match(/Msg:\s*"([\s\S]*)"\s*(?:\n\s*⏰|$)/);
  return {
    title: (s.split('\n').find(l => l.trim()) || '').trim(),
    uid: get(/UID:?\s*([A-Za-z0-9_-]{3,40})/),
    user: get(/User:\s*([^\n]+)/),
    email: get(/Email:\s*([^\n]+)/),
    order: get(/Order:?\s*#?(\d{3,15})/),
    mode: get(/Mode:\s*([A-Za-z]+)/).toUpperCase(),
    merchant: get(/Merchant:\s*([^\n]+)/),
    inr: get(/INR(?: Paid)?:\s*([^\n]+)/),
    usdt: get(/USDT:\s*([^\n]+)/),
    rate: get(/Rate:\s*([^\n]+)/),
    upi: get(/UPI:\s*([^\n]+)/),
    time: get(/⏰\s*([^\n]+?)\s*IST/),
    msg: msg ? msg[1] : get(/Msg:\s*([^\n]+)/),
    proof: /Payment proof:\s*attached/i.test(s),
  };
}
/** Order id + UID inside any P2P text (used to remember messages). */
function p2pIds(raw) {
  const f = p2pFields(raw);
  return f.order ? { orderId: f.order, uid: f.uid ? f.uid.toUpperCase() : '' } : null;
}
/** Re-draw a P2P app message as a clean HTML card. null = not a P2P message, send unchanged. */
function p2pPretty(raw) {
  const s = String(raw || ''); if (!s) return null;
  const f = p2pFields(s), T = f.title.toUpperCase();
  let kind = null;
  if (/NEW ORDER/.test(T)) kind = 'new';
  else if (/PAYMENT CLAIMED/.test(T)) kind = 'claim';
  else if (/RELEASE REQUEST/.test(T)) kind = 'release';
  else if (/USER MSG/.test(T)) kind = 'msg';
  else if (/TRADE COMPLETE/.test(T)) kind = 'done';
  else if (/TRADE (REJECTED|CANCELLED|CANCELED)/.test(T)) kind = 'cancel';
  else if (/USER PHOTO/.test(T)) kind = 'photo';
  else if (/PAYMENT PROOF/.test(T)) kind = 'proof';
  if (!kind || !f.order) return null;
  const uid = (f.uid || '—').toUpperCase();
  const who = `👤 <b>${hEsc(f.user || 'User')}</b> · ${hCode(uid)}`;
  const mail = f.email && f.email !== '—' ? `📧 ${hEsc(f.email)}` : null;
  const ord = `🧾 Order ${hCode('#' + f.order)}${f.merchant ? ` · ${hEsc(f.merchant)}` : ''}`;
  const when = `<i>⏰ ${hEsc(f.time || nowIST())} IST</i>`;
  const replyHint = '<i>↩️ Reply to this message to chat with the user</i>';
  let lines;
  if (kind === 'new') lines = [`🆕 <b>NEW P2P ORDER · ${hEsc(f.mode || 'BUY')}</b>`, '', who, mail, ord, `💵 ${hEsc(f.inr)}  ⇄  <b>${hEsc(f.usdt)}</b>${f.rate ? ` @ ${hEsc(f.rate)}` : ''}`, '', `${when} · auto-cancel in 30 min`];
  else if (kind === 'claim') lines = ['💸 <b>P2P BUY · PAYMENT CLAIMED</b>', '', who, mail, ord, `💵 Paid <b>${hEsc(f.inr)}</b>  →  <b>${hEsc(f.usdt)}</b>`, f.upi ? `📲 UPI ${hCode(f.upi)}` : null, f.proof ? '🖼 Payment proof attached below' : null, '', `${when} · timer 12h`, '', '✅ Approve = complete  ·  ❌ Reject = cancel', replyHint];
  else if (kind === 'release') lines = ['🔓 <b>P2P SELL · RELEASE REQUEST</b>', '', who, mail, ord, `🪙 <b>${hEsc(f.usdt)}</b>  →  ${hEsc(f.inr)}`, '', when, '', '✅ Approve = complete  ·  ❌ Reject = refund USDT', replyHint];
  else if (kind === 'msg') lines = [`💬 <b>${hEsc(f.user || 'User')}</b> · ${hCode(uid)} · Order ${hCode('#' + f.order)}`, `<blockquote>${hEsc(f.msg || '')}</blockquote>`, '<i>↩️ Reply to answer this user</i>'];
  else if (kind === 'done') lines = ['✅ <b>P2P TRADE COMPLETED</b>', `🧾 Order ${hCode('#' + f.order)} · ${hCode(uid)}${f.usdt ? ` · <b>${hEsc(f.usdt)}</b>` : ''}`];
  else if (kind === 'cancel') lines = ['❌ <b>P2P TRADE CANCELLED</b>', `🧾 Order ${hCode('#' + f.order)} · ${hCode(uid)}`];
  else if (kind === 'photo') lines = [`🖼 <b>User photo</b> · ${hCode(uid)} · Order ${hCode('#' + f.order)}`, '<i>↩️ Reply to answer this user</i>'];
  else lines = [`🧾 <b>Payment proof</b> · ${hCode(uid)} · Order ${hCode('#' + f.order)}`];
  const text = lines.filter(l => l != null).join('\n').replace(/\n{3,}/g, '\n\n');
  return { text, orderId: f.order, uid: uid === '—' ? '' : uid, kind };
}
/** Which order does an admin message target? Returns null when it is not a P2P chat message. */
function p2pTargetFromText(body, reply) {
  const t = String(body || '').trim();
  if (reply) {
    const hint = String(reply.text || reply.caption || '');
    const om = hint.match(/Order[:\s]*#?(\d{5,15})/i);
    if (!om) return null;
    const um = hint.match(/(?:UID[:\s]*|· )([A-Z0-9]{4,12})\b/);
    return { orderId: om[1], uid: um ? um[1] : '', text: t };
  }
  if (/^#[A-Z0-9]{4,10}\s+[\d.]+$/i.test(t)) return null;      // "#UID 500" = balance command, not chat
  const hm = t.match(/^#([A-Za-z0-9]{4,15})(?:[\s:,-]+([\s\S]*))?$/);
  if (!hm) return null;
  const tok = hm[1];
  return /^\d{5,15}$/.test(tok) ? { orderId: tok, uid: '', text: (hm[2] || '').trim() } : { orderId: '', uid: tok.toUpperCase(), text: (hm[2] || '').trim() };
}
// </P2P_PRETTY>

async function p2pRemember(mid, ids) {
  if (!db || !mid || !ids || !ids.orderId) return;
  try {
    await db.ref(`botState/p2pMsgs/${mid}`).set({ orderId: String(ids.orderId), uid: ids.uid || '', ts: Date.now() });
    if (Math.random() < 0.02) { // occasional clean-up of week-old entries
      const old = (await db.ref('botState/p2pMsgs').orderByChild('ts').endAt(Date.now() - 7 * 86400_000).limitToFirst(200).once('value')).val() || {};
      for (const k of Object.keys(old)) db.ref(`botState/p2pMsgs/${k}`).remove().catch(() => {});
    }
  } catch (e) { log('P2P', `remember ${e.message}`); }
}
async function tgPhotoToDataUrlMain(photos) {
  const list = [...photos].sort((a, b) => (a.file_size || 0) - (b.file_size || 0));
  const pick = list.filter(p => (p.file_size || 0) <= 700_000).pop() || list[0];
  const j = await (await fetch(`${TG_API}/getFile?file_id=${encodeURIComponent(pick.file_id)}`)).json();
  if (!j.ok) return null;
  const r = await fetch(`https://api.telegram.org/file/bot${TG_TOKEN}/${j.result.file_path}`);
  if (!r.ok) return null;
  return `data:image/jpeg;base64,${Buffer.from(await r.arrayBuffer()).toString('base64')}`;
}
async function p2pStrictUser(uid) {
  if (!uid) return null;
  const f = await findUserByUID(uid).catch(() => null);
  return f && String(shownUID(f.fuid, f.user)).toUpperCase() === String(uid).toUpperCase() ? f : null;
}
async function p2pResolveTarget({ orderId, uid }) {
  if (orderId) {
    const f = await p2pStrictUser(uid);
    if (f) { const o = (await db.ref(`users/${f.fuid}/orders/${orderId}`).once('value')).val(); if (o) return { fuid: f.fuid, orderId, order: o, uid: uid.toUpperCase() }; }
    const hit = await findOrderById(orderId);
    if (hit) return { fuid: hit.fuid, orderId, order: hit.order, uid: shownUID(hit.fuid, hit.user) };
    return { error: `Order ${hCode('#' + orderId)} is already finished — the user did not get this message.` };
  }
  const f = await p2pStrictUser(uid);
  if (!f) return { error: `UID ${hCode(uid)} not found.` };
  const orders = (await db.ref(`users/${f.fuid}/orders`).once('value')).val() || {};
  const list = Object.entries(orders).filter(([, o]) => o && typeof o === 'object').sort((a, b) => (Number(b[1].time) || 0) - (Number(a[1].time) || 0));
  if (!list.length) return { error: `UID ${hCode(uid)} has no open P2P order.` };
  return { fuid: f.fuid, orderId: list[0][0], order: list[0][1], uid: uid.toUpperCase(), many: list.length };
}
/** Append the admin's message to users/{fuid}/chats/{orderId} once (dedup by Telegram id). */
async function p2pDeliverAdminMsg(fuid, orderId, order, tgMsgId, text, img) {
  const arr = (v) => Array.isArray(v) ? v.filter(x => x != null) : (v && typeof v === 'object' ? Object.values(v) : []);
  let dup = false;
  const tx = await db.ref(`users/${fuid}/chats/${orderId}`).transaction(cur => {
    dup = false;
    const st = cur && typeof cur === 'object' ? { ...cur } : { messages: [], tgMsgId: null, cbId: null, dealStage: 'init', upiSubmitted: false, seenMsgIds: [] };
    const seen = arr(st.seenMsgIds), msgs = arr(st.messages);
    if (seen.includes(tgMsgId)) { dup = true; return; }
    const m = { type: 'them', sender: order.merchant || 'Merchant', text: text || '', ts: Date.now() };
    if (img) { m.kind = 'image'; m.imageBase64 = img; }
    msgs.push(m); seen.push(tgMsgId);
    st.messages = msgs; st.seenMsgIds = seen.slice(-300);
    return st;
  });
  return tx.committed || dup;
}
/** Admin message in the main bot → P2P chat. true = handled (never reaches search/menu). */
async function p2pAdminReply(msg, text) {
  const body = String(msg.text || msg.caption || '').trim();
  if (body.startsWith('/')) return false;
  const hasPhoto = Array.isArray(msg.photo) && msg.photo.length > 0;
  const rt = msg.reply_to_message;
  let target = null;
  if (rt) {
    if (rt.from && !rt.from.is_bot) return false;
    const mapped = (await db.ref(`botState/p2pMsgs/${rt.message_id}`).once('value')).val();
    target = mapped ? { orderId: mapped.orderId, uid: mapped.uid || '', text: body } : p2pTargetFromText(body, rt);
  } else {
    target = p2pTargetFromText(body, null);
  }
  if (!target) return false;
  if (!target.text && !hasPhoto) { await tgSendH('✍️ Write a message after the UID, e.g. <code>#AB12CD Payment received?</code>', { reply_to_message_id: msg.message_id }); return true; }
  const dest = await p2pResolveTarget(target);
  if (dest.error) { await tgSendH(`⚠️ ${dest.error}`, { reply_to_message_id: msg.message_id }); return true; }
  let img = null;
  if (hasPhoto) {
    img = await tgPhotoToDataUrlMain(msg.photo).catch(() => null);
    if (!img) { await tgSendH('⚠️ Could not deliver that photo — please send it again.', { reply_to_message_id: msg.message_id }); return true; }
  }
  const ok = await p2pDeliverAdminMsg(dest.fuid, dest.orderId, dest.order, msg.message_id, target.text, img);
  if (!ok) { await tgSendH('⚠️ Message not delivered — please try again.', { reply_to_message_id: msg.message_id }); return true; }
  await p2pRemember(msg.message_id, { orderId: dest.orderId, uid: dest.uid });
  tgReact(msg.message_id).catch(() => {});
  if (dest.many > 1) tgSendH(`ℹ️ ${hCode(dest.uid)} has ${dest.many} open orders — sent to the newest, ${hCode('#' + dest.orderId)}. Reply on a specific order's message to pick another.`, { reply_to_message_id: msg.message_id }).catch(() => {});
  log('P2P', `admin msg → UID ${dest.uid} order #${dest.orderId}${img ? ' [photo]' : ''}`);
  return true;
}

// ─── TELEGRAM PROXY — used by the P2P iframe ────────────────────────
// The iframe can't talk to Telegram directly (we hold the single poll lock
// and the bot token is server-only). These endpoints mimic the small subset
// of the Bot API the iframe uses.
app.get('/api/p2p/config', async (req, res) => {
  try {
    const s = await getP2PSettings();
    const h = istHour();
    const out = {
      buyMin: Math.min(s.buyMin, s.buyMax), buyMax: Math.max(s.buyMin, s.buyMax),
      sellMin: Math.min(s.sellMin, s.sellMax), sellMax: Math.max(s.sellMin, s.sellMax),
      buyLimMin: Math.min(s.buyLimMin, s.buyLimMax), buyLimMax: Math.max(s.buyLimMin, s.buyLimMax),
      sellLimMin: Math.min(s.sellLimMin, s.sellLimMax), sellLimMax: Math.max(s.sellLimMin, s.sellLimMax),
      night: s.nightOn !== false && h >= 1 && h < 5,
      nightCount: 2 + (new Date().getUTCDate() % 2),
    };
    const fuid = String(req.query.fuid || '').replace(/[^A-Za-z0-9_-]/g, '');
    if (fuid) Object.assign(out, await userLimitsInfo(fuid));
    res.set('Cache-Control', 'no-store').json(out);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

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
  const pretty = (m === 'sendMessage' || m === 'editMessageText') ? p2pPretty(body.text) : null;
  const rawP2P = pretty ? String(body.text || '') : '';
  if (pretty) { body.text = pretty.text; body.parse_mode = 'HTML'; body.disable_web_page_preview = true; }
  try {
    const r = await tgFetch(m, body);
    if (r.ok && m === 'sendMessage') p2pRemember(r.result?.message_id, pretty || p2pIds(body.text));
    if (r.ok && pretty && m === 'sendMessage') p2pGroupLogFromText(rawP2P, pretty.kind);
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
    const prettyCap = caption ? p2pPretty(String(caption)) : null;
    if (prettyCap) { fd.append('caption', prettyCap.text.slice(0, 1024)); fd.append('parse_mode', 'HTML'); }
    else if (caption) fd.append('caption', String(caption).slice(0, 1024));
    if (reply_to_message_id) fd.append('reply_to_message_id', String(reply_to_message_id));
    const blob = new Blob([buf], { type: 'image/jpeg' });
    fd.append('photo', blob, 'photo.jpg');
    const r = await fetch(`${TG_API}/sendPhoto`, { method: 'POST', body: fd });
    const j = await r.json();
    if (j.ok) { log('P2P', `photo sent msgId=${j.result?.message_id || '—'} size=${buf.length}b`); p2pRemember(j.result?.message_id, prettyCap || p2pIds(caption)); if (prettyCap) p2pGroupLogFromText(String(caption), prettyCap.kind); }
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

// ════════════════════════════════════════════════════════════════════
// V43 EMAILS — one email per event (mailLog/{key}), server-side for:
// deposit/withdrawal request received, auto-detected & assigned deposits,
// P2P completed / cancelled. Failures alert the admin in the bot.
// ════════════════════════════════════════════════════════════════════
const SERVER_MAIL_RECENT = new Map(); // `${email}|${cat}` -> ts
function mailCat(subject) { const t = String(subject || ''); return /p2p/i.test(t) ? 'p2p' : /deposit/i.test(t) ? 'dep' : /withdraw/i.test(t) ? 'wit' : ''; }
function noteServerMail(to, cat) {
  if (!to || !cat) return;
  SERVER_MAIL_RECENT.set(`${String(to).toLowerCase()}|${cat}`, Date.now());
  if (SERVER_MAIL_RECENT.size > 2000) { const c = Date.now() - 3600_000; for (const [k, t] of SERVER_MAIL_RECENT) if (t < c) SERVER_MAIL_RECENT.delete(k); }
}
function serverMailedRecently(to, cat, ms = 15 * 60_000) { const t = SERVER_MAIL_RECENT.get(`${String(to || '').toLowerCase()}|${cat}`); return !!t && Date.now() - t < ms; }
let MAIL_ALERT_AT = 0;
function mailFixHint(reason) {
  const r = String(reason || '');
  if (/not configured|not set up|missing/i.test(r)) return 'Add RESEND_API_KEY (or SMTP_USER + SMTP_PASS) in Render → Environment, then redeploy.';
  if (/domain|verify|testing emails|own email|not allowed|403/i.test(r)) return 'Resend only sends to everyone after your domain is verified — set RESEND_FROM to an address on that verified domain.';
  if (/auth|535|invalid login|credentials/i.test(r)) return 'The SMTP username/password is wrong — re-check SMTP_USER and SMTP_PASS.';
  if (/timeout|ETIMEDOUT|ECONNREFUSED|ENOTFOUND/i.test(r)) return 'The mail server did not answer — check SMTP_HOST / SMTP_PORT or use Resend.';
  return 'Run /mailtest your@email to see the exact problem.';
}
function mailAlert(reason) {
  if (Date.now() - MAIL_ALERT_AT < 30 * 60_000) return;
  MAIL_ALERT_AT = Date.now();
  tgSendH(hcard('⚠️ EMAIL NOT DELIVERED', [`❗ ${hEsc(String(reason || 'unknown error').slice(0, 300))}`, '', `🔧 ${hEsc(mailFixHint(reason))}`, '', 'Check any time with /mailstatus or /mailtest your@email'])).catch(() => {});
}
async function sendUserTxMail(key, user, mail) {
  const to = String(user?.email || '').trim();
  const uid = user?.uid || '—';
  if (!to || !to.includes('@')) { log('MAIL', `skip ${key} UID=${uid} — no email`); return false; }
  if (!RESEND_API_KEY && !mailer) { log('MAIL', `skip ${key} — mail not configured`); mailAlert('Email is not set up on the server.'); return false; }
  const k = tapKey(`mail_${key}`);
  if (db) { const tx = await db.ref(`mailLog/${k}`).transaction(c => c ? undefined : { ts: Date.now(), to, by: INSTANCE_ID }); if (!tx.committed) return false; }
  try {
    const info = await sendMailAny({ to, subject: mail.subject, html: mailHtml({ ...mail, uid }), text: `${mail.subject}\n\n${mail.message}\n\n${mail.amount ? 'Amount: ' + mail.amount + '\n\n' : ''}BIEXC — Need help? Open Customer Care in the BIEXC app.` });
    noteServerMail(to, mailCat(mail.subject));
    log('MAIL', `✅ sent ${key} UID=${uid} → ${to} msg=${info.messageId || 'ok'}`);
    return true;
  } catch (e) {
    lastMailError = e.message;
    if (db) await db.ref(`mailLog/${k}`).remove().catch(() => {});
    log('MAIL', `❌ failed ${key} UID=${uid} → ${to}: ${e.message}`);
    mailAlert(e.message);
    return false;
  }
}
function queueUserTxMail(key, user, mail) { setTimeout(() => { sendUserTxMail(key, user, mail).catch(e => log('MAIL', `crash ${e.message}`)); }, 0); }
function queueRequestMail(type, user, req, cbId) {
  const coin = String(req.coin || 'USDT').toUpperCase(), amount = `${r8(Number(req.amt) || 0)} ${coin}`, dep = type === 'dep';
  queueUserTxMail(`req_${type}_${cbId}`, user, {
    subject: dep ? `Deposit Request Received — ${amount}` : `Withdrawal Request Received — ${amount}`,
    headline: dep ? 'Deposit Request Received' : 'Withdrawal Request Received',
    message: dep
      ? 'We have received your deposit request. It will be credited to your Spot Wallet as soon as it is verified — you will get another email then.'
      : 'We have received your withdrawal request. The amount is on hold while our team reviews it, and you will get another email as soon as it is sent.',
    amount, status: 'PENDING', label: dep ? 'Deposit' : 'Withdrawal', type, coin,
    network: req.network || req.chain || '', address: req.address || req.addr || '', txid: req.txid || '', hid: req.hid,
    stage2: 'In review', stage3: dep ? 'Credited' : 'Sent',
  });
}
function queueP2PMail(user, orderId, order, action) {
  if (!user || !order || !orderId) return;
  const mode = String(order.mode || 'BUY').toUpperCase(), usdt = r8(Number(order.usdt) || 0), inr = Number(order.inr) || 0;
  const done = action === 'complete', label = mode === 'SELL' ? 'P2P Sell' : 'P2P Buy', amount = `${usdt} USDT`;
  const message = done
    ? (mode === 'SELL'
      ? `Your P2P sell order #${orderId} is complete. ${amount} was released to the buyer for ₹${inr.toLocaleString('en-IN')}.`
      : `Your P2P buy order #${orderId} is complete. ${amount} has been credited to your Spot Wallet.`)
    : (mode === 'SELL'
      ? `Your P2P sell order #${orderId} was cancelled. The ${amount} held for this order has been returned to your Spot Wallet.`
      : `Your P2P buy order #${orderId} was cancelled and no USDT was credited. If you already paid, please contact support with your payment proof.`);
  queueUserTxMail(`p2p_${orderId}_${done ? 'complete' : 'reject'}`, user, {
    subject: done ? `${label} Completed — ${amount}` : `${label} Order Cancelled — ${amount}`,
    headline: done ? `${label} Completed` : `${label} Cancelled`, message, amount,
    status: done ? 'COMPLETED' : 'CANCELLED', label, type: mode === 'SELL' ? 'wit' : 'dep', coin: 'USDT',
    hid: `p2p_${orderId}`, stage2: 'Payment', stage3: done ? 'Completed' : 'Cancelled',
  });
}
function mailStatusCard() {
  const yes = (v) => v ? '✅' : '❌';
  return hcard('📧 EMAIL STATUS', [
    `${yes(RESEND_API_KEY)} Resend ${RESEND_API_KEY ? `· from ${hCode(RESEND_FROM)}` : '· not set'}`,
    `${yes(smtpReady)} SMTP ${SMTP_USER ? `${hCode(SMTP_USER)} · ${hEsc(SMTP_HOST)}:${SMTP_PORT}` : '· not set'}${smtpDisabledReason ? `\n   ${hEsc(smtpDisabledReason.slice(0, 200))}` : ''}`,
    '',
    `📤 Last sent: ${hEsc(lastMailSentAt || '—')}${lastMailProvider ? ` via ${hEsc(lastMailProvider)}` : ''}`,
    `❗ Last error: ${hEsc((lastMailError || '—').slice(0, 300))}`,
    lastMailError ? `🔧 ${hEsc(mailFixHint(lastMailError))}` : null,
    '',
    'Test: <code>/mailtest you@example.com</code>',
  ]);
}
async function mailTestCmd(to) {
  to = String(to || '').trim();
  if (!to.includes('@')) return tgSendH(mailStatusCard());
  if (!RESEND_API_KEY && !mailer) return tgSendH(hcard('❌ EMAIL NOT SET UP', [hEsc(mailFixHint('not configured'))]));
  try {
    const info = await sendMailAny({ to, subject: 'BIEXC test email', html: mailHtml({ subject: 'BIEXC test email', headline: 'Email is working', message: 'This is a test from the BIEXC bot. Deposit, withdrawal and P2P emails will arrive like this.', amount: '0 USDT', status: 'COMPLETED', uid: 'TEST', label: 'Test', coin: 'USDT', type: 'dep' }), text: 'BIEXC test email — email is working.' });
    return tgSendH(hcard('✅ TEST EMAIL SENT', [`📧 ${hEsc(to)}`, `📮 via ${hEsc(lastMailProvider || '—')} · ${hCode(info.messageId || 'ok')}`, '', 'Not in the inbox? Check Spam/Promotions.']));
  } catch (e) {
    lastMailError = e.message;
    return tgSendH(hcard('❌ TEST EMAIL FAILED', [`📧 ${hEsc(to)}`, `❗ ${hEsc(e.message.slice(0, 300))}`, '', `🔧 ${hEsc(mailFixHint(e.message))}`]));
  }
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
    mailAlert('Email is not set up on the server.');
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

BIEXC — Need help? Open Customer Care in the BIEXC app.`
    });
    log('MAIL', `✅ sent ${label} ${mail.status} UID=${uid} → ${to} msg=${info.messageId || 'ok'}`);
    noteServerMail(to, ctx.type);
    return true;
  } catch (e) {
    lastMailError = e.message;
    log('MAIL', `❌ failed ${label} UID=${uid} → ${to}: ${e.message}`);
    mailAlert(e.message);
    return false;
  }
}

// ─── BIEXC EMAIL DESIGN (Style A · Clean White) ───────────────────────────
// Always white: light-only colour scheme for every mail client that honours it.
const MAIL_LOGO_PNG = Buffer.from('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', 'base64');
const MAIL_BASE_URL = String(RENDER_URL || 'https://p2p-telegram-bot.onrender.com').replace(/\/$/, '');
const MAIL_LOGO_URL = MAIL_BASE_URL + '/mail/logo.png';
const mEsc = (v) => String(v == null ? '' : v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const MF = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif";
const MM = "'SF Mono',Menlo,Consolas,'Courier New',monospace";
const MW = 'background-color:#ffffff;background-image:linear-gradient(#ffffff,#ffffff);';

function mailShell({ title, preheader, inner }) {
  let h = '<!doctype html><html lang="en" xmlns="http://www.w3.org/1999/xhtml"><head>';
  h += '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">';
  h += '<meta name="color-scheme" content="light only"><meta name="supported-color-schemes" content="light only">';
  h += '<title>' + mEsc(title) + '</title>';
  h += '<style>:root{color-scheme:light only;supported-color-schemes:light only;}';
  h += 'body,table,td,div,p,a,span{color-scheme:light only;}';
  h += '@media (prefers-color-scheme:dark){body,.bx-page{background:#f5f5f5 !important;}.bx-card,.bx-body{background:#ffffff !important;}.bx-foot{background:#fafafa !important;}.bx-t{color:#1E2329 !important;}.bx-s{color:#474D57 !important;}.bx-m{color:#707A8A !important;}}';
  h += '[data-ogsc] .bx-t{color:#1E2329 !important;}[data-ogsc] .bx-s{color:#474D57 !important;}[data-ogsb] .bx-card,[data-ogsb] .bx-body{background:#ffffff !important;}[data-ogsb] .bx-page{background:#f5f5f5 !important;}';
  h += '@media (max-width:520px){.bx-pad{padding:22px 18px !important;}.bx-amt{font-size:26px !important;}.bx-h1{font-size:20px !important;}.bx-code{font-size:28px !important;letter-spacing:6px !important;}.bx-foot{padding:16px 18px !important;}.bx-head{padding:16px 18px !important;}}';
  h += 'a{text-decoration:none;}</style></head>';
  h += '<body class="bx-page" style="margin:0;padding:0;background-color:#f5f5f5;font-family:' + MF + ';-webkit-font-smoothing:antialiased;">';
  h += '<div style="display:none;max-height:0;overflow:hidden;opacity:0;">' + mEsc(preheader || title) + '</div>';
  h += '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="bx-page" style="background-color:#f5f5f5;"><tr><td align="center" style="padding:28px 12px;">';
  h += '<table role="presentation" width="560" cellpadding="0" cellspacing="0" border="0" class="bx-card" style="max-width:560px;width:100%;' + MW + 'border:1px solid #eaecef;border-radius:12px;overflow:hidden;">';
  // header
  h += '<tr><td class="bx-body bx-head" style="' + MW + 'padding:18px 28px;border-bottom:3px solid #F0B90B;"><table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>';
  h += '<td style="vertical-align:middle;padding-right:9px;"><img src="' + MAIL_LOGO_URL + '" width="26" height="26" alt="" style="display:block;border:0;width:26px;height:26px;"></td>';
  h += '<td class="bx-t" style="vertical-align:middle;font-size:19px;font-weight:800;letter-spacing:2.5px;color:#1E2329;font-family:' + MF + ';">BIEXC</td></tr></table></td></tr>';
  // body
  h += '<tr><td class="bx-body bx-pad" style="' + MW + 'padding:28px;">' + inner + '</td></tr>';
  // footer
  h += '<tr><td class="bx-foot" style="background-color:#fafafa;padding:16px 28px;font-size:11.5px;line-height:1.7;color:#929AA5;font-family:' + MF + ';">';
  h += '<span class="bx-m" style="color:#929AA5;">Stay safe: BIEXC will never ask for your password or verification code.<br>';
  h += 'Need help? Open <b style="color:#707A8A;">Customer Care</b> in the BIEXC app · © ' + new Date().getFullYear() + ' BIEXC</span></td></tr>';
  h += '</table></td></tr></table></body></html>';
  return h;
}
const mH1 = (t) => '<div class="bx-t bx-h1" style="font-size:22px;font-weight:700;line-height:1.3;color:#1E2329;margin:0 0 10px;">' + mEsc(t) + '</div>';
const noTg = (t) => String(t == null ? '' : t).replace(/(contact\s+)?support\s+at\s+(https?:\/\/)?t\.me\/biexc10/gi, 'contact Customer Care in the BIEXC app').replace(/(https?:\/\/)?t\.me\/biexc10/gi, 'Customer Care in the BIEXC app');
const mP = (t) => '<div class="bx-s" style="font-size:14px;line-height:1.65;color:#474D57;margin:0;">' + mEsc(noTg(t)).replace(/\n/g, '<br>') + '</div>';
const mBtn = (text, href) => '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:24px;"><tr><td style="background-color:#F0B90B;border-radius:8px;"><a href="' + mEsc(href) + '" style="display:inline-block;padding:12px 26px;font-size:14px;font-weight:700;color:#1E2329;text-decoration:none;font-family:' + MF + ';">' + mEsc(text) + '</a></td></tr></table>';
const mCode = (code) => '<div class="bx-t bx-code" style="margin:24px 0 10px;font-size:34px;font-weight:700;letter-spacing:8px;color:#1E2329;font-family:' + MM + ';">' + mEsc(String(code).replace(/\D/g, '').replace(/^(\d{3})(\d{3})$/, '$1 $2') || code) + '</div>';
function mRows(rows) {
  const r = rows.filter(x => x && x[1] !== undefined && x[1] !== null && String(x[1]) !== '');
  if (!r.length) return '';
  return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:10px;font-size:13px;">' + r.map(([k, v, opt]) => {
    const o = opt || {};
    if (o.block) return '<tr><td colspan="2" style="padding:10px 0;border-top:1px solid #f0f1f2;"><div class="bx-m" style="color:#707A8A;font-size:12px;margin-bottom:4px;">' + mEsc(k) + '</div><div class="bx-t" style="color:#1E2329;font-size:12.5px;font-weight:600;font-family:' + MM + ';word-break:break-all;line-height:1.5;">' + mEsc(v) + '</div></td></tr>';
    return '<tr><td class="bx-m" style="padding:10px 0;border-top:1px solid #f0f1f2;color:#707A8A;vertical-align:top;">' + mEsc(k) + '</td><td class="bx-t" style="padding:10px 0;border-top:1px solid #f0f1f2;text-align:right;color:' + (o.color || '#1E2329') + ';font-weight:600;' + (o.mono ? 'font-family:' + MM + ';' : '') + 'word-break:break-all;">' + mEsc(v) + '</td></tr>';
  }).join('') + '</table>';
}
function explorerUrl(txid, network) {
  const t = String(txid || '').trim(), n = String(network || '').toUpperCase();
  if (/^0x[0-9a-fA-F]{64}$/.test(t)) return /ERC|ETH/.test(n) && !/BEP|BSC/.test(n) ? 'https://etherscan.io/tx/' + t : 'https://bscscan.com/tx/' + t;
  if (/^[0-9a-fA-F]{64}$/.test(t) && /TRC|TRON/.test(n)) return 'https://tronscan.org/#/transaction/' + t;
  return '';
}
const shortHash = (t) => { t = String(t || ''); return t.length > 20 ? t.slice(0, 8) + '…' + t.slice(-6) : t; };

// Deposit / withdrawal / P2P / general notices — same options as before.
function mailHtml(opts) {
  const o = opts || {};
  const status = String(o.status || '').toUpperCase(), type = o.type || '';
  const isOk = status === 'COMPLETED', isBad = status === 'CANCELLED' || status === 'REJECTED';
  const label = o.label || '';
  const headline = o.headline || (label ? label + (isOk ? ' completed' : isBad ? ' cancelled' : ' update') : (o.subject || 'Account update'));
  const coin = o.coin || '', network = o.network || '', address = o.address || '', txid = o.txid || o.txnId || '';
  const pill = isOk ? ['Completed', '#03A66D', '#E7F8F0'] : isBad ? [status === 'REJECTED' ? 'Rejected' : 'Cancelled', '#CF304A', '#FDECEE'] : status && status !== 'NEW_MESSAGE' ? ['Pending', '#C99400', '#FEF6D8'] : null;
  const amtColor = isBad ? '#929AA5' : !isOk ? '#1E2329' : type === 'dep' ? '#03A66D' : type === 'wit' ? '#CF304A' : '#1E2329';
  const sign = isBad ? '' : type === 'dep' ? '+' : type === 'wit' ? '−' : '';
  const ist = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true });
  const ref = o.hid ? 'BIEXC-' + String(o.hid).slice(-8).toUpperCase() : txid ? 'BIEXC-' + String(txid).slice(-8).toUpperCase() : '';
  let inner = '';
  if (pill) inner += '<div style="margin-bottom:14px;"><span style="display:inline-block;padding:4px 10px;border-radius:999px;background-color:' + pill[2] + ';color:' + pill[1] + ';font-size:11px;font-weight:700;letter-spacing:.4px;">' + pill[0] + '</span></div>';
  inner += mH1(headline) + mP(o.message || '');
  if (o.amount) inner += '<div class="bx-amt" style="margin:22px 0 6px;font-size:30px;font-weight:700;color:' + amtColor + ';' + (isBad ? 'text-decoration:line-through;' : '') + '">' + sign + mEsc(o.amount) + '</div>';
  const exp = explorerUrl(txid, network);
  const rows = [
    ['Network', network],
    [type === 'wit' ? 'To address' : 'From address', address, { block: true }],
    ['Transaction hash', exp ? shortHash(txid) : txid, exp ? { mono: true } : { block: true }],
  ];
  if (o.oldBal !== undefined && o.newBal !== undefined) rows.push(['New balance', r8(o.newBal) + (coin ? ' ' + coin : ''), { mono: true }]);
  rows.push(['Account UID', o.uid && o.uid !== 'TEST' ? o.uid : '', { mono: true }]);
  if (ref) rows.push(['Reference', ref, { mono: true }]);
  if (o.amount || label) rows.push(['Time', ist]);
  inner += mRows(rows);
  if (exp) inner += mBtn(/tronscan/.test(exp) ? 'View on Tronscan' : /etherscan/.test(exp) ? 'View on Etherscan' : 'View on BscScan', exp);
  return mailShell({ title: o.subject || headline, preheader: headline + (o.amount ? ' · ' + o.amount : ''), inner });
}

// OTP / sign-in / sign-up / KYC codes.
function otpMailHtml({ code, purpose, minutes } = {}) {
  const p = String(purpose || 'login');
  const what = p === 'signup' ? 'create your BIEXC account' : p === 'kyc' ? 'confirm your identity' : p === 'withdraw' ? 'confirm your withdrawal' : 'finish signing in to BIEXC';
  const inner = mH1('Your verification code') + mP('Use this code to ' + what + '. It expires in ' + (minutes || 10) + ' minutes.') + mCode(code)
    + '<div class="bx-m" style="font-size:12px;color:#707A8A;">Never share this code. BIEXC staff will never ask for it.</div>';
  return mailShell({ title: 'BIEXC verification code', preheader: 'Your BIEXC code is ' + code, inner });
}

// Password reset — code version (current app) or link version.
function resetMailHtml({ code, link, minutes } = {}) {
  let inner = mH1('Reset your password');
  inner += mP('We received a request to reset the password for your BIEXC account. ' + (code ? 'Enter this code in the app to choose a new password.' : 'Tap the button below to choose a new one.') + ' It expires in ' + (minutes || (code ? 10 : 60)) + ' minutes.');
  inner += code ? mCode(code) : (link ? mBtn('Reset password', link) : '');
  inner += '<div class="bx-m" style="margin-top:20px;font-size:12px;color:#707A8A;">Didn\'t ask for this? Ignore this email — your password stays the same.</div>';
  return mailShell({ title: 'Reset your BIEXC password', preheader: code ? 'Your BIEXC reset code is ' + code : 'Reset your BIEXC password', inner });
}

// Admin announcements (/allmail).
// V48: optional tags inside the /allmail text —
//   [style:diwali-a] | [style:diwali-b] | [style:diwali-c]   festive header + colours
//   [highlight: 🎁 $20 Diwali Bonus]                         big highlighted box
//   [button: Claim my bonus | https://biexc.in/?promo=DIWALI20]  tappable button
//   **bold text**
const AM_STYLES = {
  // Photo banner + brand colours (exchange-style festive mail).
  'diwali': { name: 'Diwali photo banner', btnBg: '#F0B90B', btnFg: '#1E2329', hlBg: '#FFF8E1', hlBd: '#F0B90B', hlFg: '#1E2329', bold: '#1E2329', num: '#F0B90B',
    hero: (t) => '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 22px;"><tr><td style="border-radius:10px;background-color:#1a0d06;">'
      + '<img src="' + MAIL_BASE_URL + '/mail/diwali-hero.jpg" width="504" alt="Happy Diwali from BIEXC" style="display:block;width:100%;max-width:504px;height:auto;border:0;border-radius:10px;"></td></tr></table>'
      + '<div style="font-size:11px;font-weight:700;letter-spacing:1.6px;color:#C99400;margin:0 0 6px;">DIWALI 2026</div>' + mH1(t) },
  // Minimal white with a small lamp mark.
  'diwali-lite': { name: 'Diwali minimal white', btnBg: '#1E2329', btnFg: '#FFFFFF', hlBg: '#FAFAFA', hlBd: '#EAECEF', hlFg: '#1E2329', bold: '#1E2329', num: '#1E2329',
    hero: (t) => '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 14px;"><tr><td style="vertical-align:middle;padding-right:12px;"><img src="' + MAIL_BASE_URL + '/mail/diya.png" width="44" alt="" style="display:block;width:44px;height:auto;border:0;"></td>'
      + '<td style="vertical-align:middle;font-size:11px;font-weight:700;letter-spacing:1.6px;color:#C99400;">DIWALI 2026</td></tr></table>' + mH1(t) },
};
function amSplit(body) {
  let style = '';
  const text = String(body || '').replace(/\[style:\s*([a-z0-9-]+)\s*\]\s*/i, (_, x) => { style = x.toLowerCase(); return ''; }).trim();
  return { style: AM_STYLES[style] ? style : '', text };
}
const AM_TAG = /\[(button|highlight|steps):\s*([^\]\n]+)\]/gi;
function amPlain(body) {
  const { text } = amSplit(body);
  return text.replace(AM_TAG, (_, k, v) => { if (k.toLowerCase() === 'button') { const [l, u] = v.split('|').map(x => x.trim()); return u && /^https?:\/\//i.test(u) ? `${l}: ${u}` : l; } if (k.toLowerCase() === 'steps') return v.split('|').map((x, n) => `${n + 1}. ${x.trim()}`).join('\n'); return v.trim(); })
    .replace(/\*\*([^*\n]+)\*\*/g, '$1');
}
function amText(t, st) {
  const boldC = st ? st.bold : '#1E2329';
  const h = mEsc(noTg(t)).replace(/\*\*([^*\n]{1,200})\*\*/g, '<b style="color:' + boldC + ';">$1</b>').replace(/\n/g, '<br>');
  return h.trim() ? '<div class="bx-s" style="font-size:14px;line-height:1.7;color:#474D57;margin:0;">' + h + '</div>' : '';
}
function amButton(label, href, st) {
  const bg = st ? st.btnBg : '#F0B90B', fg = st ? st.btnFg : '#1E2329';
  if (!/^https?:\/\//i.test(href || '')) return amText(label, st);
  return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:20px 0 18px;"><tr><td align="center">'
    + '<table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr><td align="center" style="background-color:' + bg + ';border-radius:999px;box-shadow:0 6px 16px rgba(0,0,0,.12);">'
    + '<a href="' + mEsc(href) + '" style="display:inline-block;padding:15px 34px;font-size:16px;font-weight:800;color:' + fg + ';text-decoration:none;font-family:' + MF + ';">' + mEsc(label) + '</a></td></tr></table>'
    + '</td></tr></table>';
}
function amHighlight(v, st) {
  const bg = st ? st.hlBg : '#FFF7E0', bd = st ? st.hlBd : '#F0B90B', fg = st ? st.hlFg : '#B26A00';
  if (st && st.ticket) {
    return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:18px 0;"><tr>'
      + '<td style="background-color:' + bg + ';border:2px dashed ' + bd + ';border-right:none;border-radius:12px 0 0 12px;padding:16px 14px;font-size:12px;font-weight:800;letter-spacing:2px;color:' + fg + ';" width="38%" align="center">DIWALI<br>BONUS</td>'
      + '<td style="background-color:' + bg + ';border:2px dashed ' + bd + ';border-radius:0 12px 12px 0;padding:16px 14px;font-size:24px;font-weight:900;color:' + fg + ';" align="center">' + mEsc(v) + '</td></tr></table>';
  }
  return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:18px 0;"><tr><td align="center" style="background-color:' + bg + ';border:2px solid ' + bd + ';border-radius:12px;padding:18px 14px;">'
    + '<div class="bx-amt" style="font-size:26px;font-weight:900;line-height:1.3;color:' + fg + ';">' + mEsc(v) + '</div></td></tr></table>';
}
function amSteps(v, st) {
  const c = st ? st.num : '#F0B90B', fg = c === '#1E2329' ? '#FFFFFF' : '#1E2329';
  const rows = v.split('|').map(x => x.trim()).filter(Boolean).map((x, n) => '<tr><td width="34" style="vertical-align:top;padding:0 0 12px;"><div style="width:24px;height:24px;line-height:24px;border-radius:12px;background-color:' + c + ';color:' + fg + ';font-size:12px;font-weight:800;text-align:center;">' + (n + 1) + '</div></td>'
    + '<td class="bx-s" style="vertical-align:top;padding:2px 0 12px;font-size:14px;line-height:1.5;color:#474D57;">' + mEsc(x).replace(/\*\*([^*]{1,120})\*\*/g, '<b style="color:#1E2329;">$1</b>') + '</td></tr>').join('');
  return '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:16px 0 4px;">' + rows + '</table>';
}
function allMailHtml(subject, body, name) {
  const { style, text } = amSplit(body);
  const st = AM_STYLES[style] || null;
  let inner = st ? st.hero(subject) : mH1(subject);
  let src = (name ? 'Hi ' + name + ',\n\n' : '') + text, last = 0, mm;
  AM_TAG.lastIndex = 0;
  while ((mm = AM_TAG.exec(src))) {
    inner += amText(src.slice(last, mm.index).replace(/^\n+|\n+$/g, ''), st);
    if (mm[1].toLowerCase() === 'button') { const [l, u] = mm[2].split('|').map(x => x.trim()); inner += amButton(l, u, st); }
    else if (mm[1].toLowerCase() === 'steps') inner += amSteps(mm[2], st);
    else inner += amHighlight(mm[2].trim(), st);
    last = AM_TAG.lastIndex;
  }
  inner += amText(src.slice(last).replace(/^\n+/, ''), st);
  inner += '<div class="bx-m" style="margin-top:22px;font-size:12px;color:#707A8A;">You are receiving this because you have a BIEXC account.</div>';
  return mailShell({ title: subject, preheader: amPlain(body).replace(/\s+/g, ' ').slice(0, 90), inner });
}

// V48 festive mail art (embedded, no extra upload needed).
const MAIL_DIWALI_HERO = Buffer.from('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', 'base64');
const MAIL_DIWALI_DIYA = Buffer.from('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', 'base64');
app.get('/mail/diwali-hero.jpg', (_req, res) => { res.set('Content-Type', 'image/jpeg'); res.set('Cache-Control', 'public, max-age=604800'); res.send(MAIL_DIWALI_HERO); });
app.get('/mail/diya.png', (_req, res) => { res.set('Content-Type', 'image/png'); res.set('Cache-Control', 'public, max-age=604800'); res.send(MAIL_DIWALI_DIYA); });

// Email logo: the repo's own logo.png when present, built-in BIEXC mark otherwise.
let MAIL_LOGO_FILE = null;
try { const p = path.join(__dirname, 'logo.png'); const b = fs.existsSync(p) ? fs.readFileSync(p) : null; if (b && b.length && b.length < 400_000) MAIL_LOGO_FILE = b; } catch {}
app.get('/mail/logo.png', (_req, res) => { res.set('Content-Type', 'image/png'); res.set('Cache-Control', 'public, max-age=86400'); res.send(MAIL_LOGO_FILE || MAIL_LOGO_PNG); });

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
    // The server already emailed this user about this deposit/withdrawal/P2P → skip the app's duplicate.
    const cat = mailCat(subject);
    if (cat && serverMailedRecently(to_email, cat)) { log('MAIL', `skip app duplicate (${cat}) → ${to_email}`); return res.json({ ok: true, skipped: 'duplicate' }); }
    await sendMailAny({
      to: to_email,
      subject,
      html: mailHtml({ subject, message: message || '', amount: amount || '', status: status || '', uid: uid || '', label: '', coin: '' }),
      text: `${subject}

${noTg(message || '')}

${amount ? 'Amount: ' + amount : ''}

BIEXC — Need help? Open Customer Care in the BIEXC app.`
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
mountEmailOtp(app, { admin, db, log, mailTemplates: { otp: otpMailHtml, reset: resetMailHtml }, sendMail: sendMailAny });
mountKycOtp(app);
mountAutoDeposit(app);
if (db) {
  REF = createReferral({ admin, db, log, mutateBalance, pushHistory, pushNotif, tgSend });
  REF.mount(app);
}

app.get('/*splat', (_req, res) => {
  if (fs.existsSync(INDEX_FILE)) return res.sendFile(INDEX_FILE);
  res.status(200).type('html').send('<!doctype html><title>BIEXC Bot</title><h1>BIEXC Bot is running</h1>');
});

app.listen(PORT, () => log('HTTP', `🌐 Listening on :${PORT}`));

// Start the WhatsApp session (persisted in Firebase → survives restarts)
if (String(process.env.WA_ENABLED || 'true') !== 'false') {
  WA.start(db).then(s => log('WA', `engine started (state=${s.state}) → send /whatsapp in Telegram to link`))
              .catch(e => log('WA', `engine failed: ${e.message}`));
}

// Telegram "/" command list
tgFetch('setMyCommands', { commands: [
  { command: 'menu', description: 'Admin dashboard' },
  { command: 'cancelalltrades', description: 'Reject all pending deposits, withdrawals and P2P trades' },
  { command: 'resetnokyc', description: 'Delete all non-KYC accounts (asks to confirm)' },
  { command: 'resethousebalance', description: 'Set all user balances to 0 (asks to confirm)' },
  { command: 'activeuser', description: 'Users with non-zero balance' },
  { command: 'whatsapp', description: 'WhatsApp linked account / QR' },
  { command: 'complete', description: 'Complete a P2P order: /complete ORDERID' },
  { command: 'reject', description: 'Reject a P2P order: /reject ORDERID' },
  { command: 'today', description: 'Today report (IST)' },
  { command: 'unmatched', description: 'Deposits waiting to be assigned' },
  { command: 'mailstatus', description: 'Is user email working?' },
  { command: 'mailtest', description: 'Send a test email: /mailtest you@mail.com' },
] }).catch(() => {});

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
  if(SUPPORT_ENABLED){await supLoadActive();supportPollLoop().catch(e=>log('SUP',`Customer-care stopped: ${e.message}`));}
  else log('SUP','Configure TG_TOKEN_SUPPORT + TG_CHAT_SUPPORT for customer care.');
  log('INIT', `📍 Resumed from updateId=${lastUpdateId}`);
  // If we boot after 9am IST, skip today's summary so restarts don't spam it.
  if (new Date(Date.now() + IST_OFF).getUTCHours() >= 9) lastSummaryDay = istDayKey();
  await sendMenu().catch(() => {});
  setInterval(() => { dailySummaryTick(); }, 5 * 60_000);
  setInterval(() => { beatInstanceLock(); }, 20_000);
  startAutoDepositJobs();
  startPollWatchdog();
  pollLoop().catch(e => {
    log('FATAL', `pollLoop died: ${e.message} — restarting in 2s`);
    setTimeout(() => pollLoop().catch(() => process.exit(1)), 2000);
  });
})();

process.on('uncaughtException',  e => log('UNCAUGHT', e.stack || e.message));
process.on('unhandledRejection', e => log('UNHANDLED', e?.stack || e?.message || String(e)));
