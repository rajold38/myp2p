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
  [{ text: '📅 Today', callback_data: 'menu_today' }],
  [{ text: '📈 Today', callback_data: 'menu_today' }, { text: '📢 Broadcasts', callback_data: 'menu_broad' }],
  [{ text: '✉️ New broadcast', callback_data: 'menu_bcnew' }, { text: '↻ Refresh', callback_data: 'menu_home' }],
  [{ text: '📱 WhatsApp', callback_data: 'wa_panel' }, { text: '💱 P2P rates', callback_data: 'menu_p2prates' }],
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
  return tgSend(card('📧 EMAIL ALL USERS?', [['👥 Recipients', `${list.length} emails`], ['⏱ Approx time', `${mins} min`], ['💬 Text', text.slice(0, 300)]]),
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
      try { await sendMailAny({ to: r.email, subject, html: allMailHtml(subject, body, r.name), text: (r.name ? `Hi ${r.name},\n\n` : '') + body }); job.sent++; }
      catch (e) { job.failed++; lastErr = e.message; if (/rate|429|quota|limit/i.test(e.message)) await new Promise(x => setTimeout(x, 5000)); }
      if (Date.now() - lastReport > 60000) { lastReport = Date.now(); tgSend(`📧 Progress: ${job.sent} sent · ${job.failed} failed · ${job.total} total`, { parse_mode: undefined }).catch(() => {}); }
      await new Promise(x => setTimeout(x, ALLMAIL_GAP_MS));
    }
    ALLMAIL.job = null;
    log('ALLMAIL', `done sent=${job.sent} failed=${job.failed}`);
    tgSend(`${job.stop ? '⏹ Stopped' : '✅ Done'} — ${job.sent} sent · ${job.failed} failed · ${job.total} total${lastErr ? '\nLast error: ' + lastErr.slice(0, 200) : ''}`, { parse_mode: undefined }).catch(() => {});
  })().catch(e => { ALLMAIL.job = null; tgSend('Email job crashed: ' + e.message, { parse_mode: undefined }).catch(() => {}); });
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
const MAIL_LOGO_PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAGAAAABgCAYAAADimHc4AAAJ3UlEQVR42u2dzW8URxrGn7e6M56e6fkCzCa5oL2sFHFYidWwElEQaAV2iEU2isJtj0Tav2Lsv2IlOOTAYZNBEQkyBgchW1kpEjFB2ig5I3IgS8Cer57pmaG73j3MjDMYG/q7e/DUFeHp+j1d9Va979NVwLRN27RNaGOAmEFTEnELMRUheuAMUKUC8VMVOgBwBWJKJir4VSgAUL+VudK+k/mxdq1QHP7bVISo4DduZy/xdzrL/+hs3Ml+/3C5UJpEEcQkwcdVCLoAu3E7eymfo4v1Jlt1g61smsoHNWv14XKhRAQ5SSKIiYVfZ4sIqhBQa83JFUFMMvztTkywCGLS4U+6CIldP3MFAkdBTuCPNylhlfKktru8sWmqc0cWGjVmCCLIqQAu4NPSAJgb+JMoAiVw2hFEkPXlQukNzfp3ZobmGgZbcAh/0kSgpMJXNWs1m6VyrcaWEO7gT5IIImnwH47gp6lcr/Mzr/AnJTCLpME/OIRfa7JFhDd8dzDhIoikwvfz5k+SCOJ1h590EShG+EQEro/N+WHB3yswW6Y6VxwEZiIC7xsBRgWU1rp+kCy+oWfoeBTwd4pgdPh7VumD3CljEwDiECGe4XcVYn0ditWTX+glOr7V5L6iRAMfABQF6laT+3qJjls9+cX6OhRcjYdFPAL8DD51CjZILHWacjOTRsqWsKP6eVvCzqSR6jTlJkgsnToFGz/voyloPN3wZEUr62laBahk9iCFCPelkBJSm4EAuGZ0eW72nLkxnvrYVzthXoNKp2E9ua6V9RytgsMVYRs+cc1o8dzseXNj9Az7chlKp2HxGtTZ8+aG0eI5ENe0GQgpg38bkwg/ERuxKERIKvzEpCL2FIH9B+Ykw489BuwZE1a0sq7RqiBRanfZFjRwQrxu8EMZAcwDu6AXo9T2SDg3GAmS+decRoqXkRAkfK5AjPqVaAFGW3oiMC1BehahCmX2vLnR2uK/9CzeKOVIkdI5uKDh0xLkqF9BiyAChC+IwA8+KxQ732ZvPF5On6AlSF5zv8OlC7C5CuXwx51fLVOda3d5o5Qn1YkIgcJfg0pLkI+X0yc632ZvPPisUByKEBg3ChC+rC8XSinNuqnl6a+9tnza6vC52XM+AFSh0AXYTrOlgcMfxqNchlZmsuKQ2eS7fVN9vxhgZY2Cgr8DUl9LIwXmmmH4BOEwZR0K/OtaWddpFUQls4t+KU+poMubIgT4lqIgZfZgA1TSc7T65LpWHgVY12/IMGd/ZKFR29xjOgoNfm47RWIrClJh1BMoaPjjb2agYPb4PRDEdm4naPi7pEaCLvRTWPCjEEFLUdnsMRQRwFTnIi8VpAiuhw9Xfg+4TgKjEBBmDxI8nI5WhtNR1f3maud01OnyfxUB0zD5rC/4VSjbG0AHScGd5c36aDrysux2Cx+LQPNarqQU5E03ZUQpIbU0BHgsDTxc5XgeCV9pb2uC3jx8vnN/bQ3qaR8rre20+CDgOsrIjo8EuyHez3/UqmERcJPaFi46TTgKIoKEbn+ezVK51nLu2xmOBFsIKmkpuvFoRZ8drfc9jgQ68nfz0eHznftcgfAD/9GKPqul6IYQg4DrNB0uBNRai59ls1SGbn9OBImj7nbM5Bj+mEs5p9HFRost8lDDlQxb10jpW88VxT3Nn8OOksf/+5wLL6VS2TC95Z1YwirkSG2ZfLlwpv0pV6Hgk8Hu2fcI2M0i3jDYJo8FdEFQDJPtbJrKqs/l3DA94Bn+uAvPK3wAIAG1YbCdz9HFxu3sJboAG1cH+SNfArzEn+/pQcdFiMujs+cKzmefiKDU62y5FUF4gB+IeyEOo1TYRjAiqG5FEHHAj0OEqFx4bkUQccGPUoQoLZBuRaDd8vmAty9T/LTxNfWTpnr2jx816kFYBkd/48G1QnE2b30TlQVy+NtWsUhqszVYHe1k/JwADBAqoP+d+IOWhfGvXI7+UW9GA398OZfXSe32+X6/r35cnG88HK12vMIHgPqtwpFUyvoynaJjTcPb8tmXCHlSWy2+0ob+zze/e2xiCUwY9On3Yc4AFsEamzNMfExKgCP2ijFBmD0GEd4hpX8ogJQ5AQAp/UNEeMfsMZiiNSIwA1ICTHxMY3MGi+Dx10mMr6kBoDjf3LJp5l2z57wKFdAUJLUUhKKg1u7we8Wz5j2vm6zxHTMAKp4177U7/J6ioKalwvEdvWxaNXu8YdPMu8X55tbOES12bmy4AnHgTK2x6bIU6Bv+dsZUzs0udH5ghhJIxWkQ0JXZhc4PRkuGav56Wcb0wJlaY5jI5FemIqJcNURlHYnSBukmXS2cpH3DGglR+naiskG6rRUIp7n3oEWIwzQVtgheCjXCwfwZuAhxOtbCEsFrleyV81/QIgRcoiRPWdSARfBTonT08EGJEDT8UTralw3Spwh+68OOH9yvCCEU5/nhV9rbv13PHKMlyDUvlhefIgRRnCePnXe1RA3LGXFgxlpTFfzJMOXJ2Q/Me1E4IoKE72oEeB0JYdpSMmn6sy2h6Rp948v85XIkxGpLcSNC2MashsFW34IEBeDAcyhC0MYsz7vAl4lANCi+h+2KEwKqIAizO+Y7CkMEhk0UzvE3U3Puq2JCks25O0eCZapzZpfvlg5RioifGl2fIKqDhJyTgP+CAy+okdDlOSJ+WjpEKbPLd62AD34KJBE1EqG40Kg9/kWdN01eaRjywyC+Dai7WG2FIsI5c6NhyA9Nk1ce/6LOFwM+dSvQz212ltu8foE+gv/bl5m3MkX62m02NoxPlPbqY6IEGD0gAGAR5An+c2ty8bUq6K2WB9NU0CJgcQA96BNVpp+pxtwocfDHd6N92H4da0kXQSQWfg/SL/ygA/NrKUAUpcIkiyBed/hJFyE2AbgCsR1w9fDPCnpBBH3sc6kY758RccHHInhrVT+ZSdNN0DDgivCfRwgIsw8bRKVMmm5ureonsTiw4+yfEXAUtL4OBSwrmbw42Omirwj/AddpUwSUThf9TF4cBMvK+joUHI1nRTg9tnI/Hls56mj+tPHU7qnzUTnwxuG3u7xh99T5/GnjaVzwYw3Co1NHiiGbv/aCv/n8B4IcF4d4D+2LyIG3G/yk3CUQ/6F9EYiQ5IsckpML2qXyVW/xM793CCT9Fo3pFSb7PRe023RUHJY3O21eLeVIBbufjiblEp/EXXI2shoWFxq17N/a802TLxeKpDK7OrRveo1VIOmK6UVusccEV98sTxr8RE5BL+yYP4HkKpTCmfanzRZfLu4xHU0i/MQL4FSESYWf+CnIyXTEDEwq/Ilrr+OV5hMtQv1W5kr7TubH2rVCcQo/ahEAqlQgfqpC3162TluM8WHaYgCPcM70n7Zpm7ao2v8BvwpT8B5LI0AAAAAASUVORK5CYII=', 'base64');
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
function allMailHtml(subject, body, name) {
  const inner = mH1(subject) + mP((name ? 'Hi ' + name + ',\n\n' : '') + (body || ''))
    + '<div class="bx-m" style="margin-top:22px;font-size:12px;color:#707A8A;">You are receiving this because you have a BIEXC account.</div>';
  return mailShell({ title: subject, preheader: String(body || '').slice(0, 90), inner });
}

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
