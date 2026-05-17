// ════════════════════════════════════════════════════════════════════
// PRO P2P Terminal — Telegram Backup Bot (24/7) — v2.0
// ════════════════════════════════════════════════════════════════════

import express from 'express';
import admin from 'firebase-admin';
import fetch from 'node-fetch';

// ─── ENV VARS ───────────────────────────────────────────────────────
const PORT              = process.env.PORT || 10000;
const TG_TOKEN          = process.env.TG_TOKEN;
const TG_CHAT           = process.env.TG_CHAT;
const FIREBASE_DB_URL   = process.env.FIREBASE_DB_URL;
const FIREBASE_SA_JSON  = process.env.FIREBASE_SERVICE_ACCOUNT;
const RENDER_URL        = process.env.RENDER_EXTERNAL_URL || '';

if (!TG_TOKEN || !TG_CHAT || !FIREBASE_DB_URL || !FIREBASE_SA_JSON) {
  console.error('❌ Missing env vars. Need: TG_TOKEN, TG_CHAT, FIREBASE_DB_URL, FIREBASE_SERVICE_ACCOUNT');
  process.exit(1);
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

console.log('✅ Firebase Admin initialized');
console.log('✅ Bot ready, watching Firebase for pending requests...');

// ─── TELEGRAM HELPERS ───────────────────────────────────────────────
async function tgFetch(endpoint, body) {
  try {
    const r = await fetch(`${TG_API}/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    return await r.json();
  } catch (e) { console.error('TG fetch error:', e.message); return { ok: false }; }
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

async function tgSend(text) {
  return tgFetch('sendMessage', { chat_id: TG_CHAT, text, parse_mode: 'Markdown' });
}
async function tgEdit(msgId, text) {
  return tgFetch('editMessageText', { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: 'Markdown' });
}
async function tgAnswer(cbQueryId, text) {
  return tgFetch('answerCallbackQuery', { callback_query_id: cbQueryId, text });
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
    return null;
  } catch (e) {
    console.warn('⚠️ orderByChild failed, scanning:', e.message);
    const allSnap = await db.ref('users').once('value');
    let result = null;
    allSnap.forEach(child => {
      if (child.val()?.uid?.toUpperCase() === uid.toUpperCase()) {
        result = { firebaseUid: child.key, user: child.val() };
      }
    });
    return result;
  }
}

function fmtMsg(type, user, req) {
  const head = type === 'dep' ? '💰 *NEW DEPOSIT REQUEST*' : '💸 *NEW WITHDRAWAL REQUEST*';
  const lines = [
    head, '',
    `👤 UID: \`${user.uid || '—'}\``,
    `👤 Name: *${user.name || '—'}*`,
    `📧 Email: ${user.email || '—'}`,
    `💵 Amount: *${req.amt} USDT*`,
  ];
  if (req.network)  lines.push(`🌐 Network: ${req.network}`);
  if (req.txid)     lines.push(`🔗 TXID: \`${req.txid}\``);
  if (req.address)  lines.push(`📬 Address: \`${req.address}\``);
  if (req.method)   lines.push(`💳 Method: ${req.method}`);
  if (req.utr)      lines.push(`🧾 UTR: \`${req.utr}\``);
  if (req.proof)    lines.push(`🖼 Proof: ${req.proof}`);
  lines.push('', `⏰ ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })} IST`);
  return lines.join('\n');
}

// ─── PENDING REQUEST WATCHER (FIXED: pendingReqs plural, keyed by cbId) ──
async function maybeSendButtons(firebaseUid, type, req, cbId) {
  if (!req || !cbId) return;
  if (req.botMsgId) {
    // already sent — just cache it for click handling
    sentByCbId.set(cbId, { firebaseUid, type, msgId: req.botMsgId, hid: req.hid, amt: parseFloat(req.amt) });
    return;
  }
  const ageMs = Date.now() - (req.ts || 0);
  if (req.msgId && ageMs < 90_000) {
    sentByCbId.set(cbId, { firebaseUid, type, msgId: req.msgId, hid: req.hid, amt: parseFloat(req.amt) });
    return;
  }

  // Lock per-cbId so two replicas don't double-send
  const lockRef = db.ref(`users/${firebaseUid}/pendingReqs/${type}/${cbId}/botLock`);
  const tx = await lockRef.transaction(cur => (cur ? undefined : Date.now()));
  if (!tx.committed) return;

  const userSnap = await db.ref(`users/${firebaseUid}`).once('value');
  const user = userSnap.val() || {};
  const text = fmtMsg(type, user, req);
  const msgId = await tgSendButtons(text, cbId);
  if (msgId) {
    await db.ref(`users/${firebaseUid}/pendingReqs/${type}/${cbId}`).update({ botMsgId: msgId, cbId });
    sentByCbId.set(cbId, { firebaseUid, type, msgId, hid: req.hid, amt: parseFloat(req.amt) });
    console.log(`📤 Sent ${type} buttons UID=${user.uid} amt=${req.amt} cbId=${cbId}`);
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
      await maybeSendButtons(firebaseUid, type, req, cbId).catch(e => console.error(`${type} err`, e));
    }
  }
}

db.ref('users').on('child_changed', async (snap) => {
  await processPendingMap(snap.key, snap.val() || {});
});

const BOT_START_TIME = Date.now();
db.ref('users').on('child_added', async (snap) => {
  await processPendingMap(snap.key, snap.val() || {}, BOT_START_TIME - 10_000);
});

// ─── APPROVE / REJECT HANDLERS ──────────────────────────────────────
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

async function handleApprove(firebaseUid, type, ctx) {
  const userRef = db.ref(`users/${firebaseUid}`);
  const userSnap = await userRef.once('value');
  const user = userSnap.val();
  if (!user) return null;
  const oldBal = parseFloat(user.balance || 0);
  let newBal = oldBal;
  if (type === 'dep') {
    newBal = parseFloat((oldBal + ctx.amt).toFixed(8));
    await userRef.update({ balance: newBal });
  }
  await updateHistoryStatus(firebaseUid, ctx.hid, 'COMPLETED');
  await db.ref(`users/${firebaseUid}/pendingReqs/${type}/${ctx.cbId}`).remove();
  return { oldBal, newBal };
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
    await userRef.update({ balance: newBal });
  }
  await updateHistoryStatus(firebaseUid, ctx.hid, 'REJECTED');
  await db.ref(`users/${firebaseUid}/pendingReqs/${type}/${ctx.cbId}`).remove();
  return { oldBal, newBal };
}

// ─── TELEGRAM POLLING ───────────────────────────────────────────────
let lastUpdateId = 0;

async function pollUpdates() {
  const res = await fetch(`${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=25&allowed_updates=${encodeURIComponent('["callback_query","message"]')}`);
  const data = await res.json();
  if (!data.ok) return;
  for (const upd of data.result || []) {
    lastUpdateId = upd.update_id;
    try { await handleUpdate(upd); } catch (e) { console.error('handleUpdate err:', e); }
  }
}

async function handleUpdate(upd) {
  // ── CALLBACK QUERIES ──
  const cb = upd.callback_query;
  if (cb) {
    const chatId = String(cb.message?.chat?.id || '');
    if (chatId !== String(TG_CHAT)) { await tgAnswer(cb.id, 'Unauthorized'); return; }
    const data = cb.data || '';
    const m = data.match(/^(approve|reject)_(.+)$/);
    if (!m) return;
    const action = m[1], cbId = m[2];

    let ctx = sentByCbId.get(cbId);
    if (!ctx) {
      // Recover from Firebase (bot restarted)
      const snap = await db.ref('users').once('value');
      snap.forEach(child => {
        const reqs = child.val()?.pendingReqs || {};
        for (const type of ['dep', 'wit']) {
          const map = reqs[type] || {};
          for (const [storedCbId, r] of Object.entries(map)) {
            if (storedCbId === cbId || r?.cbId === cbId) {
              ctx = { firebaseUid: child.key, type, hid: r.hid, amt: parseFloat(r.amt), msgId: r.botMsgId, cbId };
            }
          }
        }
      });
    }
    if (!ctx) { await tgAnswer(cb.id, '⚠️ Request expired or already handled'); return; }
    ctx.cbId = cbId;

    if (action === 'approve') {
      await tgAnswer(cb.id, '✅ Approved!');
      const r = await handleApprove(ctx.firebaseUid, ctx.type, ctx);
      const label = ctx.type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL';
      const tail = r ? `\nBalance: ${r.oldBal.toFixed(2)} → *${r.newBal.toFixed(2)} USDT*` : '';
      if (ctx.msgId) await tgEdit(ctx.msgId, `✅ *${label} APPROVED*\n\nAmount: *${ctx.amt} USDT*${tail}`);
    } else {
      await tgAnswer(cb.id, '❌ Rejected!');
      const r = await handleReject(ctx.firebaseUid, ctx.type, ctx);
      const label = ctx.type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL';
      const tail = ctx.type === 'wit' && r ? `\n🔴 Refunded: *${r.newBal.toFixed(2)} USDT*` : '';
      if (ctx.msgId) await tgEdit(ctx.msgId, `❌ *${label} REJECTED*\n\nAmount: *${ctx.amt} USDT*${tail}`);
    }
    sentByCbId.delete(cbId);
    return;
  }

  // ── ADMIN COMMANDS ──
  const msg = upd.message;
  if (!msg) return;
  const chatId = String(msg.chat.id);
  if (chatId !== String(TG_CHAT)) return;
  const text = (msg.text || '').trim();

  if (text === '/start' || text === '/help') {
    await tgSend([
      '🤖 *PRO P2P Admin Bot*', '',
      '📋 *USER COMMANDS:*',
      '`/users` — List all users',
      '`/user UID` — Get user details',
      '`/balance UID AMT` — Set exact balance',
      '`/credit UID AMT` — Add to balance',
      '`/debit UID AMT` — Deduct from balance',
      '`/ban UID` — Ban user (block login)',
      '`/unban UID` — Unban user',
      '`/trades` — All pending trades',
      '`/cancel TRADEID` — Cancel a trade/request', '',
      '📋 *QUICK:*',
      '`#UID AMT` — Quick credit', '',
      '📋 *SYSTEM:*',
      '`/ping` — Health check',
    ].join('\n'));
    return;
  }

  if (text === '/ping') {
    await tgSend('🟢 Bot online — ' + new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) + ' IST');
    return;
  }

  // /users
  if (text === '/users') {
    const snap = await db.ref('users').once('value');
    if (!snap.exists()) { await tgSend('No users found.'); return; }
    const lines = ['👥 *ALL USERS*\n'];
    let count = 0;
    snap.forEach(child => {
      const u = child.val();
      if (!u || !u.uid) return;
      const status = u.banned ? '🚫' : '✅';
      lines.push(`${status} \`${u.uid}\` — *${u.name || 'Unknown'}* — 💰 ${parseFloat(u.balance || 0).toFixed(2)} USDT`);
      count++;
    });
    lines.push(`\n📊 Total: ${count} users`);
    let chunk = '';
    for (const line of lines) {
      if ((chunk + line + '\n').length > 4000) { await tgSend(chunk); chunk = ''; }
      chunk += line + '\n';
    }
    if (chunk) await tgSend(chunk);
    return;
  }

  // /user UID
  const userMatch = text.match(/^\/user\s+([A-Z0-9]{2,15})$/i);
  if (userMatch) {
    const targetUID = userMatch[1].toUpperCase();
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    const u = found.user;
    await tgSend([
      `👤 *USER DETAILS*`, '',
      `🆔 UID: \`${u.uid || '—'}\``,
      `📛 Name: *${u.name || '—'}*`,
      `📧 Email: ${u.email || '—'}`,
      `📱 Phone: ${u.phone || '—'}`,
      `💰 Balance: *${parseFloat(u.balance || 0).toFixed(2)} USDT*`,
      `🔑 Status: ${u.banned ? '🚫 *BANNED*' : '✅ Active'}`,
      `📅 Joined: ${u.createdAt ? new Date(u.createdAt).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) : '—'}`,
    ].join('\n'));
    return;
  }

  // /balance UID AMT
  const setBalMatch = text.match(/^\/balance\s+([A-Z0-9]{2,15})\s+([\d.]+)$/i);
  if (setBalMatch) {
    const targetUID = setBalMatch[1].toUpperCase();
    const newBal = parseFloat(setBalMatch[2]);
    if (isNaN(newBal) || newBal < 0) { await tgSend('❌ Invalid amount.'); return; }
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    const oldBal = parseFloat(found.user.balance || 0);
    await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal });
    await tgSend(`✅ *BALANCE SET*\n\n👤 \`${targetUID}\`\n📛 ${found.user.name || 'Unknown'}\n💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`);
    return;
  }

  // /credit
  const creditMatch = text.match(/^\/credit\s+([A-Z0-9]{2,15})\s+([\d.]+)$/i);
  if (creditMatch) {
    const targetUID = creditMatch[1].toUpperCase();
    const addAmt = parseFloat(creditMatch[2]);
    if (!addAmt || addAmt <= 0) { await tgSend('❌ Invalid amount.'); return; }
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    const oldBal = parseFloat(found.user.balance || 0);
    const newBal = parseFloat((oldBal + addAmt).toFixed(8));
    await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal });
    await db.ref(`users/${found.firebaseUid}/history`).push({
      hid: 'h_admin_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
      type: 'ADMIN_CREDIT', amt: addAmt, status: 'COMPLETED',
      uid: found.user.uid || targetUID,
      date: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
      isoDate: new Date().toISOString(), ts: Date.now(),
      note: `Admin credited ${addAmt} USDT`, network: 'INTERNAL',
    });
    await tgSend(`✅ *CREDITED*\n\n👤 \`${targetUID}\`\n📛 ${found.user.name || 'Unknown'}\n➕ +${addAmt} USDT\n💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`);
    return;
  }

  // /debit
  const debitMatch = text.match(/^\/debit\s+([A-Z0-9]{2,15})\s+([\d.]+)$/i);
  if (debitMatch) {
    const targetUID = debitMatch[1].toUpperCase();
    const deductAmt = parseFloat(debitMatch[2]);
    if (!deductAmt || deductAmt <= 0) { await tgSend('❌ Invalid amount.'); return; }
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    const oldBal = parseFloat(found.user.balance || 0);
    const newBal = Math.max(0, parseFloat((oldBal - deductAmt).toFixed(8)));
    await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal });
    await db.ref(`users/${found.firebaseUid}/history`).push({
      hid: 'h_admin_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
      type: 'ADMIN_DEBIT', amt: deductAmt, status: 'COMPLETED',
      uid: found.user.uid || targetUID,
      date: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
      isoDate: new Date().toISOString(), ts: Date.now(),
      note: `Admin debited ${deductAmt} USDT`, network: 'INTERNAL',
    });
    await tgSend(`✅ *DEBITED*\n\n👤 \`${targetUID}\`\n📛 ${found.user.name || 'Unknown'}\n➖ -${deductAmt} USDT\n💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`);
    return;
  }

  // /ban
  const banMatch = text.match(/^\/ban\s+([A-Z0-9]{2,15})$/i);
  if (banMatch) {
    const targetUID = banMatch[1].toUpperCase();
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    await db.ref(`users/${found.firebaseUid}`).update({ banned: true });
    await tgSend(`🚫 *USER BANNED*\n\n👤 \`${targetUID}\`\n📛 ${found.user.name || 'Unknown'}\n📧 ${found.user.email || '—'}`);
    return;
  }

  // /unban
  const unbanMatch = text.match(/^\/unban\s+([A-Z0-9]{2,15})$/i);
  if (unbanMatch) {
    const targetUID = unbanMatch[1].toUpperCase();
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    await db.ref(`users/${found.firebaseUid}`).update({ banned: false });
    await tgSend(`✅ *USER UNBANNED*\n\n👤 \`${targetUID}\`\n📛 ${found.user.name || 'Unknown'}`);
    return;
  }

  // /trades
  if (text === '/trades') {
    const snap = await db.ref('users').once('value');
    const lines = ['📊 *ALL PENDING / ONGOING TRADES*\n'];
    let count = 0;
    snap.forEach(child => {
      const u = child.val();
      if (!u) return;
      const reqs = u.pendingReqs || {};
      for (const type of ['dep', 'wit']) {
        const map = reqs[type] || {};
        for (const [cbId, r] of Object.entries(map)) {
          if (!r || typeof r !== 'object' || cbId === 'botLock') continue;
          const label = type === 'dep' ? '💰 DEP' : '💸 WIT';
          lines.push(`${label} | \`${u.uid || child.key}\` | *${r.amt} USDT* | cbId: \`${cbId}\``);
          count++;
        }
      }
      const orders = u.orders || {};
      for (const [oid, ord] of Object.entries(orders)) {
        if (ord && ord.status && ['pending', 'active', 'ongoing', 'payment_sent'].includes(String(ord.status).toLowerCase())) {
          lines.push(`🔄 P2P | \`${u.uid || child.key}\` | Order: \`${oid}\` | ${ord.status} | ${ord.amt || ord.amount || '?'} USDT`);
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
    return;
  }

  // /cancel TRADEID
  const cancelMatch = text.match(/^\/cancel\s+(\S+)$/i);
  if (cancelMatch) {
    const targetId = cancelMatch[1];
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
              await db.ref(`users/${fuid}`).update({ balance: newBal });
            }
            await updateHistoryStatus(fuid, req.hid, 'CANCELLED');
            await db.ref(`users/${fuid}/pendingReqs/${type}/${cbId}`).remove();
            if (req.botMsgId) {
              await tgEdit(req.botMsgId, `🚫 *${type === 'dep' ? 'DEPOSIT' : 'WITHDRAWAL'} CANCELLED BY ADMIN*\n\nAmount: *${req.amt} USDT*${type === 'wit' ? '\n🔴 Funds refunded' : ''}`).catch(() => {});
            }
            sentByCbId.delete(cbId);
            await tgSend(`✅ *CANCELLED*\n\n👤 \`${u.uid || fuid}\`\n📛 ${u.name || 'Unknown'}\nType: ${type.toUpperCase()}\nAmt: *${req.amt} USDT*`);
            found = true;
            break;
          }
        }
        if (found) break;
      }
      if (found) break;
    }
    if (!found) await tgSend(`❌ Trade \`${targetId}\` not found or already resolved.`);
    return;
  }

  // #UID AMOUNT quick credit
  const balMatch = text.match(/^#([A-Z0-9]{2,15})\s+([\d.]+)$/i);
  if (balMatch) {
    const targetUID = balMatch[1].toUpperCase();
    const addAmt = parseFloat(balMatch[2]);
    if (!addAmt || addAmt <= 0) { await tgSend('❌ Invalid. Usage: `#UID AMOUNT`'); return; }
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    const oldBal = parseFloat(found.user.balance || 0);
    const newBal = parseFloat((oldBal + addAmt).toFixed(8));
    await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal });
    await db.ref(`users/${found.firebaseUid}/history`).push({
      hid: 'h_admin_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
      type: 'P2PPRO_CREDIT', amt: addAmt, status: 'COMPLETED',
      uid: found.user.uid || targetUID,
      date: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
      isoDate: new Date().toISOString(), ts: Date.now(),
      sender: 'P2PPRO', note: 'P2PPRO sended you ' + addAmt + ' USDT',
      network: 'INTERNAL', txid: 'p2ppro_' + Date.now()
    });
    await tgSend(`✅ *CREDITED*\n\n👤 \`${targetUID}\`\n📛 ${found.user.name || 'Unknown'}\n➕ +${addAmt} USDT\n💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`);
  }
}

// ─── POLL LOOP w/ EXPONENTIAL BACKOFF ───────────────────────────────
let pollRetryDelay = 2000;
async function pollLoop() {
  while (true) {
    try {
      await pollUpdates();
      pollRetryDelay = 2000;
    } catch (e) {
      console.error('pollLoop error:', e.message);
      await new Promise(r => setTimeout(r, pollRetryDelay));
      pollRetryDelay = Math.min(pollRetryDelay * 2, 30000);
    }
  }
}
pollLoop();

// ─── SELF-PING (prevents Render spin-down) ──────────────────────────
if (RENDER_URL) {
  setInterval(async () => {
    try { await fetch(`${RENDER_URL}/health`); console.log('🏓 Self-ping OK'); }
    catch (e) { console.warn('Self-ping failed:', e.message); }
  }, 10 * 60 * 1000);
}

// ─── EXPRESS HEALTH ─────────────────────────────────────────────────
const app = express();
app.get('/', (req, res) => res.send('PRO P2P Bot is running ✅'));
app.get('/health', (req, res) => res.json({
  ok: true, uptime: process.uptime(), lastUpdateId, cached: sentByCbId.size
}));
app.listen(PORT, () => console.log(`🌐 Health server on :${PORT}`));
