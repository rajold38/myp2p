// ════════════════════════════════════════════════════════════════════
// PRO P2P Terminal — Telegram Backup Bot (24/7)
//
// Watches Firebase Realtime DB for pending deposit/withdrawal requests.
// Sends Approve/Reject buttons to admin on Telegram.
// On button click, updates balance + history in Firebase.
//
// Works even when the site is closed — that's the whole point.
//
// The frontend ALSO does the same polling when the user is online.
// Both write atomically to Firebase, so there's no double-credit.
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

if (!TG_TOKEN || !TG_CHAT || !FIREBASE_DB_URL || !FIREBASE_SA_JSON) {
  console.error('❌ Missing env vars. Need: TG_TOKEN, TG_CHAT, FIREBASE_DB_URL, FIREBASE_SERVICE_ACCOUNT');
  process.exit(1);
}

// ─── FIREBASE ADMIN INIT ────────────────────────────────────────────
let serviceAccount;
try {
  serviceAccount = JSON.parse(FIREBASE_SA_JSON);
} catch (e) {
  console.error('❌ FIREBASE_SERVICE_ACCOUNT is not valid JSON:', e.message);
  process.exit(1);
}

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
  } catch (e) {
    console.error('TG fetch error:', e.message);
    return { ok: false };
  }
}

async function tgSendButtons(text, cbId) {
  const r = await tgFetch('sendMessage', {
    chat_id: TG_CHAT,
    text,
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [[
        { text: '✅ APPROVE', callback_data: `approve_${cbId}` },
        { text: '❌ REJECT',  callback_data: `reject_${cbId}` }
      ]]
    }
  });
  return r.ok ? r.result.message_id : null;
}

async function tgSend(text) {
  return tgFetch('sendMessage', { chat_id: TG_CHAT, text, parse_mode: 'Markdown' });
}

async function tgEdit(msgId, text) {
  return tgFetch('editMessageText', {
    chat_id: TG_CHAT, message_id: msgId, text, parse_mode: 'Markdown'
  });
}

async function tgAnswer(cbQueryId, text) {
  return tgFetch('answerCallbackQuery', { callback_query_id: cbQueryId, text });
}

// ─── PENDING REQUEST WATCHER ────────────────────────────────────────
// Frontend writes pending requests to: users/{firebaseUid}/pendingReq/{dep|wit}
// We watch new ones and send Telegram buttons if frontend hasn't already (no msgId)
// or if the request is older than 90 seconds (frontend offline).

const sentByCbId = new Map(); // cbId -> { firebaseUid, type, msgId }

async function findUserByUID(uid) {
  const snap = await db.ref('users').orderByChild('uid').equalTo(uid).once('value');
  if (!snap.exists()) return null;
  const val = snap.val();
  const firebaseUid = Object.keys(val)[0];
  return { firebaseUid, user: val[firebaseUid] };
}

function fmtMsg(type, user, req) {
  const head = type === 'dep' ? '💰 *NEW DEPOSIT REQUEST*' : '💸 *NEW WITHDRAWAL REQUEST*';
  const lines = [
    head,
    '',
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

async function maybeSendButtons(firebaseUid, type, req) {
  // Skip if already has msgId (frontend handled it) and is fresh
  const ageMs = Date.now() - (req.ts || 0);
  if (req.botMsgId) return; // bot already sent
  if (req.msgId && ageMs < 90_000) return; // frontend has it

  // Acquire lock so two bot replicas don't double-send
  const lockRef = db.ref(`users/${firebaseUid}/pendingReq/${type}/botLock`);
  const tx = await lockRef.transaction(cur => (cur ? undefined : Date.now()));
  if (!tx.committed) return;

  const userSnap = await db.ref(`users/${firebaseUid}`).once('value');
  const user = userSnap.val() || {};
  const cbId = req.cbId || `${type}_${Date.now()}_${Math.random().toString(36).slice(2,7)}`;

  const text = fmtMsg(type, user, req);
  const msgId = await tgSendButtons(text, cbId);
  if (msgId) {
    await db.ref(`users/${firebaseUid}/pendingReq/${type}`).update({
      botMsgId: msgId,
      cbId
    });
    sentByCbId.set(cbId, { firebaseUid, type, msgId, hid: req.hid, amt: req.amt });
    console.log(`📤 Sent ${type} buttons for UID=${user.uid} amt=${req.amt}`);
  }
}

// Listen to all users for pending requests
db.ref('users').on('child_changed', async (snap) => {
  const firebaseUid = snap.key;
  const data = snap.val() || {};
  const pending = data.pendingReq || {};
  if (pending.dep) await maybeSendButtons(firebaseUid, 'dep', pending.dep).catch(e => console.error('dep err', e));
  if (pending.wit) await maybeSendButtons(firebaseUid, 'wit', pending.wit).catch(e => console.error('wit err', e));
});
db.ref('users').on('child_added', async (snap) => {
  const firebaseUid = snap.key;
  const data = snap.val() || {};
  const pending = data.pendingReq || {};
  if (pending.dep) await maybeSendButtons(firebaseUid, 'dep', pending.dep).catch(e => console.error('dep err', e));
  if (pending.wit) await maybeSendButtons(firebaseUid, 'wit', pending.wit).catch(e => console.error('wit err', e));
});

// ─── APPROVE / REJECT HANDLERS ──────────────────────────────────────
async function handleApprove(firebaseUid, type, ctx) {
  const userRef = db.ref(`users/${firebaseUid}`);
  const userSnap = await userRef.once('value');
  const user = userSnap.val();
  if (!user) return;

  const oldBal = parseFloat(user.balance || 0);
  let newBal = oldBal;

  if (type === 'dep') {
    newBal = parseFloat((oldBal + ctx.amt).toFixed(8));
    await userRef.update({ balance: newBal });
  }
  // For withdrawal: balance was already deducted at request time; approval just confirms.

  // Update history status
  if (ctx.hid) {
    const histSnap = await db.ref(`users/${firebaseUid}/history`).once('value');
    if (histSnap.exists()) {
      histSnap.forEach(child => {
        if (child.val().hid === ctx.hid) {
          child.ref.update({ status: 'COMPLETED' });
        }
      });
    }
  }

  await db.ref(`users/${firebaseUid}/pendingReq/${type}`).remove();
  return { oldBal, newBal };
}

async function handleReject(firebaseUid, type, ctx) {
  const userRef = db.ref(`users/${firebaseUid}`);
  const userSnap = await userRef.once('value');
  const user = userSnap.val();
  if (!user) return;

  const oldBal = parseFloat(user.balance || 0);
  let newBal = oldBal;

  if (type === 'wit') {
    // Refund withdrawal
    newBal = parseFloat((oldBal + ctx.amt).toFixed(8));
    await userRef.update({ balance: newBal });
  }

  if (ctx.hid) {
    const histSnap = await db.ref(`users/${firebaseUid}/history`).once('value');
    if (histSnap.exists()) {
      histSnap.forEach(child => {
        if (child.val().hid === ctx.hid) {
          child.ref.update({ status: 'REJECTED' });
        }
      });
    }
  }

  await db.ref(`users/${firebaseUid}/pendingReq/${type}`).remove();
  return { oldBal, newBal };
}

// ─── TELEGRAM POLLING (long-poll) ───────────────────────────────────
let lastUpdateId = 0;

async function pollUpdates() {
  try {
    const res = await fetch(`${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=50&allowed_updates=["callback_query","message"]`);
    const data = await res.json();
    if (!data.ok) return;
    for (const upd of data.result || []) {
      lastUpdateId = upd.update_id;
      try { await handleUpdate(upd); } catch (e) { console.error('handleUpdate err:', e); }
    }
  } catch (e) {
    console.error('poll err:', e.message);
    await new Promise(r => setTimeout(r, 2000));
  }
}

async function handleUpdate(upd) {
  // ── CALLBACK QUERIES (Approve / Reject) ──
  const cb = upd.callback_query;
  if (cb) {
    const chatId = String(cb.message?.chat?.id || '');
    if (chatId !== String(TG_CHAT)) {
      await tgAnswer(cb.id, 'Unauthorized');
      return;
    }
    const data = cb.data || '';
    const m = data.match(/^(approve|reject)_(.+)$/);
    if (!m) return;
    const action = m[1], cbId = m[2];

    // Lookup context (in-memory cache OR scan Firebase)
    let ctx = sentByCbId.get(cbId);
    if (!ctx) {
      // Scan Firebase to recover state (bot restarted mid-flow)
      const snap = await db.ref('users').once('value');
      snap.forEach(child => {
        const p = child.val()?.pendingReq || {};
        if (p.dep && p.dep.cbId === cbId) ctx = { firebaseUid: child.key, type: 'dep', hid: p.dep.hid, amt: p.dep.amt, msgId: p.dep.botMsgId };
        if (p.wit && p.wit.cbId === cbId) ctx = { firebaseUid: child.key, type: 'wit', hid: p.wit.hid, amt: p.wit.amt, msgId: p.wit.botMsgId };
      });
    }
    if (!ctx) {
      await tgAnswer(cb.id, '⚠️ Request expired or already handled');
      return;
    }

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
      '🤖 *PRO P2P Backup Bot*',
      '',
      'Commands:',
      '`/ping` — health check',
      '`#UID AMOUNT` — credit balance to a UID',
      '',
      'Approve/Reject buttons appear automatically for new deposit/withdrawal requests.'
    ].join('\n'));
    return;
  }

  if (text === '/ping') {
    await tgSend('🟢 Bot online — ' + new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) + ' IST');
    return;
  }

  // Admin credit: #UID AMOUNT
  const balMatch = text.match(/^#([A-Z0-9]{4,10})\s+([\d.]+)$/i);
  if (balMatch) {
    const targetUID = balMatch[1].toUpperCase();
    const addAmt = parseFloat(balMatch[2]);
    if (!addAmt || addAmt <= 0) {
      await tgSend('❌ Invalid amount.\nUsage: `#UID AMOUNT`');
      return;
    }
    const found = await findUserByUID(targetUID);
    if (!found) { await tgSend(`❌ UID \`${targetUID}\` not found.`); return; }
    const oldBal = parseFloat(found.user.balance || 0);
    const newBal = parseFloat((oldBal + addAmt).toFixed(8));
    await db.ref(`users/${found.firebaseUid}`).update({ balance: newBal });
    const histEntry = {
      hid: 'h_admin_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5),
      type: 'P2PPRO_CREDIT', amt: addAmt, status: 'COMPLETED',
      uid: found.user.uid || targetUID,
      date: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
      isoDate: new Date().toISOString(), ts: Date.now(),
      sender: 'P2PPRO', note: 'P2PPRO sended you ' + addAmt + ' USDT',
      network: 'INTERNAL', txid: 'p2ppro_' + Date.now()
    };
    await db.ref(`users/${found.firebaseUid}/history`).push(histEntry);
    await tgSend(
      `✅ *BALANCE CREDITED*\n\n` +
      `👤 UID: \`${targetUID}\`\n` +
      `👤 ${found.user.name || 'Unknown'}\n` +
      `📧 ${found.user.email || '—'}\n` +
      `➕ +${addAmt} USDT\n` +
      `💰 ${oldBal.toFixed(2)} → *${newBal.toFixed(2)} USDT*`
    );
  }
}

// Long-poll loop
async function pollLoop() {
  while (true) {
    await pollUpdates();
  }
}
pollLoop();

// ─── EXPRESS HEALTH SERVER (Render needs an open port) ──────────────
const app = express();
app.get('/', (req, res) => res.send('PRO P2P Bot is running ✅'));
app.get('/health', (req, res) => res.json({
  ok: true,
  uptime: process.uptime(),
  lastUpdateId,
  cached: sentByCbId.size
}));
app.listen(PORT, () => console.log(`🌐 Health server on :${PORT}`));
