/**
 * PRO P2P — Telegram Bot v5 (FULL BACKEND)
 *
 * NEW IN v5:
 * ✅ Frontend ne TG calls karna band — bot khud Firebase watch karta hai
 * ✅ Bot detects: new deposits, withdrawals, orders, trade-stage changes
 * ✅ Bot forwards user chat → Telegram automatically
 * ✅ Bot forwards admin Telegram reply → Firebase chat automatically
 * ✅ TG_TOKEN ab sirf Render env var mein — frontend mein NOT exposed
 *
 * DEPLOY ON: Render (Web Service, Node 18+, free tier OK)
 * ENV VARS REQUIRED:
 *   TG_TOKEN     = your bot token
 *   TG_CHAT      = primary admin chat id
 *   TG_CHAT_LOG  = log chat id
 *   FB_URL       = https://my-p2p-5d11a-default-rtdb.firebaseio.com
 *   FB_SECRET    = (optional) firebase database secret
 *   PORT         = (Render auto-sets)
 */

const https = require("https");
const http  = require("http");

// ── CONFIG (from env, with fallbacks) ────────────────
const TG_TOKEN    = process.env.TG_TOKEN    || "8665516559:AAGROglGKRrQl7lx4EyjoV7SkG0LHI-GJW0";
const TG_CHAT     = process.env.TG_CHAT     || "8515209984";
const TG_CHAT_LOG = process.env.TG_CHAT_LOG || "64552009";
const FB_URL      = process.env.FB_URL      || "https://my-p2p-5d11a-default-rtdb.firebaseio.com";
const FB_SECRET   = process.env.FB_SECRET   || "";
const TG_API      = `https://api.telegram.org/bot${TG_TOKEN}`;
const PORT        = process.env.PORT || 3000;

const FB_POLL_INTERVAL = 2000; // 2 seconds

// ── STATE ────────────────────────────────────────────
let lastUpdateId      = 0;
const seenUpdates     = new Set();
const processingCb    = new Set();
const seenDeposits    = new Set();   // firebaseUid+hid
const seenWithdrawals = new Set();
const seenOrders      = new Set();   // firebaseUid+ordId
const seenChatStages  = new Map();   // ordId -> last dealStage
const seenUserMsgs    = new Set();   // ordId+msgId  (avoid duplicate forwards)
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── HTTP HELPER ──────────────────────────────────────
function httpReq(url, method = "GET", body = null, timeoutMs = 35_000) {
  return new Promise((resolve, reject) => {
    const u   = new URL(url);
    const lib = u.protocol === "https:" ? https : http;
    const opt = {
      hostname: u.hostname,
      path:     u.pathname + u.search,
      method,
      headers:  { "Content-Type": "application/json" },
      timeout:  timeoutMs
    };
    const r = lib.request(opt, (res) => {
      let d = "";
      res.on("data", c => d += c);
      res.on("end", () => { try { resolve(JSON.parse(d)); } catch(e) { resolve({}); } });
    });
    r.on("timeout", () => { r.destroy(); reject(new Error("Request timeout")); });
    r.on("error", reject);
    if (body) r.write(JSON.stringify(body));
    r.end();
  });
}

// ── FIREBASE HELPERS ─────────────────────────────────
const auth = FB_SECRET ? `?auth=${FB_SECRET}` : "";
async function fbGet(path)        { try { return await httpReq(`${FB_URL}/${path}.json${auth}`, "GET"); } catch(e) { return null; } }
async function fbSet(path, val)   { try { await httpReq(`${FB_URL}/${path}.json${auth}`, "PUT", val); } catch(e) {} }
async function fbPatch(path, val) { try { await httpReq(`${FB_URL}/${path}.json${auth}`, "PATCH", val); } catch(e) {} }
async function fbDelete(path)     { try { await httpReq(`${FB_URL}/${path}.json${auth}`, "DELETE"); } catch(e) {} }
async function fbPush(path, val)  { try { return await httpReq(`${FB_URL}/${path}.json${auth}`, "POST", val); } catch(e) { return null; } }

// ── TELEGRAM HELPERS ─────────────────────────────────
async function tgPost(method, body) { try { return await httpReq(`${TG_API}/${method}`, "POST", body, 15_000); } catch(e) { return { ok: false }; } }
const tgSend     = (text) => tgPost("sendMessage", { chat_id: TG_CHAT,     text, parse_mode: "Markdown" });
const tgSendLog  = (text) => tgPost("sendMessage", { chat_id: TG_CHAT_LOG, text, parse_mode: "Markdown" });
const tgSendBoth = (text) => { tgSend(text); tgSendLog(text); };
const tgAnswer   = (id, text, alert = false) => tgPost("answerCallbackQuery", { callback_query_id: id, text, show_alert: alert });
const tgEdit     = (msgId, text) => tgPost("editMessageText", { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: "Markdown" });

async function tgButtons(text, cbId) {
  const keyboard = { inline_keyboard: [[
    { text: "✅ APPROVE", callback_data: `approve_${cbId}` },
    { text: "❌ REJECT",  callback_data: `reject_${cbId}`  }
  ]]};
  const d = await tgPost("sendMessage", { chat_id: TG_CHAT,     text, parse_mode: "Markdown", reply_markup: keyboard });
  await    tgPost("sendMessage",        { chat_id: TG_CHAT_LOG, text, parse_mode: "Markdown", reply_markup: keyboard });
  return d.ok ? d.result.message_id : null;
}

// ── UTILS ────────────────────────────────────────────
function nowIST() { return new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }); }
function genCbId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
}
function genTxId(prefix) {
  const c = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let id = "";
  for (let i = 0; i < 6; i++) id += c[Math.floor(Math.random() * c.length)];
  return `${prefix}-${id}`;
}

// ── UPDATE HISTORY STATUS ────────────────────────────
async function updateHistStatus(firebaseUid, hid, status) {
  const hist = await fbGet(`users/${firebaseUid}/history`);
  if (!hist || typeof hist !== "object") return;
  for (const [key, val] of Object.entries(hist)) {
    if (val && val.hid === hid) {
      await fbPatch(`users/${firebaseUid}/history/${key}`, { status });
      return;
    }
  }
}

// ── NOTIFY CLIENT ────────────────────────────────────
async function notifyClient(firebaseUid, cbId, payload) {
  const safeKey = cbId.replace(/[.#$\[\]/]/g, "_");
  await fbSet(`users/${firebaseUid}/adminActions/${safeKey}`, {
    ...payload, ts: Date.now(), processed: false
  });
}

// ════════════════════════════════════════════════════
// FIREBASE WATCHER — har 2 sec scan kare
// ════════════════════════════════════════════════════
async function pollFirebase() {
  const users = await fbGet("users");
  if (!users || typeof users !== "object") return;

  for (const [firebaseUid, userData] of Object.entries(users)) {
    if (!userData) continue;
    const userLabel = `${userData.uid || "?"} | ${userData.name || "?"}`;

    // ─── 1. NEW DEPOSITS ──────────────────────────
    const dep = userData.pendingReqs?.dep;
    if (dep && dep.hid && !dep.cbId) {
      const key = `${firebaseUid}:${dep.hid}`;
      if (!seenDeposits.has(key)) {
        seenDeposits.add(key);
        const cbId = genCbId("dep");
        const text = `📥 *NEW DEPOSIT REQUEST*\n\n👤 ${userLabel}\n📧 ${userData.email || "—"}\n💰 *${dep.amt} USDT*\n🌐 Network: ${dep.network || "BEP20"}\n🔖 TxID: \`${dep.txid || "—"}\`\n📋 HID: \`${dep.hid}\`\n⏰ ${nowIST()} IST`;
        const msgId = await tgButtons(text, cbId);
        await fbPatch(`users/${firebaseUid}/pendingReqs/dep`, { cbId, msgId });
        console.log(`📥 New DEP ${dep.amt} USDT → ${userData.uid}`);
      }
    }

    // ─── 2. NEW WITHDRAWALS ───────────────────────
    const wit = userData.pendingReqs?.wit;
    if (wit && wit.hid && !wit.cbId) {
      const key = `${firebaseUid}:${wit.hid}`;
      if (!seenWithdrawals.has(key)) {
        seenWithdrawals.add(key);
        const cbId = genCbId("wit");
        const text = `💼 *NEW WITHDRAWAL REQUEST*\n\n👤 ${userLabel}\n📧 ${userData.email || "—"}\n💸 *${wit.amt} USDT*\n📍 Address: \`${wit.address || "—"}\`\n🌐 Network: ${wit.network || "BEP20"}\n📋 HID: \`${wit.hid}\`\n⏰ ${nowIST()} IST`;
        const msgId = await tgButtons(text, cbId);
        await fbPatch(`users/${firebaseUid}/pendingReqs/wit`, { cbId, msgId });
        console.log(`💼 New WIT ${wit.amt} USDT → ${userData.uid}`);
      }
    }

    // ─── 3. NEW ORDERS / TRADE STAGE CHANGES ──────
    const orders = userData.orders;
    const chats  = userData.chats;
    if (orders && typeof orders === "object") {
      for (const [ordId, order] of Object.entries(orders)) {
        if (!order) continue;
        const ordKey = `${firebaseUid}:${ordId}`;

        // New order announcement
        if (!seenOrders.has(ordKey)) {
          seenOrders.add(ordKey);
          tgSendLog(`🆕 *NEW ORDER PLACED*\n\n👤 ${userLabel}\n📋 \`#${order.id}\`\n💹 *${order.mode}* ${order.usdt?.toFixed(2)} USDT\n💵 ₹${Number(order.inr).toLocaleString()}\n🏪 ${order.merchant || "—"}\n⏰ ${nowIST()} IST`);
          console.log(`🆕 New order #${order.id} → ${userData.uid}`);
        }

        // Trade reached pending_verify stage → send approve/reject buttons
        // Source of truth = Firebase cbId field. If absent → bot must send buttons.
        // Works for BOTH "I HAVE PAID" (BUY) and "RELEASE USDT" (SELL) flows.
        const cs = chats?.[ordId];
        if (cs && cs.dealStage === "pending_verify" && !cs.cbId) {
          const cbId = genCbId("trd");
          const isBuy = order.mode === "BUY";
          const headline = isBuy
            ? "💰 *BUYER CLAIMS PAID — VERIFY INR*"
            : "🚀 *SELLER WANTS TO RELEASE USDT — VERIFY*";
          const actionLine = isBuy
            ? `_Buyer paid ₹${Number(order.inr).toLocaleString()} via UPI. Verify receipt then APPROVE to release ${order.usdt?.toFixed(2)} USDT._`
            : `_Seller is releasing ${order.usdt?.toFixed(2)} USDT after receiving INR. Verify INR receipt then APPROVE._`;
          const text =
            `${headline}\n\n` +
            `👤 ${userLabel}\n` +
            `📋 Order: \`#${order.id}\`\n` +
            `💹 Mode: *${order.mode}*\n` +
            `🪙 USDT: *${order.usdt?.toFixed(2)}*\n` +
            `💵 INR: ₹${Number(order.inr).toLocaleString()}\n` +
            `📊 Rate: ₹${order.rate}\n` +
            `🏪 Merchant: ${order.merchant || "—"}\n` +
            (cs.utr ? `🔖 UTR: \`${cs.utr}\`\n` : "") +
            `⏰ ${nowIST()} IST\n\n${actionLine}`;
          const msgId = await tgButtons(text, cbId);
          await fbPatch(`users/${firebaseUid}/chats/${ordId}`, { cbId, tgMsgId: msgId });
          console.log(`🤝 Trade ${isBuy ? "PAID-claim" : "RELEASE-request"} #${order.id} → ${userData.uid} cbId=${cbId}`);
        }
      }
    }

    // ─── 4. FORWARD USER CHAT MESSAGES TO TELEGRAM ──
    if (chats && typeof chats === "object") {
      for (const [ordId, cs] of Object.entries(chats)) {
        if (!cs?.userMsgs) continue;
        for (const [msgId, msg] of Object.entries(cs.userMsgs)) {
          if (!msg || msg.forwarded) continue;
          const fwdKey = `${ordId}:${msgId}`;
          if (seenUserMsgs.has(fwdKey)) continue;
          seenUserMsgs.add(fwdKey);
          const text = `💬 *USER CHAT* — \`#${ordId}\`\n👤 ${userLabel}\n\n${msg.text || "(empty)"}\n\n_Reply with:_ \`/r ${ordId} your message\``;
          await tgSend(text);
          await fbPatch(`users/${firebaseUid}/chats/${ordId}/userMsgs/${msgId}`, { forwarded: true });
        }
      }
    }
  }
}

// ════════════════════════════════════════════════════
// HANDLE DEPOSIT / WITHDRAWAL / TRADE (same as v4)
// ════════════════════════════════════════════════════
async function findCbIdOwner(cbId) {
  const users = await fbGet("users");
  if (!users) return null;
  for (const [firebaseUid, userData] of Object.entries(users)) {
    if (!userData) continue;
    const dep = userData.pendingReqs?.dep;
    if (dep?.cbId === cbId) return { firebaseUid, type: "dep", data: dep };
    const wit = userData.pendingReqs?.wit;
    if (wit?.cbId === cbId) return { firebaseUid, type: "wit", data: wit };
    const chats = userData.chats;
    if (chats) {
      for (const [ordId, cs] of Object.entries(chats)) {
        if (cs?.cbId === cbId) return { firebaseUid, type: "trade", ordId, data: cs };
      }
    }
  }
  return null;
}

async function handleDeposit(firebaseUid, dep, action) {
  const t = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;
  if (action === "approve") {
    const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + dep.amt).toFixed(8));
    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, dep.hid, "COMPLETED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    await notifyClient(firebaseUid, dep.cbId, { action: "approve", type: "dep", amt: dep.amt, newBalance: newBal });
    if (dep.msgId) tgEdit(dep.msgId, `✅ *DEPOSIT APPROVED*\n\n👤 \`${userData.uid}\` | *${userData.name}*\n💰 *+${dep.amt} USDT*\n💼 New Bal: *${newBal.toFixed(2)}*\n⏰ ${t} IST`);
    tgSendBoth(`✅ DEP credited ${dep.amt} → ${userData.uid}`);
  } else {
    await updateHistStatus(firebaseUid, dep.hid, "REJECTED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    await notifyClient(firebaseUid, dep.cbId, { action: "reject", type: "dep", amt: dep.amt });
    if (dep.msgId) tgEdit(dep.msgId, `❌ *DEPOSIT REJECTED*\n\n👤 \`${userData.uid}\`\n💰 ${dep.amt} USDT\n⏰ ${t} IST`);
  }
}

async function handleWithdrawal(firebaseUid, wit, action) {
  const t = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;
  if (action === "approve") {
    await updateHistStatus(firebaseUid, wit.hid, "COMPLETED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/wit`);
    await notifyClient(firebaseUid, wit.cbId, { action: "approve", type: "wit", amt: wit.amt });
    if (wit.msgId) tgEdit(wit.msgId, `✅ *WITHDRAWAL SENT*\n\n👤 \`${userData.uid}\`\n💸 ${wit.amt} USDT\n⏰ ${t} IST`);
    tgSendBoth(`✅ WIT sent ${wit.amt} → ${userData.uid}`);
  } else {
    const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + wit.amt).toFixed(8));
    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, wit.hid, "REJECTED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/wit`);
    await notifyClient(firebaseUid, wit.cbId, { action: "reject", type: "wit", amt: wit.amt, newBalance: newBal });
    if (wit.msgId) tgEdit(wit.msgId, `❌ *WITHDRAWAL REFUNDED*\n\n👤 \`${userData.uid}\`\n💰 ${wit.amt} USDT\n⏰ ${t} IST`);
  }
}

async function handleTrade(firebaseUid, ordId, chatState, action) {
  const t = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;
  const order = await fbGet(`users/${firebaseUid}/orders/${ordId}`);
  if (!order) { await tgSend(`⚠️ Order \`#${ordId}\` not found.`); return; }

  if (action === "approve") {
    const txId = genTxId("TRD");
    if (order.mode === "BUY") {
      const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
      await fbSet(`users/${firebaseUid}/balance`, parseFloat((bal + order.usdt).toFixed(8)));
    }
    await fbPush(`users/${firebaseUid}/history`, {
      hid: `h_${Date.now()}_${Math.random().toString(36).slice(2,5)}`,
      type: order.mode, amt: order.usdt, status: "COMPLETED",
      uid: userData.uid, date: t, isoDate: new Date().toISOString(), ts: Date.now(),
      merchant: order.merchant, rate: order.rate, inr: order.inr, txid: txId, network: "BEP20"
    });
    await fbDelete(`users/${firebaseUid}/orders/${ordId}`);
    await fbDelete(`users/${firebaseUid}/chats/${ordId}`);
    await notifyClient(firebaseUid, chatState.cbId, { action: "approve", type: "trade", ordId, usdt: order.usdt, mode: order.mode, txId });
    if (chatState.tgMsgId) tgEdit(chatState.tgMsgId, `✅ *TRADE COMPLETE*\n\n👤 \`${userData.uid}\`\n📋 \`#${order.id}\`\n🪙 ${order.usdt.toFixed(2)} USDT\n💵 ₹${Number(order.inr).toLocaleString()}\n🔖 \`${txId}\`\n⏰ ${t} IST`);
    tgSendBoth(`✅ TRADE done #${order.id} → ${userData.uid}`);
  } else {
    if (order.mode === "SELL") {
      const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
      await fbSet(`users/${firebaseUid}/balance`, parseFloat((bal + order.usdt).toFixed(8)));
    }
    await fbPush(`users/${firebaseUid}/history`, {
      hid: `h_${Date.now()}_${Math.random().toString(36).slice(2,5)}`,
      type: order.mode + "_CANCEL", amt: order.usdt, status: "CANCELLED",
      uid: userData.uid, date: t, isoDate: new Date().toISOString(), ts: Date.now(),
      merchant: order.merchant, rate: order.rate, inr: order.inr
    });
    await fbDelete(`users/${firebaseUid}/orders/${ordId}`);
    await fbDelete(`users/${firebaseUid}/chats/${ordId}`);
    await notifyClient(firebaseUid, chatState.cbId, { action: "reject", type: "trade", ordId, usdt: order.usdt, mode: order.mode });
    if (chatState.tgMsgId) tgEdit(chatState.tgMsgId, `❌ *TRADE REJECTED*\n\n👤 \`${userData.uid}\`\n📋 \`#${order.id}\`\n⏰ ${t} IST`);
  }
}

async function processCallback(cbId, action) {
  const entry = await findCbIdOwner(cbId);
  if (!entry) return false;
  const { firebaseUid, type, ordId, data } = entry;
  if (type === "dep") { await handleDeposit(firebaseUid, data, action); return true; }
  if (type === "wit") { await handleWithdrawal(firebaseUid, data, action); return true; }
  if (type === "trade") {
    const fresh = await fbGet(`users/${firebaseUid}/chats/${ordId}`);
    if (!fresh) return false;
    await handleTrade(firebaseUid, ordId, fresh, action);
    return true;
  }
  return false;
}

// ════════════════════════════════════════════════════
// ADMIN REPLY → FIREBASE CHAT
// Format: /r <ordId> <message>
// ════════════════════════════════════════════════════
async function handleAdminReply(ordId, replyText) {
  const users = await fbGet("users");
  if (!users) return false;
  for (const [firebaseUid, userData] of Object.entries(users)) {
    if (!userData?.chats?.[ordId]) continue;
    const msgRef = await fbPush(`users/${firebaseUid}/chats/${ordId}/adminMsgs`, {
      from: "admin", text: replyText, ts: Date.now(), date: nowIST()
    });
    await tgSend(`✅ Reply sent to \`#${ordId}\` (${userData.uid})`);
    return true;
  }
  return false;
}

// ════════════════════════════════════════════════════
// ADMIN COMMANDS (credit / stats / lookup) — from v4
// ════════════════════════════════════════════════════
async function handleCredit(targetUID, addAmt) {
  const users = await fbGet("users");
  if (!users) { await tgSend("❌ DB empty."); return; }
  for (const [firebaseUid, u] of Object.entries(users)) {
    if (!u || (u.uid || "").toUpperCase() !== targetUID.toUpperCase()) continue;
    const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + addAmt).toFixed(8));
    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await fbPush(`users/${firebaseUid}/history`, {
      hid: `h_admin_${Date.now()}`, type: "P2PPRO_CREDIT", amt: addAmt, status: "COMPLETED",
      uid: u.uid, date: nowIST(), isoDate: new Date().toISOString(), ts: Date.now(),
      sender: "P2PPRO", note: `P2PPRO sended you ${addAmt} USDT`,
      network: "INTERNAL", txid: `p2ppro_${Date.now()}`
    });
    await notifyClient(firebaseUid, `credit_${Date.now()}`, { action: "credit", type: "credit", amt: addAmt, newBalance: newBal });
    await tgSendBoth(`✅ *CREDITED*\n👤 \`${targetUID}\` | *${u.name}*\n➕ +${addAmt} USDT → ${newBal.toFixed(2)}`);
    return;
  }
  await tgSend(`❌ UID \`${targetUID}\` not found.`);
}

async function handleStats() {
  const users = await fbGet("users");
  if (!users) { await tgSend("❌ No data."); return; }
  let n = 0, bal = 0, pd = 0, pw = 0, ao = 0;
  for (const [, u] of Object.entries(users)) {
    if (!u) continue;
    n++; bal += parseFloat(u.balance || 0);
    if (u.pendingReqs?.dep) pd++;
    if (u.pendingReqs?.wit) pw++;
    if (u.orders) ao += Object.keys(u.orders).length;
  }
  await tgSend(`📊 *STATS*\n👥 ${n} users\n💰 ${bal.toFixed(2)} USDT total\n📥 ${pd} deps | 💼 ${pw} wits\n🤝 ${ao} active orders\n⏰ ${nowIST()}`);
}

// ════════════════════════════════════════════════════
// TELEGRAM POLLER
// ════════════════════════════════════════════════════
async function pollTelegram() {
  const data = await httpReq(
    `${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=25&allowed_updates=["message","callback_query"]`,
    "GET", null, 35_000
  );
  if (!data.ok || !data.result?.length) return;

  for (const update of data.result) {
    const uid = update.update_id;
    lastUpdateId = Math.max(lastUpdateId, uid);
    if (seenUpdates.has(uid)) continue;
    seenUpdates.add(uid);
    if (seenUpdates.size > 1000) {
      const arr = [...seenUpdates].sort((a,b) => a-b);
      arr.slice(0, 500).forEach(x => seenUpdates.delete(x));
    }

    // Callback (approve/reject)
    if (update.callback_query) {
      const cb = update.callback_query;
      const d  = cb.data || "";
      const chatId = String(cb.message?.chat?.id || "");
      if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;
      const am = d.match(/^approve_(.+)$/);
      const rm = d.match(/^reject_(.+)$/);
      if (!am && !rm) continue;
      const cbId = am ? am[1] : rm[1];
      const action = am ? "approve" : "reject";
      if (processingCb.has(cbId)) { await tgAnswer(cb.id, "⏳ Already processing..."); continue; }
      processingCb.add(cbId);
      await tgAnswer(cb.id, action === "approve" ? "✅ Approving..." : "❌ Rejecting...");
      try {
        const found = await processCallback(cbId, action);
        if (!found) await tgSend(`⚠️ cbId not found: \`${cbId}\``);
      } catch(e) { await tgSend(`❌ Error: ${e.message}`); }
      finally { processingCb.delete(cbId); }
      continue;
    }

    // Text messages
    if (update.message) {
      const text = (update.message.text || "").trim();
      const chatId = String(update.message.chat.id);
      if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;

      // Admin reply: /r <ordId> <message>
      const replyMatch = text.match(/^\/r\s+(\S+)\s+([\s\S]+)$/i);
      if (replyMatch) {
        const ok = await handleAdminReply(replyMatch[1], replyMatch[2]);
        if (!ok) await tgSend(`❌ Order \`#${replyMatch[1]}\` not found.`);
        continue;
      }

      if (text === "/help" || text === "/start") {
        await tgSend(`📖 *PRO P2P Bot v5*\n\n✅ Auto-detects new orders/deposits\n✅ Auto-forwards user chat\n\n*Commands:*\n\`/r <ordId> <msg>\` — reply to user chat\n\`#UID AMOUNT\` — credit user\n\`/allstats\` — stats`);
        continue;
      }
      if (text === "/allstats") { await handleStats(); continue; }

      const balMatch = text.match(/^#([A-Z0-9]{4,10})\s+([\d.]+)$/i);
      if (balMatch) {
        const amt = parseFloat(balMatch[2]);
        if (!amt || amt <= 0) { await tgSend("❌ Invalid amount."); continue; }
        await handleCredit(balMatch[1].toUpperCase(), amt);
        continue;
      }
    }
  }
}

// ════════════════════════════════════════════════════
// START
// ════════════════════════════════════════════════════
http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`PRO P2P Bot v5 ✅\nUptime: ${Math.floor(process.uptime())}s\nFirebase polling: every ${FB_POLL_INTERVAL}ms\n`);
}).listen(PORT, () => console.log(`Keep-alive on :${PORT}`));

console.log("🤖 PRO P2P Bot v5 — Full Backend Mode");
console.log(`📡 Telegram polling + Firebase polling (every ${FB_POLL_INTERVAL}ms)`);

// Telegram poll loop
(async () => {
  while (true) {
    try { await pollTelegram(); }
    catch(e) { console.error("TG poll error:", e.message); await sleep(3000); }
  }
})();

// Firebase poll loop
(async () => {
  while (true) {
    try { await pollFirebase(); }
    catch(e) { console.error("FB poll error:", e.message); }
    await sleep(FB_POLL_INTERVAL);
  }
})();
