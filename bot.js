/**
 * PRO P2P — bot.js v6
 *
 * HOW IT WORKS WITH index.html:
 * ─────────────────────────────
 * index.html writes to Firebase:
 *   pendingActions/<cbId>  → deposit / withdraw / trade context
 *   users/<fuid>/pendingReqs/dep|wit  → persisted pending state
 *   users/<fuid>/chats/<ordId>        → chat + dealStage
 *   users/<fuid>/orders/<ordId>       → order data
 *
 * bot.js reads from Firebase every 2s:
 *   - Watches pendingActions/ for new requests → sends TG buttons
 *   - Watches callback_queries → APPROVE/REJECT → writes result to
 *     users/<fuid>/botResults/<key>  ← index.html listens here live
 *   - Forwards admin TG replies → users/<fuid>/botResults/<key> as chat_message
 *   - Handles /allstats, #UID AMOUNT credit commands
 *
 * DEPLOY ON: Render (Web Service, Node 18+)
 * ENV VARS:
 *   TG_TOKEN     = bot token
 *   TG_CHAT      = primary admin chat id
 *   TG_CHAT_LOG  = log chat id
 *   FB_URL       = https://my-p2p-5d11a-default-rtdb.firebaseio.com
 *   FB_SECRET    = firebase database secret (optional)
 *   PORT         = auto-set by Render
 */

const https = require("https");
const http  = require("http");

// ── CONFIG ───────────────────────────────────────────
const TG_TOKEN    = process.env.TG_TOKEN    || "7284611660:AAGb49YDYdH6my_WT5DS8jBhWsGGmeAOPkI";
const TG_CHAT     = process.env.TG_CHAT     || "8515209984";
const TG_CHAT_LOG = process.env.TG_CHAT_LOG || "64552009";
const FB_URL      = process.env.FB_URL      || "https://my-p2p-5d11a-default-rtdb.firebaseio.com";
const FB_SECRET   = process.env.FB_SECRET   || "";
const TG_API      = `https://api.telegram.org/bot${TG_TOKEN}`;
const PORT        = process.env.PORT || 3000;

// ── STATE ────────────────────────────────────────────
let lastUpdateId      = 0;
const seenUpdates     = new Set();
const processingCb    = new Set();
const seenActions     = new Set();   // cbIds already sent to TG
const seenAdminMsgs   = new Set();   // msgId already forwarded to Firebase
const sleep = ms => new Promise(r => setTimeout(r, ms));

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
    const r = lib.request(opt, res => {
      let d = "";
      res.on("data", c => d += c);
      res.on("end", () => { try { resolve(JSON.parse(d)); } catch(e) { resolve({}); } });
    });
    r.on("timeout", () => { r.destroy(); reject(new Error("timeout")); });
    r.on("error", reject);
    if (body) r.write(JSON.stringify(body));
    r.end();
  });
}

// ── FIREBASE ─────────────────────────────────────────
const authQ = FB_SECRET ? `?auth=${FB_SECRET}` : "";
const fbGet    = async p => { try { return await httpReq(`${FB_URL}/${p}.json${authQ}`, "GET"); } catch(e) { return null; } };
const fbSet    = async (p,v) => { try { await httpReq(`${FB_URL}/${p}.json${authQ}`, "PUT",    v); } catch(e) {} };
const fbPatch  = async (p,v) => { try { await httpReq(`${FB_URL}/${p}.json${authQ}`, "PATCH",  v); } catch(e) {} };
const fbDelete = async p     => { try { await httpReq(`${FB_URL}/${p}.json${authQ}`, "DELETE"); } catch(e) {} };
const fbPush   = async (p,v) => { try { return await httpReq(`${FB_URL}/${p}.json${authQ}`, "POST", v); } catch(e) { return null; } };

// ── TELEGRAM ─────────────────────────────────────────
const tgPost     = async (m, b) => { try { return await httpReq(`${TG_API}/${m}`, "POST", b, 15_000); } catch(e) { return {ok:false}; } };
const tgSend     = t => tgPost("sendMessage", { chat_id: TG_CHAT,     text: t, parse_mode: "Markdown" });
const tgSendLog  = t => tgPost("sendMessage", { chat_id: TG_CHAT_LOG, text: t, parse_mode: "Markdown" });
const tgSendBoth = t => { tgSend(t); tgSendLog(t); };
const tgAnswer   = (id, t) => tgPost("answerCallbackQuery", { callback_query_id: id, text: t });
const tgEditMain = (mid, t) => tgPost("editMessageText", { chat_id: TG_CHAT, message_id: mid, text: t, parse_mode: "Markdown" });

async function tgButtons(text, cbId) {
  const kb = { inline_keyboard: [[
    { text: "✅ APPROVE", callback_data: `approve_${cbId}` },
    { text: "❌ REJECT",  callback_data: `reject_${cbId}`  }
  ]]};
  // Send to BOTH chats with buttons
  const d = await tgPost("sendMessage", { chat_id: TG_CHAT,     text, parse_mode: "Markdown", reply_markup: kb });
           await tgPost("sendMessage", { chat_id: TG_CHAT_LOG, text, parse_mode: "Markdown", reply_markup: kb });
  return d.ok ? d.result.message_id : null;
}

// ── UTILS ────────────────────────────────────────────
const nowIST = () => new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });

// Safe Firebase key (no . # $ [ ] / )
const safeKey = s => String(s).replace(/[.#$[\]/]/g, "_");

// Write result to user's botResults — index.html listens to this path
async function writeResult(firebaseUid, key, payload) {
  const k = safeKey(key);
  await fbSet(`users/${firebaseUid}/botResults/${k}`, {
    ...payload,
    ts: Date.now(),
    processed: false
  });
}

// Update history entry status by hid
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

// ════════════════════════════════════════════════════════
// POLL pendingActions/ — index.html writes here when user
// initiates deposit / withdrawal / trade-claim
// Bot sends TG buttons the FIRST TIME it sees each cbId
// ════════════════════════════════════════════════════════
async function pollPendingActions() {
  const actions = await fbGet("pendingActions");
  if (!actions || typeof actions !== "object") return;

  for (const [cbId, ctx] of Object.entries(actions)) {
    if (!ctx || ctx.botSent || seenActions.has(cbId)) continue;

    seenActions.add(cbId);
    const t = nowIST();

    try {
      let text = "";
      let msgId = null;

      if (ctx.type === "deposit") {
        text =
          `📥 *DEPOSIT REQUEST*\n\n` +
          `👤 UID: \`${ctx.uid || "?"}\`\n` +
          `👤 User: *${ctx.userName || "Guest"}*\n` +
          `📧 Email: \`${ctx.userEmail || "—"}\`\n` +
          `💰 Amount: *${ctx.amt} USDT*\n` +
          `🌐 Network: ${ctx.network || "BEP20"}\n` +
          `📍 Address: \`${ctx.address || "—"}\`\n` +
          `🔗 TxID: \`${ctx.txid || "—"}\`\n` +
          `⏰ ${t} IST\n\n` +
          `✅ APPROVE = credit USDT\n❌ REJECT = decline`;
        msgId = await tgButtons(text, cbId);

      } else if (ctx.type === "withdraw") {
        text =
          `💼 *WITHDRAWAL REQUEST*\n\n` +
          `👤 UID: \`${ctx.uid || "?"}\`\n` +
          `👤 User: *${ctx.userName || "Guest"}*\n` +
          `📧 Email: \`${ctx.userEmail || "—"}\`\n` +
          `💰 Amount: *${ctx.amt} USDT*\n` +
          `🌐 Network: ${ctx.network || "BEP20"}\n` +
          `📤 Address: \`${ctx.address || "—"}\`\n` +
          `🔗 TxID: \`${ctx.txid || "—"}\`\n` +
          `⏰ ${t} IST\n\n` +
          `✅ APPROVE = send funds\n❌ REJECT = refund`;
        msgId = await tgButtons(text, cbId);

      } else if (ctx.type === "trade") {
        const isBuy = ctx.orderMode === "BUY";
        const headline = isBuy
          ? `💸 *P2P BUY — PAYMENT CLAIMED*`
          : `🔓 *P2P SELL — USDT RELEASE REQUEST*`;
        text =
          `${headline}\n\n` +
          `👤 UID: \`${ctx.uid || "?"}\`\n` +
          `👤 User: *${ctx.userName || "Guest"}*\n` +
          `📧 Email: \`${ctx.userEmail || "—"}\`\n` +
          `📋 Order: \`#${ctx.orderId}\`\n` +
          `🏪 Merchant: *${ctx.merchant || "—"}*\n` +
          (isBuy
            ? `💵 INR Paid: *₹${Number(ctx.inr).toLocaleString()}*\n` +
              `🪙 USDT: *${Number(ctx.usdt).toFixed(2)} USDT*\n`
            : `🪙 USDT: *${Number(ctx.usdt).toFixed(2)} USDT*\n` +
              `💵 INR: *₹${Number(ctx.inr).toLocaleString()}*\n`
          ) +
          `⏰ ${t} IST\n\n` +
          `✅ APPROVE = TRADE COMPLETE\n` +
          `❌ REJECT = ${isBuy ? "TRADE CANCELED" : "REFUND USDT"}\n` +
          `💬 *Reply to this message* to chat with user`;
        msgId = await tgButtons(text, cbId);
      }

      // Mark as sent in Firebase so we don't re-send on next poll
      await fbPatch(`pendingActions/${cbId}`, { botSent: true, msgId: msgId || null });
      console.log(`📨 Sent TG buttons for ${ctx.type} cbId=${cbId} → uid=${ctx.uid}`);

    } catch(e) {
      // Remove from seenActions so we retry next poll
      seenActions.delete(cbId);
      console.error(`⚠️ pollPendingActions error for ${cbId}:`, e.message);
    }
  }
}

// ════════════════════════════════════════════════════════
// HANDLE APPROVE / REJECT
// Writes result to users/<fuid>/botResults/<key>
// index.html's subscribeBotResults() picks this up in real-time
// ════════════════════════════════════════════════════════
async function handleApprove(cbId, ctx) {
  const { type, firebaseUid, hid, amt, orderId, orderMode, usdt, inr, merchant, rate, msgId } = ctx;
  const t = nowIST();

  if (type === "deposit") {
    // Credit balance
    const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + parseFloat(amt)).toFixed(8));
    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, hid, "COMPLETED");
    // Clean up pendingReqs
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    // Clean up pendingActions
    await fbDelete(`pendingActions/${cbId}`);
    // Notify frontend
    await writeResult(firebaseUid, `dep_${cbId}`, {
      type: "deposit", action: "approve", cbId, hid, amt: parseFloat(amt), newBalance: newBal
    });
    if (msgId) tgEditMain(msgId, `✅ *DEPOSIT APPROVED*\n\nUID: \`${ctx.uid}\`\nAmount: *${amt} USDT*\nNew Balance: *${newBal.toFixed(2)} USDT*\n⏰ ${t} IST`);
    tgSendLog(`✅ DEP approved +${amt} USDT → ${ctx.uid}`);
    console.log(`✅ DEP approved ${amt} → ${ctx.uid}`);

  } else if (type === "withdraw") {
    await updateHistStatus(firebaseUid, hid, "COMPLETED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/wit`);
    await fbDelete(`pendingActions/${cbId}`);
    await writeResult(firebaseUid, `wit_${cbId}`, {
      type: "withdraw", action: "approve", cbId, hid, amt: parseFloat(amt)
    });
    if (msgId) tgEditMain(msgId, `✅ *WITHDRAWAL SENT*\n\nUID: \`${ctx.uid}\`\nAmount: *${amt} USDT*\n⏰ ${t} IST`);
    tgSendLog(`✅ WIT approved ${amt} USDT → ${ctx.uid}`);
    console.log(`✅ WIT approved ${amt} → ${ctx.uid}`);

  } else if (type === "trade") {
    const txId = `0x${[...Array(64)].map(()=>"0123456789abcdef"[Math.floor(Math.random()*16)]).join("")}`;
    // Credit balance for BUY
    if (orderMode === "BUY") {
      const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
      await fbSet(`users/${firebaseUid}/balance`, parseFloat((bal + parseFloat(usdt)).toFixed(8)));
    }
    // Add to history
    await fbPush(`users/${firebaseUid}/history`, {
      hid: `h_${Date.now()}_${Math.random().toString(36).slice(2,5)}`,
      type: orderMode, amt: parseFloat(usdt), status: "COMPLETED",
      uid: ctx.uid, date: t, isoDate: new Date().toISOString(), ts: Date.now(),
      merchant, rate, inr, txid: txId, network: "BEP20"
    });
    // Remove order + chat
    await fbDelete(`users/${firebaseUid}/orders/${orderId}`);
    await fbDelete(`users/${firebaseUid}/chats/${orderId}`);
    await fbDelete(`pendingActions/${cbId}`);
    // Notify frontend
    await writeResult(firebaseUid, `trd_${cbId}`, {
      type: "trade", action: "approve", cbId, orderId: String(orderId), orderMode, usdt: parseFloat(usdt)
    });
    if (msgId) tgEditMain(msgId, `✅ *TRADE COMPLETE*\n\nUID: \`${ctx.uid}\`\nOrder: \`#${orderId}\`\nUSDT: *${Number(usdt).toFixed(2)}*\n⏰ ${t} IST`);
    tgSendLog(`✅ TRADE done #${orderId} → ${ctx.uid}`);
    console.log(`✅ TRADE approved #${orderId} → ${ctx.uid}`);
  }
}

async function handleReject(cbId, ctx) {
  const { type, firebaseUid, hid, amt, orderId, orderMode, usdt, msgId } = ctx;
  const t = nowIST();

  if (type === "deposit") {
    await updateHistStatus(firebaseUid, hid, "REJECTED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    await fbDelete(`pendingActions/${cbId}`);
    await writeResult(firebaseUid, `dep_${cbId}`, {
      type: "deposit", action: "reject", cbId, hid, amt: parseFloat(amt)
    });
    if (msgId) tgEditMain(msgId, `❌ *DEPOSIT REJECTED*\n\nUID: \`${ctx.uid}\`\nAmount: *${amt} USDT*\n⏰ ${t} IST`);
    console.log(`❌ DEP rejected ${amt} → ${ctx.uid}`);

  } else if (type === "withdraw") {
    // Refund balance
    const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + parseFloat(amt)).toFixed(8));
    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, hid, "REJECTED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/wit`);
    await fbDelete(`pendingActions/${cbId}`);
    await writeResult(firebaseUid, `wit_${cbId}`, {
      type: "withdraw", action: "reject", cbId, hid, amt: parseFloat(amt), newBalance: newBal
    });
    if (msgId) tgEditMain(msgId, `❌ *WITHDRAWAL REJECTED*\n\nUID: \`${ctx.uid}\`\nAmount: *${amt} USDT*\nFunds returned ✅\n⏰ ${t} IST`);
    console.log(`❌ WIT rejected ${amt} → ${ctx.uid} — refunded`);

  } else if (type === "trade") {
    const t2 = nowIST();
    // Refund USDT for SELL
    if (orderMode === "SELL") {
      const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
      await fbSet(`users/${firebaseUid}/balance`, parseFloat((bal + parseFloat(usdt)).toFixed(8)));
    }
    // Cancel history entry
    await fbPush(`users/${firebaseUid}/history`, {
      hid: `h_${Date.now()}_${Math.random().toString(36).slice(2,5)}`,
      type: orderMode + "_CANCEL", amt: parseFloat(usdt), status: "CANCELLED",
      uid: ctx.uid, date: t2, isoDate: new Date().toISOString(), ts: Date.now(),
      merchant: ctx.merchant, rate: ctx.rate, inr: ctx.inr
    });
    await fbDelete(`users/${firebaseUid}/orders/${orderId}`);
    await fbDelete(`users/${firebaseUid}/chats/${orderId}`);
    await fbDelete(`pendingActions/${cbId}`);
    await writeResult(firebaseUid, `trd_${cbId}`, {
      type: "trade", action: "reject", cbId, orderId: String(orderId), orderMode, usdt: parseFloat(usdt)
    });
    if (msgId) tgEditMain(msgId, `❌ *TRADE REJECTED*\n\nUID: \`${ctx.uid}\`\nOrder: \`#${orderId}\`\n⏰ ${t} IST`);
    console.log(`❌ TRADE rejected #${orderId} → ${ctx.uid}`);
  }
}

// Find context by cbId — checks pendingActions first, then users
async function findCtxByCbId(cbId) {
  // Primary: pendingActions (index.html writes here)
  const ctx = await fbGet(`pendingActions/${cbId}`);
  if (ctx && ctx.firebaseUid && ctx.type) return ctx;

  // Fallback: scan users (for old-style or re-opened sessions)
  const users = await fbGet("users");
  if (!users || typeof users !== "object") return null;

  for (const [firebaseUid, userData] of Object.entries(users)) {
    if (!userData) continue;
    // Deposit
    const dep = userData.pendingReqs?.dep;
    if (dep?.cbId === cbId) return { ...dep, type: "deposit", firebaseUid };
    // Withdrawal
    const wit = userData.pendingReqs?.wit;
    if (wit?.cbId === cbId) return { ...wit, type: "withdraw", firebaseUid };
    // Trade chats
    const chats = userData.chats || {};
    for (const [ordId, cs] of Object.entries(chats)) {
      if (!cs) continue;
      if (cs.cbId === cbId) {
        const order = userData.orders?.[ordId] || {};
        return {
          type: "trade", firebaseUid,
          cbId, orderId: ordId, orderMode: order.mode,
          usdt: order.usdt, inr: order.inr,
          merchant: order.merchant, rate: order.rate,
          uid: userData.uid, msgId: cs.tgMsgId
        };
      }
    }
  }
  return null;
}

// ════════════════════════════════════════════════════════
// FORWARD ADMIN TG REPLIES → Firebase botResults
// When admin types in the bot chat referencing an order ID or replies
// to a trade message, forward it as a chat_message to the user
// ════════════════════════════════════════════════════════
async function forwardAdminReply(msgText, replyToMsgId) {
  if (!replyToMsgId && !msgText) return;

  const users = await fbGet("users");
  if (!users || typeof users !== "object") return;

  for (const [firebaseUid, userData] of Object.entries(users)) {
    if (!userData?.chats) continue;
    for (const [ordId, cs] of Object.entries(userData.chats)) {
      if (!cs) continue;
      const matches =
        (replyToMsgId && cs.tgMsgId && Number(replyToMsgId) === Number(cs.tgMsgId)) ||
        msgText.includes(String(ordId)) ||
        msgText.includes(String(userData.uid || ""));

      if (matches) {
        const msgKey = `chat_${Date.now()}_${Math.random().toString(36).slice(2,5)}`;
        const seen = `${ordId}:${msgKey}`;
        if (seenAdminMsgs.has(seen)) continue;
        seenAdminMsgs.add(seen);

        await writeResult(firebaseUid, msgKey, {
          type: "chat_message",
          orderId: String(ordId),
          senderName: "Admin",
          text: msgText,
          tgMsgId: null
        });
        console.log(`💬 Admin reply forwarded → order #${ordId} uid=${userData.uid}`);
        return; // Stop after first match
      }
    }
  }
}

// ════════════════════════════════════════════════════════
// ADMIN COMMANDS
// ════════════════════════════════════════════════════════
async function handleCredit(targetUID, addAmt) {
  const users = await fbGet("users");
  if (!users) { await tgSend("❌ DB empty."); return; }
  for (const [firebaseUid, u] of Object.entries(users)) {
    if (!u || (u.uid || "").toUpperCase() !== targetUID.toUpperCase()) continue;
    const bal = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + addAmt).toFixed(8));
    await fbSet(`users/${firebaseUid}/balance`, newBal);
    const hid = `h_admin_${Date.now()}_${Math.random().toString(36).slice(2,5)}`;
    await fbPush(`users/${firebaseUid}/history`, {
      hid, type: "P2PPRO_CREDIT", amt: addAmt, status: "COMPLETED",
      uid: u.uid, date: nowIST(), isoDate: new Date().toISOString(), ts: Date.now(),
      sender: "P2PPRO", note: `P2PPRO sended you ${addAmt} USDT`,
      network: "INTERNAL", txid: `p2ppro_${Date.now()}`
    });
    // Notify frontend live
    await writeResult(firebaseUid, `credit_${Date.now()}`, {
      type: "credit", action: "credit", amt: addAmt, newBalance: newBal,
      note: `P2PPRO sended you ${addAmt} USDT`
    });
    await tgSendBoth(`✅ *CREDITED*\n\n👤 UID: \`${targetUID}\`\n👤 User: *${u.name || "—"}*\n📧 ${u.email || "—"}\n➕ *+${addAmt} USDT*\n💰 New: *${newBal.toFixed(2)} USDT*`);
    console.log(`💳 Credit +${addAmt} → ${targetUID}`);
    return;
  }
  await tgSend(`❌ UID \`${targetUID}\` not found.`);
}

async function handleStats() {
  const users = await fbGet("users");
  if (!users) { await tgSend("❌ No data."); return; }
  let n = 0, totalBal = 0, depPending = 0, witPending = 0, activeOrders = 0;
  for (const [, u] of Object.entries(users)) {
    if (!u) continue;
    n++; totalBal += parseFloat(u.balance || 0);
    if (u.pendingReqs?.dep) depPending++;
    if (u.pendingReqs?.wit) witPending++;
    if (u.orders) activeOrders += Object.keys(u.orders).length;
  }
  const pendingActs = await fbGet("pendingActions");
  const pendingCnt = pendingActs ? Object.keys(pendingActs).length : 0;
  await tgSend(
    `📊 *PRO P2P STATS*\n\n` +
    `👥 Users: *${n}*\n` +
    `💰 Total Balance: *${totalBal.toFixed(2)} USDT*\n` +
    `📥 Pending Deposits: *${depPending}*\n` +
    `💼 Pending Withdrawals: *${witPending}*\n` +
    `🤝 Active Orders: *${activeOrders}*\n` +
    `⏳ Pending Actions: *${pendingCnt}*\n` +
    `⏰ ${nowIST()} IST`
  );
}

// ════════════════════════════════════════════════════════
// TELEGRAM POLL
// ════════════════════════════════════════════════════════
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
    // Trim seenUpdates to avoid memory leak
    if (seenUpdates.size > 2000) {
      const arr = [...seenUpdates].sort((a,b) => a-b);
      arr.slice(0, 1000).forEach(x => seenUpdates.delete(x));
    }

    // ── CALLBACK QUERY (approve/reject buttons) ──
    if (update.callback_query) {
      const cb = update.callback_query;
      const d  = cb.data || "";
      const chatId = String(cb.message?.chat?.id || "");
      // Accept from BOTH admin chats
      if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;

      const am = d.match(/^approve_(.+)$/);
      const rm = d.match(/^reject_(.+)$/);
      if (!am && !rm) continue;

      const cbId   = am ? am[1] : rm[1];
      const action = am ? "approve" : "reject";

      if (processingCb.has(cbId)) {
        await tgAnswer(cb.id, "⏳ Already processing...");
        continue;
      }
      processingCb.add(cbId);
      await tgAnswer(cb.id, action === "approve" ? "✅ Processing approval..." : "❌ Processing rejection...");

      try {
        const ctx = await findCtxByCbId(cbId);
        if (!ctx) {
          await tgSend(`⚠️ Request not found for cbId: \`${cbId}\`\nMay already be processed.`);
        } else if (action === "approve") {
          await handleApprove(cbId, ctx);
        } else {
          await handleReject(cbId, ctx);
        }
      } catch(e) {
        console.error(`❌ CB error ${cbId}:`, e.message);
        await tgSend(`❌ Error processing \`${cbId}\`: ${e.message}`);
      } finally {
        processingCb.delete(cbId);
      }
      continue;
    }

    // ── TEXT MESSAGES ──
    if (update.message) {
      const msg    = update.message;
      const text   = (msg.text || "").trim();
      const chatId = String(msg.chat.id);
      if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;
      if (!text) continue;

      // /help or /start
      if (text === "/help" || text === "/start") {
        await tgSend(
          `📖 *PRO P2P Bot v6*\n\n` +
          `✅ Auto-processes approve/reject\n` +
          `✅ Works even when site is closed\n` +
          `✅ Supports multiple simultaneous orders\n\n` +
          `*Commands:*\n` +
          `\`#UID AMOUNT\` — credit user balance\n` +
          `\`/allstats\` — view platform stats\n\n` +
          `*Reply to any trade message* to chat with the user directly.`
        );
        continue;
      }

      // /allstats
      if (text === "/allstats") {
        await handleStats();
        continue;
      }

      // #UID AMOUNT — credit command
      const balMatch = text.match(/^#([A-Z0-9]{4,10})\s+([\d.]+)$/i);
      if (balMatch) {
        const amt = parseFloat(balMatch[2]);
        if (!amt || amt <= 0) { await tgSend("❌ Invalid amount.\n\nUsage: `#UID AMOUNT`"); continue; }
        await handleCredit(balMatch[1].toUpperCase(), amt);
        continue;
      }

      // Bot/command messages — skip
      if (text.startsWith("/") || msg.from?.is_bot) continue;

      // Any other admin reply — try to forward to relevant trade chat
      const replyToMsgId = msg.reply_to_message?.message_id || null;
      await forwardAdminReply(text, replyToMsgId);
    }
  }
}

// ════════════════════════════════════════════════════════
// MAIN LOOPS
// ════════════════════════════════════════════════════════
http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(
    `PRO P2P Bot v6 ✅\n` +
    `Uptime: ${Math.floor(process.uptime())}s\n` +
    `TG polling: active\n` +
    `Firebase polling: every 2000ms\n`
  );
}).listen(PORT, () => console.log(`🌐 Health check on :${PORT}`));

console.log("🤖 PRO P2P Bot v6 — Starting");
console.log(`📡 TG_CHAT=${TG_CHAT} | TG_CHAT_LOG=${TG_CHAT_LOG}`);
console.log(`🔥 Firebase: ${FB_URL}`);

// Telegram long-poll loop
(async () => {
  console.log("📨 Telegram poller started");
  while (true) {
    try { await pollTelegram(); }
    catch(e) { console.error("TG poll error:", e.message); await sleep(3000); }
  }
})();

// Firebase pendingActions watcher loop (every 2s)
(async () => {
  console.log("🔥 Firebase pendingActions watcher started");
  while (true) {
    try { await pollPendingActions(); }
    catch(e) { console.error("FB poll error:", e.message); }
    await sleep(2000);
  }
})();
