/**
 * PRO P2P — Standalone Telegram Bot
 * ════════════════════════════════════════════════════
 * NO Firebase Functions needed. NO credit card.
 * Deploy FREE on Render.com or Railway.app
 * Works 24/7. Approve/Reject works even when site closed.
 * ════════════════════════════════════════════════════
 */

const https = require("https");
const http  = require("http");

// ── CONFIG ──────────────────────────────────────────
const TG_TOKEN    = "8665516559:AAGROglGKRrQl7lx4EyjoV7SkG0LHI-GJW0";
const TG_CHAT     = "8515209984";
const TG_CHAT_LOG = "64552009";
const FB_URL      = "https://my-p2p-5d11a-default-rtdb.firebaseio.com";
const FB_SECRET   = process.env.FB_SECRET || ""; // set in Render env vars
const TG_API      = `https://api.telegram.org/bot${TG_TOKEN}`;
const PORT        = process.env.PORT || 3000;

let lastUpdateId  = 0;
const processed   = new Set();

// ── HTTP HELPER ─────────────────────────────────────
function req(url, method = "GET", body = null) {
  return new Promise((resolve, reject) => {
    const u   = new URL(url);
    const lib = u.protocol === "https:" ? https : http;
    const opt = {
      hostname: u.hostname,
      path:     u.pathname + u.search,
      method,
      headers:  { "Content-Type": "application/json" }
    };
    const r = lib.request(opt, (res) => {
      let d = "";
      res.on("data", c => d += c);
      res.on("end", () => {
        try { resolve(JSON.parse(d)); }
        catch(e) { resolve({}); }
      });
    });
    r.on("error", reject);
    if (body) r.write(JSON.stringify(body));
    r.end();
  });
}

// ── FIREBASE HELPERS ────────────────────────────────
const auth = FB_SECRET ? `?auth=${FB_SECRET}` : "";

async function fbGet(path) {
  try {
    const res = await req(`${FB_URL}/${path}.json${auth}`);
    return res;
  } catch(e) { return null; }
}

async function fbSet(path, val) {
  try {
    await req(`${FB_URL}/${path}.json${auth}`, "PUT", val);
  } catch(e) { console.error("fbSet error:", e.message); }
}

async function fbPatch(path, val) {
  try {
    await req(`${FB_URL}/${path}.json${auth}`, "PATCH", val);
  } catch(e) { console.error("fbPatch error:", e.message); }
}

async function fbDelete(path) {
  try {
    await req(`${FB_URL}/${path}.json${auth}`, "DELETE");
  } catch(e) { console.error("fbDelete error:", e.message); }
}

async function fbPush(path, val) {
  try {
    await req(`${FB_URL}/${path}.json${auth}`, "POST", val);
  } catch(e) { console.error("fbPush error:", e.message); }
}

// ── TELEGRAM HELPERS ────────────────────────────────
async function tgPost(method, body) {
  try {
    return await req(`${TG_API}/${method}`, "POST", body);
  } catch(e) { return { ok: false }; }
}

const tgSend     = (text) => tgPost("sendMessage", { chat_id: TG_CHAT, text, parse_mode: "Markdown" });
const tgSendLog  = (text) => tgPost("sendMessage", { chat_id: TG_CHAT_LOG, text, parse_mode: "Markdown" });
const tgSendBoth = (text) => { tgSend(text); tgSendLog(text); };
const tgAnswer   = (id, text) => tgPost("answerCallbackQuery", { callback_query_id: id, text });
const tgEdit     = (msgId, text) => tgPost("editMessageText", { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: "Markdown" });

async function tgButtons(text, cbId) {
  const keyboard = { inline_keyboard: [[
    { text: "✅ APPROVE", callback_data: `approve_${cbId}` },
    { text: "❌ REJECT",  callback_data: `reject_${cbId}` }
  ]]};
  const d = await tgPost("sendMessage", { chat_id: TG_CHAT, text, parse_mode: "Markdown", reply_markup: keyboard });
  await tgPost("sendMessage", { chat_id: TG_CHAT_LOG, text, parse_mode: "Markdown", reply_markup: keyboard });
  return d.ok ? d.result.message_id : null;
}

// ── UTILS ───────────────────────────────────────────
function nowIST() {
  return new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
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
  if (!hist) return;
  for (const [key, val] of Object.entries(hist)) {
    if (val.hid === hid) {
      await fbPatch(`users/${firebaseUid}/history/${key}`, { status });
      return;
    }
  }
}

// ── NOTIFY CLIENT VIA FIREBASE ───────────────────────
// Client's onValue listener picks this up instantly when site is open
// When site is closed and user reopens, it fires immediately on load
async function notifyClient(firebaseUid, cbId, payload) {
  await fbSet(`users/${firebaseUid}/adminActions/${cbId}`, {
    ...payload, ts: Date.now(), processed: true
  });
}

// ════════════════════════════════════════════════════
// HANDLE DEPOSIT
// ════════════════════════════════════════════════════
async function handleDeposit(firebaseUid, userData, dep, action) {
  const t = nowIST();

  if (action === "approve") {
    const bal    = (await fbGet(`users/${firebaseUid}/balance`)) || 0;
    const newBal = parseFloat((parseFloat(bal) + dep.amt).toFixed(8));

    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, dep.hid, "COMPLETED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    await notifyClient(firebaseUid, dep.cbId, {
      action: "approve", type: "dep", amt: dep.amt, newBal
    });

    if (dep.msgId) tgEdit(dep.msgId,
      `✅ *DEPOSIT APPROVED*\n\n👤 UID: \`${userData.uid || "?"}\`\n👤 *${userData.name || "?"}*\n💰 *+${dep.amt} USDT*\n💼 New Balance: *${newBal.toFixed(2)} USDT*\n⏰ ${t} IST`
    );
    tgSendBoth(`✅ *DEPOSIT CREDITED*\n\n👤 ${userData.uid} | ${userData.name}\n➕ +${dep.amt} USDT → Balance: ${newBal.toFixed(2)} USDT`);
    console.log(`✅ DEP approved ${dep.amt} USDT → ${userData.uid}`);

  } else {
    await updateHistStatus(firebaseUid, dep.hid, "REJECTED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    await notifyClient(firebaseUid, dep.cbId, {
      action: "reject", type: "dep", amt: dep.amt
    });

    if (dep.msgId) tgEdit(dep.msgId,
      `❌ *DEPOSIT REJECTED*\n\n👤 UID: \`${userData.uid || "?"}\`\n💰 *${dep.amt} USDT*\n⏰ ${t} IST`
    );
    console.log(`❌ DEP rejected ${dep.amt} USDT → ${userData.uid}`);
  }
}

// ════════════════════════════════════════════════════
// HANDLE WITHDRAWAL
// ════════════════════════════════════════════════════
async function handleWithdrawal(firebaseUid, userData, wit, action) {
  const t = nowIST();

  if (action === "approve") {
    await updateHistStatus(firebaseUid, wit.hid, "COMPLETED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/wit`);
    await notifyClient(firebaseUid, wit.cbId, {
      action: "approve", type: "wit", amt: wit.amt
    });

    if (wit.msgId) tgEdit(wit.msgId,
      `✅ *WITHDRAWAL APPROVED*\n\n👤 UID: \`${userData.uid || "?"}\`\n💸 *${wit.amt} USDT sent*\n⏰ ${t} IST`
    );
    tgSendBoth(`✅ *WITHDRAWAL SENT*\n\n👤 ${userData.uid}\n💸 ${wit.amt} USDT`);
    console.log(`✅ WIT approved ${wit.amt} USDT → ${userData.uid}`);

  } else {
    const bal    = (await fbGet(`users/${firebaseUid}/balance`)) || 0;
    const newBal = parseFloat((parseFloat(bal) + wit.amt).toFixed(8));

    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, wit.hid, "REJECTED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/wit`);
    await notifyClient(firebaseUid, wit.cbId, {
      action: "reject", type: "wit", amt: wit.amt, newBal
    });

    if (wit.msgId) tgEdit(wit.msgId,
      `❌ *WITHDRAWAL REJECTED*\n\n👤 UID: \`${userData.uid || "?"}\`\n💰 *${wit.amt} USDT refunded*\n⏰ ${t} IST`
    );
    console.log(`❌ WIT rejected ${wit.amt} USDT refunded → ${userData.uid}`);
  }
}

// ════════════════════════════════════════════════════
// HANDLE TRADE
// ════════════════════════════════════════════════════
async function handleTrade(firebaseUid, userData, ordId, order, chatState, action) {
  const t = nowIST();
  if (!order) { console.log("Order not found:", ordId); return; }

  if (action === "approve") {
    const txId = genTxId("TRD");

    if (order.mode === "BUY") {
      const bal    = (await fbGet(`users/${firebaseUid}/balance`)) || 0;
      const newBal = parseFloat((parseFloat(bal) + order.usdt).toFixed(8));
      await fbSet(`users/${firebaseUid}/balance`, newBal);
    }

    await fbPush(`users/${firebaseUid}/history`, {
      hid:      `h_${Date.now()}_${Math.random().toString(36).slice(2,5)}`,
      type:     order.mode,
      amt:      order.usdt,
      status:   "COMPLETED",
      uid:      userData.uid || "",
      date:     t,
      isoDate:  new Date().toISOString(),
      ts:       Date.now(),
      merchant: order.merchant,
      rate:     order.rate,
      inr:      order.inr,
      txid:     txId,
      network:  "BEP20"
    });

    await fbDelete(`users/${firebaseUid}/orders/${ordId}`);
    await fbDelete(`users/${firebaseUid}/chats/${ordId}`);
    await notifyClient(firebaseUid, chatState.cbId, {
      action: "approve", type: "trade", ordId, usdt: order.usdt, mode: order.mode, txId
    });

    if (chatState.tgMsgId) tgEdit(chatState.tgMsgId,
      `✅ *TRADE COMPLETE*\n\n👤 UID: \`${userData.uid}\`\n📋 Order: \`#${order.id}\`\n🪙 *${order.usdt.toFixed(2)} USDT*\n💵 ₹${Number(order.inr).toLocaleString()}\n🔖 TxID: \`${txId}\`\n⏰ ${t} IST`
    );
    tgSendBoth(`✅ *TRADE DONE*\n\n👤 ${userData.uid} | #${order.id}\n${order.mode} ${order.usdt.toFixed(2)} USDT`);
    console.log(`✅ TRADE approved #${order.id} → ${userData.uid}`);

  } else {
    if (order.mode === "SELL") {
      const bal    = (await fbGet(`users/${firebaseUid}/balance`)) || 0;
      const newBal = parseFloat((parseFloat(bal) + order.usdt).toFixed(8));
      await fbSet(`users/${firebaseUid}/balance`, newBal);
    }

    await fbPush(`users/${firebaseUid}/history`, {
      hid:      `h_${Date.now()}_${Math.random().toString(36).slice(2,5)}`,
      type:     order.mode + "_CANCEL",
      amt:      order.usdt,
      status:   "CANCELLED",
      uid:      userData.uid || "",
      date:     t,
      isoDate:  new Date().toISOString(),
      ts:       Date.now(),
      merchant: order.merchant,
      rate:     order.rate,
      inr:      order.inr
    });

    await fbDelete(`users/${firebaseUid}/orders/${ordId}`);
    await fbDelete(`users/${firebaseUid}/chats/${ordId}`);
    await notifyClient(firebaseUid, chatState.cbId, {
      action: "reject", type: "trade", ordId, usdt: order.usdt, mode: order.mode
    });

    if (chatState.tgMsgId) tgEdit(chatState.tgMsgId,
      `❌ *TRADE REJECTED*\n\n👤 UID: \`${userData.uid}\`\n📋 Order: \`#${order.id}\`\n⏰ ${t} IST`
    );
    console.log(`❌ TRADE rejected #${order.id} → ${userData.uid}`);
  }
}

// ════════════════════════════════════════════════════
// FIND OWNER OF cbId AND PROCESS
// ════════════════════════════════════════════════════
async function processCallback(cbId, action) {
  const users = await fbGet("users");
  if (!users) return false;

  for (const [firebaseUid, userData] of Object.entries(users)) {
    // Check deposit
    const dep = userData.pendingReqs?.dep;
    if (dep && dep.cbId === cbId) {
      await handleDeposit(firebaseUid, userData, dep, action);
      return true;
    }

    // Check withdrawal
    const wit = userData.pendingReqs?.wit;
    if (wit && wit.cbId === cbId) {
      await handleWithdrawal(firebaseUid, userData, wit, action);
      return true;
    }

    // Check trade orders via chats
    const chats = userData.chats;
    if (chats) {
      for (const [ordId, chatState] of Object.entries(chats)) {
        if (chatState.cbId === cbId) {
          const order = userData.orders?.[ordId] || null;
          await handleTrade(firebaseUid, userData, ordId, order, chatState, action);
          return true;
        }
      }
    }
  }

  return false;
}

// ════════════════════════════════════════════════════
// ADMIN COMMANDS
// ════════════════════════════════════════════════════
async function handleCredit(targetUID, addAmt) {
  const users = await fbGet("users");
  if (!users) { await tgSend("❌ Database empty."); return; }

  for (const [firebaseUid, userData] of Object.entries(users)) {
    if ((userData.uid || "").toUpperCase() !== targetUID.toUpperCase()) continue;

    const bal    = (await fbGet(`users/${firebaseUid}/balance`)) || 0;
    const oldBal = parseFloat(bal);
    const newBal = parseFloat((oldBal + addAmt).toFixed(8));

    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await fbPush(`users/${firebaseUid}/history`, {
      hid:     `h_admin_${Date.now()}`,
      type:    "P2PPRO_CREDIT",
      amt:     addAmt,
      status:  "COMPLETED",
      uid:     userData.uid,
      date:    nowIST(),
      isoDate: new Date().toISOString(),
      ts:      Date.now(),
      sender:  "P2PPRO",
      note:    `P2PPRO sended you ${addAmt} USDT`,
      network: "INTERNAL",
      txid:    `p2ppro_${Date.now()}`
    });

    await tgSendBoth(
      `✅ *BALANCE CREDITED*\n\n👤 UID: \`${targetUID}\`\n👤 *${userData.name || "?"}*\n📧 ${userData.email || "—"}\n➕ +${addAmt} USDT\n💰 Old: ${oldBal.toFixed(2)} → New: ${newBal.toFixed(2)} USDT`
    );
    return;
  }
  await tgSend(`❌ UID \`${targetUID}\` not found.`);
}

async function handleStats() {
  const users = await fbGet("users");
  if (!users) { await tgSend("❌ No data."); return; }
  let userCount = 0, totalBal = 0, pendingDep = 0, pendingWit = 0, activeOrders = 0;
  for (const [, u] of Object.entries(users)) {
    userCount++;
    totalBal += parseFloat(u.balance || 0);
    if (u.pendingReqs?.dep) pendingDep++;
    if (u.pendingReqs?.wit) pendingWit++;
    if (u.orders) activeOrders += Object.keys(u.orders).length;
  }
  await tgSend(
    `📊 *SITE STATS*\n\n👥 Users: *${userCount}*\n💰 Total USDT: *${totalBal.toFixed(2)}*\n📥 Pending Deposits: *${pendingDep}*\n💼 Pending Withdrawals: *${pendingWit}*\n🤝 Active Orders: *${activeOrders}*\n⏰ ${nowIST()} IST`
  );
}

async function handleTxLookup(txid) {
  const users = await fbGet("users");
  if (!users) { await tgSend("❌ No data."); return; }

  for (const [firebaseUid, userData] of Object.entries(users)) {
    // Search history
    const hist = userData.history;
    if (hist) {
      for (const [, h] of Object.entries(hist)) {
        if ((h.txid || "").toUpperCase() === txid || (h.hid || "").toUpperCase() === txid) {
          const st     = (h.status || "PENDING").toUpperCase();
          const stIcon = { COMPLETED:"✅", REJECTED:"❌", CANCELLED:"🚫", PENDING:"⏳" }[st] || "⏳";
          let msg = `🔍 *TRANSACTION*\n\n📋 Type: *${h.type}*\n💰 *${parseFloat(h.amt).toFixed(2)} USDT*\n👤 \`${h.uid}\` | *${userData.name || "?"}*\n📅 ${h.date || "—"}\n\n${stIcon} *${st}*`;
          if (h.txid)     msg += `\n🔖 \`${h.txid}\``;
          if (h.merchant) msg += `\n🏪 ${h.merchant}`;
          if (h.inr)      msg += `\n💵 ₹${Number(h.inr).toLocaleString()}`;

          if (st === "PENDING") {
            const pr = userData.pendingReqs;
            const pending = (pr?.dep?.hid === h.hid) ? pr.dep : (pr?.wit?.hid === h.hid) ? pr.wit : null;
            if (pending) { await tgButtons(msg + "\n\n⚡ *Re-send approval:*", pending.cbId); return; }
          }
          await tgSend(msg);
          return;
        }
      }
    }

    // Search active orders
    const ordId = txid.replace("ORD-", "");
    const order = userData.orders?.[ordId];
    if (order) {
      const cs  = userData.chats?.[ordId] || {};
      let msg = `🔍 *ACTIVE ORDER*\n\n📋 \`#${order.id}\`\n👤 \`${userData.uid}\` | *${userData.name}*\n💹 *${order.mode}*\n🪙 ${order.usdt.toFixed(2)} USDT | ₹${Number(order.inr).toLocaleString()}\n📊 Stage: ${cs.dealStage || "init"}\n\n⏳ *PENDING*`;
      if (cs.cbId && cs.dealStage === "pending_verify") {
        await tgButtons(msg + "\n\n⚡ *Approval buttons:*", cs.cbId);
      } else {
        await tgSend(msg);
      }
      return;
    }
  }
  await tgSend(`❌ Not found: \`${txid}\`\n\nTry: \`#DEP-XXXXXX\` \`#WIT-XXXXXX\` \`#ORD-123456789\``);
}

// ════════════════════════════════════════════════════
// MAIN POLL LOOP — checks Telegram every 2 seconds
// ════════════════════════════════════════════════════
async function poll() {
  try {
    const data = await req(`${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=2&allowed_updates=["message","callback_query"]`);
    if (!data.ok || !data.result?.length) return;

    for (const update of data.result) {
      lastUpdateId = update.update_id;
      if (processed.has(lastUpdateId)) continue;
      processed.add(lastUpdateId);
      if (processed.size > 500) {
        const arr = [...processed];
        arr.slice(0, 250).forEach(x => processed.delete(x));
      }

      // ── CALLBACK: Approve / Reject button ──────────
      if (update.callback_query) {
        const cb     = update.callback_query;
        const d      = cb.data || "";
        const chatId = String(cb.message?.chat?.id || "");

        if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;

        const am = d.match(/^approve_(.+)$/);
        const rm = d.match(/^reject_(.+)$/);
        if (!am && !rm) continue;

        const cbId   = am ? am[1] : rm[1];
        const action = am ? "approve" : "reject";

        await tgAnswer(cb.id, action === "approve" ? "✅ Processing..." : "❌ Processing...");
        console.log(`${action.toUpperCase()} callback: ${cbId}`);

        const found = await processCallback(cbId, action);
        if (!found) {
          await tgAnswer(cb.id, "⚠️ Already processed");
          await tgSend(`⚠️ *Not found or already processed*\n\ncbId: \`${cbId}\``);
        }
        continue;
      }

      // ── TEXT MESSAGES ───────────────────────────────
      if (update.message) {
        const msg    = update.message;
        const text   = (msg.text || "").trim();
        const chatId = String(msg.chat.id);

        if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;

        // /help or /start
        if (text === "/help" || text === "/start" || text === "/txhelp") {
          await tgSend(
            `📖 *PRO P2P BOT — 24/7 SERVER*\n\n` +
            `✅ Approve/Reject work even when site is closed\n\n` +
            `*Lookup:*\n` +
            `\`#DEP-XXXXXX\` — deposit\n` +
            `\`#WIT-XXXXXX\` — withdrawal\n` +
            `\`#ORD-123456789\` — active order\n\n` +
            `*Credit:*\n\`#UID AMOUNT\`\n\n` +
            `*Stats:* \`/allstats\``
          );
          continue;
        }

        // /allstats
        if (text === "/allstats") {
          await handleStats();
          continue;
        }

        // #TXID lookup
        const txMatch = text.match(/^#([A-Z0-9_\-]{4,20})$/i);
        if (txMatch) {
          const txid = txMatch[1].toUpperCase();
          await tgSend(`🔍 Looking up \`${txid}\`...`);
          await handleTxLookup(txid);
          continue;
        }

        // #UID AMOUNT credit
        const balMatch = text.match(/^#([A-Z0-9]{4,10})\s+([\d.]+)$/i);
        if (balMatch) {
          const uid = balMatch[1].toUpperCase();
          const amt = parseFloat(balMatch[2]);
          if (!amt || amt <= 0) { await tgSend("❌ Invalid amount."); continue; }
          await tgSend(`⏳ Crediting \`${uid}\` with ${amt} USDT...`);
          await handleCredit(uid, amt);
          continue;
        }
      }
    }
  } catch(e) {
    console.error("Poll error:", e.message);
  }
}

// ════════════════════════════════════════════════════
// START
// ════════════════════════════════════════════════════

// Keep-alive HTTP server (required by Render free tier)
http.createServer((req, res) => {
  res.writeHead(200);
  res.end("PRO P2P Bot running ✅");
}).listen(PORT, () => {
  console.log(`Keep-alive server on port ${PORT}`);
});

// Start polling
console.log("🤖 PRO P2P Bot started — polling Telegram every 2s");
console.log(`Firebase: ${FB_URL}`);

setInterval(poll, 2000);
poll(); // run immediately on start
