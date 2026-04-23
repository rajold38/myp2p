/**
 * PRO P2P — Telegram Bot (FIXED v4)
 * KEY FIX: processCallback always does a fresh Firebase scan — never relies
 * solely on the in-memory index. This means approve/reject always works even
 * for Order 1 after Order 2, 3, 4... are created.
 */

const https = require("https");
const http  = require("http");

// ── CONFIG ──────────────────────────────────────────
const TG_TOKEN    = "8665516559:AAGROglGKRrQl7lx4EyjoV7SkG0LHI-GJW0";
const TG_CHAT     = "8515209984";
const TG_CHAT_LOG = "64552009";
const FB_URL      = "https://my-p2p-5d11a-default-rtdb.firebaseio.com";
const FB_SECRET   = process.env.FB_SECRET || "";
const TG_API      = `https://api.telegram.org/bot${TG_TOKEN}`;
const PORT        = process.env.PORT || 3000;

// ── STATE ───────────────────────────────────────────
let lastUpdateId   = 0;
const seenUpdates  = new Set();
const processingCb = new Set();

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── HTTP HELPER ─────────────────────────────────────
function req(url, method = "GET", body = null, timeoutMs = 35_000) {
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
      res.on("end", () => {
        try { resolve(JSON.parse(d)); }
        catch(e) { resolve({}); }
      });
    });
    r.on("timeout", () => { r.destroy(); reject(new Error("Request timeout")); });
    r.on("error", reject);
    if (body) r.write(JSON.stringify(body));
    r.end();
  });
}

// ── FIREBASE HELPERS ─────────────────────────────────
const auth = FB_SECRET ? `?auth=${FB_SECRET}` : "";

async function fbGet(path) {
  try { return await req(`${FB_URL}/${path}.json${auth}`, "GET", null, 10_000); }
  catch(e) { console.error(`fbGet(${path}):`, e.message); return null; }
}
async function fbSet(path, val) {
  try { await req(`${FB_URL}/${path}.json${auth}`, "PUT", val, 10_000); }
  catch(e) { console.error(`fbSet(${path}):`, e.message); }
}
async function fbPatch(path, val) {
  try { await req(`${FB_URL}/${path}.json${auth}`, "PATCH", val, 10_000); }
  catch(e) { console.error(`fbPatch(${path}):`, e.message); }
}
async function fbDelete(path) {
  try { await req(`${FB_URL}/${path}.json${auth}`, "DELETE", null, 10_000); }
  catch(e) { console.error(`fbDelete(${path}):`, e.message); }
}
async function fbPush(path, val) {
  try { return await req(`${FB_URL}/${path}.json${auth}`, "POST", val, 10_000); }
  catch(e) { console.error(`fbPush(${path}):`, e.message); return null; }
}

// ── TELEGRAM HELPERS ─────────────────────────────────
async function tgPost(method, body) {
  try { return await req(`${TG_API}/${method}`, "POST", body, 15_000); }
  catch(e) { return { ok: false }; }
}
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

// ── UTILS ─────────────────────────────────────────────
function nowIST() {
  return new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
}
function genTxId(prefix) {
  const c = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let id = "";
  for (let i = 0; i < 6; i++) id += c[Math.floor(Math.random() * c.length)];
  return `${prefix}-${id}`;
}

// ── UPDATE HISTORY STATUS ─────────────────────────────
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

// ── NOTIFY CLIENT VIA FIREBASE ────────────────────────
// Client has onValue listener on adminActions — this triggers it instantly
async function notifyClient(firebaseUid, cbId, payload) {
  const safeKey = cbId.replace(/[.#$\[\]/]/g, "_");
  await fbSet(`users/${firebaseUid}/adminActions/${safeKey}`, {
    ...payload,
    ts:        Date.now(),
    processed: false
  });
}

// ════════════════════════════════════════════════════
// CORE FIX: Fresh Firebase scan to resolve ANY cbId
// This replaces the buggy index-only approach.
// Scans: dep pendingReqs, wit pendingReqs, ALL chat states across ALL orders.
// ════════════════════════════════════════════════════
async function findCbIdOwner(cbId) {
  const users = await fbGet("users");
  if (!users || typeof users !== "object") return null;

  for (const [firebaseUid, userData] of Object.entries(users)) {
    if (!userData) continue;

    // Check deposit
    const dep = userData.pendingReqs?.dep;
    if (dep?.cbId === cbId) {
      return { firebaseUid, type: "dep", data: dep };
    }

    // Check withdrawal
    const wit = userData.pendingReqs?.wit;
    if (wit?.cbId === cbId) {
      return { firebaseUid, type: "wit", data: wit };
    }

    // Check ALL chat states (supports multiple active orders)
    const chats = userData.chats;
    if (chats && typeof chats === "object") {
      for (const [ordId, cs] of Object.entries(chats)) {
        if (cs && cs.cbId === cbId) {
          return { firebaseUid, type: "trade", ordId, data: cs };
        }
      }
    }
  }

  return null; // Not found anywhere
}

// ════════════════════════════════════════════════════
// HANDLE DEPOSIT
// ════════════════════════════════════════════════════
async function handleDeposit(firebaseUid, dep, action) {
  const t        = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;

  if (action === "approve") {
    const bal    = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + dep.amt).toFixed(8));

    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, dep.hid, "COMPLETED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    await notifyClient(firebaseUid, dep.cbId, {
      action: "approve", type: "dep", amt: dep.amt, newBalance: newBal
    });

    if (dep.msgId) tgEdit(dep.msgId,
      `✅ *DEPOSIT APPROVED*\n\n👤 UID: \`${userData.uid || "?"}\`\n👤 *${userData.name || "?"}*\n💰 *+${dep.amt} USDT*\n💼 Balance: *${newBal.toFixed(2)} USDT*\n⏰ ${t} IST`
    );
    tgSendBoth(`✅ *DEPOSIT CREDITED*\n\n👤 ${userData.uid} | ${userData.name}\n➕ +${dep.amt} USDT → ${newBal.toFixed(2)} USDT`);
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
async function handleWithdrawal(firebaseUid, wit, action) {
  const t        = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;

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
    const bal    = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + wit.amt).toFixed(8));

    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, wit.hid, "REJECTED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/wit`);
    await notifyClient(firebaseUid, wit.cbId, {
      action: "reject", type: "wit", amt: wit.amt, newBalance: newBal
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
async function handleTrade(firebaseUid, ordId, chatState, action) {
  const t        = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;

  // Always fetch the order fresh from Firebase
  const order = await fbGet(`users/${firebaseUid}/orders/${ordId}`);
  if (!order) {
    console.log(`Order not found in Firebase: ${ordId} for user ${firebaseUid}`);
    await tgSend(`⚠️ Order \`#${ordId}\` not found — may already be completed.`);
    return;
  }

  if (action === "approve") {
    const txId = genTxId("TRD");

    if (order.mode === "BUY") {
      const bal    = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
      const newBal = parseFloat((bal + order.usdt).toFixed(8));
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
      const bal    = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
      const newBal = parseFloat((bal + order.usdt).toFixed(8));
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
// PROCESS CALLBACK — always does a fresh scan
// ════════════════════════════════════════════════════
async function processCallback(cbId, action) {
  console.log(`🔍 Scanning Firebase for cbId: ${cbId}`);
  const entry = await findCbIdOwner(cbId);

  if (!entry) {
    console.log(`cbId not found anywhere: ${cbId}`);
    return false;
  }

  const { firebaseUid, type, ordId, data } = entry;
  console.log(`Found cbId ${cbId} → type=${type} uid=${firebaseUid}`);

  if (type === "dep") {
    await handleDeposit(firebaseUid, data, action);
    return true;
  }

  if (type === "wit") {
    await handleWithdrawal(firebaseUid, data, action);
    return true;
  }

  if (type === "trade") {
    // data is the chatState; fetch fresh from Firebase to be sure
    const freshChatState = await fbGet(`users/${firebaseUid}/chats/${ordId}`);
    if (!freshChatState) {
      console.log(`Chat state gone for ordId ${ordId} — already processed?`);
      return false;
    }
    await handleTrade(firebaseUid, ordId, freshChatState, action);
    return true;
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
    if (!userData) continue;
    if ((userData.uid || "").toUpperCase() !== targetUID.toUpperCase()) continue;

    const bal    = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + addAmt).toFixed(8));

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

    const creditCbId = `credit_${Date.now()}`;
    await notifyClient(firebaseUid, creditCbId, {
      action:     "credit",
      type:       "credit",
      amt:        addAmt,
      newBalance: newBal
    });

    await tgSendBoth(
      `✅ *BALANCE CREDITED*\n\n👤 UID: \`${targetUID}\`\n👤 *${userData.name || "?"}*\n📧 ${userData.email || "—"}\n➕ +${addAmt} USDT\n💰 Old: ${bal.toFixed(2)} → New: ${newBal.toFixed(2)} USDT`
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
    if (!u) continue;
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
    if (!userData) continue;

    // Search history
    const hist = userData.history;
    if (hist) {
      for (const [, h] of Object.entries(hist)) {
        if (!h) continue;
        const matchTx  = (h.txid || "").toUpperCase() === txid;
        const matchHid = (h.hid  || "").toUpperCase() === txid;
        if (matchTx || matchHid) {
          const st     = (h.status || "PENDING").toUpperCase();
          const stIcon = { COMPLETED:"✅", REJECTED:"❌", CANCELLED:"🚫", PENDING:"⏳" }[st] || "⏳";
          let msg = `🔍 *TRANSACTION*\n\n📋 Type: *${h.type}*\n💰 *${parseFloat(h.amt).toFixed(2)} USDT*\n👤 \`${h.uid}\` | *${userData.name || "?"}*\n📅 ${h.date || "—"}\n\n${stIcon} *${st}*`;
          if (h.txid)     msg += `\n🔖 \`${h.txid}\``;
          if (h.merchant) msg += `\n🏪 ${h.merchant}`;
          if (h.inr)      msg += `\n💵 ₹${Number(h.inr).toLocaleString()}`;

          if (st === "PENDING") {
            const pr      = userData.pendingReqs;
            const pending = pr?.dep?.hid === h.hid ? pr.dep : pr?.wit?.hid === h.hid ? pr.wit : null;
            if (pending) { await tgButtons(msg + "\n\n⚡ *Re-send approval:*", pending.cbId); return; }
          }
          await tgSend(msg);
          return;
        }
      }
    }

    // Search active orders
    const ordIdStr = txid.replace(/^ORD-?/i, "");
    const allOrders = userData.orders;
    if (allOrders && typeof allOrders === "object") {
      for (const [oKey, order] of Object.entries(allOrders)) {
        if (!order) continue;
        if (String(order.id) === String(ordIdStr) || String(order.id).toUpperCase() === txid) {
          const cs  = userData.chats?.[oKey] || {};
          let msg = `🔍 *ACTIVE ORDER*\n\n📋 \`#${order.id}\`\n👤 \`${userData.uid}\` | *${userData.name}*\n💹 *${order.mode}*\n🪙 ${order.usdt.toFixed(2)} USDT | ₹${Number(order.inr).toLocaleString()}\n📊 Stage: ${cs.dealStage || "init"}\n\n⏳ *PENDING*`;
          if (cs.cbId && cs.dealStage === "pending_verify") {
            await tgButtons(msg + "\n\n⚡ *Approval buttons:*", cs.cbId);
          } else {
            await tgSend(msg);
          }
          return;
        }
      }
    }
  }
  await tgSend(`❌ Not found: \`${txid}\`\n\nTry: \`#DEP-XXXXXX\` \`#WIT-XXXXXX\` \`#ORD-123456789\``);
}

// ════════════════════════════════════════════════════
// MAIN POLL
// ════════════════════════════════════════════════════
async function poll() {
  const data = await req(
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

    // ── CALLBACK ──────────────────────────────────
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

      if (processingCb.has(cbId)) {
        await tgAnswer(cb.id, "⏳ Already processing...");
        continue;
      }
      processingCb.add(cbId);

      await tgAnswer(cb.id, action === "approve" ? "✅ Approving..." : "❌ Rejecting...");
      console.log(`${action.toUpperCase()} → ${cbId}`);

      try {
        const found = await processCallback(cbId, action);
        if (!found) {
          await tgSend(
            `⚠️ *Not found or already processed*\n\ncbId: \`${cbId}\`\n\nOrder may have already been completed or cancelled.\nUse \`#ORD-XXXXXXXX\` to check status.`
          );
        }
      } catch(e) {
        console.error("processCallback error:", e.message);
        await tgSend(`❌ *Error:* \`${e.message}\``);
      } finally {
        processingCb.delete(cbId);
      }
      continue;
    }

    // ── TEXT MESSAGES ──────────────────────────────
    if (update.message) {
      const msg    = update.message;
      const text   = (msg.text || "").trim();
      const chatId = String(msg.chat.id);

      if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;

      if (text === "/help" || text === "/start" || text === "/txhelp") {
        await tgSend(
          `📖 *PRO P2P Bot v4 — Multi-Order Fixed*\n\n` +
          `✅ All orders always approvable\n` +
          `✅ Fresh Firebase scan per callback\n` +
          `✅ No stale index issues\n\n` +
          `*Lookup:*\n` +
          `\`#DEP-XXXXXX\` — deposit\n` +
          `\`#WIT-XXXXXX\` — withdrawal\n` +
          `\`#ORD-123456789\` — active order\n\n` +
          `*Credit:*\n\`#UID AMOUNT\`\n\n` +
          `*Stats:* \`/allstats\``
        );
        continue;
      }

      if (text === "/allstats") {
        await handleStats();
        continue;
      }

      const txMatch = text.match(/^#([A-Z0-9_\-]{4,20})$/i);
      if (txMatch) {
        const txid = txMatch[1].toUpperCase();
        await tgSend(`🔍 Looking up \`${txid}\`...`);
        await handleTxLookup(txid);
        continue;
      }

      const balMatch = text.match(/^#([A-Z0-9]{4,10})\s+([\d.]+)$/i);
      if (balMatch) {
        const targetUID = balMatch[1].toUpperCase();
        const amt       = parseFloat(balMatch[2]);
        if (!amt || amt <= 0) { await tgSend("❌ Invalid amount."); continue; }
        await tgSend(`⏳ Crediting \`${targetUID}\` with ${amt} USDT...`);
        await handleCredit(targetUID, amt);
        continue;
      }

      // Forward chat messages to deal page (handled by client poll)
    }
  }
}

// ════════════════════════════════════════════════════
// START
// ════════════════════════════════════════════════════
http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`PRO P2P Bot v4 ✅\nUptime: ${Math.floor(process.uptime())}s\n`);
}).listen(PORT, () => {
  console.log(`Keep-alive server on port ${PORT}`);
});

console.log("🤖 PRO P2P Bot v4 started — fresh Firebase scan per callback");
console.log(`Firebase: ${FB_URL}`);

(async () => {
  while (true) {
    try {
      await poll();
    } catch(e) {
      console.error("Poll loop error:", e.message);
      await sleep(3000);
    }
  }
})();
