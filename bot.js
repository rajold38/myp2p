/**
 * PRO P2P — Telegram Bot (FIXED v2)
 * ════════════════════════════════════════════════════
 * FIXES:
 *  1. notifyClient now writes processed:false so frontend actually handles it
 *  2. In-memory cbId→firebaseUid index = no full DB scan on every button press
 *  3. True long-polling (25s timeout) = near-instant button response
 *  4. cbId-level dedup = no double-processing even on rapid clicks
 *  5. Credit command now notifies client instantly (toast/alert on site)
 *  6. All old buttons still work (index rebuilt every 30s + fallback scan)
 * ════════════════════════════════════════════════════
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
const seenUpdates  = new Set();   // dedup by Telegram update_id
const processingCb = new Set();   // dedup by cbId (prevents double-click race)

/**
 * cbIdIndex: Map<cbId, firebaseUid>
 * Built from Firebase every INDEX_TTL ms. On a cache miss we fall back
 * to a full scan (and then update the cache). This means:
 * - Normal case (recent button): O(1) lookup, responds in <300ms
 * - Old/uncached button: falls back to full scan once, then cached
 */
const cbIdIndex   = new Map();
let   indexBuiltAt = 0;
const INDEX_TTL    = 30_000; // rebuild index every 30s

// ── SLEEP ────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── HTTP HELPER (with timeout) ───────────────────────
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
const tgSend     = (text) => tgPost("sendMessage", { chat_id: TG_CHAT, text, parse_mode: "Markdown" });
const tgSendLog  = (text) => tgPost("sendMessage", { chat_id: TG_CHAT_LOG, text, parse_mode: "Markdown" });
const tgSendBoth = (text) => { tgSend(text); tgSendLog(text); };
const tgAnswer   = (id, text, alert = false) => tgPost("answerCallbackQuery", { callback_query_id: id, text, show_alert: alert });
const tgEdit     = (msgId, text) => tgPost("editMessageText", { chat_id: TG_CHAT, message_id: msgId, text, parse_mode: "Markdown" });

async function tgButtons(text, cbId) {
  const keyboard = { inline_keyboard: [[
    { text: "✅ APPROVE", callback_data: `approve_${cbId}` },
    { text: "❌ REJECT",  callback_data: `reject_${cbId}`  }
  ]]};
  const d = await tgPost("sendMessage", { chat_id: TG_CHAT, text, parse_mode: "Markdown", reply_markup: keyboard });
  await tgPost("sendMessage", { chat_id: TG_CHAT_LOG, text, parse_mode: "Markdown", reply_markup: keyboard });
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

// ── NOTIFY CLIENT ─────────────────────────────────────
// ★ FIX: was `processed: true` — client was skipping every action!
// Now writes processed:false so the frontend's onValue listener handles it,
// marks it processed itself, then deletes it.
async function notifyClient(firebaseUid, cbId, payload) {
  const key = cbId.replace(/[.#$\[\]/]/g, "_"); // Firebase key-safe
  await fbSet(`users/${firebaseUid}/adminActions/${key}`, {
    ...payload,
    ts:        Date.now(),
    processed: false   // ← THE KEY FIX
  });
}

// ════════════════════════════════════════════════════
// cbId INDEX — O(1) lookup instead of full DB scan
// ════════════════════════════════════════════════════
async function buildCbIndex() {
  try {
    const users = await fbGet("users");
    if (!users || typeof users !== "object") return;

    cbIdIndex.clear();

    for (const [firebaseUid, userData] of Object.entries(users)) {
      if (!userData) continue;

      // Deposit pending req
      const dep = userData.pendingReqs?.dep;
      if (dep?.cbId) cbIdIndex.set(dep.cbId, { firebaseUid, type: "dep" });

      // Withdrawal pending req
      const wit = userData.pendingReqs?.wit;
      if (wit?.cbId) cbIdIndex.set(wit.cbId, { firebaseUid, type: "wit" });

      // Trade chat states
      const chats = userData.chats;
      if (chats && typeof chats === "object") {
        for (const [ordId, cs] of Object.entries(chats)) {
          if (cs?.cbId) cbIdIndex.set(cs.cbId, { firebaseUid, type: "trade", ordId });
        }
      }
    }

    indexBuiltAt = Date.now();
    console.log(`📇 cbId index built: ${cbIdIndex.size} entries`);
  } catch(e) {
    console.error("buildCbIndex error:", e.message);
  }
}

// Resolve a cbId to its owner — uses index first, falls back to full scan
async function resolveCbId(cbId) {
  // 1. Try in-memory index
  if (cbIdIndex.has(cbId)) return cbIdIndex.get(cbId);

  // 2. Index stale or miss → full scan + rebuild index
  console.log(`cbId not in index, doing full scan: ${cbId}`);
  await buildCbIndex();
  if (cbIdIndex.has(cbId)) return cbIdIndex.get(cbId);

  return null;
}

// ════════════════════════════════════════════════════
// HANDLE DEPOSIT
// ════════════════════════════════════════════════════
async function handleDeposit(firebaseUid, dep, action) {
  const t = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;

  if (action === "approve") {
    const bal    = parseFloat((await fbGet(`users/${firebaseUid}/balance`)) || 0);
    const newBal = parseFloat((bal + dep.amt).toFixed(8));

    await fbSet(`users/${firebaseUid}/balance`, newBal);
    await updateHistStatus(firebaseUid, dep.hid, "COMPLETED");
    await fbDelete(`users/${firebaseUid}/pendingReqs/dep`);
    // Notify client — processed:false so frontend picks it up
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
  const t = nowIST();
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
  const t = nowIST();
  const userData = await fbGet(`users/${firebaseUid}`);
  if (!userData) return;
  const order = await fbGet(`users/${firebaseUid}/orders/${ordId}`);
  if (!order) { console.log("Order not found:", ordId); return; }

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
// PROCESS CALLBACK — uses index, no full scan needed
// ════════════════════════════════════════════════════
async function processCallback(cbId, action) {
  const entry = await resolveCbId(cbId);
  if (!entry) return false;

  const { firebaseUid, type, ordId } = entry;

  if (type === "dep") {
    const dep = await fbGet(`users/${firebaseUid}/pendingReqs/dep`);
    if (!dep || dep.cbId !== cbId) return false; // already processed
    await handleDeposit(firebaseUid, dep, action);
    cbIdIndex.delete(cbId);
    return true;
  }

  if (type === "wit") {
    const wit = await fbGet(`users/${firebaseUid}/pendingReqs/wit`);
    if (!wit || wit.cbId !== cbId) return false;
    await handleWithdrawal(firebaseUid, wit, action);
    cbIdIndex.delete(cbId);
    return true;
  }

  if (type === "trade") {
    const chatState = await fbGet(`users/${firebaseUid}/chats/${ordId}`);
    if (!chatState || chatState.cbId !== cbId) return false;
    await handleTrade(firebaseUid, ordId, chatState, action);
    cbIdIndex.delete(cbId);
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

    // ★ NEW: Notify client so they get instant toast/alert popup on site
    const creditCbId = `credit_${Date.now()}`;
    await notifyClient(firebaseUid, creditCbId, {
      action:      "credit",
      type:        "credit",
      amt:         addAmt,
      newBalance:  newBal
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
    `📊 *SITE STATS*\n\n👥 Users: *${userCount}*\n💰 Total USDT: *${totalBal.toFixed(2)}*\n📥 Pending Deposits: *${pendingDep}*\n💼 Pending Withdrawals: *${pendingWit}*\n🤝 Active Orders: *${activeOrders}*\n📇 Cached cbIds: *${cbIdIndex.size}*\n⏰ ${nowIST()} IST`
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
        const matchTx  = (h.txid  || "").toUpperCase() === txid;
        const matchHid = (h.hid   || "").toUpperCase() === txid;
        if (matchTx || matchHid) {
          const st     = (h.status || "PENDING").toUpperCase();
          const stIcon = { COMPLETED:"✅", REJECTED:"❌", CANCELLED:"🚫", PENDING:"⏳" }[st] || "⏳";
          let msg = `🔍 *TRANSACTION*\n\n📋 Type: *${h.type}*\n💰 *${parseFloat(h.amt).toFixed(2)} USDT*\n👤 \`${h.uid}\` | *${userData.name || "?"}*\n📅 ${h.date || "—"}\n\n${stIcon} *${st}*`;
          if (h.txid)     msg += `\n🔖 \`${h.txid}\``;
          if (h.merchant) msg += `\n🏪 ${h.merchant}`;
          if (h.inr)      msg += `\n💵 ₹${Number(h.inr).toLocaleString()}`;

          if (st === "PENDING") {
            const pr = userData.pendingReqs;
            const pending = pr?.dep?.hid === h.hid ? pr.dep : pr?.wit?.hid === h.hid ? pr.wit : null;
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
// MAIN POLL — true long-polling, processes updates
// ════════════════════════════════════════════════════
async function poll() {
  // Long-poll: Telegram holds the connection up to 25s if no updates.
  // This means buttons respond in <500ms instead of waiting up to 2s.
  const data = await req(
    `${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=25&allowed_updates=["message","callback_query"]`,
    "GET", null,
    35_000  // must be > timeout+buffer
  );

  if (!data.ok || !data.result?.length) return;

  for (const update of data.result) {
    const uid = update.update_id;
    lastUpdateId = Math.max(lastUpdateId, uid);

    // Dedup by Telegram update_id
    if (seenUpdates.has(uid)) continue;
    seenUpdates.add(uid);
    // Keep set bounded
    if (seenUpdates.size > 1000) {
      const arr = [...seenUpdates].sort((a,b) => a-b);
      arr.slice(0, 500).forEach(x => seenUpdates.delete(x));
    }

    // ── CALLBACK: Approve / Reject ──────────────────
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

      // ★ cbId-level dedup: prevents double-click race condition
      if (processingCb.has(cbId)) {
        await tgAnswer(cb.id, "⏳ Already processing...");
        continue;
      }
      processingCb.add(cbId);

      // Instant feedback to admin
      await tgAnswer(cb.id, action === "approve" ? "✅ Approving..." : "❌ Rejecting...");
      console.log(`${action.toUpperCase()} → ${cbId}`);

      try {
        const found = await processCallback(cbId, action);
        if (!found) {
          await tgSend(`⚠️ *Not found or already processed*\n\ncbId: \`${cbId}\`\n\nTry \`#ORD-XXXXXXXX\` or \`#DEP-XXXXXX\` to look it up.`);
        }
      } catch(e) {
        console.error("processCallback error:", e.message);
        await tgSend(`❌ *Error processing*\n\n\`${e.message}\``);
      } finally {
        // Always release lock so button works again on retry
        processingCb.delete(cbId);
      }
      continue;
    }

    // ── TEXT MESSAGES ───────────────────────────────
    if (update.message) {
      const msg    = update.message;
      const text   = (msg.text || "").trim();
      const chatId = String(msg.chat.id);

      if (chatId !== String(TG_CHAT) && chatId !== String(TG_CHAT_LOG)) continue;

      if (text === "/help" || text === "/start" || text === "/txhelp") {
        await tgSend(
          `📖 *PRO P2P BOT v2 — 24/7 SERVER*\n\n` +
          `✅ Approve/Reject works even 24h later\n` +
          `✅ Near-instant response (long-polling)\n` +
          `✅ Old buttons never expire\n\n` +
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
}

// ════════════════════════════════════════════════════
// START
// ════════════════════════════════════════════════════

// Keep-alive HTTP server (required by Render free tier)
http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`PRO P2P Bot v2 ✅\nUptime: ${Math.floor(process.uptime())}s\nIndexed cbIds: ${cbIdIndex.size}`);
}).listen(PORT, () => {
  console.log(`Keep-alive server on port ${PORT}`);
});

// Build initial cbId index before starting poll loop
buildCbIndex().then(() => {
  console.log("🤖 PRO P2P Bot v2 started — long-polling Telegram");
  console.log(`Firebase: ${FB_URL}`);

  // Rebuild index every 30s to catch any new pending requests
  setInterval(buildCbIndex, 30_000);

  // ★ True long-poll loop — no fixed interval, runs immediately after each response
  (async () => {
    while (true) {
      try {
        await poll();
      } catch(e) {
        console.error("Poll loop error:", e.message);
        await sleep(3000); // back-off on error
      }
      // No sleep here — poll() itself blocks for up to 25s when idle
    }
  })();
});
