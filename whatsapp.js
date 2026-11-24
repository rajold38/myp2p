// ════════════════════════════════════════════════════════════════════
// WhatsApp engine (Baileys) — powers free unlimited OTP delivery
//
// Link ONCE with a QR → stays linked. Fixes vs the old version:
//  * Auth state is loaded ONCE and kept in memory across reconnects. The old
//    code re-read Firebase on every reconnect, so right after a QR scan (WA
//    always sends "restart required" 515) it loaded the OLD creds before the
//    new ones were saved → a fresh QR again → "bar bar login".
//  * Every Firebase write goes through a queue; we wait for it to flush before
//    reconnecting / shutting down, so creds are never lost.
//  * Session is wiped ONLY when WhatsApp really logs us out (401 / 403).
//    440 (same session opened elsewhere, e.g. during a Render redeploy) just
//    backs off and retries — it no longer kills the login.
//  * Exponential back-off, SIGTERM flush, idle mode when nobody wants a QR.
//  * onEvent(fn) → 'qr' | 'linked' | 'loggedout' | 'replaced' events for the bot.
// ════════════════════════════════════════════════════════════════════

import fs from 'fs';
import path from 'path';
import QRCode from 'qrcode';
import makeWASocket, {
  initAuthCreds,
  BufferJSON,
  proto,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore
} from '@whiskeysockets/baileys';

const nowIST = () => new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
const log = (msg) => console.log(`[${nowIST()} IST] [WA] ${msg}`);

const enc = (k) => Buffer.from(String(k)).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');

const silent = { level: 'silent', error(){}, warn(){}, info(){}, debug(){}, trace(){}, fatal(){}, child(){ return silent; } };

// ─── auth state backed by Firebase RTDB (fallback: local file) ──────
async function makeAuthState(db, dbPath = 'wa_session') {
  const useDb = Boolean(db);
  const fileDir = path.join(process.cwd(), '.wa-session');
  if (!useDb && !fs.existsSync(fileDir)) fs.mkdirSync(fileDir, { recursive: true });

  // serialised write queue → no lost / out-of-order writes
  let queue = Promise.resolve();
  const enqueue = (fn) => { queue = queue.then(fn).catch(e => log(`session write failed: ${e.message}`)); return queue; };

  const readRaw = async (key) => {
    if (useDb) {
      const snap = await db.ref(`${dbPath}/${enc(key)}`).get();
      return snap.exists() ? snap.val() : null;
    }
    const f = path.join(fileDir, `${enc(key)}.json`);
    return fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : null;
  };
  const writeRaw = (key, val) => enqueue(async () => {
    if (useDb) return db.ref(`${dbPath}/${enc(key)}`).set(val);
    fs.writeFileSync(path.join(fileDir, `${enc(key)}.json`), val);
  });
  const removeRaw = (key) => enqueue(async () => {
    if (useDb) return db.ref(`${dbPath}/${enc(key)}`).remove();
    const f = path.join(fileDir, `${enc(key)}.json`);
    if (fs.existsSync(f)) fs.unlinkSync(f);
  });

  const readData = async (key) => {
    try {
      const raw = await readRaw(key);
      if (!raw) return null;
      return JSON.parse(raw, BufferJSON.reviver);
    } catch { return null; }
  };
  const writeData = (key, data) => writeRaw(key, JSON.stringify(data, BufferJSON.replacer));

  let creds = (await readData('creds')) || initAuthCreds();

  const state = {
    creds,
    keys: {
      get: async (type, ids) => {
        const out = {};
        await Promise.all(ids.map(async (id) => {
          let value = await readData(`${type}-${id}`);
          if (type === 'app-state-sync-key' && value) value = proto.Message.AppStateSyncKeyData.fromObject(value);
          if (value) out[id] = value;
        }));
        return out;
      },
      set: async (data) => {
        const tasks = [];
        for (const type of Object.keys(data)) {
          for (const id of Object.keys(data[type])) {
            const value = data[type][id];
            tasks.push(value ? writeData(`${type}-${id}`, value) : removeRaw(`${type}-${id}`));
          }
        }
        await Promise.all(tasks);
      }
    }
  };

  return {
    state,
    saveCreds: () => writeData('creds', state.creds),
    flush: () => queue,
    clearAll: async () => {
      await queue;
      if (useDb) await db.ref(dbPath).remove();
      else if (fs.existsSync(fileDir)) fs.rmSync(fileDir, { recursive: true, force: true });
      state.creds = initAuthCreds();
    }
  };
}

// ─── engine ─────────────────────────────────────────────────────────
let sock = null;
let authRef = null;
let db_ = null;
let connState = 'offline';   // offline | connecting | qr | open | idle
let lastQR = null, lastQRat = 0, lastQRDataUrl = null;
let pairingCode = null;
let meNumber = null, meName = null, linkedAt = null;
let starting = false;
let reconnectTimer = null;
let stopped = false;
let failStreak = 0;
let qrWantedUntil = 0;       // only keep generating QRs while someone is waiting
let lastReason = null;
let pendingLink = false;
const listeners = new Set();

export function onEvent(fn) { listeners.add(fn); return () => listeners.delete(fn); }
const emit = (type, info = {}) => { for (const fn of listeners) { try { fn(type, info); } catch {} } };

const isRegistered = () => Boolean(authRef?.state?.creds?.me?.id);

export function status() {
  return {
    ok: true,
    state: connState,
    linked: connState === 'open',
    registered: isRegistered(),
    number: meNumber,
    name: meName,
    linkedAt,
    hasQR: Boolean(lastQR) && connState !== 'open',
    qrAgeSec: lastQR ? Math.round((Date.now() - lastQRat) / 1000) : null,
    pairingCode,
    lastReason
  };
}

export async function getQR() {
  if (connState === 'open' || !lastQR) return null;
  if (!lastQRDataUrl) lastQRDataUrl = await QRCode.toDataURL(lastQR, { margin: 2, width: 420, errorCorrectionLevel: 'M' });
  return { qr: lastQR, dataUrl: lastQRDataUrl, ageSec: Math.round((Date.now() - lastQRat) / 1000) };
}

/** PNG buffer of the current QR (for Telegram sendPhoto). */
export async function getQRBuffer() {
  if (connState === 'open' || !lastQR) return null;
  return QRCode.toBuffer(lastQR, { type: 'png', margin: 3, width: 520, errorCorrectionLevel: 'M' });
}

function scheduleReconnect(ms) {
  if (stopped) return;
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(() => { start(db_).catch(e => log(e.message)); }, ms);
}

export async function start(db) {
  if (starting || (sock && connState !== 'offline' && connState !== 'idle')) return status();
  starting = true;
  stopped = false;
  db_ = db || db_;
  try {
    if (!authRef) authRef = await makeAuthState(db_);   // load ONCE, keep in memory
    const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: undefined }));
    connState = 'connecting';

    const s = makeWASocket({
      version,
      auth: { creds: authRef.state.creds, keys: makeCacheableSignalKeyStore(authRef.state.keys, silent) },
      logger: silent,
      printQRInTerminal: false,
      markOnlineOnConnect: false,          // phone keeps getting notifications
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      browser: ['BIEXC Server', 'Chrome', '121.0.0'],
      generateHighQualityLinkPreview: false,
      keepAliveIntervalMs: 20_000,
      connectTimeoutMs: 60_000,
      defaultQueryTimeoutMs: 60_000,
      retryRequestDelayMs: 500
    });
    sock = s;

    s.ev.on('creds.update', () => { authRef.saveCreds(); });

    s.ev.on('connection.update', async (u) => {
      if (s !== sock) return;              // ignore events from an old socket
      const { connection, lastDisconnect, qr } = u;

      if (qr) {
        if (isRegistered()) { log('unexpected QR while registered — ignoring'); }
        else if (Date.now() > qrWantedUntil) {
          // nobody is waiting for a QR → go idle instead of spamming QRs
          connState = 'idle'; lastQR = null; lastQRDataUrl = null;
          log('not linked — idle. Send /whatsapp in the Telegram bot to get a QR');
          try { s.end(undefined); } catch {}
          sock = null;
          return;
        } else {
          lastQR = qr; lastQRat = Date.now(); lastQRDataUrl = null; connState = 'qr';
          log('📱 New QR generated');
          pendingLink = true;
          emit('qr', { at: lastQRat });
        }
      }

      if (connection === 'open') {
        const firstLink = pendingLink; pendingLink = false;
        connState = 'open'; lastQR = null; lastQRDataUrl = null; pairingCode = null; failStreak = 0; lastReason = null;
        qrWantedUntil = 0;
        meNumber = (s.user?.id || '').split(':')[0].split('@')[0] || null;
        meName = s.user?.name || s.user?.verifiedName || null;
        if (!linkedAt) linkedAt = Date.now();
        await authRef.saveCreds(); await authRef.flush();
        log(`✅ WhatsApp connected as ${meNumber} — session saved`);
        emit(firstLink ? 'linked' : 'reconnected', { number: meNumber, name: meName });
      }

      if (connection === 'close') {
        const code = lastDisconnect?.error?.output?.statusCode;
        lastReason = code || null;
        sock = null;
        await authRef.flush();             // make sure fresh creds are on disk/DB

        const loggedOut = code === DisconnectReason.loggedOut || code === 403;
        if (loggedOut) {
          log(`❌ logged out by WhatsApp (code=${code}) — session cleared`);
          await authRef.clearAll().catch(() => {});
          connState = 'idle'; meNumber = null; meName = null; linkedAt = null; lastQR = null;
          emit('loggedout', { code });
          return;                          // wait for /whatsapp to relink
        }
        if (code === DisconnectReason.connectionReplaced) {
          connState = 'connecting';
          log('⚠️ session opened by another server instance (440) — retrying in 30s, session kept');
          emit('replaced', { code });
          return scheduleReconnect(30_000);
        }
        if (connState === 'idle') return;
        connState = 'connecting';
        // 515 restartRequired (normal right after QR scan) → reconnect immediately
        const delay = code === DisconnectReason.restartRequired ? 500
          : Math.min(60_000, 2000 * 2 ** Math.min(failStreak++, 5));
        log(`connection closed (code=${code || '?'}) — reconnecting in ${Math.round(delay / 1000)}s`);
        scheduleReconnect(delay);
      }
    });
  } catch (e) {
    connState = 'offline';
    log(`start failed: ${e.message}`);
    scheduleReconnect(Math.min(60_000, 5000 * 2 ** Math.min(failStreak++, 4)));
  } finally {
    starting = false;
  }
  return status();
}

/** Ask for a QR (used by the Telegram /whatsapp command). Resolves when a QR
 *  is ready or the account is already linked. */
export async function requestQR(timeoutMs = 25_000) {
  if (connState === 'open') return { linked: true };
  qrWantedUntil = Date.now() + 3 * 60_000;   // keep QRs coming for 3 minutes
  if (!sock) { clearTimeout(reconnectTimer); await start(db_); }
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (connState === 'open') return { linked: true };
    if (lastQR && connState === 'qr') return { linked: false, qr: true };
    await new Promise(r => setTimeout(r, 400));
  }
  return { linked: false, qr: Boolean(lastQR) };
}

export async function requestPairingCode(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.length < 8) throw new Error('bad_phone');
  if (connState === 'open') throw new Error('already_linked');
  await requestQR();
  const code = await sock.requestPairingCode(digits);
  pairingCode = code;
  log(`🔗 pairing code for ${digits}: ${code}`);
  return code;
}

export async function isOnWhatsApp(jidPhone) {
  if (connState !== 'open' || !sock) return null;
  try {
    const r = await sock.onWhatsApp(jidPhone);
    return Array.isArray(r) && r[0]?.exists ? r[0].jid : null;
  } catch { return null; }
}

export async function sendText(phoneDigits, text) {
  if (connState !== 'open' || !sock) throw new Error('whatsapp_not_linked');
  const jid = (await isOnWhatsApp(phoneDigits)) || `${phoneDigits}@s.whatsapp.net`;
  await sock.sendMessage(jid, { text });
  return true;
}

/** Real unlink (removes the device from the phone too). */
export async function logout() {
  stopped = true;
  clearTimeout(reconnectTimer);
  try { await sock?.logout(); } catch {}
  try { await authRef?.clearAll(); } catch {}
  sock = null; connState = 'idle'; lastQR = null; lastQRDataUrl = null;
  meNumber = null; meName = null; linkedAt = null; pairingCode = null;
  stopped = false;
  return true;
}

/** Graceful shutdown: close the socket WITHOUT logging out + flush writes. */
export async function shutdown() {
  stopped = true;
  clearTimeout(reconnectTimer);
  try { sock?.end(undefined); } catch {}
  try { await authRef?.flush(); } catch {}
}

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.once(sig, async () => {
    log(`${sig} — saving WhatsApp session before exit`);
    await Promise.race([shutdown(), new Promise(r => setTimeout(r, 4000))]);
    process.exit(0);
  });
}
