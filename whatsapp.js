// ════════════════════════════════════════════════════════════════════
// WhatsApp engine (Baileys) — powers free unlimited OTP delivery
//
// * Session (creds + signal keys) is stored in Firebase Realtime DB, so the
//   login SURVIVES restarts / redeploys / Render spin-down → no logout.
//   If Firebase is unavailable we fall back to ./.wa-session on disk.
// * Exposes: start(), status(), getQR(), requestPairingCode(), sendText(),
//   isOnWhatsApp(), logout()
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

// ─── key-safe encoding for Firebase paths ───────────────────────────
const enc = (k) => Buffer.from(String(k)).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');

// ─── auth state backed by Firebase RTDB (fallback: local file) ──────
async function makeAuthState(db, dbPath = 'wa_session') {
  const useDb = Boolean(db);
  const fileDir = path.join(process.cwd(), '.wa-session');
  if (!useDb && !fs.existsSync(fileDir)) fs.mkdirSync(fileDir, { recursive: true });

  const readRaw = async (key) => {
    if (useDb) {
      const snap = await db.ref(`${dbPath}/${enc(key)}`).get();
      return snap.exists() ? snap.val() : null;
    }
    const f = path.join(fileDir, `${enc(key)}.json`);
    return fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : null;
  };
  const writeRaw = async (key, val) => {
    if (useDb) return db.ref(`${dbPath}/${enc(key)}`).set(val);
    fs.writeFileSync(path.join(fileDir, `${enc(key)}.json`), val);
  };
  const removeRaw = async (key) => {
    if (useDb) return db.ref(`${dbPath}/${enc(key)}`).remove();
    const f = path.join(fileDir, `${enc(key)}.json`);
    if (fs.existsSync(f)) fs.unlinkSync(f);
  };

  const readData = async (key) => {
    try {
      const raw = await readRaw(key);
      if (!raw) return null;
      return JSON.parse(raw, BufferJSON.reviver);
    } catch { return null; }
  };
  const writeData = (key, data) => writeRaw(key, JSON.stringify(data, BufferJSON.replacer));

  const creds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
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
    },
    saveCreds: () => writeData('creds', creds),
    clearAll: async () => {
      if (useDb) return db.ref(dbPath).remove();
      if (fs.existsSync(fileDir)) fs.rmSync(fileDir, { recursive: true, force: true });
    }
  };
}

// ─── engine ─────────────────────────────────────────────────────────
let sock = null;
let authRef = null;
let db_ = null;
let connState = 'offline';   // offline | connecting | qr | open
let lastQR = null;           // raw qr string
let lastQRat = 0;
let lastQRDataUrl = null;
let pairingCode = null;
let meNumber = null;
let starting = false;
let reconnectTimer = null;
let stopped = false;

export function status() {
  return {
    ok: true,
    state: connState,
    linked: connState === 'open',
    number: meNumber,
    hasQR: Boolean(lastQR) && connState !== 'open',
    qrAgeSec: lastQR ? Math.round((Date.now() - lastQRat) / 1000) : null,
    pairingCode
  };
}

export async function getQR() {
  if (connState === 'open' || !lastQR) return null;
  if (!lastQRDataUrl) {
    lastQRDataUrl = await QRCode.toDataURL(lastQR, { margin: 1, width: 320, errorCorrectionLevel: 'M' });
  }
  return { qr: lastQR, dataUrl: lastQRDataUrl, ageSec: Math.round((Date.now() - lastQRat) / 1000) };
}

export async function start(db) {
  if (starting || (sock && connState === 'open')) return status();
  starting = true;
  stopped = false;
  db_ = db || db_;
  try {
    authRef = await makeAuthState(db_);
    const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: undefined }));
    connState = 'connecting';

    sock = makeWASocket({
      version,
      auth: {
        creds: authRef.state.creds,
        keys: makeCacheableSignalKeyStore(authRef.state.keys, { level: 'silent', child: () => ({ level: 'silent', error(){}, warn(){}, info(){}, debug(){}, trace(){}, child: () => ({ error(){}, warn(){}, info(){}, debug(){}, trace(){}, child(){ return this; } }) }), error(){}, warn(){}, info(){}, debug(){}, trace(){} })
      },
      logger: { level: 'silent', error(){}, warn(){}, info(){}, debug(){}, trace(){}, child(){ return this; } },
      printQRInTerminal: false,
      markOnlineOnConnect: false,
      syncFullHistory: false,
      browser: ['BIEXC', 'Chrome', '121.0.0'],
      generateHighQualityLinkPreview: false,
      keepAliveIntervalMs: 25_000
    });

    sock.ev.on('creds.update', () => { authRef.saveCreds().catch(e => log(`creds save failed: ${e.message}`)); });

    sock.ev.on('connection.update', async (u) => {
      const { connection, lastDisconnect, qr } = u;
      if (qr) {
        lastQR = qr; lastQRat = Date.now(); lastQRDataUrl = null; connState = 'qr';
        log('📱 New QR generated — open /api/wa/qr to link');
      }
      if (connection === 'open') {
        connState = 'open'; lastQR = null; lastQRDataUrl = null; pairingCode = null;
        meNumber = (sock?.user?.id || '').split(':')[0] || null;
        log(`✅ WhatsApp linked as ${meNumber} — session saved (survives restarts)`);
      }
      if (connection === 'close') {
        const code = lastDisconnect?.error?.output?.statusCode;
        const loggedOut = code === DisconnectReason.loggedOut;
        connState = loggedOut ? 'offline' : 'connecting';
        log(`connection closed (code=${code || '?'})${loggedOut ? ' — logged out from phone' : ' — reconnecting'}`);
        sock = null;
        if (loggedOut) {
          // only wipe the session when WhatsApp itself revoked it
          await authRef.clearAll().catch(() => {});
        }
        if (!stopped) {
          clearTimeout(reconnectTimer);
          reconnectTimer = setTimeout(() => { starting = false; start(db_).catch(e => log(e.message)); }, loggedOut ? 3000 : 2000);
        }
      }
    });
  } catch (e) {
    connState = 'offline';
    log(`start failed: ${e.message}`);
    if (!stopped) {
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(() => { starting = false; start(db_).catch(() => {}); }, 10_000);
    }
  } finally {
    starting = false;
  }
  return status();
}

export async function requestPairingCode(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.length < 8) throw new Error('bad_phone');
  if (connState === 'open') throw new Error('already_linked');
  if (!sock) await start(db_);
  // wait until the socket produced its first QR (socket is ready by then)
  for (let i = 0; i < 20 && !lastQR && connState !== 'open'; i++) await new Promise(r => setTimeout(r, 500));
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

export async function logout() {
  stopped = true;
  clearTimeout(reconnectTimer);
  try { await sock?.logout(); } catch {}
  try { await authRef?.clearAll(); } catch {}
  sock = null; connState = 'offline'; lastQR = null; lastQRDataUrl = null; meNumber = null; pairingCode = null;
  stopped = false;
  setTimeout(() => start(db_).catch(() => {}), 1500);
  return true;
}
