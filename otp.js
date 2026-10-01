// ════════════════════════════════════════════════════════════════════
// WhatsApp OTP login — backend half of the frontend "Phone (OTP)" flow
//
// Routes mounted here (exactly what index.html calls):
//   POST /api/otp/send    { phone }              -> { ok, phone, waitSec }
//   POST /api/otp/verify  { phone, code, name }  -> { ok, token, uid, phone }
//   GET  /api/wa/qr       -> live link page (QR image + copy button)
//   GET  /api/wa/status   -> JSON status
//   POST /api/wa/pair     { phone } -> 8-digit pairing code (no QR needed)
//   POST /api/wa/logout   { key }   -> unlink (admin only)
//
// Error codes returned match the frontend's otpErrText():
//   whatsapp_not_linked | not_on_whatsapp | too_soon | rate_limited |
//   bad_phone | invalid_code | expired | no_code | too_many_tries
// ════════════════════════════════════════════════════════════════════

import * as WA from './whatsapp.js';

const CODE_TTL_MS   = 5 * 60 * 1000;   // code valid 5 minutes
const RESEND_MS     = 60 * 1000;       // 60s between codes
const MAX_PER_HOUR  = 5;
const MAX_TRIES     = 5;

const store = new Map();   // phone -> { code, exp, tries, lastSent, sentTimes[] }

const norm = (p) => String(p || '').replace(/\D/g, '');
const gen  = () => String(Math.floor(100000 + Math.random() * 900000));

function msg(code) {
  return `*${code}* is your BIEXC verification code.\n\nIt expires in 5 minutes.\nNever share this code with anyone — BIEXC staff will never ask for it.`;
}

export function mountOtp(app, { admin, db, log = console.log, adminKey = '' }) {
  const L = (m) => log('OTP', m);

  // ── send ──────────────────────────────────────────────────────────
  app.post('/api/otp/send', async (req, res) => {
    try {
      const phone = norm(req.body?.phone);
      if (phone.length < 8 || phone.length > 15) return res.json({ ok: false, error: 'bad_phone' });

      const st = store.get(phone) || { sentTimes: [], tries: 0 };
      const now = Date.now();
      if (st.lastSent && now - st.lastSent < RESEND_MS) {
        return res.json({ ok: false, error: 'too_soon', waitSec: Math.ceil((RESEND_MS - (now - st.lastSent)) / 1000) });
      }
      st.sentTimes = (st.sentTimes || []).filter(t => now - t < 3600_000);
      if (st.sentTimes.length >= MAX_PER_HOUR) return res.json({ ok: false, error: 'rate_limited' });

      if (!WA.status().linked) return res.json({ ok: false, error: 'whatsapp_not_linked' });
      const jid = await WA.isOnWhatsApp(phone);
      if (!jid) return res.json({ ok: false, error: 'not_on_whatsapp' });

      const code = gen();
      await WA.sendText(phone, msg(code));

      st.code = code; st.exp = now + CODE_TTL_MS; st.tries = 0; st.lastSent = now;
      st.sentTimes.push(now);
      store.set(phone, st);
      L(`code sent → +${phone}`);
      res.json({ ok: true, phone: '+' + phone, waitSec: RESEND_MS / 1000 });
    } catch (e) {
      L(`send error: ${e.message}`);
      res.json({ ok: false, error: e.message === 'whatsapp_not_linked' ? 'whatsapp_not_linked' : 'send_failed' });
    }
  });

  // ── verify → Firebase custom token ────────────────────────────────
  app.post('/api/otp/verify', async (req, res) => {
    try {
      const phone = norm(req.body?.phone);
      const code  = String(req.body?.code || '').replace(/\D/g, '');
      const name  = String(req.body?.name || '').trim().slice(0, 60);
      if (!phone) return res.json({ ok: false, error: 'bad_phone' });

      const st = store.get(phone);
      if (!st || !st.code) return res.json({ ok: false, error: 'no_code' });
      if (Date.now() > st.exp) { store.delete(phone); return res.json({ ok: false, error: 'expired' }); }
      if (st.tries >= MAX_TRIES) { store.delete(phone); return res.json({ ok: false, error: 'too_many_tries' }); }
      if (code !== st.code) {
        st.tries++;
        return res.json({ ok: false, error: 'invalid_code', left: Math.max(0, MAX_TRIES - st.tries) });
      }
      store.delete(phone);

      const e164 = '+' + phone;
      let user;
      try { user = await admin.auth().getUserByPhoneNumber(e164); }
      catch {
        user = await admin.auth().createUser({
          phoneNumber: e164,
          displayName: name || `User ${phone.slice(-4)}`
        });
      }
      if (name && !user.displayName) {
        await admin.auth().updateUser(user.uid, { displayName: name }).catch(() => {});
      }

      const token = await admin.auth().createCustomToken(user.uid, { phone: e164, login: 'whatsapp' });

      if (db) {
        await db.ref(`users/${user.uid}`).update({
          phone: e164,
          ...(name ? { name } : {}),
          lastLogin: Date.now(),
          loginMethod: 'whatsapp'
        }).catch(() => {});
      }

      L(`verified ${e164} → uid=${user.uid}`);
      res.json({ ok: true, token, uid: user.uid, phone: e164 });
    } catch (e) {
      L(`verify error: ${e.message}`);
      res.json({ ok: false, error: 'verify_failed' });
    }
  });

  // ── status ────────────────────────────────────────────────────────
  app.get('/api/wa/status', (_req, res) => res.json(WA.status()));

  // ── pairing code (link without scanning) ──────────────────────────
  app.post('/api/wa/pair', async (req, res) => {
    try { res.json({ ok: true, code: await WA.requestPairingCode(req.body?.phone) }); }
    catch (e) { res.json({ ok: false, error: e.message }); }
  });

  // ── unlink (admin) ────────────────────────────────────────────────
  app.post('/api/wa/logout', async (req, res) => {
    if (adminKey && String(req.body?.key || '') !== adminKey) return res.status(403).json({ ok: false, error: 'forbidden' });
    await WA.logout();
    res.json({ ok: true });
  });

  // ── raw QR JSON (for polling) ─────────────────────────────────────
  app.get('/api/wa/qr.json', async (_req, res) => {
    const s = WA.status();
    const q = await WA.getQR();
    res.json({ ...s, qr: q?.qr || null, dataUrl: q?.dataUrl || null });
  });

  // ── QR page: scan format + click-to-copy ──────────────────────────
  app.get('/api/wa/qr', async (_req, res) => {
    const s = WA.status();
    const q = await WA.getQR();
    res.type('html').send(qrPage(s, q));
  });
}

function qrPage(s, q) {
  const linked = s.linked;
  const img = q?.dataUrl
    ? `<img id="qrimg" src="${q.dataUrl}" alt="WhatsApp QR code">`
    : `<div class="ph">${linked ? '✅' : '⏳'}</div>`;
  return `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BIEXC · WhatsApp link</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
 background:#0B0E11;color:#EAECEF;font-family:-apple-system,'SF Pro Display',Segoe UI,Roboto,Arial,sans-serif;padding:24px}
.card{width:100%;max-width:420px;background:#181A20;border:1px solid #2B3139;border-radius:20px;padding:26px 24px 22px;text-align:center}
h1{font-size:20px;margin:0 0 4px;color:#fff}
.sub{font-size:13px;color:#848E9C;margin-bottom:18px}
.badge{display:inline-block;font-size:12px;font-weight:600;padding:5px 12px;border-radius:999px;margin-bottom:16px}
.on{background:rgba(14,203,129,.14);color:#0ECB81}
.off{background:rgba(246,70,93,.14);color:#F6465D}
.wait{background:rgba(252,213,53,.14);color:#FCD535}
.qrbox{background:#fff;border-radius:16px;padding:12px;display:inline-block;line-height:0}
.qrbox img{width:280px;height:280px;display:block}
.ph{width:280px;height:280px;display:flex;align-items:center;justify-content:center;font-size:54px;background:#1E2329;border-radius:16px;color:#5E6673}
.code{margin-top:16px;background:#1E2329;border:1px dashed #474D57;border-radius:12px;padding:12px;
 font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;word-break:break-all;color:#B7BDC6;max-height:96px;overflow:auto;text-align:left}
.pair{margin-top:14px;font-size:30px;letter-spacing:6px;font-weight:700;color:#FCD535}
button{width:100%;height:46px;margin-top:14px;border:none;border-radius:12px;background:#FCD535;color:#0B0E11;
 font-size:15px;font-weight:600;cursor:pointer;transition:.15s}
button:hover{filter:brightness(1.05)}
button.ghost{background:#2B3139;color:#EAECEF}
ol{text-align:left;color:#848E9C;font-size:13px;line-height:1.7;padding-left:18px;margin:18px 0 0}
.note{margin-top:14px;font-size:12px;color:#5E6673}
</style></head><body>
<div class="card">
  <h1>WhatsApp OTP Sender</h1>
  <div class="sub">BIEXC login codes are delivered from this WhatsApp session</div>
  <div class="badge ${linked ? 'on' : (s.state === 'qr' ? 'wait' : 'off')}">
    ${linked ? '● LINKED' + (s.number ? ' · +' + s.number : '') : (s.state === 'qr' ? '● SCAN THE QR' : '● ' + String(s.state).toUpperCase())}
  </div>
  <div>${linked ? '<div class="ph">✅</div>' : `<div class="qrbox">${img}</div>`}</div>
  ${s.pairingCode ? `<div class="pair">${s.pairingCode}</div>` : ''}
  ${(!linked && q?.qr) ? `<div class="code" id="raw">${q.qr.replace(/</g, '&lt;')}</div>
  <button onclick="copyRaw(this)">📋 Click to copy QR code</button>` : ''}
  <button class="ghost" onclick="location.reload()">↻ Refresh</button>
  ${linked ? '<div class="note">Session is stored in the database — restarts and redeploys will NOT log you out.</div>'
           : `<ol>
    <li>Open WhatsApp on the phone that should send OTPs</li>
    <li>Settings → Linked devices → Link a device</li>
    <li>Scan the QR above (it refreshes automatically)</li>
    <li>Once linked it stays logged in permanently</li>
  </ol>`}
</div>
<script>
function copyRaw(btn){
  var t=document.getElementById('raw').innerText;
  (navigator.clipboard?navigator.clipboard.writeText(t):Promise.reject()).catch(function(){
    var a=document.createElement('textarea');a.value=t;document.body.appendChild(a);a.select();document.execCommand('copy');a.remove();
  }).finally(function(){
    var o=btn.textContent;btn.textContent='✅ Copied!';setTimeout(function(){btn.textContent=o;},1500);
  });
}
// auto-refresh while waiting to link (QR rotates every ~20s)
${linked ? '' : "setTimeout(function(){location.reload();},15000);"}
</script>
</body></html>`;
}
