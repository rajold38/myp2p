// ════════════════════════════════════════════════════════════════════
// Email OTP (Resend) — required for email Sign Up and email Sign In.
// Google sign-in does not use this (skips OTP).
//
//   POST /api/email-otp/send    { email, purpose:'signup'|'login', password, name? }
//        -> { ok, waitSec } | { ok:false, error }
//   POST /api/email-otp/verify  { email, purpose, code }
//        -> { ok, token }  (Firebase custom token → signInWithCustomToken)
//
// Env vars:
//   RESEND_API_KEY        re_xxx  (resend.com → API Keys)
//   MAIL_FROM             e.g.  BIEXC <noreply@yourdomain.com>  (verified domain)
//   FIREBASE_WEB_API_KEY  the "apiKey" from the website's firebaseConfig
// Errors: bad_email | weak_password | email_exists | invalid_credentials |
//         too_soon | rate_limited | send_failed | no_code | expired |
//         invalid_code | too_many_tries | not_configured | verify_failed
// ════════════════════════════════════════════════════════════════════
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const TTL = 10 * 60 * 1000;      // code valid 10 min
const RESEND_MS = 60 * 1000;     // 60s between sends
const MAX_PER_HOUR = 6;
const MAX_TRIES = 5;
const store = new Map();         // key email|purpose -> state
const ipHits = new Map();        // ip -> [timestamps]

const normEmail = (e) => String(e || '').trim().toLowerCase();
const okEmail = (e) => /^[^\s@]{1,64}@[^\s@]{1,190}\.[a-z]{2,}$/i.test(e) && e.length <= 254;
const hash = (c) => crypto.createHash('sha256').update(String(c)).digest('hex');
const gen = () => String(crypto.randomInt(100000, 1000000));

// Public base URL used for the "Tap to copy" button in the email.
// Falls back to the Render URL; set PUBLIC_URL to use your own domain.
const copyBase = () => String(process.env.PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').trim().replace(/\/+$/, '');

// ── BIEXC logo shown in the email header ─────────────────────────────
// Two ways to set it (either one works, LOGO_URL wins):
//   LOGO_URL=https://yourdomain.com/logo.png      a public image link
//   LOGO_FILE=logo.png                            a file inside the repo
// With no LOGO_FILE, logo.png / logo.jpg in the project root is picked up
// automatically and sent *inside* the email (no hosting needed).
const LOGO_CID = 'biexc-logo';
const LOGO_DEFAULT_URL = 'https://id-preview--189dcad7-b984-44c6-8cec-f4a956f21376.lovable.app/__l5e/assets-v1/1296e733-46a8-4585-8a72-d61d45688e7c/biexc-logo.png';
const LOGO_MIME = { '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif', '.webp': 'image/webp' };
let _logo;                                        // undefined = not looked yet

function logoAsset() {
  if (_logo !== undefined) return _logo;
  const external = String(process.env.LOGO_URL || LOGO_DEFAULT_URL).trim();
  if (/^https?:\/\//i.test(external)) return (_logo = { src: external, attachment: null });
  const here = path.dirname(fileURLToPath(import.meta.url));
  const candidates = [process.env.LOGO_FILE, 'logo.png', 'logo.jpg', 'logo.jpeg', 'assets/logo.png']
    .filter(Boolean)
    .map(p => (path.isAbsolute(p) ? p : path.join(here, p)));
  for (const file of candidates) {
    try {
      const ext = path.extname(file).toLowerCase();
      if (!LOGO_MIME[ext] || !fs.existsSync(file)) continue;
      const buf = fs.readFileSync(file);
      if (!buf.length || buf.length > 400_000) continue;   // keep the mail light
      return (_logo = {
        src: `cid:${LOGO_CID}`,
        attachment: {
          content: buf.toString('base64'),
          filename: path.basename(file),
          contentDisposition: 'inline',
          cid: LOGO_CID,
        },
      });
    } catch { /* bad logo file — fall back to the letter badge */ }
  }
  return (_logo = null);
}

function mailHtml(code, purpose, name, base) {
  const isSignup = purpose === 'signup';
  const isReset = purpose === 'reset';
  const title = isReset ? 'Reset your password' : isSignup ? 'Verify your email address' : 'Confirm it\u2019s you';
  const preheader = isReset ? 'Use this code to set a new password for your BIEXC account.' : isSignup
    ? 'One quick step left \u2014 enter this code to finish creating your BIEXC account.'
    : 'A sign-in was requested for your account. Use this secure code to continue.';
  const greet = name ? `Hi ${name},` : 'Hello,';
  const bodyLine = isReset
    ? 'We received a request to reset the password of your BIEXC account. Enter the 6-digit code below in the app, then choose your new password.'
    : isSignup
    ? 'You\u2019re just one step away from your new BIEXC account. Enter the 6-digit code below to verify this email address and finish signing up.'
    : 'We noticed a sign-in attempt to your BIEXC account. Enter the 6-digit code below to continue \u2014 this confirms it\u2019s really you.';
  const pair = String(code).replace(/(\d{3})(\d{3})/, '$1 $2');

  // Logo: real image when available, gold "B" badge otherwise.
  const logo = logoAsset();
  const logoTile = logo
    ? `<img src="${logo.src}" width="42" height="42" alt="BIEXC" style="display:block;width:42px;height:42px;border-radius:11px;border:0;outline:none;-ms-interpolation-mode:bicubic">`
    : `<div style="width:42px;height:42px;border-radius:11px;background:#f5b51b;text-align:center;font-size:23px;font-weight:900;line-height:42px;color:#0b0d12;font-family:Arial,Helvetica,sans-serif;letter-spacing:-1px">B</div>`;

  // White digit cards on a soft panel (no dark blocks anywhere).
  const digits = String(code).split('').map(d =>
    `<td style="width:46px;padding:0 4px"><div style="background:#ffffff;border:1px solid #e2e6ec;border-radius:12px;padding:15px 0;text-align:center;color:#0b0d12;font-size:28px;line-height:1;font-weight:800;font-family:Arial,Helvetica,sans-serif;box-shadow:0 2px 6px rgba(10,12,18,.06)">${d}</div></td>`
  ).join('');

  // "Tap to copy" bar pinned at the very top of the message body.
  const copyBar = base ? `
    <tr><td style="padding:24px 32px 0">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-radius:12px">
        <tr><td align="center" bgcolor="#f5b51b" style="background:#f5b51b;border-radius:12px">
          <a href="${base}/copy?c=${code}" target="_blank" style="display:block;padding:15px 12px;font-size:15px;font-weight:800;color:#0b0d12;text-decoration:none;font-family:Arial,Helvetica,sans-serif;letter-spacing:.2px">&#128203;&nbsp; Tap to copy code &nbsp;&bull;&nbsp; ${pair}</a>
        </td></tr>
      </table>
      <div style="font-size:11.5px;color:#9aa0aa;text-align:center;padding-top:8px;font-family:Arial,Helvetica,sans-serif">Opens a page that copies the code straight to your clipboard</div>
    </td></tr>` : '';

  return `<!doctype html>
<html lang="en" xmlns:v="urn:schemas-microsoft-com:vml">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="x-apple-disable-message-reformatting">
<title>${title}</title>
</head>
<body style="margin:0;padding:0;background:#eceef1;color:#15171a;font-family:Arial,Helvetica,sans-serif;-webkit-font-smoothing:antialiased">
<div style="display:none;max-height:0;overflow:hidden;opacity:0">${preheader}&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;</div>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;background:#eceef1">
<tr><td align="center" style="padding:36px 12px 28px">

  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;max-width:560px;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 1px 3px rgba(10,12,18,.08)">

    <!-- Header (light) -->
    <tr><td style="background:#ffffff;padding:26px 32px 22px;text-align:center">
      <table role="presentation" cellpadding="0" cellspacing="0" align="center"><tr>
        <td style="width:42px;height:42px">${logoTile}</td>
        <td style="padding-left:12px;text-align:left">
          <div style="font-size:20px;font-weight:900;color:#0b0d12;letter-spacing:3px;font-family:Arial,Helvetica,sans-serif">BIEXC</div>
          <div style="font-size:10px;color:#8a9099;letter-spacing:2px;padding-top:2px">SECURE CRYPTO EXCHANGE</div>
        </td>
      </tr></table>
    </td></tr>
    <tr><td style="height:4px;background:#f5b51b;font-size:0">&nbsp;</td></tr>
${copyBar}

    <!-- Body -->
    <tr><td style="padding:26px 32px 8px">
      <div style="font-size:13px;font-weight:700;color:#c9930f;letter-spacing:2px;text-transform:uppercase;padding-bottom:8px">${isReset ? 'Password reset' : isSignup ? 'Account verification' : 'Sign-in confirmation'}</div>
      <h1 style="font-size:26px;line-height:1.25;margin:0 0 12px;color:#0b0d12;font-weight:800;font-family:Arial,Helvetica,sans-serif">${title}</h1>
      <p style="font-size:15px;line-height:1.65;margin:0;color:#5c6370;font-family:Arial,Helvetica,sans-serif">${greet} ${bodyLine}</p>
    </td></tr>

    <!-- Code panel (white cards on light panel) -->
    <tr><td style="padding:22px 32px 0">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" bgcolor="#f4f6f9" style="background:#f4f6f9;border:1px solid #e9ecf1;border-radius:14px">
        <tr><td align="center" style="padding:20px 12px 4px">
          <table role="presentation" cellpadding="0" cellspacing="0" align="center" selectall="true"><tr>${digits}</tr></table>
        </td></tr>
        <tr><td align="center" style="padding:16px 12px 20px">
          <table role="presentation" cellpadding="0" cellspacing="0"><tr>
            <td style="background:#ffffff;border:1px solid #e7dcbb;border-radius:100px;padding:8px 16px;font-size:12.5px;color:#7a6224;font-weight:700;font-family:Arial,Helvetica,sans-serif">&#9202;&nbsp; Expires in 10 minutes</td>
          </tr></table>
        </td></tr>
      </table>
    </td></tr>

    <!-- Security note -->
    <tr><td style="padding:24px 32px 8px">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;border:1px solid #e9ecf1;border-radius:12px">
        <tr><td style="padding:16px 18px 4px;font-size:12px;font-weight:800;color:#0b0d12;letter-spacing:1px;font-family:Arial,Helvetica,sans-serif">KEEP YOUR ACCOUNT SAFE</td></tr>
        <tr><td style="padding:0 18px 14px">
          <p style="font-size:13px;line-height:1.7;margin:8px 0 0;color:#5c6370;font-family:Arial,Helvetica,sans-serif">&#9679;&nbsp; Never share this code with anyone &mdash; not even BIEXC support.<br>&#9679;&nbsp; BIEXC staff will <strong style="color:#3a404b">never</strong> call, email or message you asking for it.<br>&#9679;&nbsp; Didn\u2019t request this? Ignore this email or reset your password to secure your account.</p>
        </td></tr>
      </table>
    </td></tr>
    <tr><td style="padding:14px 32px 32px">
      <p style="font-size:12px;line-height:1.6;color:#9aa0aa;margin:0;font-family:Arial,Helvetica,sans-serif">This code was sent because someone tried to ${isReset ? 'reset the password of a BIEXC account' : isSignup ? 'create a BIEXC account' : 'sign in to a BIEXC account'} with this email address. If it was you, you\u2019re all set.</p>
    </td></tr>

    <!-- Footer (light) -->
    <tr><td style="background:#f7f8fa;border-top:1px solid #eceef2;padding:24px 32px;text-align:center">
      <div style="font-size:12px;font-weight:900;color:#0b0d12;letter-spacing:2px;font-family:Arial,Helvetica,sans-serif;padding-bottom:6px">BIEXC</div>
      <div style="font-size:11px;color:#7c828c;line-height:1.7;font-family:Arial,Helvetica,sans-serif">
        This is an automated security message &mdash; please do not reply.<br>
        Need help? Contact support from the Help section in the BIEXC app.<br>
        &copy; 2026 BIEXC. All rights reserved.
      </div>
    </td></tr>
  </table>

</td></tr>
</table>
</body></html>`;
}

// Tiny page opened by the "Tap to copy" button: copies the code, confirms it.
function copyPage(code) {
  const safe = String(code).replace(/\D/g, '').slice(0, 6);
  return `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>Code copied \u2014 BIEXC</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#eceef1;color:#15171a;font-family:Arial,Helvetica,sans-serif;padding:26px 14px}
.card{max-width:420px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 1px 3px rgba(10,12,18,.08)}
.top{background:#f5b51b;height:4px}
.hd{padding:22px 22px 6px;text-align:center}
.logo{display:inline-block;width:38px;height:38px;line-height:38px;border-radius:10px;background:#f5b51b;color:#0b0d12;font-size:21px;font-weight:900}
.nm{font-size:16px;font-weight:900;letter-spacing:3px;color:#0b0d12;padding-top:8px}
.bd{padding:10px 22px 26px;text-align:center}
.chk{width:62px;height:62px;line-height:62px;margin:6px auto 14px;border-radius:50%;background:#e9f8ef;border:1px solid #b9e6c9;color:#1f9d55;font-size:30px;font-weight:900}
.chk.no{background:#fff6e6;border-color:#f0dcae;color:#b8860b}
h1{font-size:20px;margin:0 0 8px;color:#0b0d12}
p{font-size:13.5px;line-height:1.6;color:#5c6370;margin:0 0 16px}
.code{width:100%;padding:14px 8px;font-size:26px;font-weight:900;letter-spacing:8px;text-align:center;color:#0b0d12;background:#f4f6f9;border:1px solid #e2e6ec;border-radius:12px;font-family:Arial,Helvetica,sans-serif}
.btn{display:block;margin:14px 0 0;padding:13px 10px;background:#f5b51b;color:#0b0d12;font-size:14.5px;font-weight:800;text-decoration:none;border-radius:12px;border:0;width:100%}
.tip{font-size:12px;color:#9aa0aa;padding-top:16px;line-height:1.6}
.ft{background:#f7f8fa;border-top:1px solid #eceef2;padding:14px 20px;text-align:center;font-size:11px;color:#7c828c;line-height:1.6}
</style></head>
<body>
<div class="card">
  <div class="top"></div>
  <div class="hd"><span class="logo">B</span><div class="nm">BIEXC</div></div>
  <div class="bd">
    <div class="chk" id="chk">&#10003;</div>
    <h1 id="t">Code copied</h1>
    <p id="m">Switch back to the BIEXC app and paste it into the verification box.</p>
    <input id="f" class="code" readonly value="${safe}" autocapitalize="off" autocomplete="off" spellcheck="false" aria-label="Verification code">
    <button class="btn" id="again" type="button">Copy again</button>
    <div class="tip">If nothing happened, tap the code above, select it and copy it manually.</div>
  </div>
  <div class="ft">For your security this page never stores your code.<br>&copy; 2026 BIEXC</div>
</div>
<input type="hidden" id="v" value="${safe}">
<script>
var C=(document.getElementById('v')||{}).value||'';
function ok(){document.getElementById('chk').innerHTML='&#10003;';document.getElementById('t').textContent='Code copied';
 document.getElementById('m').textContent='Switch back to the BIEXC app and paste it into the verification box.';}
function fallback(){var done=false;try{var i=document.getElementById('f');i.focus();i.select();i.setSelectionRange(0,99);
 done=document.execCommand('copy');}catch(e){done=false}done?ok():man();}
function man(){document.getElementById('chk').className='chk no';document.getElementById('chk').innerHTML='&#9998;';
 document.getElementById('t').textContent='Tap the code to copy';
 document.getElementById('m').textContent='Your browser blocked automatic copying. Tap the code above, select all and copy.';
 var i=document.getElementById('f');try{i.focus();i.select();}catch(e){}}
function cp(){var p=null;try{if(navigator.clipboard&&window.isSecureContext)p=navigator.clipboard.writeText(C);}catch(e){p=null}
 if(p&&p.then){p.then(ok,fallback);}else{fallback();}}
document.getElementById('again').addEventListener('click',cp);
document.getElementById('f').addEventListener('click',function(){try{this.focus();this.select();}catch(e){}});
if(C){setTimeout(cp,200);}
</script>
</body></html>`;
}

async function sendMail(to, code, purpose, name) {
  const key = process.env.RESEND_API_KEY;
  const from = process.env.MAIL_FROM;
  if (!key || !from) throw new Error('not_configured');
  const logo = logoAsset();
  const r = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from,
      to: [to],
      subject: purpose === 'reset' ? `${code} is your BIEXC password reset code` : `${code} is your BIEXC verification code`,
      html: mailHtml(code, purpose, name, copyBase()),
      text: `${code} is your BIEXC verification code.\n\nIt expires in 10 minutes. Never share this code with anyone — BIEXC staff will never ask for it.\n\nIf you didn't request it, ignore this email.`,
      headers: { 'X-Entity-Ref-ID': `otp-${purpose}-${Date.now()}` },
      ...(logo?.attachment ? { attachments: [logo.attachment] } : {})
    })
  });
  if (!r.ok) throw new Error('resend ' + r.status + ' ' + (await r.text()).slice(0, 200));
}

async function checkPassword(email, password) {
  const k = process.env.FIREBASE_WEB_API_KEY;
  if (!k) throw new Error('not_configured');
  const r = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=${k}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password, returnSecureToken: false })
  });
  return r.ok;
}

export function mountEmailOtp(app, { admin, db, log = console.log }) {
  const L = (m) => log('MAILOTP', m);

  // Page opened by the "Tap to copy code" button inside the email.
  app.get('/copy', (req, res) => {
    const c = String(req.query?.c || '').replace(/\D/g, '');
    if (!/^\d{6}$/.test(c)) {
      return res.status(400).type('html').send(
        '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
        '<title>BIEXC</title><body style="margin:0;background:#eceef1;font-family:Arial,Helvetica,sans-serif;' +
        'display:flex;min-height:100vh;align-items:center;justify-content:center"><div style="background:#fff;' +
        'border-radius:16px;padding:28px 24px;max-width:340px;text-align:center;box-shadow:0 1px 3px rgba(10,12,18,.08)">' +
        '<div style="font-size:30px">&#9888;&#65039;</div><h1 style="font-size:18px;color:#0b0d12;margin:10px 0 6px">' +
        'This link is not valid</h1><p style="font-size:13px;color:#5c6370;line-height:1.6;margin:0">Request a new ' +
        'code from the BIEXC app and use the button in the newest email.</p></div></body>'
      );
    }
    res.set('Cache-Control', 'no-store');
    res.type('html').send(copyPage(c));
  });


  app.post('/api/email-otp/send', async (req, res) => {
    try {
      const email = normEmail(req.body?.email);
      const purpose = req.body?.purpose === 'signup' ? 'signup' : 'login';
      const password = String(req.body?.password || '');
      const name = String(req.body?.name || '').trim().slice(0, 60);
      if (!okEmail(email)) return res.json({ ok: false, error: 'bad_email' });

      const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
      const now = Date.now();
      const hits = (ipHits.get(ip) || []).filter(t => now - t < 3600_000);
      if (hits.length >= 20) return res.json({ ok: false, error: 'rate_limited' });

      const key = email + '|' + purpose;
      const st = store.get(key) || { sent: [] };
      if (st.lastSent && now - st.lastSent < RESEND_MS) return res.json({ ok: false, error: 'too_soon', waitSec: Math.ceil((RESEND_MS - (now - st.lastSent)) / 1000) });
      st.sent = (st.sent || []).filter(t => now - t < 3600_000);
      if (st.sent.length >= MAX_PER_HOUR) return res.json({ ok: false, error: 'rate_limited' });

      if (purpose === 'signup') {
        if (password.length < 8 || password.length > 128) return res.json({ ok: false, error: 'weak_password' });
        const exists = await admin.auth().getUserByEmail(email).then(() => true).catch(() => false);
        if (exists) return res.json({ ok: false, error: 'email_exists' });
        st.password = password; st.name = name;
      } else {
        if (!password) return res.json({ ok: false, error: 'invalid_credentials' });
        if (!(await checkPassword(email, password))) return res.json({ ok: false, error: 'invalid_credentials' });
      }

      const code = gen();
      await sendMail(email, code, purpose, st.name);
      st.code = hash(code); st.exp = now + TTL; st.tries = 0; st.lastSent = now; st.sent.push(now);
      store.set(key, st); hits.push(now); ipHits.set(ip, hits);
      L(`code sent → ${email} (${purpose})`);
      res.json({ ok: true, waitSec: RESEND_MS / 1000 });
    } catch (e) {
      L('send error: ' + e.message);
      res.json({ ok: false, error: e.message === 'not_configured' ? 'not_configured' : 'send_failed' });
    }
  });

  app.post('/api/email-otp/verify', async (req, res) => {
    try {
      const email = normEmail(req.body?.email);
      const purpose = req.body?.purpose === 'signup' ? 'signup' : 'login';
      const code = String(req.body?.code || '').replace(/\D/g, '');
      const key = email + '|' + purpose;
      const st = store.get(key);
      if (!st || !st.code) return res.json({ ok: false, error: 'no_code' });
      if (Date.now() > st.exp) { store.delete(key); return res.json({ ok: false, error: 'expired' }); }
      if (st.tries >= MAX_TRIES) { store.delete(key); return res.json({ ok: false, error: 'too_many_tries' }); }
      if (hash(code) !== st.code) { st.tries++; return res.json({ ok: false, error: 'invalid_code', left: MAX_TRIES - st.tries }); }
      store.delete(key);

      let user;
      if (purpose === 'signup') {
        user = await admin.auth().getUserByEmail(email).catch(() => null);
        if (!user) user = await admin.auth().createUser({ email, password: st.password, displayName: st.name || email.split('@')[0], emailVerified: true });
      } else {
        user = await admin.auth().getUserByEmail(email);
        if (!user.emailVerified) await admin.auth().updateUser(user.uid, { emailVerified: true }).catch(() => {});
      }
      const token = await admin.auth().createCustomToken(user.uid, { login: 'email_otp' });
      if (db) await db.ref(`users/${user.uid}`).update({ email, ...(st.name ? { name: st.name } : {}), lastLogin: Date.now(), loginMethod: 'email_otp', emailVerified: true }).catch(() => {});
      L(`verified ${email} → ${user.uid}`);
      res.json({ ok: true, token });
    } catch (e) {
      L('verify error: ' + e.message);
      res.json({ ok: false, error: 'verify_failed' });
    }
  });

  // ── Forgot password: code by email (Resend) → set new password ─────
  //   POST /api/password-reset/send    { email }                 -> { ok, waitSec }
  //   POST /api/password-reset/confirm { email, code, password } -> { ok, token }
  app.post('/api/password-reset/send', async (req, res) => {
    try {
      const email = normEmail(req.body?.email);
      if (!okEmail(email)) return res.json({ ok: false, error: 'bad_email' });
      const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
      const now = Date.now();
      const hits = (ipHits.get(ip) || []).filter(t => now - t < 3600_000);
      if (hits.length >= 20) return res.json({ ok: false, error: 'rate_limited' });
      const user = await admin.auth().getUserByEmail(email).catch(() => null);
      if (!user) return res.json({ ok: false, error: 'no_account' });
      const key = email + '|reset';
      const st = store.get(key) || { sent: [] };
      if (st.lastSent && now - st.lastSent < RESEND_MS) return res.json({ ok: false, error: 'too_soon', waitSec: Math.ceil((RESEND_MS - (now - st.lastSent)) / 1000) });
      st.sent = (st.sent || []).filter(t => now - t < 3600_000);
      if (st.sent.length >= MAX_PER_HOUR) return res.json({ ok: false, error: 'rate_limited' });
      const code = gen();
      await sendMail(email, code, 'reset', user.displayName || '');
      st.code = hash(code); st.exp = now + TTL; st.tries = 0; st.lastSent = now; st.sent.push(now);
      store.set(key, st); hits.push(now); ipHits.set(ip, hits);
      L(`reset code sent → ${email}`);
      res.json({ ok: true, waitSec: RESEND_MS / 1000 });
    } catch (e) {
      L('reset send error: ' + e.message);
      res.json({ ok: false, error: e.message === 'not_configured' ? 'not_configured' : 'send_failed' });
    }
  });

  app.post('/api/password-reset/confirm', async (req, res) => {
    try {
      const email = normEmail(req.body?.email);
      const code = String(req.body?.code || '').replace(/\D/g, '');
      const password = String(req.body?.password || '');
      if (password.length < 8 || password.length > 128) return res.json({ ok: false, error: 'weak_password' });
      const key = email + '|reset';
      const st = store.get(key);
      if (!st || !st.code) return res.json({ ok: false, error: 'no_code' });
      if (Date.now() > st.exp) { store.delete(key); return res.json({ ok: false, error: 'expired' }); }
      if (st.tries >= MAX_TRIES) { store.delete(key); return res.json({ ok: false, error: 'too_many_tries' }); }
      if (hash(code) !== st.code) { st.tries++; return res.json({ ok: false, error: 'invalid_code', left: MAX_TRIES - st.tries }); }
      store.delete(key);
      const user = await admin.auth().getUserByEmail(email);
      await admin.auth().updateUser(user.uid, { password, emailVerified: true });
      await admin.auth().revokeRefreshTokens(user.uid).catch(() => {});   // log out other devices
      const token = await admin.auth().createCustomToken(user.uid, { login: 'password_reset' });
      if (db) await db.ref(`users/${user.uid}`).update({ passwordChangedAt: Date.now(), lastLogin: Date.now() }).catch(() => {});
      L(`password reset ✓ ${email}`);
      res.json({ ok: true, token });
    } catch (e) {
      L('reset confirm error: ' + e.message);
      res.json({ ok: false, error: 'verify_failed' });
    }
  });
}
