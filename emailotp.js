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
// Order: LOGO_URL env → the server's own /mail/logo.png (serves repo logo.png)
// → logo.png inside the email (inline) → gold "B" badge.
const LOGO_CID = 'biexc-logo';
const LOGO_MIME = { '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif', '.webp': 'image/webp' };
let _logo;                                        // undefined = not looked yet

function logoAsset() {
  if (_logo !== undefined) return _logo;
  const external = String(process.env.LOGO_URL || '').trim();
  if (/^https?:\/\//i.test(external)) return (_logo = { src: external, attachment: null });
  const base = copyBase();
  if (/^https:\/\//i.test(base)) return (_logo = { src: base + '/mail/logo.png', attachment: null });
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
        attachment: { content: buf.toString('base64'), filename: path.basename(file), contentDisposition: 'inline', cid: LOGO_CID },
      });
    } catch { /* bad logo file — fall back to the letter badge */ }
  }
  return (_logo = null);
}

// ── Email design: BIEXC "Clean White" (same look as deposit/withdraw mails).
// Light-only colour scheme so it stays white even when the phone is in dark mode.
const F = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif";
const MONO = "'SF Mono',Menlo,Consolas,'Courier New',monospace";
const WHITE = 'background-color:#ffffff;background-image:linear-gradient(#ffffff,#ffffff);';
const escH = (v) => String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

function mailHtml(code, purpose, name, base) {
  const isSignup = purpose === 'signup';
  const isReset = purpose === 'reset';
  const title = isReset ? 'Reset your password' : isSignup ? 'Verify your email' : 'Your verification code';
  const what = isReset ? 'reset the password of your BIEXC account. Enter it in the app, then choose a new password'
    : isSignup ? 'finish creating your BIEXC account' : 'finish signing in to BIEXC';
  const greet = name ? `Hi ${escH(name)},<br><br>` : '';
  const pair = String(code).replace(/(\d{3})(\d{3})/, '$1 $2');
  const logo = logoAsset();
  const logoTile = logo
    ? `<img src="${logo.src}" width="26" height="26" alt="" style="display:block;width:26px;height:26px;border:0;outline:none;">`
    : `<div style="width:26px;height:26px;border-radius:6px;background:#F0B90B;text-align:center;font-size:15px;font-weight:900;line-height:26px;color:#1E2329;font-family:${F}">B</div>`;
  const copyBtn = base ? `<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:20px;"><tr><td style="background-color:#F0B90B;border-radius:8px;"><a href="${base}/copy?c=${code}" target="_blank" style="display:inline-block;padding:12px 24px;font-size:14px;font-weight:700;color:#1E2329;text-decoration:none;font-family:${F};">Tap to copy code</a></td></tr></table>` : '';
  const note = isReset
    ? 'Didn\u2019t ask for this? Ignore this email \u2014 your password stays the same.'
    : 'Never share this code. BIEXC staff will never ask for it.';

  return `<!doctype html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="x-apple-disable-message-reformatting">
<meta name="color-scheme" content="light only"><meta name="supported-color-schemes" content="light only">
<title>${title}</title>
<style>:root{color-scheme:light only;supported-color-schemes:light only;}
body,table,td,div,p,a,span{color-scheme:light only;}
@media (prefers-color-scheme:dark){body,.bx-page{background:#f5f5f5 !important;}.bx-card,.bx-body{background:#ffffff !important;}.bx-foot{background:#fafafa !important;}.bx-t{color:#1E2329 !important;}.bx-s{color:#474D57 !important;}.bx-m{color:#707A8A !important;}}
[data-ogsc] .bx-t{color:#1E2329 !important;}[data-ogsc] .bx-s{color:#474D57 !important;}[data-ogsb] .bx-card,[data-ogsb] .bx-body{background:#ffffff !important;}[data-ogsb] .bx-page{background:#f5f5f5 !important;}
@media (max-width:520px){.bx-pad,.bx-foot,.bx-head{padding-left:18px !important;padding-right:18px !important;}.bx-h1{font-size:20px !important;}.bx-code{font-size:28px !important;letter-spacing:6px !important;}}
a{text-decoration:none;}</style>
</head>
<body class="bx-page" style="margin:0;padding:0;background-color:#f5f5f5;font-family:${F};-webkit-font-smoothing:antialiased;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;">${isReset ? 'Your BIEXC reset code is ' : 'Your BIEXC code is '}${pair}&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="bx-page" style="background-color:#f5f5f5;"><tr><td align="center" style="padding:28px 12px;">
  <table role="presentation" width="560" cellpadding="0" cellspacing="0" border="0" class="bx-card" style="max-width:560px;width:100%;${WHITE}border:1px solid #eaecef;border-radius:12px;overflow:hidden;">
    <tr><td class="bx-body bx-head" style="${WHITE}padding:18px 28px;border-bottom:3px solid #F0B90B;">
      <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
        <td style="vertical-align:middle;padding-right:9px;">${logoTile}</td>
        <td class="bx-t" style="vertical-align:middle;font-size:19px;font-weight:800;letter-spacing:2.5px;color:#1E2329;font-family:${F};">BIEXC</td>
      </tr></table>
    </td></tr>
    <tr><td class="bx-body bx-pad" style="${WHITE}padding:28px;">
      <div class="bx-t bx-h1" style="font-size:22px;font-weight:700;line-height:1.3;color:#1E2329;margin:0 0 10px;">${title}</div>
      <div class="bx-s" style="font-size:14px;line-height:1.65;color:#474D57;">${greet}Use this code to ${what}. It expires in 10 minutes.</div>
      <div class="bx-t bx-code" style="margin:24px 0 4px;font-size:34px;font-weight:700;letter-spacing:8px;color:#1E2329;font-family:${MONO};">${pair}</div>
      ${copyBtn}
      <div class="bx-m" style="margin-top:20px;font-size:12px;color:#707A8A;">${note}</div>
    </td></tr>
    <tr><td class="bx-foot" style="background-color:#fafafa;padding:16px 28px;font-size:11.5px;line-height:1.7;color:#929AA5;font-family:${F};">
      <span class="bx-m" style="color:#929AA5;">Stay safe: BIEXC will never ask for your password or verification code.<br>
      Need help? Open <b style="color:#707A8A;">Customer Care</b> in the BIEXC app &middot; &copy; ${new Date().getFullYear()} BIEXC</span>
    </td></tr>
  </table>
</td></tr></table>
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
      text: `${code} is your BIEXC ${purpose === 'reset' ? 'password reset' : 'verification'} code.\n\nIt expires in 10 minutes. Never share this code with anyone — BIEXC staff will never ask for it.\n\nIf you didn't request it, ignore this email.`,
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
