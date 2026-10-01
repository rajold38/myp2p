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

function mailHtml(code, purpose) {
  const isSignup = purpose === 'signup';
  const title = isSignup ? 'Verify your email' : 'Confirm your sign in';
  const preheader = isSignup ? 'Complete your BIEXC account setup.' : 'Use this secure code to sign in to BIEXC.';
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${title}</title></head>
<body style="margin:0;padding:0;background:#f2f3f5;color:#15171a;font-family:Arial,Helvetica,sans-serif">
<div style="display:none;max-height:0;overflow:hidden;opacity:0">${preheader}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;background:#f2f3f5"><tr><td align="center" style="padding:32px 14px">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;max-width:560px;background:#ffffff;border:1px solid #e2e4e8;border-radius:12px;overflow:hidden">
<tr><td style="height:5px;background:#f5b51b;font-size:0">&nbsp;</td></tr>
<tr><td style="padding:28px 32px 20px">
<table role="presentation" cellpadding="0" cellspacing="0"><tr><td style="width:38px;height:38px;border-radius:8px;background:#f5b51b;text-align:center;font-size:19px;font-weight:900;color:#111111">B</td><td style="padding-left:10px;font-size:20px;font-weight:900;color:#111318;letter-spacing:1px">BIEXC</td></tr></table>
<h1 style="font-size:25px;line-height:1.25;margin:28px 0 8px;color:#111318;font-weight:800">${title}</h1>
<p style="font-size:15px;line-height:1.6;margin:0;color:#666d78">${isSignup ? 'Enter this one-time code to finish creating your account.' : 'Enter this one-time code to securely access your account.'}</p>
</td></tr>
<tr><td style="padding:0 32px"><div style="background:#111318;border-radius:10px;padding:22px 12px;text-align:center;color:#ffffff;font-size:34px;line-height:1;font-weight:800;letter-spacing:9px">${code}</div></td></tr>
<tr><td style="padding:18px 32px 28px">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#fff8e5;border-left:3px solid #f5b51b;border-radius:6px"><tr><td style="padding:12px 14px;color:#5f5742;font-size:13px;line-height:1.55"><strong style="color:#2a261d">Valid for 10 minutes.</strong> Never share this code. BIEXC support will never ask for it.</td></tr></table>
<p style="font-size:12px;line-height:1.6;color:#8a9099;margin:20px 0 0">If you did not request this, you can safely ignore this email. No changes will be made to your account.</p>
</td></tr>
<tr><td style="border-top:1px solid #eceef1;padding:18px 32px;color:#969ca5;font-size:11px;line-height:1.5">Security notification from BIEXC<br>Automated email — please do not reply.</td></tr>
</table>
</td></tr></table></body></html>`;
}

async function sendMail(to, code, purpose) {
  const key = process.env.RESEND_API_KEY;
  const from = process.env.MAIL_FROM;
  if (!key || !from) throw new Error('not_configured');
  const r = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from, to: [to], subject: `${code} is your BIEXC verification code`, html: mailHtml(code, purpose), text: `${code} is your BIEXC verification code. It expires in 10 minutes.` })
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
      await sendMail(email, code, purpose);
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
}
