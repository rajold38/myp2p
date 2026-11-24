import crypto from 'node:crypto';

export const LOGIN_LINK_TTL_MS = 5 * 60 * 1000;

// Process-local bearer grants: no credentials or login secrets enter Firebase.
// Restarting the bot invalidates every outstanding link (fail closed).
export function createAdminLogin({ db, auth, telegram, chatId, appUrl, now = Date.now }) {
  const grants = new Map();

  async function authorized(cb) {
    if (!cb?.from?.id || cb.from.is_bot || String(cb.message?.chat?.id) !== String(chatId)) return false;
    if (cb.message.chat.type === 'private') return String(cb.from.id) === String(chatId);
    const member = await telegram('getChatMember', { chat_id: chatId, user_id: cb.from.id });
    return member?.ok === true && ['creator', 'administrator'].includes(member.result?.status);
  }

  async function issue(cb, found) {
    if (!(await authorized(cb))) throw new Error('Unauthorized');
    if (!found?.fuid || !found.user || found.user.banned || found.user.frozen) throw new Error('Account unavailable');
    const account = await auth.getUser(found.fuid);
    if (account.disabled) throw new Error('Account unavailable');
    const url = new URL(appUrl);
    if (url.protocol !== 'https:') throw new Error('Configure ADMIN_LOGIN_APP_URL with your HTTPS frontend URL');
    url.username = ''; url.password = ''; url.search = '';
    for (const [key, value] of grants) if (value.expiresAt <= now() || value.fuid === found.fuid) grants.delete(key);
    if (grants.size >= 500) throw new Error('Too many active login links');
    const secret = crypto.randomBytes(32).toString('base64url');
    const hash = crypto.createHash('sha256').update(secret).digest('hex');
    const expiresAt = now() + LOGIN_LINK_TTL_MS;
    // Write the audit before issuing; audit failure must not silently grant access.
    await db.ref('botMeta/adminLoginAudit').push({ action: 'ISSUED', fuid: found.fuid, by: String(cb.from.id), at: now(), expiresAt });
    grants.set(hash, { fuid: found.fuid, by: String(cb.from.id), expiresAt });
    url.hash = 'admin-login=' + secret;
    return { url: url.href, expiresAt };
  }

  async function consume(secret) {
    if (typeof secret !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(secret)) throw new Error('Link expired or already used');
    const hash = crypto.createHash('sha256').update(secret).digest('hex');
    const grant = grants.get(hash);
    // Claim synchronously before any await: simultaneous clicks cannot both win.
    grants.delete(hash);
    if (!grant || grant.expiresAt <= now()) throw new Error('Link expired or already used');
    const user = (await db.ref('users/' + grant.fuid).once('value')).val();
    const account = await auth.getUser(grant.fuid);
    if (!user || user.banned || user.frozen || account.disabled) throw new Error('Account unavailable');
    await db.ref('botMeta/adminLoginAudit').push({ action: 'REDEEMED', fuid: grant.fuid, by: grant.by, at: now() });
    const token = await auth.createCustomToken(grant.fuid);
    return { token, fuid: grant.fuid };
  }

  function mount(app) {
    app.post('/api/admin-login/consume', async (req, res) => {
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Referrer-Policy', 'no-referrer');
      try { res.json({ ok: true, ...(await consume(req.body?.secret)) }); }
      catch { res.status(400).json({ ok: false, error: 'This login link expired, was already used, or the account is unavailable. Generate a new link in Telegram.' }); }
    });
  }
  return { issue, consume, mount };
}