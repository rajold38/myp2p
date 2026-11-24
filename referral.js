// ════════════════════════════════════════════════════════════════════
// REFER & EARN — server-side, tamper-proof
//
//  Rules
//   • Every user gets one permanent code  BX + 6 chars   (referralCodes/{CODE} = fuid)
//   • A new account can be linked to a referrer ONCE, within BIND_WINDOW of sign-up,
//     never to itself, never if it already made a deposit.
//   • Reward unlocks when the friend (a) has KYC done and (b) has approved deposits
//     totalling ≥ MIN_DEPOSIT USD within DEPOSIT_DAYS of joining.
//     → referrer gets REF_REWARD USDT, friend gets FRIEND_REWARD USDT cashback.
//   • Paid exactly once (transaction lock on users/{friend}/referral/rewarded).
//
//  Data
//   users/{fuid}/refer            { code, createdAt, invited, rewarded, earned }
//   users/{fuid}/refer/invites/{friendFuid}
//        { name, uid, ts, status: registered|kyc|deposited|rewarded|expired, deposit, reward }
//   users/{friend}/referral       { by, byUid, byName, code, at, depositUsd, rewarded, rewardedAt }
// ════════════════════════════════════════════════════════════════════

const REF_REWARD    = Number(process.env.REF_REWARD || 10);
const FRIEND_REWARD = Number(process.env.REF_FRIEND_REWARD || 15);
const MIN_DEPOSIT   = Number(process.env.REF_MIN_DEPOSIT || 50);
const DEPOSIT_DAYS  = Number(process.env.REF_DEPOSIT_DAYS || 14);
const BIND_WINDOW   = 7 * 86400e3;
const STABLE = new Set(['USDT', 'USDC', 'FDUSD', 'BUSD', 'DAI', 'TUSD']);
const POOL = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const r2 = (n) => Math.round((+n || 0) * 100) / 100;
const clean = (c) => String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 16);
const mask = (s) => { s = String(s || 'User').trim(); return s.length <= 2 ? s[0] + '***' : s[0] + '***' + s[s.length - 1]; };

export function kycDone(u) {
  const k = u?.kycLevels || {};
  return (k.level || 0) >= 2 || k.l2status === 'PENDING' || k.l2status === 'APPROVED' || u?.kycStatus === 'APPROVED';
}

async function usdValue(coin, amt) {
  const C = String(coin || 'USDT').toUpperCase();
  if (STABLE.has(C)) return +amt || 0;
  try {
    const r = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${C}USDT`);
    const j = await r.json();
    const p = parseFloat(j.price);
    if (p > 0) return (+amt || 0) * p;
  } catch (e) {}
  return 0;
}

export function createReferral({ admin, db, log = () => {}, mutateBalance, pushHistory, pushNotif, tgSend }) {
  const L = (m) => log('REFER', m);

  async function authUser(req) {
    const h = req.headers.authorization || '';
    const tok = (h.startsWith('Bearer ') ? h.slice(7) : '') || req.body?.idToken || '';
    if (!tok) return null;
    try { return (await admin.auth().verifyIdToken(tok)).uid; } catch (e) { return null; }
  }

  async function ensureCode(fuid) {
    const cur = (await db.ref(`users/${fuid}/refer/code`).once('value')).val();
    if (cur) {
      const own = (await db.ref(`referralCodes/${cur}`).once('value')).val();
      if (!own) await db.ref(`referralCodes/${cur}`).set(fuid);
      if (!own || own === fuid) return cur;
    }
    for (let i = 0; i < 12; i++) {
      let c = 'BX';
      for (let j = 0; j < 6; j++) c += POOL[Math.floor(Math.random() * POOL.length)];
      const tx = await db.ref(`referralCodes/${c}`).transaction((v) => (v ? undefined : fuid));
      if (tx.committed) {
        await db.ref(`users/${fuid}/refer`).update({ code: c, createdAt: Date.now() });
        return c;
      }
    }
    throw new Error('code_gen_failed');
  }

  async function lookup(code) {
    const c = clean(code);
    if (c.length < 4) return null;
    const fuid = (await db.ref(`referralCodes/${c}`).once('value')).val();
    if (!fuid) return null;
    const u = (await db.ref(`users/${fuid}`).once('value')).val();
    if (!u || u.banned) return null;
    return { fuid, code: c, user: u };
  }

  async function bind(fuid, code) {
    const ref = await lookup(code);
    if (!ref) return { ok: false, error: 'invalid_code' };
    if (ref.fuid === fuid) return { ok: false, error: 'self' };
    const me = (await db.ref(`users/${fuid}`).once('value')).val();
    if (!me) return { ok: false, error: 'no_user' };
    if (me.referral?.by) return { ok: false, error: 'already', by: mask(me.referral.byName) };
    if (me.createdAt && Date.now() - me.createdAt > BIND_WINDOW) return { ok: false, error: 'too_late' };
    const hist = Object.values(me.history || {});
    if (hist.some((h) => /DEPOSIT/i.test(h.type || '') && h.status === 'COMPLETED')) return { ok: false, error: 'has_deposit' };
    // referrer must not be referred by me (no loops)
    if (ref.user.referral?.by === fuid) return { ok: false, error: 'loop' };

    const now = Date.now();
    const tx = await db.ref(`users/${fuid}/referral`).transaction((cur) => (cur && cur.by ? undefined : {
      by: ref.fuid, byUid: ref.user.uid || '', byName: ref.user.name || 'User', code: ref.code, at: now, depositUsd: 0, rewarded: false,
    }));
    if (!tx.committed) return { ok: false, error: 'already' };
    await db.ref(`users/${ref.fuid}/refer/invites/${fuid}`).set({
      name: mask(me.name || me.email || 'Friend'), uid: me.uid || '', ts: now,
      status: kycDone(me) ? 'kyc' : 'registered', deposit: 0, reward: 0,
    });
    await db.ref(`users/${ref.fuid}/refer/invited`).transaction((v) => (v || 0) + 1);
    await pushNotif(ref.fuid, { title: 'New referral joined 🎉', body: `${mask(me.name || 'A friend')} joined BIEXC with your code. You earn ${REF_REWARD} USDT when they finish KYC and deposit $${MIN_DEPOSIT}+.`, type: 'INFO' }).catch(() => {});
    await pushNotif(fuid, { title: 'Invite code applied ✓', body: `Deposit $${MIN_DEPOSIT}+ within ${DEPOSIT_DAYS} days after KYC to get ${FRIEND_REWARD} USDT welcome cashback.`, type: 'INFO' }).catch(() => {});
    tgSend?.(`🎁 *NEW REFERRAL*\n\n👤 Friend: \`${me.uid || '—'}\` ${me.name || ''}\n🤝 Referrer: \`${ref.user.uid || '—'}\` ${ref.user.name || ''}\n🏷 Code: \`${ref.code}\``).catch?.(() => {});
    L(`bind ${me.uid} → ${ref.user.uid} (${ref.code})`);
    return { ok: true, by: mask(ref.user.name), code: ref.code };
  }

  /** Re-evaluate one friend: update invite status and pay once when eligible. */
  async function evaluate(fuid) {
    const me = (await db.ref(`users/${fuid}`).once('value')).val();
    const R = me?.referral;
    if (!R?.by || R.rewarded) return;
    const inv = `users/${R.by}/refer/invites/${fuid}`;
    const expired = Date.now() - (R.at || me.createdAt || Date.now()) > DEPOSIT_DAYS * 86400e3 && (R.depositUsd || 0) < MIN_DEPOSIT;
    const k = kycDone(me);
    const depOk = (R.depositUsd || 0) >= MIN_DEPOSIT;
    const status = expired ? 'expired' : depOk ? (k ? 'rewarded' : 'deposited') : k ? 'kyc' : 'registered';
    const curInv = (await db.ref(inv).once('value')).val() || {};
    const want = { status: status === 'rewarded' ? 'deposited' : status, deposit: r2(R.depositUsd || 0) };
    if (curInv.status !== want.status || curInv.deposit !== want.deposit) await db.ref(inv).update(want);
    if (expired || !depOk || !k) return;

    const lock = await db.ref(`users/${fuid}/referral/rewarded`).transaction((v) => (v ? undefined : true));
    if (!lock.committed) return;
    const now = Date.now();
    await db.ref(`users/${fuid}/referral`).update({ rewardedAt: now });
    const a = await mutateBalance(R.by, 'USDT', REF_REWARD);
    const b = FRIEND_REWARD > 0 ? await mutateBalance(fuid, 'USDT', FRIEND_REWARD) : null;
    const refUser = (await db.ref(`users/${R.by}`).once('value')).val() || {};
    await pushHistory(R.by, { type: 'REFERRAL_REWARD', coin: 'USDT', amt: REF_REWARD, status: 'COMPLETED', uid: refUser.uid, sender: 'BIEXC', note: `Referral reward — ${mask(me.name)}` });
    if (b) await pushHistory(fuid, { type: 'WELCOME_BONUS', coin: 'USDT', amt: FRIEND_REWARD, status: 'COMPLETED', uid: me.uid, sender: 'BIEXC', note: 'Referral welcome cashback' });
    await db.ref(inv).update({ status: 'rewarded', reward: REF_REWARD, rewardedAt: now });
    await db.ref(`users/${R.by}/refer/rewarded`).transaction((v) => (v || 0) + 1);
    await db.ref(`users/${R.by}/refer/earned`).transaction((v) => r2((v || 0) + REF_REWARD));
    await pushNotif(R.by, { title: `+${REF_REWARD} USDT referral reward 💰`, body: `${mask(me.name)} completed KYC and deposited. Reward added to your wallet.`, type: 'INFO' }).catch(() => {});
    if (b) await pushNotif(fuid, { title: `+${FRIEND_REWARD} USDT welcome cashback 🎁`, body: 'Thanks for joining with an invite. Cashback added to your wallet.', type: 'INFO' }).catch(() => {});
    tgSend?.(`💰 *REFERRAL PAID*\n\n🤝 Referrer \`${refUser.uid || '—'}\` +${REF_REWARD} USDT (${a ? a.oldBal + ' → ' + a.newBal : 'failed'})\n👤 Friend \`${me.uid || '—'}\` +${FRIEND_REWARD} USDT`).catch?.(() => {});
    L(`paid ${refUser.uid} +${REF_REWARD} / ${me.uid} +${FRIEND_REWARD}`);
  }

  /** Call after an approved deposit. */
  async function onDeposit(fuid, coin, amt) {
    try {
      const R = (await db.ref(`users/${fuid}/referral`).once('value')).val();
      if (!R?.by || R.rewarded) return;
      const usd = await usdValue(coin, amt);
      if (usd > 0) await db.ref(`users/${fuid}/referral/depositUsd`).transaction((v) => r2((v || 0) + usd));
      await evaluate(fuid);
    } catch (e) { L(`onDeposit err ${e.message}`); }
  }

  async function summary(fuid) {
    const code = await ensureCode(fuid);
    const u = (await db.ref(`users/${fuid}`).once('value')).val() || {};
    const invites = Object.entries(u.refer?.invites || {}).map(([k, v]) => ({ id: k, ...v })).sort((a, b) => (b.ts || 0) - (a.ts || 0));
    const canBind = !u.referral?.by && (!u.createdAt || Date.now() - u.createdAt <= BIND_WINDOW);
    return {
      ok: true, code,
      invited: invites.length,
      rewarded: invites.filter((i) => i.status === 'rewarded').length,
      pending: invites.filter((i) => i.status !== 'rewarded' && i.status !== 'expired').length,
      earned: r2(invites.reduce((s, i) => s + (i.status === 'rewarded' ? +i.reward || 0 : 0), 0)),
      invites,
      referredBy: u.referral?.by ? { name: mask(u.referral.byName), code: u.referral.code, rewarded: !!u.referral.rewarded, depositUsd: r2(u.referral.depositUsd || 0) } : null,
      canBind,
      rules: { REF_REWARD, FRIEND_REWARD, MIN_DEPOSIT, DEPOSIT_DAYS },
    };
  }

  function mount(app) {
    app.get('/api/referral/check', async (req, res) => {
      try {
        const r = await lookup(req.query.code);
        if (!r) return res.json({ ok: true, valid: false });
        res.json({ ok: true, valid: true, code: r.code, name: mask(r.user.name) });
      } catch (e) { res.json({ ok: false, error: 'server' }); }
    });
    app.get('/api/referral/rules', (_req, res) => res.json({ ok: true, REF_REWARD, FRIEND_REWARD, MIN_DEPOSIT, DEPOSIT_DAYS }));
    app.post('/api/referral/me', async (req, res) => {
      const fuid = await authUser(req);
      if (!fuid) return res.status(401).json({ ok: false, error: 'auth' });
      try { res.json(await summary(fuid)); } catch (e) { L(`me err ${e.message}`); res.json({ ok: false, error: 'server' }); }
    });
    app.post('/api/referral/bind', async (req, res) => {
      const fuid = await authUser(req);
      if (!fuid) return res.status(401).json({ ok: false, error: 'auth' });
      try {
        // the user node may be created a moment after sign-in
        for (let i = 0; i < 6; i++) { if ((await db.ref(`users/${fuid}/createdAt`).once('value')).exists()) break; await new Promise((r) => setTimeout(r, 700)); }
        res.json(await bind(fuid, req.body?.code));
      } catch (e) { L(`bind err ${e.message}`); res.json({ ok: false, error: 'server' }); }
    });
  }

  return { mount, onDeposit, evaluate, ensureCode, kycDone, mask };
}
