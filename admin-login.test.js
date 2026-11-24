import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createAdminLogin, LOGIN_LINK_TTL_MS } from './admin-login.js';

function fixture(options = {}) {
  let time = 1000;
  const signed = [], audits = [];
  const user = { uid: '12345678', name: 'Test user' };
  const cb = { from: { id: 7 }, message: { chat: { id: 7, type: 'private' } } };
  const found = { fuid: 'firebase-target-user', user };
  const login = createAdminLogin({
    db: { ref: () => ({ push: async (entry) => { audits.push(entry); }, once: async () => ({ val: () => user }) }) },
    auth: { getUser: async () => ({ disabled: false }), createCustomToken: async (uid) => { signed.push(uid); return 'test-custom-token'; } },
    telegram: async () => ({ ok: true, result: { status: 'member' } }),
    chatId: '7', appUrl: 'https://biexc.in/', now: () => time, ...options
  });
  return { login, cb, found, signed, audits, user, advance: (ms) => { time += ms; } };
}
const secretOf = (grant) => new URL(grant.url).hash.split('=')[1];

test('clicking a generated link grants exactly the selected user account', async () => {
  const f = fixture();
  const grant = await f.login.issue(f.cb, f.found);
  const result = await f.login.consume(secretOf(grant));
  assert.equal(result.fuid, 'firebase-target-user');
  assert.deepEqual(f.signed, ['firebase-target-user']);
  assert.equal(new URL(grant.url).origin, 'https://biexc.in');
  assert.deepEqual(f.audits.map((a) => a.action), ['ISSUED', 'REDEEMED']);
});
test('non-administrators cannot generate account access links', async () => {
  const f = fixture();
  f.cb.from.id = 9;
  await assert.rejects(f.login.issue(f.cb, f.found), /Unauthorized/);
  assert.equal(f.audits.length, 0);
});
test('ordinary members in an admin group cannot generate links', async () => {
  const f = fixture(); f.cb.message.chat.type = 'supergroup';
  await assert.rejects(f.login.issue(f.cb, f.found), /Unauthorized/);
});
test('group administrators can generate links', async () => {
  const f = fixture({ telegram: async () => ({ ok: true, result: { status: 'administrator' } }) });
  f.cb.message.chat.type = 'supergroup';
  assert.ok((await f.login.issue(f.cb, f.found)).url);
});
test('simultaneous uses of one link sign in only once', async () => {
  const f = fixture(); const grant = await f.login.issue(f.cb, f.found);
  const results = await Promise.allSettled([f.login.consume(secretOf(grant)), f.login.consume(secretOf(grant))]);
  assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1);
  assert.equal(f.signed.length, 1);
});
test('login links expire after five minutes', async () => {
  const f = fixture(); const grant = await f.login.issue(f.cb, f.found);
  assert.equal(LOGIN_LINK_TTL_MS, 300000); f.advance(LOGIN_LINK_TTL_MS);
  await assert.rejects(f.login.consume(secretOf(grant)), /expired/);
  assert.equal(f.signed.length, 0);
});
test('regenerating a link invalidates the previous link for that user', async () => {
  const f = fixture(); const old = await f.login.issue(f.cb, f.found);
  const next = await f.login.issue(f.cb, f.found);
  await assert.rejects(f.login.consume(secretOf(old)), /expired/);
  assert.equal((await f.login.consume(secretOf(next))).fuid, f.found.fuid);
});
test('accounts banned after generation cannot be accessed', async () => {
  const f = fixture(); const grant = await f.login.issue(f.cb, f.found);
  f.user.banned = true;
  await assert.rejects(f.login.consume(secretOf(grant)), /unavailable/);
  assert.equal(f.signed.length, 0);
});
test('server restart invalidates all earlier links', async () => {
  const first = fixture(), restarted = fixture();
  const grant = await first.login.issue(first.cb, first.found);
  await assert.rejects(restarted.login.consume(secretOf(grant)), /expired/);
});
test('database never receives the bearer secret', async () => {
  const f = fixture(); const grant = await f.login.issue(f.cb, f.found);
  assert.equal(JSON.stringify(f.audits).includes(secretOf(grant)), false);
});