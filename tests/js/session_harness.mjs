/**
 * Executable harness for index.html's authentication session.
 *
 * tests/test_auth_session.py slices the SESSION CORE + SESSION ENTRY blocks out
 * of index.html and substitutes them for the two markers below, so every
 * scenario here runs the SHIPPED code — not a re-implementation of it. One node
 * process per scenario (`--scenario name`); `--store path` persists the fake
 * localStorage across processes, which is what makes the "reload the browser"
 * scenarios real reloads rather than an in-process re-read.
 *
 * Run one directly:
 *   python -m pytest tests/test_auth_session.py     (normal)
 *   node <tmpfile> --scenario logout_clears_everything
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';

// ── argv ───────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const arg = (n) => { const i = argv.indexOf(n); return i < 0 ? null : argv[i + 1]; };
const SCENARIO = arg('--scenario');
const STORE = arg('--store');

// ── fake localStorage ──────────────────────────────────────────────────────
// Stored keys are own ENUMERABLE properties, because the core walks
// `Object.keys(localStorage)` to find every partition; the methods are not.
const localStorage = {};
Object.defineProperties(localStorage, {
  getItem: { value: (k) => (Object.prototype.hasOwnProperty.call(localStorage, k) ? String(localStorage[k]) : null) },
  setItem: { value: (k, v) => { localStorage[String(k)] = String(v); } },
  removeItem: { value: (k) => { delete localStorage[k]; } },
});
if (STORE && fs.existsSync(STORE)) Object.assign(localStorage, JSON.parse(fs.readFileSync(STORE, 'utf8')));
const saveStore = () => { if (STORE) fs.writeFileSync(STORE, JSON.stringify({ ...localStorage })); };

// ── fake network ───────────────────────────────────────────────────────────
const API_URL = 'https://api.test';
const calls = [];
let ROUTER = async () => { throw new Error('no route installed'); };
const route = (fn) => { ROUTER = fn; };
const countCalls = (needle) => calls.filter((c) => c === needle).length;
const res = (status, body) => ({ status, ok: status >= 200 && status < 300, json: async () => body });
const revokes = [];                      // Authorization headers seen by /auth/logout
function fetch(url, init) {
  const path = String(url).slice(API_URL.length);
  calls.push(path);
  if (path === '/auth/logout') revokes.push(((init || {}).headers || {}).Authorization || null);
  return ROUTER(path, init);
}
function defer() {
  let resolve, reject;
  const promise = new Promise((a, b) => { resolve = a; reject = b; });
  return { promise, resolve, reject };
}
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); };

// ── stubs for everything the extracted blocks call outside themselves ──────
const ui = { rendered: [], toasts: [], feeds: 0, sidebarClosed: 0, profiles: [], prefs: 0 };
function renderAuthState(opts) { ui.rendered.push(opts || {}); }
function toast(msg) { ui.toasts.push(String(msg)); }
function sbClose() { ui.sidebarClosed++; }
function loadFeed() { ui.feeds++; }
function maybeAutoEnablePush() {}
function updateProfileUI(d) { ui.profiles.push(d); }
function loadUserPrefs() { ui.prefs++; }
let _autoPushTried = false;

// ── payloads ───────────────────────────────────────────────────────────────
const PAIR = (uid, n = 1) => ({
  token: `access-${uid}-${n}`, access_token: `access-${uid}-${n}`,
  refresh_token: `refresh-${uid}-${n}`, user_id: uid, name: `User ${uid}`,
});

// ── storage assertions ─────────────────────────────────────────────────────
const storedPartitions = () => Object.keys(localStorage)
  .filter((k) => k.startsWith('sb21:u:'))
  .map((k) => [k, JSON.parse(localStorage[k])]);

function assertNoStoredCredentials(where) {
  const parts = storedPartitions();
  assert.ok(parts.length, `${where}: expected at least one partition on disk`);
  for (const [k, v] of parts) {
    assert.equal(v.token ?? null, null, `${where}: ${k} still holds an access token`);
    assert.equal(v.refresh ?? null, null, `${where}: ${k} still holds a refresh token`);
    assert.equal(v.userId ?? null, null, `${where}: ${k} still holds a user id`);
    assert.equal(v.meCache ?? null, null, `${where}: ${k} still holds a cached identity`);
  }
}

/* __SESSION_CORE__ */

/* __SESSION_ENTRY__ */

// ═══ SCENARIOS ═════════════════════════════════════════════════════════════
// Each one is independent: a fresh process, and (unless --store says otherwise)
// a fresh empty localStorage.

const signIn = async (uid, n = 1) => {
  route(async (p) => (p === '/login' ? res(200, PAIR(uid, n)) : res(404, {})));
  const gen = newAuthAttempt();
  const data = await api('/login', { method: 'POST', body: '{}' });
  return finishSignIn(data, gen, 'hi');
};

const SCENARIOS = {
  // ── Successful login ─────────────────────────────────────────────────────
  async login_success() {
    assert.equal(await signIn(7), true);
    assert.equal(ST.token, 'access-7-1');
    assert.equal(ST.refresh, 'refresh-7-1');
    assert.equal(ST.userId, 7);
    assert.equal(isSignedIn(), true);
    assert.equal(localStorage.getItem('sb21:cur'), '7');
    assert.equal(JSON.parse(localStorage.getItem('sb21:u:7')).token, 'access-7-1');
    assert.equal(ui.rendered.length, 1, 'the auth UI is derived once, by finishSignIn');
    assert.deepEqual(ui.toasts, ['hi']);
  },

  // ── Successful signup ────────────────────────────────────────────────────
  async signup_success() {
    route(async (p) => (p === '/signup' ? res(200, { ...PAIR(9), message: 'Account created' }) : res(404, {})));
    const gen = newAuthAttempt();
    const data = await api('/signup', { method: 'POST', body: '{}' });
    assert.equal(finishSignIn(data, gen, 'welcome'), true);
    assert.equal(ST.userId, 9);
    assert.equal(ST.token, 'access-9-1');
    assert.equal(localStorage.getItem('sb21:cur'), '9');
  },

  // ── Failed login leaves nothing behind ───────────────────────────────────
  async failed_login_no_partial_session() {
    route(async () => res(401, { detail: 'Invalid credentials' }));
    const gen = newAuthAttempt();
    let err = null;
    try {
      const data = await api('/login', { method: 'POST', body: '{}' });
      finishSignIn(data, gen, 'hi');
    } catch (e) { err = e; }
    assert.equal(err.message, 'Invalid credentials');
    assert.equal(isSignedIn(), false);
    assert.equal(ST.token, null);
    assert.equal(_curUid, 'anon');
    // …and a 401 from /login must not spend a refresh attempt.
    assert.equal(countCalls('/auth/refresh'), 0);
  },

  // ── A 200 that carries no session is still a failure ─────────────────────
  async tokenless_response_is_rejected() {
    route(async () => res(200, { message: 'ok', user_id: 4 }));
    const gen = newAuthAttempt();
    const data = await api('/login', { method: 'POST', body: '{}' });
    assert.throws(() => finishSignIn(data, gen, 'hi'), /no session was returned/);
    assert.equal(isSignedIn(), false);
    assert.equal(ui.rendered.length, 0, 'no signed-in UI over a session that does not exist');
  },

  // ── Normal logout ────────────────────────────────────────────────────────
  async logout_clears_everything() {
    await signIn(7);
    ST.likes = [1, 2, 3]; ST.feedCats = ['tech']; ST.meCache = { name: 'User 7' }; persist();
    // A partition left behind by an earlier account, still holding live tokens.
    localStorage.setItem('sb21:u:99', JSON.stringify({ token: 'old-access', refresh: 'old-refresh', userId: 99, likes: [42] }));
    signOut();
    assert.equal(isSignedIn(), false);
    assert.equal(ST.token, null);
    assert.equal(ST.refresh, null);
    assert.equal(ST.userId, null);
    assert.equal(ST.username, '');
    assert.equal(_curUid, 'anon');
    assert.equal(localStorage.getItem('sb21:cur'), 'anon');
    assertNoStoredCredentials('after signOut');
    // Non-sensitive data in the account's own partition survives.
    const u7 = JSON.parse(localStorage.getItem('sb21:u:7'));
    assert.deepEqual(u7.likes, [1, 2, 3]);
    assert.deepEqual(u7.feedCats, ['tech']);
    assert.deepEqual(JSON.parse(localStorage.getItem('sb21:u:99')).likes, [42],
      'an older account keeps its own data, minus the credentials');
    // The logged-out screen is asked for explicitly, and the sidebar closes.
    assert.equal(ui.rendered.at(-1).showSignIn, true);
    assert.equal(ui.sidebarClosed, 1);
    assert.ok(ui.toasts.includes('👋 Signed out everywhere'));
    // The server was asked to revoke, with the token that was about to be
    // scrubbed — a logout that only cleared the device would leave the access
    // and refresh tokens valid on every other one.
    assert.deepEqual(revokes, ['Bearer access-7-1']);
    saveStore();
  },

  // ── The revoke is best-effort: logout completes without the server ───────
  async logout_completes_when_the_revoke_fails() {
    await signIn(7);
    route(async (p) => {
      if (p === '/auth/logout') throw new TypeError('Failed to fetch');   // offline
      return res(200, {});
    });
    signOut();
    await settle();
    assert.deepEqual(revokes, ['Bearer access-7-1'], 'it still tried');
    assert.equal(isSignedIn(), false, 'and the device is cleared regardless');
    assertNoStoredCredentials('after a failed revoke');
  },

  // ── A session the server already rejected is not worth a revoke ──────────
  async rejected_refresh_does_not_spend_a_revoke() {
    await signIn(7);
    route(async (p) => (p === '/auth/refresh' ? res(401, { detail: 'Session expired' })
                                              : res(401, { detail: 'expired' })));
    await assert.rejects(() => api('/me'));
    await settle();
    assert.equal(isSignedIn(), false);
    assert.deepEqual(revokes, [], 'the token is already dead server-side');
  },

  // ── Reload after logout (step 2 runs in a second process) ────────────────
  async assert_boot_is_anonymous() {
    assert.equal(isSignedIn(), false, 'a reload after logout must not restore the session');
    assert.equal(ST.token, null);
    assert.equal(ST.refresh, null);
    assert.equal(_curUid, 'anon');
    assertNoStoredCredentials('after reload');
  },

  // ── Reload with a valid session (step 2 runs in a second process) ────────
  async login_and_save() { await signIn(7); saveStore(); },
  async assert_boot_restores_session() {
    assert.equal(isSignedIn(), true, 'a stored session must still boot signed in');
    assert.equal(ST.token, 'access-7-1');
    assert.equal(ST.refresh, 'refresh-7-1');
    assert.equal(_curUid, '7');
  },

  // ── Repeated logout clicks ───────────────────────────────────────────────
  async repeated_logout_is_safe() {
    await signIn(7);
    signOut(); signOut(); signOut();
    assert.equal(isSignedIn(), false);
    assert.equal(_curUid, 'anon');
    assertNoStoredCredentials('after three logouts');
    assert.equal(ui.feeds, 4, 'one feed repaint per logout, plus the sign-in one');
  },

  // ── Expired access token: refresh once, retry once ───────────────────────
  async expired_token_refreshes_and_retries_once() {
    await signIn(7);
    let meHits = 0;
    route(async (p) => {
      if (p === '/me') { meHits++; return meHits === 1 ? res(401, { detail: 'expired' }) : res(200, { name: 'User 7' }); }
      if (p === '/auth/refresh') return res(200, { access_token: 'access-7-2', token: 'access-7-2', refresh_token: 'refresh-7-2' });
      return res(404, {});
    });
    const me = await api('/me');
    assert.deepEqual(me, { name: 'User 7' });
    assert.equal(meHits, 2, 'exactly one retry');
    assert.equal(countCalls('/auth/refresh'), 1, 'exactly one refresh');
    assert.equal(ST.token, 'access-7-2');
    assert.equal(ST.refresh, 'refresh-7-2', 'the rotated refresh token is persisted');
    assert.equal(JSON.parse(localStorage.getItem('sb21:u:7')).token, 'access-7-2');
  },

  // ── Two 401s at once share one refresh ───────────────────────────────────
  async concurrent_401s_share_one_refresh() {
    await signIn(7);
    const gate = defer();
    route(async (p) => {
      if (p === '/auth/refresh') { await gate.promise; return res(200, { access_token: 'access-7-2' }); }
      return calls.filter((c) => c === p).length === 1 ? res(401, {}) : res(200, { ok: p });
    });
    const both = Promise.all([api('/me'), api('/bookmarks')]);
    await settle();
    gate.resolve();
    const [a, b] = await both;
    assert.deepEqual(a, { ok: '/me' });
    assert.deepEqual(b, { ok: '/bookmarks' });
    assert.equal(countCalls('/auth/refresh'), 1, 'one refresh for the session, not one per 401');
  },

  // ── A rejected refresh token clears the session, without looping ─────────
  async rejected_refresh_clears_session() {
    await signIn(7);
    route(async (p) => (p === '/auth/refresh' ? res(401, { detail: 'Session expired' }) : res(401, { detail: 'expired' })));
    let err = null;
    try { await api('/me'); } catch (e) { err = e; }
    assert.ok(err, '/me must not resolve');
    assert.equal(err.sessionChanged, true, 'the caller is told the session ended, not handed a 401 to retry');
    assert.equal(countCalls('/auth/refresh'), 1, 'a rejected refresh is never retried');
    assert.equal(countCalls('/me'), 1, 'and the request is not replayed');
    assert.equal(isSignedIn(), false);
    assertNoStoredCredentials('after a rejected refresh');
    assert.equal(ui.rendered.at(-1).showSignIn, true);
    assert.ok(ui.toasts.some((t) => /Session expired/i.test(t)));
    // A second call has nothing to refresh with — no loop, no further attempts.
    await assert.rejects(() => api('/me'));
    assert.equal(countCalls('/auth/refresh'), 1);
  },

  // ── A network failure during refresh must NOT log anyone out ─────────────
  async refresh_network_error_keeps_session() {
    await signIn(7);
    route(async (p) => {
      if (p === '/auth/refresh') throw new TypeError('Failed to fetch');
      return res(401, { detail: 'expired' });
    });
    await assert.rejects(() => api('/me'), /expired/);
    assert.equal(isSignedIn(), true, 'an unreachable backend is not a logout');
    assert.equal(ST.refresh, 'refresh-7-1');
    assert.equal(JSON.parse(localStorage.getItem('sb21:u:7')).refresh, 'refresh-7-1');
  },

  // ── Logout while a refresh is in flight ──────────────────────────────────
  async logout_during_refresh_cannot_restore() {
    await signIn(7);
    const gate = defer();
    route(async (p) => {
      if (p === '/auth/refresh') { await gate.promise; return res(200, { access_token: 'access-7-2', refresh_token: 'refresh-7-2' }); }
      return res(401, { detail: 'expired' });
    });
    const pending = api('/me');
    await settle();                       // the refresh is now in flight
    signOut();
    gate.resolve();                       // …and answers after the logout
    await assert.rejects(() => pending);
    await settle();
    assert.equal(isSignedIn(), false, 'a late refresh must not sign the reader back in');
    assert.equal(ST.token, null);
    assert.equal(ST.refresh, null);
    assertNoStoredCredentials('after a late refresh');
  },

  // ── Logout while an ordinary request is in flight ────────────────────────
  async logout_during_request_cannot_restore() {
    await signIn(7);
    const gate = defer();
    route(async (p) => { if (p === '/me') { await gate.promise; return res(200, { name: 'User 7', stats: {} }); } return res(404, {}); });
    const pending = api('/me');
    await settle();
    signOut();
    gate.resolve();
    const err = await pending.then(() => null, (e) => e);
    assert.ok(err, 'the response must not be handed to a caller that would store it');
    assert.equal(err.sessionChanged, true);
    assert.equal(isSignedIn(), false);
    assertNoStoredCredentials('after a late authenticated response');
  },

  // ── A late login response cannot beat a newer one ────────────────────────
  async late_login_response_cannot_win() {
    const slow = defer();
    route(async (p, init) => {
      const body = JSON.parse(init.body);
      if (body.who === 'A') { await slow.promise; return res(200, PAIR(1)); }
      return res(200, PAIR(2));
    });
    const genA = newAuthAttempt();
    const pendingA = api('/login', { method: 'POST', body: JSON.stringify({ who: 'A' }) });
    await settle();
    const genB = newAuthAttempt();
    const dataB = await api('/login', { method: 'POST', body: JSON.stringify({ who: 'B' }) });
    assert.equal(finishSignIn(dataB, genB, 'B'), true);
    slow.resolve();
    const dataA = await pendingA;
    assert.equal(finishSignIn(dataA, genA, 'A'), false, 'the superseded attempt is dropped');
    assert.equal(ST.userId, 2, 'the newer sign-in still owns the session');
    assert.equal(ST.token, 'access-2-1');
    assert.equal(_curUid, '2');
  },

  // ── A login response arriving after a logout cannot revive it ────────────
  async login_response_after_logout_is_dropped() {
    const slow = defer();
    route(async (p) => { await slow.promise; return res(200, PAIR(5)); });
    const gen = newAuthAttempt();
    const pending = api('/login', { method: 'POST', body: '{}' });
    await settle();
    signOut();
    slow.resolve();
    const data = await pending;           // /login is unauthenticated, so it resolves
    assert.equal(finishSignIn(data, gen, 'hi'), false);
    assert.equal(isSignedIn(), false);
    assertNoStoredCredentials('after a late login response');
  },

  // ── Account B after account A ────────────────────────────────────────────
  async user_b_after_user_a_is_clean() {
    await signIn(7);
    ST.likes = [11]; ST.savedCache = { 11: { title: "A's private bookmark" } }; persist();
    signOut();
    route(async (p) => (p === '/login' ? res(200, PAIR(8)) : res(404, {})));
    const gen = newAuthAttempt();
    assert.equal(finishSignIn(await api('/login', { method: 'POST', body: '{}' }), gen, 'hi'), true);
    assert.equal(ST.userId, 8);
    assert.equal(_curUid, '8');
    assert.equal(ST.token, 'access-8-1');
    assert.deepEqual(ST.likes, [], "B does not inherit A's likes");
    assert.deepEqual(ST.savedCache, {}, "B does not inherit A's bookmarks");
    const u7 = JSON.parse(localStorage.getItem('sb21:u:7'));
    assert.equal(u7.token ?? null, null, "A's partition keeps no token once B is signed in");
    assert.equal(u7.refresh ?? null, null);
    assert.deepEqual(u7.likes, [11], "A's own data is still A's");
  },

  // ── An unattributed session never lands in the previous user's partition ─
  async tokenless_user_id_does_not_reuse_a_partition() {
    await signIn(7);
    route(async () => res(200, { access_token: 'access-x', refresh_token: 'refresh-x' }));  // no user_id
    const gen = newAuthAttempt();
    assert.equal(finishSignIn(await api('/login', { method: 'POST', body: '{}' }), gen, 'hi'), true);
    assert.notEqual(_curUid, '7', "a session with no user id must not be filed under the last account");
    assert.equal(JSON.parse(localStorage.getItem('sb21:u:7')).token ?? null, null);
  },

  // ── Anonymous browsing is untouched ──────────────────────────────────────
  async anonymous_request_never_reauthenticates() {
    localStorage.setItem('sb21:u:anon', JSON.stringify({ likes: [] }));
    route(async () => res(200, { articles: [] }));
    const out = await api('/feed');
    assert.deepEqual(out, { articles: [] });
    assert.equal(countCalls('/auth/refresh'), 0);
    assert.equal(isSignedIn(), false, 'an anonymous feed load must not produce a session');
    // A 401 on an anonymous call has no refresh token to spend either.
    route(async () => res(401, { detail: 'nope' }));
    await assert.rejects(() => api('/feed'), /nope/);
    assert.equal(countCalls('/auth/refresh'), 0);
  },

  // ── A logout mid-flight must not break an anonymous request ──────────────
  async logout_does_not_break_anonymous_requests() {
    const gate = defer();
    route(async () => { await gate.promise; return res(200, { articles: [1] }); });
    const pending = api('/feed');
    await settle();
    signOut();
    gate.resolve();
    assert.deepEqual(await pending, { articles: [1] });
  },

  // ── The refresh endpoint itself is never refreshed ───────────────────────
  async refresh_endpoint_is_never_refreshed() {
    await signIn(7);
    route(async () => res(401, { detail: 'Session expired' }));
    await assert.rejects(() => api('/auth/refresh', { method: 'POST', body: '{}' }));
    assert.equal(countCalls('/auth/refresh'), 1, 'no refresh-of-the-refresh');
  },
};

// ── run ────────────────────────────────────────────────────────────────────
const fn = SCENARIOS[SCENARIO];
if (!fn) {
  console.error(`unknown scenario: ${SCENARIO}\nknown: ${Object.keys(SCENARIOS).join(', ')}`);
  process.exit(2);
}
try {
  await fn();
  saveStore();
  console.log(`ok ${SCENARIO}`);
} catch (e) {
  console.error(`FAIL ${SCENARIO}: ${e && e.message}`);
  console.error(e && e.stack);
  process.exit(1);
}
