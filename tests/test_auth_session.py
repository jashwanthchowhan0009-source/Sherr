"""The client's authentication session, executed rather than grepped.

index.html is a single 690KB file with no build step, so its logic has only ever
been covered by string assertions (test_session_refresh.py, test_desktop_layout.py).
String assertions cannot see a RACE, and every confirmed logout bug in this file
was one:

  * signOut() called switchUser('anon'), whose first act is persist() — so the
    still-populated token/refresh were written back into the partition being
    left, and a 180-day refresh token outlived the logout on disk.
  * tryRefresh() wrote its answer into whatever `ST` happened to be current. A
    refresh already in flight when the reader signed out landed afterwards and
    put a live access token into the anonymous state: logged out, then silently
    logged back in.
  * a rejected refresh token was never cleared, so the client re-attempted it
    forever and never dropped to sign-in.

So: this extracts the SESSION CORE and SESSION ENTRY blocks from index.html
verbatim, substitutes them into tests/js/session_harness.mjs (fake
localStorage + fake fetch + stubs for the UI calls), and runs each scenario in
its own node process. The code under test is the shipped code; if someone
deletes a generation check, a scenario here fails.

The "reload the browser" scenarios are two processes sharing one on-disk
localStorage, so the boot path really re-runs.

Skipped (not failed) when node is unavailable, since the rest of the suite is
pure Python.
"""
import json
import os
import shutil
import subprocess

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(_ROOT, "index.html")
HARNESS = os.path.join(_ROOT, "tests", "js", "session_harness.mjs")

NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is not installed")


def _read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _slice(text, start_marker, end_marker, what):
    i = text.find(start_marker)
    assert i >= 0, f"{what}: start marker missing from index.html — {start_marker!r}"
    j = text.find(end_marker, i)
    assert j >= 0, f"{what}: end marker missing from index.html — {end_marker!r}"
    return text[i:j + len(end_marker)]


def _build(tmp_path):
    """Write a runnable harness containing the REAL session code."""
    html = _read(INDEX)
    core = _slice(html, "/* ══ SESSION CORE (start)", "/* ══ SESSION CORE (end)", "session core")
    entry = _slice(html, "/* ══ SESSION ENTRY (start)", "/* ══ SESSION ENTRY (end) ══ */", "session entry")
    # Sanity: the blocks must actually contain what the scenarios drive.
    for needed in ("function signOut(", "function applyAuth(", "function tryRefresh(", "async function api("):
        assert needed in core, f"session core no longer contains {needed}"
    assert "function finishSignIn(" in entry

    src = _read(HARNESS)
    assert "/* __SESSION_CORE__ */" in src and "/* __SESSION_ENTRY__ */" in src
    src = src.replace("/* __SESSION_CORE__ */", core).replace("/* __SESSION_ENTRY__ */", entry)
    out = tmp_path / "session_under_test.mjs"
    out.write_text(src, encoding="utf-8")
    return str(out)


def _run(script, scenario, store=None):
    cmd = [NODE, script, "--scenario", scenario]
    if store:
        cmd += ["--store", str(store)]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=60)


def _ok(script, scenario, store=None):
    p = _run(script, scenario, store)
    assert p.returncode == 0, f"{scenario} failed:\n{p.stdout}\n{p.stderr}"
    return p


# Every scenario that runs standalone, in its own process with empty storage.
SCENARIOS = [
    "login_success",
    "signup_success",
    "failed_login_no_partial_session",
    "tokenless_response_is_rejected",
    "logout_clears_everything",
    "repeated_logout_is_safe",
    "expired_token_refreshes_and_retries_once",
    "concurrent_401s_share_one_refresh",
    "rejected_refresh_clears_session",
    "refresh_network_error_keeps_session",
    "logout_during_refresh_cannot_restore",
    "logout_during_request_cannot_restore",
    "late_login_response_cannot_win",
    "login_response_after_logout_is_dropped",
    "user_b_after_user_a_is_clean",
    "tokenless_user_id_does_not_reuse_a_partition",
    "anonymous_request_never_reauthenticates",
    "logout_does_not_break_anonymous_requests",
    "refresh_endpoint_is_never_refreshed",
]


@pytest.fixture(scope="module")
def harness(tmp_path_factory):
    return _build(tmp_path_factory.mktemp("authjs"))


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_session_scenario(harness, scenario):
    _ok(harness, scenario)


def test_reload_after_logout_stays_logged_out(harness, tmp_path):
    """Two processes, one localStorage: the second one is a real page reload."""
    store = tmp_path / "ls.json"
    _ok(harness, "logout_clears_everything", store)
    saved = json.loads(store.read_text())
    assert saved["sb21:cur"] == "anon"
    _ok(harness, "assert_boot_is_anonymous", store)


def test_reload_with_a_valid_session_restores_it(harness, tmp_path):
    store = tmp_path / "ls.json"
    _ok(harness, "login_and_save", store)
    assert json.loads(store.read_text())["sb21:cur"] == "7"
    _ok(harness, "assert_boot_restores_session", store)


# ── Structural checks: the parts of the flow that live in the DOM ────────────

def test_every_sign_in_entry_point_goes_through_finishSignIn():
    """doAuth / obDoLogin / obFinish / obSkip must not keep private copies of
    the post-login sequence — that is how they drifted apart in the first place."""
    html = _read(INDEX)
    for fn in ("async function doAuth(", "async function obDoLogin(",
               "async function obFinish(", "async function obSkip("):
        i = html.index(fn)
        body = html[i:i + 1400]
        assert "newAuthAttempt()" in body, f"{fn} does not claim a session generation"
        assert "finishSignIn(" in body, f"{fn} does not go through finishSignIn"
        assert "applyAuth(" not in body, f"{fn} still applies the token itself"


def test_logout_is_bound_once_and_is_the_core_one():
    html = _read(INDEX)
    assert html.count("addEventListener('click', () => signOut())") == 1
    assert html.count("document.querySelector('.sb-logout')") == 1
    # signOut lives in the session core, not next to the button.
    core = _slice(html, "/* ══ SESSION CORE (start)", "/* ══ SESSION CORE (end)", "session core")
    assert "function signOut(opts)" in core
    assert "scrubStoredCredentials()" in core
    assert "switchUser('anon')" in core
    # switchUser must never write the partition it is leaving — that flush is
    # what preserved a live refresh token across a logout.
    i = core.index("function switchUser(")
    assert "persist()" not in core[i:core.index("}", core.index("ST = Object.assign", i))]


def test_auth_ui_has_a_single_renderer():
    html = _read(INDEX)
    assert "function renderAuthState(opts)" in html
    assert "function updateSidebarAuth() { renderAuthState(); }" in html
    # The signed-in branch must exist — its absence is why the sidebar header
    # kept opening the sign-in modal after a successful login.
    i = html.index("function renderAuthState(opts)")
    body = html[i:i + 1600]
    assert "sbHead.onclick = null" in body
    assert "resetProfileUI()" in body


def test_no_backend_logout_route_is_invented():
    """The backend issues stateless HMAC tokens and exposes no revocation route.
    The client must not call one that does not exist."""
    main = _read(os.path.join(_ROOT, "main.py"))
    assert '"/auth/logout"' not in main and '"/logout"' not in main
    html = _read(INDEX)
    assert "/auth/logout" not in html and "api('/logout'" not in html
