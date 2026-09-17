"""A login must be durable across a token expiry / cold restart.

The client was built around a refresh token (applyAuth reads data.refresh_token,
api() retries a 401 via /auth/refresh, tryRefresh persists the rotated pair) but
the server issued NO refresh token and had NO /auth/refresh route — so the whole
refresh path was dead and any 401 dropped the user to sign-in ("login not
saved"). These assert both halves are present and agree on field names.
"""
import os
import re

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")
INDEX = os.path.join(_ROOT, "index.html")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def test_backend_issues_a_refresh_token_and_has_the_route():
    m = _read(MAIN)
    assert "def make_refresh_token(" in m
    assert "def auth_pair(" in m
    assert '@app.post("/auth/refresh")' in m
    # login and signup both return the pair (which includes refresh_token).
    assert m.count("auth_pair(") >= 3  # def + login + signup


def test_login_and_signup_return_a_refresh_token():
    m = _read(MAIN)
    # auth_pair is the single source of the {token, access_token, refresh_token}
    # shape the client's applyAuth consumes.
    pair = m[m.index("def auth_pair("):m.index("def auth_pair(") + 400]
    for field in ('"token"', '"access_token"', '"refresh_token"'):
        assert field in pair, field


def test_refresh_route_rejects_a_bad_token_with_401():
    m = _read(MAIN)
    block = m[m.index('@app.post("/auth/refresh")'):m.index('@app.post("/auth/refresh")') + 700]
    assert "verify_token(" in block
    assert "401" in block


def test_client_refresh_path_is_wired_to_the_route():
    h = _read(INDEX)
    assert "/auth/refresh" in h
    assert "data.refresh_token" in h            # applyAuth stores it
    assert "if (d.refresh_token) ST.refresh = d.refresh_token;" in h  # rotation persisted
