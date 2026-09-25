#!/usr/bin/env python3
"""Drive index.html's authentication lifecycle in a real browser.

tests/test_auth_session.py runs the session core under node, which is where the
logout/refresh races are asserted. This is the other half: it loads the actual
page in Chromium against a stubbed backend and checks the DOM wiring — that the
logout button is bound, that the logged-out screen and sidebar and profile all
agree, that a reload does not restore the session, and that the boot-time
refresh path behaves.

It is a MANUAL verifier, not part of the pytest suite: it needs playwright and a
Chromium build, neither of which is in requirements.txt.

    pip install playwright && playwright install chromium
    python -m http.server 8123 &            # from the repo root
    python scripts/verify_auth_browser.py   # --chromium /path/to/chrome to reuse a build

Exit code 0 = every scenario passed; the per-scenario verdicts are printed.
"""
import argparse
import json
import sys

from playwright.sync_api import sync_playwright

API = "http://localhost:8000"          # what index.html resolves to on localhost

PAIR = {"token": "acc-7", "access_token": "acc-7", "refresh_token": "ref-7",
        "user_id": 7, "name": "Ada Seven", "display_name": "Ada Seven"}
PAIR2 = {"token": "acc-7b", "access_token": "acc-7b", "refresh_token": "ref-7b"}
ME = {"id": 7, "name": "Ada Seven", "display_name": "Ada Seven", "username": "ada7",
      "email": "ada@example.com", "bio": "", "stats": {"articles_read": 12, "bookmarks": 3},
      "interests": {}, "preferences": []}

PEEK = """window.__peek = () => { try {
  return JSON.parse(localStorage.getItem('sb21:u:'+(localStorage.getItem('sb21:cur')||'anon'))||'{}');
} catch(e){ return {}; } }"""

STATE = """() => ({
  token: (window.__peek() || {}).token || null,
  refresh: (window.__peek() || {}).refresh || null,
  cur: localStorage.getItem('sb21:cur'),
  parts: Object.keys(localStorage).filter(k=>k.startsWith('sb21:u:'))
           .map(k=>[k, JSON.parse(localStorage.getItem(k))]),
  ob: !!document.getElementById('ob-screen')
      && !document.getElementById('ob-screen').classList.contains('gone'),
  modal: document.getElementById('auth-modal')
      ? document.getElementById('auth-modal').style.display : 'absent',
  sbName: (document.querySelector('.sb-name')||{}).textContent,
  profName: (document.getElementById('prof-name')||{}).textContent,
  profRead: (document.getElementById('prof-stat-read')||{}).textContent,
})"""


class Run:
    """One browser context with a scripted backend."""

    def __init__(self, browser, base, refresh_status=200, me_status=200, seed=None):
        self.calls = []
        self.errors = []
        self.ctx = browser.new_context(viewport={"width": 420, "height": 860},
                                       base_url=base)
        self.ctx.route(f"{API}/**", self._api)
        self.ctx.route("https://**", lambda r: r.abort())      # fonts/CDN offline
        self.refresh_status = refresh_status
        self.me_status = me_status
        self.pg = self.ctx.new_page()
        self.pg.on("pageerror", lambda e: self.errors.append(str(e)))
        if seed:
            self.pg.goto("/index.html", wait_until="domcontentloaded")
            self.pg.evaluate("""(s) => { localStorage.clear();
                localStorage.setItem('sb21:cur', s.uid);
                localStorage.setItem('sb21:u:'+s.uid, JSON.stringify(s.state)); }""", seed)
        self.pg.goto("/index.html", wait_until="domcontentloaded")
        self.pg.wait_for_timeout(1800)
        self.pg.evaluate(PEEK)

    def _api(self, route):
        path = route.request.url[len(API):].split("?")[0]
        self.calls.append(path)
        if path == "/auth/refresh":
            body = PAIR2 if self.refresh_status == 200 else {"detail": "Session expired"}
            return route.fulfill(status=self.refresh_status, content_type="application/json",
                                 body=json.dumps(body))
        if path == "/me" and self.me_status != 200:
            return route.fulfill(status=self.me_status, content_type="application/json",
                                 body=json.dumps({"detail": "Not authenticated"}))
        body = {"/login": PAIR, "/signup": PAIR, "/me": ME}.get(path, {"articles": []})
        route.fulfill(status=200, content_type="application/json", body=json.dumps(body))

    def state(self):
        return self.pg.evaluate(STATE)

    def reload(self):
        self.pg.reload(wait_until="domcontentloaded")
        self.pg.wait_for_timeout(1800)
        self.pg.evaluate(PEEK)

    def close(self):
        self.ctx.close()


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'' if cond else '  <- ' + detail}")
    return bool(cond)


def scenario_lifecycle(browser, base):
    print("\nsign in -> log out -> reload")
    r = Run(browser, base)
    ok = True
    boot = r.state()
    ok &= check("boots anonymous", boot["token"] is None)

    r.pg.evaluate("showAuthModal()")
    r.pg.fill("#auth-email", "ada@example.com")
    r.pg.fill("#auth-pass", "hunter2hunter2")
    r.pg.click("#auth-btn")
    r.pg.wait_for_timeout(1000)
    s = r.state()
    ok &= check("login establishes the session", s["token"] == "acc-7", str(s["token"]))
    ok &= check("login files it under the account", s["cur"] == "7", str(s["cur"]))
    ok &= check("onboarding screen is hidden", not s["ob"])
    ok &= check("auth modal is closed", s["modal"] == "none", s["modal"])

    r.pg.evaluate("document.querySelector('.sb-logout').click()")
    r.pg.wait_for_timeout(800)
    s = r.state()
    ok &= check("logout clears the access token", s["token"] is None)
    ok &= check("logout clears the refresh token", s["refresh"] is None)
    ok &= check("logout returns to the anon partition", s["cur"] == "anon", str(s["cur"]))
    ok &= check("no partition keeps credentials",
                all(not v.get("token") and not v.get("refresh") for _, v in s["parts"]))
    ok &= check("the logged-out screen is shown", s["ob"])
    ok &= check("the sidebar offers sign-in", s["sbName"] == "Sign In / Register", str(s["sbName"]))
    ok &= check("the profile no longer shows the reader",
                s["profName"] == "—" and s["profRead"] == "0",
                f'{s["profName"]}/{s["profRead"]}')

    r.pg.evaluate("document.querySelector('.sb-logout').click();"
                  "document.querySelector('.sb-logout').click()")
    r.pg.wait_for_timeout(500)
    ok &= check("repeated logout clicks are safe", r.state()["token"] is None)

    before = len(r.calls)
    r.reload()
    s = r.state()
    ok &= check("reload does not restore the session", s["token"] is None and s["refresh"] is None)
    ok &= check("reload does not call /me", "/me" not in r.calls[before:])
    ok &= check("no page errors", not r.errors, "; ".join(r.errors[:3]))
    r.close()
    return ok


def scenario_boot_refresh(browser, base):
    print("\nboot with an expired access token and a VALID refresh token")
    seed = {"uid": "7", "state": {"token": "stale", "refresh": "ref-7", "userId": 7}}
    r = Run(browser, base, refresh_status=200, me_status=401, seed=seed)
    s = r.state()
    ok = check("the session is refreshed, not dropped", s["token"] == "acc-7b", str(s["token"]))
    ok &= check("exactly one refresh", r.calls.count("/auth/refresh") == 1,
                str(r.calls.count("/auth/refresh")))
    ok &= check("the retried /me is the second one", r.calls.count("/me") == 2,
                str(r.calls.count("/me")))
    ok &= check("no page errors", not r.errors, "; ".join(r.errors[:3]))
    r.close()
    return ok


def scenario_boot_rejected_refresh(browser, base):
    print("\nboot with an expired access token and a REJECTED refresh token")
    seed = {"uid": "7", "state": {"token": "stale", "refresh": "dead", "userId": 7,
                                  "likes": [5], "meCache": {"name": "Ada Seven"}}}
    r = Run(browser, base, refresh_status=401, me_status=401, seed=seed)
    s = r.state()
    ok = check("the dead session is cleared", s["token"] is None and s["refresh"] is None)
    ok &= check("storage holds no credentials",
                all(not v.get("token") and not v.get("refresh") for _, v in s["parts"]))
    ok &= check("the sign-in screen is shown", s["ob"])
    ok &= check("the refresh is not retried in a loop", r.calls.count("/auth/refresh") == 1,
                str(r.calls.count("/auth/refresh")))
    ok &= check("the reader's own data survives",
                any(v.get("likes") == [5] for _, v in s["parts"]))
    ok &= check("no page errors", not r.errors, "; ".join(r.errors[:3]))
    r.close()
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8123", help="where index.html is served")
    ap.add_argument("--chromium", default=None, help="path to an existing chrome binary")
    a = ap.parse_args()
    with sync_playwright() as p:
        kw = {"args": ["--headless=new", "--no-sandbox"]}
        if a.chromium:
            kw["executable_path"] = a.chromium
        b = p.chromium.launch(**kw)
        ok = all([
            scenario_lifecycle(b, a.base),
            scenario_boot_refresh(b, a.base),
            scenario_boot_rejected_refresh(b, a.base),
        ])
        b.close()
    print("\n" + ("ALL SCENARIOS PASSED" if ok else "FAILURES ABOVE"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
