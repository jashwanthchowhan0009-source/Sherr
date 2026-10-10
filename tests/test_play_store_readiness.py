"""The URLs a Play Store submission depends on, asserted rather than assumed.

Each of these was missing, and each fails the submission in a different way:

  * `/.well-known/assetlinks.json` 404'd on the Render deployment — it existed
    as a file on disk and a header rule in vercel.json, but no route served it.
    Without it Digital Asset Links verification cannot pass, and the Trusted Web
    Activity opens with a browser address bar across the top: the "website in a
    shell" Play's minimum-functionality policy rejects.
  * `/privacy` did not exist. Play's reviewer loads the privacy-policy URL from
    a browser, signed out, with no app installed; a 404 ends the review.
  * `/delete-account` did not exist. An app offering account creation needs a
    deletion path reachable by someone who has already uninstalled.

The catch-all assertion is the one that protects the rest: these routes only
work because they are registered ABOVE `/{full_path:path}` (CLAUDE.md), and the
failure mode if someone adds a route below it is a 200 with 560KB of HTML.
"""
import importlib
import json
import os
import re
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
main = importlib.import_module("main")
legal = importlib.import_module("legal")


@pytest.fixture(scope="module")
def client():
    return TestClient(main.app)


# ── Digital Asset Links: the TWA's address bar depends on this ──────────────

def test_assetlinks_is_served_not_just_committed(client):
    r = client.get("/.well-known/assetlinks.json")
    assert r.status_code == 200, "the TWA cannot verify without this file"
    assert r.headers["content-type"].startswith("application/json")


def test_assetlinks_has_the_shape_the_verifier_requires(client):
    body = client.get("/.well-known/assetlinks.json").json()
    assert isinstance(body, list) and body, "must be a non-empty JSON array"
    entry = body[0]
    assert "delegate_permission/common.handle_all_urls" in entry["relation"]
    target = entry["target"]
    assert target["namespace"] == "android_app"
    assert target["package_name"]
    prints = target["sha256_cert_fingerprints"]
    assert prints, "at least one signing fingerprint is required"
    for fp in prints:
        assert re.fullmatch(r"(?:[0-9A-F]{2}:){31}[0-9A-F]{2}", fp), \
            f"not an uppercase colon-separated SHA-256 fingerprint: {fp}"


def test_assetlinks_package_matches_the_built_apk():
    """A mismatch here verifies nothing — the file would point at another app."""
    with open(os.path.join(_ROOT, ".well-known", "assetlinks.json"), encoding="utf-8") as fh:
        pkg = json.load(fh)[0]["target"]["package_name"]
    apk = os.path.join(_ROOT, "sherrbyte.apk")
    if not os.path.isfile(apk):
        pytest.skip("sherrbyte.apk not in the tree")
    import zipfile
    with zipfile.ZipFile(apk) as z:
        manifest = z.read("AndroidManifest.xml")
    assert pkg.encode("utf-16-le") in manifest, \
        f"assetlinks names {pkg}, which is not the package inside sherrbyte.apk"


# ── the pages Play's reviewer opens in a browser ────────────────────────────

@pytest.mark.parametrize("path", ["/privacy", "/terms", "/delete-account"])
def test_legal_page_loads_without_a_session(client, path):
    r = client.get(path)
    assert r.status_code == 200, f"{path} must load signed-out"
    assert r.headers["content-type"].startswith("text/html")
    assert len(r.content) > 1500, f"{path} looks like a stub"


@pytest.mark.parametrize("path", ["/privacy", "/terms", "/delete-account"])
def test_legal_page_is_not_the_spa_shell(client, path):
    """The catch-all returns index.html for anything it owns. If one of these
    ever falls through to it the page would 200 with the whole app instead."""
    body = client.get(path).text
    assert "<title>" in body and "SherrByte" in body
    assert len(body) < 60_000, "this is the SPA bundle, not the legal page"


def test_privacy_states_the_deletion_route_play_checks_for(client):
    body = client.get("/privacy").text
    assert "/delete-account" in body
    assert "delete" in body.lower()


def test_delete_account_page_covers_the_uninstalled_case(client):
    """Play requires a route for someone who no longer has the app."""
    body = client.get("/delete-account").text.lower()
    assert "uninstall" in body
    assert "mailto:" in body


def test_the_app_links_to_the_legal_pages():
    with open(os.path.join(_ROOT, "index.html"), encoding="utf-8") as fh:
        html = fh.read()
    assert "openLegal('/privacy')" in html
    assert "openLegal('/terms')" in html


def test_unconfigured_legal_values_are_visible_not_blank(monkeypatch):
    """A policy that silently prints an empty company name is worse than one
    that says it is unconfigured."""
    for k in ("LEGAL_ENTITY", "LEGAL_ADDRESS", "LEGAL_JURISDICTION"):
        monkeypatch.delenv(k, raising=False)
    assert set(legal.legal_config_gaps()) >= {"LEGAL_ENTITY", "LEGAL_ADDRESS",
                                              "LEGAL_JURISDICTION"}
    assert "not configured" in legal.privacy_html()

    monkeypatch.setenv("LEGAL_ENTITY", "White Tiger Media Pvt Ltd")
    monkeypatch.setenv("LEGAL_ADDRESS", "1 Example Road, Hyderabad")
    monkeypatch.setenv("LEGAL_JURISDICTION", "India")
    assert legal.legal_config_gaps() == []
    html = legal.privacy_html()
    assert "White Tiger Media Pvt Ltd" in html
    assert "not configured" not in html
    assert "not configured" not in legal.terms_html()


# ── PWA manifest: what Bubblewrap and the installer read ────────────────────

def test_manifest_icons_exist_at_the_size_they_claim(client):
    with open(os.path.join(_ROOT, "manifest.json"), encoding="utf-8") as fh:
        man = json.load(fh)
    import struct
    for icon in man["icons"]:
        name = icon["src"].lstrip("/")
        path = os.path.join(_ROOT, name)
        assert os.path.isfile(path), f"{name} is declared but not in the tree"
        with open(path, "rb") as fh:
            head = fh.read(33)
        w, h = struct.unpack(">II", head[16:24])
        assert f"{w}x{h}" == icon["sizes"], \
            f"{name} is {w}x{h} but the manifest declares {icon['sizes']}"


def test_manifest_has_what_installability_requires(client):
    with open(os.path.join(_ROOT, "manifest.json"), encoding="utf-8") as fh:
        man = json.load(fh)
    for key in ("name", "short_name", "start_url", "display", "icons",
                "background_color", "theme_color"):
        assert man.get(key), f"manifest is missing {key}"
    assert man["display"] in ("standalone", "fullscreen", "minimal-ui")
    sizes = {i["sizes"] for i in man["icons"]}
    assert "512x512" in sizes, "Play and the installer both want a 512px icon"
    assert any(int(s.split("x")[0]) >= 192 for s in sizes)
    assert any(i.get("purpose") == "maskable" for i in man["icons"]), \
        "without a maskable icon Android pads the launcher icon into a white box"


def test_every_manifest_icon_is_actually_served(client):
    with open(os.path.join(_ROOT, "manifest.json"), encoding="utf-8") as fh:
        man = json.load(fh)
    for icon in man["icons"]:
        r = client.get(icon["src"])
        assert r.status_code == 200, f"{icon['src']} is declared but 404s"
        assert r.headers["content-type"] == "image/png"


# ── the ordering rule the rest of this file depends on ──────────────────────

def test_the_catchall_is_still_the_last_route():
    paths = [getattr(r, "path", None) for r in main.app.routes]
    paths = [p for p in paths if p]
    assert paths[-1] == "/{full_path:path}", (
        "a route was registered below the catch-all — everything above it now "
        "returns the SPA shell with a 200 (see CLAUDE.md)")


def test_no_admin_route_is_unguarded():
    """Two were open: POST /admin/explore/refresh and GET /admin/originality."""
    with open(os.path.join(_ROOT, "main.py"), encoding="utf-8") as fh:
        src = fh.read().split("\n")
    routes = []
    for i, line in enumerate(src):
        m = re.match(r'@app\.(get|post|delete|put)\("(/admin[^"]*)"', line.strip())
        if m:
            routes.append((i, m.group(2)))
    assert routes, "no /admin routes found — did the decorator style change?"
    open_routes = []
    for idx, path in routes:
        end = len(src)
        for k in range(idx + 1, len(src)):
            if src[k].startswith("@app."):
                end = k
                break
        if "_check_admin" not in "\n".join(src[idx:end]):
            open_routes.append(path)
    assert open_routes == [], f"unauthenticated /admin routes: {open_routes}"
