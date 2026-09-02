"""Model ids: overridable, live, reported, and never silently stale.

A retired model id is the most expensive failure in this codebase. The provider
answers 404, _call_provider returns None, _rule_based_fallback supplies the
placeholder, and 25,000 articles stay on it — with nothing in the output saying
"that model does not exist". gemini-2.5-flash (retiring 2026-10-20) and
grok-2-latest (never a real id) both did exactly that.

Three defences, all tested here:
  * every id is an env var, resolved AT CALL TIME so setting it on the service
    takes effect on the next request rather than the next restart,
  * the request and the audit read the same resolver, so they cannot disagree,
  * /admin/body-audit names each provider's model and flags the ones still on a
    built-in default — the ids that go stale with nobody touching anything.

NOTE FOR WHOEVER EDITS THIS FILE: do not use importlib.reload here. An earlier
version did, and it passed alone while breaking ten tests elsewhere — other
modules hold references to ai_processor's objects, and reloading swaps them out
underneath. Everything below sets env vars and calls model_for() instead.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ai_processor  # noqa: E402

PROVIDERS = ("gemini", "groq", "openai", "grok")


# ─── every id is overridable, live ──────────────────────────────────────────

@pytest.mark.parametrize("provider,var", [
    ("gemini", "GEMINI_MODEL"), ("groq", "GROQ_MODEL"),
    ("openai", "OPENAI_MODEL"), ("grok", "GROK_MODEL"),
])
def test_the_model_id_comes_from_the_environment(monkeypatch, provider, var):
    """A model retirement must be fixable by setting a var — no deploy."""
    monkeypatch.setenv(var, "pinned-model-id")
    assert ai_processor.model_for(provider) == "pinned-model-id"


@pytest.mark.parametrize("provider", PROVIDERS)
def test_an_unset_var_falls_back_to_the_documented_default(monkeypatch, provider):
    monkeypatch.delenv(ai_processor.MODEL_ENV_VAR[provider], raising=False)
    assert ai_processor.model_for(provider) == \
        ai_processor.MODEL_DEFAULT[provider]


@pytest.mark.parametrize("provider", PROVIDERS)
def test_a_blank_var_does_not_send_an_empty_model_id(monkeypatch, provider):
    """An empty env var is a misconfiguration, not an instruction to call the
    API with no model."""
    monkeypatch.setenv(ai_processor.MODEL_ENV_VAR[provider], "   ")
    assert ai_processor.model_for(provider) == \
        ai_processor.MODEL_DEFAULT[provider]


def test_the_retired_ids_are_not_the_defaults():
    assert ai_processor.MODEL_DEFAULT["gemini"] != "gemini-2.5-flash"
    assert ai_processor.MODEL_DEFAULT["grok"] != "grok-2-latest"


def test_the_retired_ids_are_not_live_anywhere_in_the_code():
    src = open(ai_processor.__file__).read()
    code = "\n".join(l for l in src.splitlines()
                     if not l.lstrip().startswith("#"))
    for bad in ("gemini-2.5-flash", "grok-2-latest"):
        assert bad not in code, f"{bad} is still live in the code"


def test_every_provider_can_be_pinned_and_reported():
    assert set(ai_processor.MODEL_ENV_VAR) == set(PROVIDERS)
    assert set(ai_processor.MODEL_DEFAULT) == set(PROVIDERS)
    assert set(ai_processor._MODEL_FOR) == set(PROVIDERS)


# ─── the request and the audit cannot disagree ──────────────────────────────

def test_the_call_path_resolves_the_model_live_not_at_import(monkeypatch):
    """THE TRAP THIS AVOIDS: the audit reporting a pinned id while the request
    still used the one captured at boot."""
    monkeypatch.setenv("GEMINI_MODEL", "set-after-import")
    assert ai_processor._MODEL_FOR["gemini"]() == "set-after-import"


def test_no_provider_call_site_uses_a_boot_time_constant():
    src = open(ai_processor.__file__).read()
    call_path = src.split("# ─── Public API")[0]
    for const in ("GEMINI_MODEL", "GROQ_MODEL", "OPENAI_MODEL", "GROK_MODEL"):
        for line in call_path.splitlines():
            if line.lstrip().startswith("#") or "=" in line.split(const)[0][-2:]:
                continue
            if f"model={const}" in line or f"{{{const}}}" in line:
                pytest.fail(f"{const} used in the call path; use model_for()")


# ─── what the audit reports ─────────────────────────────────────────────────

def test_available_providers_reports_a_model_per_configured_provider(monkeypatch):
    monkeypatch.setattr(ai_processor.KEYS, "configured",
                        lambda: ["gemini", "grok"])
    monkeypatch.setattr(ai_processor.KEYS, "sizes",
                        lambda: {"gemini": 1, "groq": 0, "openai": 0, "grok": 1})
    monkeypatch.setenv("GROK_MODEL", "grok-pinned")
    out = ai_processor.available_providers()
    assert set(out["models"]) == {"gemini", "grok"}
    assert out["models"]["grok"]["model"] == "grok-pinned"


def test_an_unconfigured_provider_is_not_reported(monkeypatch):
    """A model for a provider with no key would be noise."""
    monkeypatch.setattr(ai_processor.KEYS, "configured", lambda: ["gemini"])
    monkeypatch.setattr(ai_processor.KEYS, "sizes",
                        lambda: {"gemini": 1, "groq": 0, "openai": 0, "grok": 0})
    assert set(ai_processor.available_providers()["models"]) == {"gemini"}


def test_the_source_distinguishes_pinned_from_defaulted(monkeypatch):
    """Which ids someone chose, and which will rot unattended."""
    monkeypatch.setattr(ai_processor.KEYS, "configured", lambda: ["gemini"])
    monkeypatch.setattr(ai_processor.KEYS, "sizes",
                        lambda: {"gemini": 1, "groq": 0, "openai": 0, "grok": 0})

    monkeypatch.setenv("GEMINI_MODEL", "pinned")
    assert ai_processor.available_providers()["models"]["gemini"]["source"] \
        == "env:GEMINI_MODEL"

    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    assert ai_processor.available_providers()["models"]["gemini"]["source"] \
        == "built-in default"


def test_the_primary_model_key_agrees_with_the_per_provider_map(monkeypatch):
    """`model` predates `models`; they must never say different things."""
    monkeypatch.setattr(ai_processor.KEYS, "configured", lambda: ["gemini"])
    monkeypatch.setattr(ai_processor.KEYS, "sizes",
                        lambda: {"gemini": 1, "groq": 0, "openai": 0, "grok": 0})
    out = ai_processor.available_providers()
    assert out["model"] == out["models"][out["primary"]]["model"]


# ─── the endpoint ───────────────────────────────────────────────────────────

def test_body_audit_reports_the_models_and_flags_a_defaulted_one(monkeypatch):
    import main
    monkeypatch.setattr(main, "available_providers", lambda: {
        "gemini": 1, "total_keys": 1, "primary": "gemini",
        "model": "gemini-x",
        "models": {"gemini": {"model": "gemini-x",
                              "source": "built-in default"},
                   "grok": {"model": "grok-y", "source": "env:GROK_MODEL"}},
        "cascade": ["gemini", "grok"]})
    # main.ADMIN_TOKEN is read at import; patch the attribute rather than the
    # env var, and rather than skipping — a test that quietly does not run is
    # the same as no test.
    monkeypatch.setattr(main, "ADMIN_TOKEN", "test-token")
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        d = c.get("/admin/body-audit", params={"token": "test-token"}).json()
    assert "ai" in d, d
    assert d["ai"]["models"]["gemini"]["model"] == "gemini-x"
    assert "gemini" in d["model_note"]
    assert "grok" not in d["model_note"]        # pinned, so not flagged
