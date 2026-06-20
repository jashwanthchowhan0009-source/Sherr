"""Shared test fixtures / path setup."""

import os
import sys
from pathlib import Path

import pytest

# Make `import app.*` work when running `pytest` from the repo root or sherrbyte/.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Deterministic test config — no external services required for unit tests.
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/test")
os.environ.setdefault("RUN_SCHEDULER", "false")


@pytest.fixture(autouse=True)
def _reset_provider_breaker():
    """The LLM router's quota circuit-breaker is module-global state. Reset it
    before every test so a 429 in one test can't short-circuit calls in the next."""
    from app.sherr import router
    router._cooldown_until = {"gemini": 0.0, "groq": 0.0}
    yield
