"""Shared test fixtures / path setup."""

import os
import sys
from pathlib import Path

# Make `import app.*` work when running `pytest` from the repo root or sherrbyte/.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Deterministic test config — no external services required for unit tests.
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/test")
os.environ.setdefault("RUN_SCHEDULER", "false")
