"""
key_pool.py — one rotating pool of API keys per provider.

WHY. The deployment carries several keys per provider (GEMINI_API_KEY,
GEMINI_API_KEY_4, GEMINI_API_KEY_9, …) but the code read exactly one name per
provider. So the spare keys were dead weight, a 429 on the single Gemini key
took the whole rewrite pass down, and GROQ_API_KEY_4 was never read at all.

TWO PROVIDERS, ONE LETTER APART. GROQ (the inference host) and GROK (xAI's
model) are different services with different endpoints, and the code was reading
GROK_API_KEY and sending it to api.groq.com — so whichever of those keys existed,
one provider was always being called with the other's credential. The prefixes
here are matched exactly for that reason: a variable belongs to a provider only
if it IS the prefix or begins with the prefix followed by an underscore.

ROTATION. A key is rotated past on the failures that are about the KEY — 429
(that key is rate limited) and 401/403 (that key is bad or revoked). Anything
else is about the request or the service, and rotating would just spend the
whole pool repeating the same failure.

Pure: environment in, pools out. No app imports, no network.
"""

from __future__ import annotations

import logging
import os
import re
import threading

log = logging.getLogger("sherbyte.keypool")

# provider -> the env-var prefix that owns its keys.
PROVIDER_PREFIXES = {
    "gemini": "GEMINI_API_KEY",
    "groq":   "GROQ_API_KEY",
    "openai": "GPT_API_KEY",
    "grok":   "GROK_API_KEY",
}

# The order a request tries providers in. Gemini first (best output for this
# prompt), then Groq (fast and cheap), then OpenAI, then Grok.
PROVIDER_ORDER = ("gemini", "groq", "openai", "grok")

# HTTP statuses that mean "this KEY is the problem, try another one".
ROTATE_STATUSES = frozenset({401, 403, 429})

_SUFFIX_RE = re.compile(r"_(\d+)$")


def _sort_key(name: str, prefix: str):
    """Base name first, then numeric suffixes ascending, then anything else
    alphabetically. Deterministic, so the same environment always produces the
    same primary key and a log line naming key #2 means the same key tomorrow."""
    if name == prefix:
        return (0, 0, "")
    tail = name[len(prefix):].lstrip("_")
    m = _SUFFIX_RE.search(name)
    if m:
        return (1, int(m.group(1)), "")
    return (2, 0, tail)


def collect(env=None) -> dict:
    """{provider: [key, ...]} from the environment.

    Values are de-duplicated: the same secret exported under two names is one
    key, not two, and counting it twice would make the pool look deeper than it
    is at exactly the moment that matters.
    """
    env = os.environ if env is None else env
    out: dict = {}
    for provider, prefix in PROVIDER_PREFIXES.items():
        names = [n for n in env
                 if n == prefix or n.startswith(prefix + "_")]
        names.sort(key=lambda n: _sort_key(n, prefix))
        keys, seen = [], set()
        for n in names:
            v = (env.get(n) or "").strip()
            if v and v not in seen:
                seen.add(v)
                keys.append(v)
        out[provider] = keys
    return out


class KeyPool:
    """The keys for one provider, with a rotating cursor.

    Thread-safe because the sync drain runs this in an executor while the
    scheduler may be running the same pass on the loop.
    """

    def __init__(self, provider: str, keys):
        self.provider = provider
        self._keys = list(keys or [])
        self._i = 0
        self._lock = threading.Lock()
        # Keys rotated past during the current attempt. Cleared by reset(), which
        # every new request calls — a key that was rate limited a minute ago is
        # usually fine now, so exhaustion is per-request, not permanent.
        self._burned: set = set()

    def __len__(self) -> int:
        return len(self._keys)

    @property
    def size(self) -> int:
        return len(self._keys)

    def current(self):
        with self._lock:
            return self._keys[self._i] if self._keys else None

    def label(self) -> str:
        """Which key is in use, for logs. Never the key itself."""
        return f"{self.provider}#{self._i + 1}/{len(self._keys)}" if self._keys \
            else f"{self.provider}#none"

    def reset(self) -> None:
        with self._lock:
            self._burned.clear()

    def rotate(self, reason: str = "") -> bool:
        """Advance to the next untried key. False when the pool is spent for
        this request, which is the signal to fall through to the next provider."""
        with self._lock:
            if not self._keys:
                return False
            self._burned.add(self._i)
            if len(self._burned) >= len(self._keys):
                return False
            for step in range(1, len(self._keys) + 1):
                nxt = (self._i + step) % len(self._keys)
                if nxt not in self._burned:
                    self._i = nxt
                    log.info("key rotated: %s -> %s (%s)", self.provider,
                             self.label(), reason or "failure")
                    return True
            return False


class PoolSet:
    """Every provider's pool, built once from the environment."""

    def __init__(self, env=None):
        self.pools = {p: KeyPool(p, k) for p, k in collect(env).items()}

    def get(self, provider: str) -> KeyPool:
        return self.pools.get(provider) or KeyPool(provider, [])

    def sizes(self) -> dict:
        return {p: self.pools[p].size for p in PROVIDER_ORDER if p in self.pools}

    def configured(self) -> list:
        """Providers with at least one key, in cascade order."""
        return [p for p in PROVIDER_ORDER if self.get(p).size]

    def reset(self) -> None:
        for p in self.pools.values():
            p.reset()

    def describe(self) -> str:
        return ", ".join(f"{p}={self.get(p).size}" for p in PROVIDER_ORDER) or "none"
