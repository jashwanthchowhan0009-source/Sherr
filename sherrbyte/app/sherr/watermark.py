"""
sherr/watermark.py — SGI (Synthetically Generated Information) labelling.

India's IT Rules 2026 amendment requires AI-generated/edited content to carry a
clear, machine-readable provenance label. Every Sherr generation passes through
here before it leaves the system.

We attach:
  • a human-visible label (settings.watermark_label),
  • a structured provenance block (generator, model, mode, timestamp),
  • an HMAC tag over the content so a downstream consumer can verify the label
    wasn't stripped/forged.

This is a metadata watermark (provenance), not a steganographic one.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone

from app.config import settings


def _signature(text: str) -> str:
    return hmac.new(
        settings.watermark_secret.encode(),
        text.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def apply(text: str, *, mode: str, model: str,
          info_object_id: str | None = None) -> dict:
    """Wrap generated text with an SGI provenance envelope."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "content": text,
        "sgi": {
            "label": settings.watermark_label,
            "generator": "Sherr",
            "model": model,
            "mode": mode,
            "info_object_id": info_object_id,
            "generated_at": now,
            "signature": _signature(text),
            "rule": "IT Rules 2026 (SGI)",
        },
    }


def verify(envelope: dict) -> bool:
    """Confirm a watermarked envelope's signature matches its content."""
    try:
        return hmac.compare_digest(
            envelope["sgi"]["signature"],
            _signature(envelope["content"]),
        )
    except (KeyError, TypeError):
        return False
