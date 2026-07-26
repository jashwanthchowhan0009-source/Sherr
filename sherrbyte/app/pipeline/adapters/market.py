"""
adapters/market.py — stocks / commodities / metals / forex → Signal.

All four are the same shape (a symbol with a % change), so one core `from_quote`
plus thin domain wrappers. Quote dict follows api/markets.py: {change_pct, ...}.
Pure: no DB, no network. Entity linkage (symbol → canonical entity) happens at
persist time via entity_ticker_map; here we emit the symbol/name as the entity.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from app.models.signal import Signal, SignalEntity
from app.pipeline.adapters.base import direction, clamp

# Market/data feeds are mechanical, not editorial — high, fixed reliability.
_MARKET_CREDIBILITY = 0.90


def from_quote(symbol: str, quote: dict, domain: str, *,
               name: Optional[str] = None, entity_type: str = "ORG",
               ts: Optional[datetime] = None, source: str = "market") -> list[Signal]:
    change_pct = float(quote.get("change_pct", quote.get("change_percent", 0.0)) or 0.0)
    disp = name or symbol
    return [Signal(
        entities=[SignalEntity(name=disp, type=entity_type)],
        domain=domain,
        ts=ts or datetime.now(timezone.utc),
        magnitude=abs(change_pct),
        direction=direction(change_pct),
        sentiment=None,                       # price moves carry no editorial sentiment
        embedding=None,
        source_id=source,
        credibility=_MARKET_CREDIBILITY,
        confidence=clamp(0.6 + min(abs(change_pct), 10.0) / 20.0),  # bigger move → surer signal
        novelty=0.0,
        ref_id=f"{domain}:{symbol}",
    )]


def from_stock(symbol: str, quote: dict, *, name=None, ts=None) -> list[Signal]:
    return from_quote(symbol, quote, "stocks", name=name, entity_type="ORG", ts=ts, source="yahoo")


def from_commodity(symbol: str, quote: dict, *, name=None, ts=None) -> list[Signal]:
    return from_quote(symbol, quote, "commodities", name=name, entity_type="MISC", ts=ts, source="yahoo")


def from_metal(symbol: str, quote: dict, *, name=None, ts=None) -> list[Signal]:
    return from_quote(symbol, quote, "metals", name=name, entity_type="MISC", ts=ts, source="yahoo")


def from_forex(symbol: str, quote: dict, *, name=None, ts=None) -> list[Signal]:
    return from_quote(symbol, quote, "forex", name=name, entity_type="MISC", ts=ts, source="yahoo")
