"""
adapters — one thin, pure function set per input domain (raw → Signal[]).

The engine's core rule: adding a new source = writing one adapter here; no engine
or detector code changes. Every adapter returns a list[Signal] and touches no DB.
"""

from app.pipeline.adapters import news, market, weather

# Domain → callable(raw, **kw) -> list[Signal]. The persistence layer / workers
# look adapters up here rather than importing each module directly.
REGISTRY = {
    "news": news.from_info_object,
    "stocks": market.from_stock,
    "commodities": market.from_commodity,
    "metals": market.from_metal,
    "forex": market.from_forex,
    "weather": weather.from_reading,
}

__all__ = ["news", "market", "weather", "REGISTRY"]
