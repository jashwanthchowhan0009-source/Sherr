"""
Unit tests for the domain adapters (Intelligence Engine V1, Step 2).

DB-free: adapters are pure functions raw → Signal[]. We assert the universal
schema fields (magnitude / direction / sentiment / credibility / entities) come
out correct for each domain. Entity resolution + persistence are integration-tested
against Postgres, not here.
"""

from app.pipeline.adapters.base import direction, sentiment_to_float, clamp
from app.pipeline.adapters.credibility import score as cred
from app.pipeline.adapters import news, market, weather


# ─── base helpers ─────────────────────────────────────────────────────────────
def test_direction_sign():
    assert direction(3.2) == 1
    assert direction(-0.4) == -1
    assert direction(0.0) == 0
    assert direction(None) == 0


def test_sentiment_mapping():
    assert sentiment_to_float("positive") == 1.0
    assert sentiment_to_float("NEGATIVE") == -1.0
    assert sentiment_to_float("neutral") == 0.0
    assert sentiment_to_float("garbage") == 0.0
    assert sentiment_to_float(0.5) == 0.5
    assert sentiment_to_float(2.0) == 1.0        # clamped


def test_clamp():
    assert clamp(1.5) == 1.0 and clamp(-1.0) == 0.0 and clamp(0.3) == 0.3


# ─── credibility ──────────────────────────────────────────────────────────────
def test_credibility_tiers():
    assert cred("Reuters") == 0.95
    assert cred("The Hindu") == 0.90
    assert cred("Times of India") == 0.70
    assert cred("Some Random Blog") == 0.5
    assert cred("") == 0.5
    assert cred("Yahoo Finance") == 0.90


# ─── news adapter ─────────────────────────────────────────────────────────────
def test_news_adapter_from_dict():
    obj = {
        "id": "abc",
        "entities": [{"name": "Tata Motors", "type": "ORG"}, {"name": "Mumbai", "type": "GPE"}],
        "sentiment": "negative",
        "importance": 0.8,
        "source_name": "Reuters",
        "wwww": {"where": "Mumbai"},
    }
    sigs = news.from_info_object(obj)
    assert len(sigs) == 1
    s = sigs[0]
    assert s.domain == "news"
    assert s.entity_names == ["Tata Motors", "Mumbai"]
    assert s.direction == -1                      # from negative sentiment
    assert s.sentiment == -1.0
    assert s.magnitude == 1.0                      # one article = one unit of news volume
    assert s.confidence == 0.8                     # editorial importance → confidence
    assert s.credibility == 0.95                   # Reuters
    assert s.location == "Mumbai"
    assert s.ref_id == "abc"


def test_news_adapter_min_magnitude_and_unknown_source():
    obj = {"entities": [], "sentiment": "neutral", "importance": 0.0, "source_name": "Blog X"}
    s = news.from_info_object(obj)[0]
    assert s.magnitude == 1.0                      # article presence floor
    assert s.direction == 0
    assert s.credibility == 0.5


# ─── market adapters ──────────────────────────────────────────────────────────
def test_stock_adapter():
    s = market.from_stock("TATAMOTORS.NS", {"change_pct": 2.5}, name="Tata Motors")[0]
    assert s.domain == "stocks"
    assert s.magnitude == 2.5 and s.direction == 1
    assert s.sentiment is None and s.embedding is None
    assert s.entity_names == ["Tata Motors"]
    assert s.credibility == 0.90


def test_forex_negative_move():
    s = market.from_forex("USDINR=X", {"change_pct": -0.7})[0]
    assert s.domain == "forex" and s.direction == -1 and s.magnitude == 0.7


def test_market_flat_quote():
    s = market.from_commodity("GC=F", {"change_pct": 0.0})[0]
    assert s.direction == 0 and s.magnitude == 0.0


# ─── weather adapter ──────────────────────────────────────────────────────────
def test_weather_anomaly():
    s = weather.from_reading("Mumbai", 120.0, baseline=40.0, metric="rainfall")[0]
    assert s.domain == "weather"
    assert s.magnitude == 80.0 and s.direction == 1     # 80mm above normal
    assert s.location == "Mumbai"
    assert s.entity_names == ["Mumbai"]


def test_weather_raw_value_no_baseline():
    s = weather.from_reading("Delhi", -3.0)[0]
    assert s.magnitude == 3.0 and s.direction == -1
