"""
Market data upstreams.

A dead tile shows an em dash, which is indistinguishable from "market closed",
"symbol wrong" and "we are being rate limited". Yahoo rate-limits datacenter
ranges hard and a Render instance looks exactly like one from the outside, so
these tests pin the two things that make that survivable: try the sibling host,
and say what happened when both refuse.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import markets  # noqa: E402

_OK = {"chart": {"result": [{"meta": {
    "regularMarketPrice": 24500.0, "chartPreviousClose": 24300.0,
    "regularMarketDayHigh": 24600, "regularMarketDayLow": 24200,
    "currency": "INR"}}]}}


class _Resp:
    def __init__(self, code, payload=None):
        self.status_code = code
        self._p = payload or {}

    def json(self):
        return self._p


class _Client:
    def __init__(self, q1, q2):
        self.q1, self.q2 = q1, q2
        self.hosts = []

    async def get(self, url, **kw):
        host = url.split("//")[1].split("/")[0]
        self.hosts.append(host)
        return self.q1 if host.startswith("query1") else self.q2


@pytest.fixture(autouse=True)
def _clean():
    markets._upstream.clear()
    yield
    markets._upstream.clear()


def _fetch(client, symbols=("^NSEI",)):
    return asyncio.run(markets._yahoo(client, list(symbols)))


def test_a_rate_limited_host_falls_through_to_its_sibling():
    """Yahoo load-balances query1/query2 and will often answer on one while
    limiting the other — which is the difference between a live ticker and an
    em dash."""
    c = _Client(_Resp(429), _Resp(200, _OK))
    out = _fetch(c)
    assert out["^NSEI"]["price"] == 24500.0
    assert c.hosts == ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]


def test_a_recovered_symbol_reports_no_error():
    _fetch(_Client(_Resp(429), _Resp(200, _OK)))
    assert markets.upstream_report() == {}


def test_a_healthy_host_costs_no_second_request():
    c = _Client(_Resp(200, _OK), _Resp(200, _OK))
    _fetch(c)
    assert c.hosts == ["query1.finance.yahoo.com"]


def test_both_hosts_refusing_records_why_per_symbol():
    """This is what /markets returns, so the reason is one URL away instead of a
    guess at a screenshot full of dashes."""
    _fetch(_Client(_Resp(429), _Resp(429)), ["^NSEI", "^BSESN"])
    assert markets.upstream_report() == {"^NSEI": "HTTP 429", "^BSESN": "HTTP 429"}


def test_a_payload_with_no_price_is_reported_not_silently_dropped():
    empty = {"chart": {"result": [{"meta": {}}]}}
    _fetch(_Client(_Resp(200, empty), _Resp(200, empty)))
    assert "no price" in markets.upstream_report()["^NSEI"]


def test_a_transport_exception_is_recorded():
    class Boom:
        hosts = []

        async def get(self, url, **kw):
            raise TimeoutError("upstream timed out")

    _fetch(Boom())
    assert "TimeoutError" in markets.upstream_report()["^NSEI"]


def test_a_later_success_clears_an_earlier_note():
    """Otherwise a single blip marks the symbol broken for the process lifetime."""
    _fetch(_Client(_Resp(429), _Resp(429)))
    assert markets.upstream_report()
    _fetch(_Client(_Resp(200, _OK), _Resp(200, _OK)))
    assert markets.upstream_report() == {}


def test_markets_exposes_the_report():
    """The endpoint has to carry it or none of the above is reachable."""
    import inspect
    assert "upstream_errors" in inspect.getsource(markets.markets_all)
