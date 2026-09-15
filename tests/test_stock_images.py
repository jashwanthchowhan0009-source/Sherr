"""Dynamic stock imagery wiring (_apply_stock_images).

The stock path was dead code — resolve_image existed but nothing called it. These
pin the wiring: in stock mode with a key, cards get a Pexels URL; a publisher
hotlink is never left in place; and without a key it is a clean no-op so a
mis-set mode does not blank every card.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import image_service   # noqa: E402
import main            # noqa: E402


def _run(coro):
    # asyncio.run makes a fresh loop per call — robust under the full suite where
    # another test may have closed the shared loop.
    return asyncio.run(coro)


def test_stock_mode_with_a_key_sets_a_pexels_url(monkeypatch):
    monkeypatch.setattr(image_service, "IMAGE_MODE", "stock")
    monkeypatch.setattr(image_service, "PEXELS_API_KEY", "k")

    async def fake_resolve(**kw):
        return {"image_url": "https://images.pexels.com/x.jpg",
                "image_source": "stock", "image_credit": "Photo: A / Pexels",
                "image_query": kw.get("top_entity") or ""}
    monkeypatch.setattr(image_service, "resolve_image", fake_resolve)

    d = {"micro_tags": ["Reliance"], "category": "markets",
         "image_url": "https://deadline.com/hero.jpg", "image_source": "thumbnail"}
    _run(main._apply_stock_images([d]))
    assert d["image_url"] == "https://images.pexels.com/x.jpg"
    assert d["image_source"] == "stock"


def test_no_stock_match_clears_the_publisher_hotlink(monkeypatch):
    monkeypatch.setattr(image_service, "IMAGE_MODE", "stock")
    monkeypatch.setattr(image_service, "PEXELS_API_KEY", "k")

    async def fake_resolve(**kw):
        return {"image_url": "", "image_source": "art", "image_credit": "",
                "image_query": ""}
    monkeypatch.setattr(image_service, "resolve_image", fake_resolve)

    d = {"micro_tags": [], "category": "tech",
         "image_url": "https://publisher.example/hero.jpg", "image_source": "thumbnail"}
    _run(main._apply_stock_images([d]))
    assert d["image_url"] == "", "a publisher hotlink must not survive stock mode"
    assert d["image_source"] == "art"


def test_no_key_is_a_no_op(monkeypatch):
    monkeypatch.setattr(image_service, "IMAGE_MODE", "stock")
    monkeypatch.setattr(image_service, "PEXELS_API_KEY", "")

    async def boom(**kw):
        raise AssertionError("resolve_image must not be called without a key")
    monkeypatch.setattr(image_service, "resolve_image", boom)

    d = {"micro_tags": ["X"], "category": "tech",
         "image_url": "https://cdn.example/own.jpg", "image_source": "own"}
    _run(main._apply_stock_images([d]))
    assert d["image_url"] == "https://cdn.example/own.jpg"   # untouched


def test_our_own_image_is_kept(monkeypatch):
    monkeypatch.setattr(image_service, "IMAGE_MODE", "stock")
    monkeypatch.setattr(image_service, "PEXELS_API_KEY", "k")

    async def boom(**kw):
        raise AssertionError("an own image must not be re-resolved")
    monkeypatch.setattr(image_service, "resolve_image", boom)

    d = {"micro_tags": ["X"], "category": "tech",
         "image_url": "https://cdn.r2.dev/own.jpg", "image_source": "own"}
    _run(main._apply_stock_images([d]))
    assert d["image_url"] == "https://cdn.r2.dev/own.jpg"
