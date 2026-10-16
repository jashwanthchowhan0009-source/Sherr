"""Pick the stories a Top-N video narrates.

Same bar as myFeed, never looser: a story is eligible only if WE wrote it
(headline differs from the publisher's, summary is not the placeholder). An
unwritten row's summary_60 can still be the publisher's own prose, and reading
that aloud in a video is the copyright line this repo already refuses to cross
(see CLAUDE.md, "Significance decides what gets written"). So when there are
fewer than N written stories, the video is SHORTER — it is never padded.

Pure ranking lives in `rank()` so it is testable without a database.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import body_state      # noqa: E402  (pure, no DB)
import significance    # noqa: E402  (pure, no DB)
import timestamps      # noqa: E402

COLUMNS = ("id, headline, source_headline, summary_60, hook, why_info, pillar_id, "
           "source_name, published_at, significance, is_trending, engagement, story_id, url")

# Window applied in Python, never in SQL: published_at is TEXT on some
# deployments and timestamptz on others (migration 018). Bounded by id instead.
SELECT_RECENT = (f"SELECT {COLUMNS} FROM sherrbyte_app.articles "
                 "WHERE ai_processed=1 AND status='published' "
                 "AND COALESCE(TRIM(headline),'') <> '' "
                 "ORDER BY id DESC LIMIT %s")


def is_written(row: dict) -> bool:
    head = (row.get("headline") or "").strip()
    if not head or head == (row.get("source_headline") or "").strip():
        return False
    summ = row.get("summary_60") or ""
    return bool(summ.strip()) and not body_state.is_stub(summ)


def _ts(row: dict):
    return timestamps.parse(row.get("published_at"))


def rank(rows: list[dict], fmt: dict, now: datetime | None = None) -> list[dict]:
    """Written-only, inside the window, highest significance first, capped per
    pillar and one row per story thread. Returns at most fmt['count'] rows."""
    now = now or datetime.now(timezone.utc)
    lo = now - timedelta(hours=float(fmt.get("lookback_hours", 24)))
    floor = fmt.get("min_significance")
    floor = significance.feed_min() if floor is None else int(floor)
    pool = []
    for r in rows:
        if not is_written(r):
            continue
        ts = _ts(r)
        if ts is None or ts < lo or ts > now + timedelta(hours=1):
            continue
        sig = r.get("significance")
        sig = -1 if sig is None else int(sig)
        if sig < floor:
            continue
        pool.append((sig, int(bool(r.get("is_trending"))), int(r.get("engagement") or 0), ts, r))
    pool.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=True)

    per_pillar: dict = {}
    seen_story: set = set()
    seen_head: set = set()
    out = []
    cap = int(fmt.get("max_per_pillar", 3))
    for _, _, _, _, r in pool:
        p = int(r.get("pillar_id") or 1)
        sid = int(r.get("story_id") or 0)
        hk = " ".join((r.get("headline") or "").lower().split())[:60]
        if per_pillar.get(p, 0) >= cap or (sid and sid in seen_story) or hk in seen_head:
            continue
        per_pillar[p] = per_pillar.get(p, 0) + 1
        if sid:
            seen_story.add(sid)
        seen_head.add(hk)
        out.append(r)
        if len(out) >= int(fmt.get("count", 10)):
            break
    return out


def from_db(fmt: dict, scan: int = 1500) -> list[dict]:
    import psycopg
    from psycopg.rows import dict_row
    url = os.environ["DATABASE_URL"]
    with psycopg.connect(url, row_factory=dict_row, prepare_threshold=None) as conn:
        rows = conn.execute(SELECT_RECENT, (scan,)).fetchall()
    return rank([dict(r) for r in rows], fmt)


def from_json(path: str, fmt: dict, apply_rank: bool = False) -> list[dict]:
    rows = json.loads(Path(path).read_text())
    rows = rows.get("stories", rows) if isinstance(rows, dict) else rows
    return rank(rows, fmt) if apply_rank else rows[: int(fmt.get("count", 10))]
