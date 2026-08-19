"""
scripts/publish_pending.py — release pending_rewrite articles into the feed.

Articles sit in pending_rewrite because the AI pass never gave them our own
headline and body: `headline` is still the publisher's, `full_body` is still the
publisher's text. Category classification fixes the CATEGORY. It does not make the
words ours.

So there are two modes, and the default is the one that does not republish
somebody else's article.

  --mode aggregator   (default)
      Classify, keep the publisher's headline WITH visible credit and an outbound
      link, and replace the body with our own stub plus that link. This is the
      link-aggregator posture — the same shape Google News operates in. A short
      factual headline carries thin copyright; the article body does not, and the
      body is what gets replaced. The feed populates and nothing of the publisher's
      prose is served.

  --mode force
      Publish exactly as stored — the publisher's headline AND body. This is what
      re-creates the original exposure, so it needs an explicit flag: it cannot
      happen by defaulting or by a typo.

Both modes write an audit row into originality_json recording which mode ran, so
"why is this article published without a rewrite" is answerable later.

    python scripts/publish_pending.py --dry-run
    python scripts/publish_pending.py
    python scripts/publish_pending.py --mode force     # deliberate, logged

Backend follows DATABASE_URL, same as backfill_originality.py: postgres via asyncpg,
otherwise the local sqlite file.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.getenv("DB_PATH", "sherrbyte.db")
DATABASE_URL = (os.getenv("SHERR_I_DATABASE_URL")
                or os.getenv("DATABASE_URL") or "").strip()
_PGBOUNCER_ONLY = {"pgbouncer", "options", "sslmode", "connect_timeout",
                   "prepared_statement_cache_size", "statement_cache_size"}


def is_postgres(url: str) -> bool:
    return url.lower().startswith(("postgres://", "postgresql://"))


def sanitize_pg_dsn(url: str) -> str:
    parts = urlsplit(url)
    if not parts.query:
        return url
    kept = [kv for kv in parts.query.split("&")
            if kv and kv.split("=", 1)[0].lower() not in _PGBOUNCER_ONLY]
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       "&".join(kept), parts.fragment))


# ─── rule-based classification ────────────────────────────────────────────────
# Pillar ids match main.PILLARS. Keywords are matched against headline + body, and
# the pillar with the most distinct hits wins. Deliberately keyword-count rather
# than first-match: a story mentioning "market" once but "election" six times is a
# politics story, and first-match would have called it business.
PILLAR_KEYWORDS: dict[int, tuple[str, ...]] = {
    1: ("election", "parliament", "government", "minister", "policy", "court",
        "supreme court", "high court", "police", "protest", "bjp", "congress",
        "cabinet", "bill", "law", "verdict", "diplomat", "border", "military",
        "army", "navy", "defence", "summit", "treaty", "sanction", "war"),
    2: ("market", "stock", "sensex", "nifty", "economy", "gdp", "inflation",
        "rbi", "bank", "rupee", "investor", "ipo", "revenue", "profit", "earnings",
        "startup", "funding", "merger", "acquisition", "tariff", "trade", "tax",
        "crypto", "bitcoin", "sebi", "budget", "fiscal", "export", "import"),
    3: ("ai", "artificial intelligence", "software", "chip", "semiconductor",
        "satellite", "isro", "nasa", "rocket", "launch", "research", "scientist",
        "study", "quantum", "robot", "cyber", "data", "algorithm", "openai",
        "google", "microsoft", "apple", "android", "app", "technology", "space"),
    4: ("film", "movie", "cinema", "actor", "actress", "music", "album", "song",
        "book", "author", "novel", "art", "gallery", "museum", "theatre",
        "festival", "award", "bollywood", "hollywood", "series", "netflix"),
    5: ("climate", "weather", "monsoon", "rain", "flood", "cyclone", "earthquake",
        "wildlife", "tiger", "forest", "pollution", "environment", "emission",
        "drought", "heatwave", "glacier", "biodiversity", "conservation"),
    6: ("health", "hospital", "doctor", "disease", "vaccine", "covid", "cancer",
        "mental health", "fitness", "nutrition", "diet", "medicine", "patient",
        "surgery", "wellness", "sleep", "therapy", "who"),
    7: ("religion", "temple", "church", "mosque", "faith", "spiritual", "ethics",
        "philosophy", "belief", "ritual", "pilgrimage", "festival of"),
    8: ("travel", "food", "recipe", "fashion", "lifestyle", "celebrity",
        "influencer", "social media", "instagram", "wedding", "restaurant"),
    9: ("cricket", "football", "match", "tournament", "ipl", "olympics", "medal",
        "player", "team", "score", "wicket", "goal", "fifa", "tennis", "f1",
        "athlete", "championship", "league", "esports", "gaming"),
}
DEFAULT_PILLAR = 1
# Fewer distinct keywords than this and the match is incidental, not topical.
MIN_KEYWORD_HITS = 2

# Keywords are matched on WORD BOUNDARIES, not as substrings. Plain `in` looked
# fine until "match" hit "matches" and put a nothing-story in sports — and the real
# damage would have been "ai" firing inside "said", "rain", "chair" and "against",
# which appear in most articles and would have dragged the whole corpus into tech.
_PATTERNS: dict[int, list[tuple[str, "re.Pattern"]]] = {
    pid: [(w, re.compile(rf"\b{re.escape(w)}(?:s|es)?\b", re.IGNORECASE))
          for w in words]
    for pid, words in PILLAR_KEYWORDS.items()
}


def classify(headline: str, body: str) -> tuple[int, dict]:
    """(pillar_id, evidence). Evidence is kept so a bad call is explainable."""
    text = f"{headline or ''} {body or ''}"
    scores: dict[int, list[str]] = {}
    for pid, pats in _PATTERNS.items():
        hits = [w for w, rx in pats if rx.search(text)]
        if hits:
            scores[pid] = hits
    if not scores:
        return DEFAULT_PILLAR, {"matched": [], "reason": "no keyword matched"}
    # One hit is not evidence. "Nothing matches here" scored a single sports point
    # off the word "matches" and would have filed a nothing-story under cricket.
    # Two distinct keywords is a low bar that still rules out incidental words.
    if max(len(h) for h in scores.values()) < MIN_KEYWORD_HITS:
        return DEFAULT_PILLAR, {
            "matched": [], "reason": f"best pillar had fewer than "
                                     f"{MIN_KEYWORD_HITS} distinct keywords",
            "weak_signals": {p: h for p, h in sorted(scores.items())}}
    # Most distinct keywords wins; ties break toward the lower pillar id, which is
    # stable across runs rather than dict-order dependent.
    best = max(sorted(scores), key=lambda p: len(scores[p]))
    return best, {"matched": scores[best][:8],
                  "score": len(scores[best]),
                  "runners_up": {p: len(h) for p, h in sorted(scores.items())
                                 if p != best}}


# ─── body replacement ─────────────────────────────────────────────────────────
STUB = ("SherrByte has not yet published its own write-up of this story. "
        "The headline and link below are the publisher's; open the source to read "
        "their full report. An original SherrByte summary will replace this note "
        "once the article has been through our rewrite pass.")


def aggregator_body(source_name: str, url: str) -> str:
    """Our text, not theirs, plus the credit and the link that make it an
    aggregator entry rather than a reproduction."""
    src = source_name or "the original publisher"
    return f"{STUB}\n\nSource: {src}\n{url or ''}".strip()


def build_update(row: dict, mode: str) -> dict:
    pillar, evidence = classify(row.get("headline"), row.get("body"))
    audit = {"published_by": "publish_pending", "mode": mode,
             "classification": evidence, "pillar_id": pillar,
             "at": datetime.now(timezone.utc).isoformat()}
    if mode == "force":
        # Nothing is rewritten. Recorded plainly so the row is not mistaken later
        # for one that passed the originality gate.
        audit["warning"] = ("published with the publisher's headline AND body, "
                            "bypassing the originality gate")
        return {"pillar_id": pillar, "body": row.get("body"), "audit": audit}
    audit["body_replaced"] = True
    return {"pillar_id": pillar,
            "body": aggregator_body(row.get("source_name"), row.get("url")),
            "audit": audit}


# ─── postgres ─────────────────────────────────────────────────────────────────
async def run_postgres(url: str, *, mode: str, dry_run: bool, limit) -> dict:
    import asyncpg
    conn = await asyncpg.connect(sanitize_pg_dsn(url), statement_cache_size=0)
    try:
        for stmt in (
            "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'published'",
            "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS originality_json JSONB DEFAULT '{}'::jsonb",
        ):
            await conn.execute(stmt)

        q = """
            SELECT io.id, io.headline, io.body, io.source_name, a.url
            FROM info_objects io
            LEFT JOIN articles a ON a.id = io.article_id
            WHERE io.status = 'pending_rewrite'
            ORDER BY io.created_at DESC
        """
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = await conn.fetch(q)

        updates, by_pillar = [], {}
        for r in rows:
            u = build_update(dict(r), mode)
            by_pillar[u["pillar_id"]] = by_pillar.get(u["pillar_id"], 0) + 1
            updates.append((u["pillar_id"], u["body"], json.dumps(u["audit"]), r["id"]))

        if not dry_run and updates:
            await conn.executemany(
                "UPDATE info_objects SET pillar_id = $1, body = $2, "
                "status = 'published', originality_json = $3::jsonb WHERE id = $4",
                updates)
        return {"backend": "postgres", "mode": mode, "dry_run": dry_run,
                "found": len(rows), "published": 0 if dry_run else len(updates),
                "by_pillar": dict(sorted(by_pillar.items()))}
    finally:
        await conn.close()


# ─── sqlite ───────────────────────────────────────────────────────────────────
def run_sqlite(path: str, *, mode: str, dry_run: bool, limit) -> dict:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    q = ("SELECT id, headline, full_body AS body, source_name, url FROM articles "
         "WHERE status = 'pending_rewrite' ORDER BY published_at DESC")
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = conn.execute(q).fetchall()

    by_pillar, n = {}, 0
    for r in rows:
        u = build_update(dict(r), mode)
        by_pillar[u["pillar_id"]] = by_pillar.get(u["pillar_id"], 0) + 1
        n += 1
        if not dry_run:
            conn.execute(
                "UPDATE articles SET pillar_id=?, full_body=?, status='published', "
                "ai_processed=1, originality_json=? WHERE id=?",
                (u["pillar_id"], u["body"], json.dumps(u["audit"]), r["id"]))
    if not dry_run:
        conn.commit()
    conn.close()
    return {"backend": "sqlite", "db": path, "mode": mode, "dry_run": dry_run,
            "found": len(rows), "published": 0 if dry_run else n,
            "by_pillar": dict(sorted(by_pillar.items()))}


def run(*, mode="aggregator", dry_run=False, limit=None, url=None, db_path=None) -> dict:
    url = DATABASE_URL if url is None else url
    if is_postgres(url):
        return asyncio.run(run_postgres(url, mode=mode, dry_run=dry_run, limit=limit))
    path = db_path or DB_PATH
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return {"error": f"{path} is missing or empty and DATABASE_URL is not a "
                         f"postgres URL — set DATABASE_URL to reach production."}
    return run_sqlite(path, mode=mode, dry_run=dry_run, limit=limit)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Publish pending_rewrite articles with rule-based categories.")
    ap.add_argument("--mode", choices=("aggregator", "force"), default="aggregator",
                    help="aggregator: replace body with our stub + credit + link "
                         "(default). force: publish the publisher's text as-is.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--database-url", default=None)
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    if args.mode == "force" and not args.dry_run:
        print("WARNING: --mode force publishes the publisher's headline AND body, "
              "bypassing the originality gate.", file=sys.stderr)
    out = run(mode=args.mode, dry_run=args.dry_run, limit=args.limit,
              url=args.database_url, db_path=args.db)
    print(json.dumps(out, indent=2, default=str))
    if out.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
