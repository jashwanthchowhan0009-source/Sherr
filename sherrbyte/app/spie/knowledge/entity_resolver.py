"""
pipeline/entity_resolver.py — canonical entity resolution (Intelligence Engine V1, Step 1).

Collapses every entity mention to ONE canonical entity id before any co-occurrence
or correlation work. "Tata Motors Ltd", "Tata Motors" and "TaMo" must all resolve to
the same id — otherwise the counts driving every downstream detector are corrupt.

Two layers:
  • Pure, deterministic normalization (no DB, no I/O) — fully unit-testable:
        normalize_name()  → a canonical string key
        coarse_type()     → collapses noisy spaCy labels to ORG/PERSON/GPE/MISC
        resolve_key()     → applies seeded synonyms, returns (norm_key, type, display)
  • Async DB resolution against the entities / entity_aliases tables (migration 010):
        resolve(conn, name, type)      → entity_id (uuid str)
        resolve_many(conn, mentions)   → [entity_id, ...]
        seed_aliases(conn)             → load the curated synonym seeds
        backfill(conn)                 → resolve every info_objects.entities mention

The async functions take an asyncpg connection explicitly (same convention as
pipeline/connector.py) so this module imports nothing heavy at top level and its
pure core stays importable in isolation.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable, Optional

log = logging.getLogger("sherbyte.entity")

# ─── Coarse type buckets ──────────────────────────────────────────────────────
# spaCy emits ~18 labels; many are noise for resolution. Collapse to four coarse
# types. Same normalized name under two coarse types stays distinct on purpose
# (e.g. "Jordan" the country vs "Jordan" the person).
_COARSE = {
    "ORG": "ORG",
    "PERSON": "PERSON", "PER": "PERSON",
    "GPE": "GPE", "LOC": "GPE", "FAC": "GPE",
}


def coarse_type(label: Optional[str]) -> str:
    return _COARSE.get((label or "").strip().upper(), "MISC")


# ─── Deterministic name normalization ─────────────────────────────────────────
# Corporate/legal suffixes that carry no identity — stripped so "Apple Inc." and
# "Apple" collapse. Kept as trailing-token removal (not substring) so we never
# mangle a name that legitimately contains one of these words mid-string.
# Only unambiguous legal/corporate suffixes — NOT descriptive words like
# "Industries", "Group" or "Holdings", which are genuine parts of a name
# ("Reliance Industries", "Adani Group"). Those short forms are handled by the
# curated seed synonyms instead, so we never over-strip a real name here.
_SUFFIX_TOKENS = {
    "ltd", "limited", "inc", "incorporated", "corp", "corporation", "co",
    "company", "plc", "llc", "lp", "llp", "pvt", "private", "gmbh",
    "sa", "ag", "nv",
}
_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")

# Possessives, stripped BEFORE punctuation is removed.
#
# Punctuation removal turns an apostrophe into a space, so "India's" normalized to
# "india s" and sat in the graph as a SEPARATE entity from "India" — same for
# "Fifa's" vs "FIFA". Every possessive form of a name was its own node, splitting
# that name's co-occurrence counts across two ids and letting a detector pair an
# entity with what is really itself.
#
# Both forms: singular ("India's") and plural ("Students'" / "Nations’").
_POSSESSIVE_RE = re.compile(r"['’]s\b", flags=re.UNICODE)
_PLURAL_POSSESSIVE_RE = re.compile(r"(\w)['’](?=\s|$)", flags=re.UNICODE)


def normalize_name(raw: str) -> str:
    """Deterministic, idempotent canonical key for an entity surface form.

    lowercase → '&'→'and' → strip possessives → strip punctuation →
    collapse whitespace → drop a leading 'the' → drop trailing corporate suffixes.
    """
    if not raw:
        return ""
    s = raw.strip().lower().replace("&", " and ")
    # Before punctuation, or the apostrophe becomes a space and the possessive
    # survives as a stray "s" token: "india s" is not "india".
    s = _POSSESSIVE_RE.sub("", s)
    s = _PLURAL_POSSESSIVE_RE.sub(r"\1", s)
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    if not s:
        return ""
    tokens = s.split(" ")
    if len(tokens) > 1 and tokens[0] == "the":
        tokens = tokens[1:]
    # Peel trailing suffix tokens, but never reduce to nothing.
    while len(tokens) > 1 and tokens[-1] in _SUFFIX_TOKENS:
        tokens = tokens[:-1]
    return " ".join(tokens)


# ─── Curated synonym seeds ────────────────────────────────────────────────────
# High-value Indian-audience entities whose short forms (TaMo, RBI, INFY) would
# never collapse from normalization alone. Each canonical is inserted once with
# source='seed'; its aliases point at it. Extend freely — resolution reads these
# through SEED_LOOKUP, so adding a row needs no code change elsewhere.
_SEED: list[dict] = [
    {"canonical": "Reserve Bank of India", "type": "ORG", "aliases": ["RBI"]},
    {"canonical": "Securities and Exchange Board of India", "type": "ORG", "aliases": ["SEBI"]},
    {"canonical": "Tata Motors", "type": "ORG", "aliases": ["TaMo", "Tata Motors Ltd"]},
    {"canonical": "Reliance Industries", "type": "ORG", "aliases": ["RIL", "Reliance"]},
    {"canonical": "Infosys", "type": "ORG", "aliases": ["INFY"]},
    {"canonical": "Tata Consultancy Services", "type": "ORG", "aliases": ["TCS"]},
    {"canonical": "State Bank of India", "type": "ORG", "aliases": ["SBI"]},
    {"canonical": "HDFC Bank", "type": "ORG", "aliases": ["HDFC"]},
    {"canonical": "Bharatiya Janata Party", "type": "ORG", "aliases": ["BJP"]},
    {"canonical": "Indian National Congress", "type": "ORG", "aliases": ["Congress"]},
    {"canonical": "Narendra Modi", "type": "PERSON", "aliases": ["Modi", "PM Modi"]},
    {"canonical": "Indian Space Research Organisation", "type": "ORG", "aliases": ["ISRO"]},
    {"canonical": "Adani Group", "type": "ORG", "aliases": ["Adani"]},
    {"canonical": "Nifty 50", "type": "MISC", "aliases": ["Nifty", "Nifty50"]},
    {"canonical": "Sensex", "type": "MISC", "aliases": ["BSE Sensex"]},
    # Names built entirely from ordinary words need a seed so the junk filter
    # keeps them (a seeded entity is always valid).
    {"canonical": "Manchester City", "type": "ORG", "aliases": ["Man City"]},
    {"canonical": "Manchester United", "type": "ORG", "aliases": ["Man United", "Man Utd"]},
    {"canonical": "World Bank", "type": "ORG", "aliases": []},
]


def _build_seed_lookup() -> dict:
    """norm(alias) → (canonical_norm, canonical_display, coarse_type). The canonical
    form maps to itself too, so a full-name mention also lands on the seed identity."""
    lut: dict[str, tuple] = {}
    for e in _SEED:
        cdisplay = e["canonical"]
        ctype = coarse_type(e["type"]) if e["type"] in _COARSE else e["type"]
        cnorm = normalize_name(cdisplay)
        forms = [cdisplay] + e.get("aliases", [])
        for form in forms:
            nf = normalize_name(form)
            if nf:
                lut[nf] = (cnorm, cdisplay, ctype)
    return lut


SEED_LOOKUP = _build_seed_lookup()


# ─── Junk-mention filter (keeps the entity graph clean) ───────────────────────
# NER types that are never graph entities — dropped regardless of the surface form.
_DROP_TYPES = {"DATE", "TIME", "ORDINAL", "CARDINAL", "PERCENT", "MONEY", "QUANTITY"}

# Function words / headline furniture that leak in as Title-Case sentence starts
# ("The", "This", "But") or section labels ("Comments", "Read", "Exclusive").
_STOP_ENTITIES = set((
    "the a an and or but so if of to in on for with at by from as is are was were "
    "be been being it its this that these those he she they them his her their our "
    "your you we i not no yes new now then here there when what which who whom how "
    "why will would can could may might must just very also more most other some "
    "such only than into out up down over under after before about above below "
    "comments read watch live breaking exclusive update video photos opinion "
    "analysis report alert news views share follow subscribe advertisement "
    # Contraction stems. Stripping the apostrophe splits "Don't" into "don"+"t" and
    # "We've" into "we"+"ve"; the stem is not a stopword on its own, so without these
    # the mention survives as an entity.
    "don doesn didn won wont cant couldn shouldn wouldn isn aren wasn weren "
    "hasn haven hadn ain ve ll re nt im youre theyre "
    # Discourse markers. A summariser starts a sentence with one, the sentence is
    # Title-Cased in a headline or a summary bullet, and the word enters the graph
    # as a named entity — "Moreover" was in the top ten patterns.
    "moreover however meanwhile furthermore therefore nevertheless nonetheless "
    "additionally consequently accordingly besides thus hence although though "
    "whereas whilst while unless until since because despite instead rather "
    "otherwise likewise similarly conversely overall finally firstly secondly "
    "lastly meanwhile notably importantly specifically particularly essentially "
    "basically actually certainly clearly obviously perhaps maybe indeed "
    "according reportedly allegedly apparently supposedly presumably"
).split())

# Generic common nouns that arrive Title-Cased in headlines and get mistaken for
# entities ("Man", "Day", "World"). A mention is dropped only when EVERY token is
# generic, so real names keep working: "Man City", "World Bank", "New York Times",
# "Times of India" all survive because at least one token isn't in this set.
# Deliberately excludes words that are real standalone entities in our domain
# (Congress, Nifty, Sensex, …) — those must never be filtered.
_COMMON_NOUNS = set((
    "man men woman women people person child children boy girl family friend "
    "day days week month year years time times hour minute moment today tomorrow "
    "world country state city town village area region place home house room "
    "life death health money price prices cost market business company deal "
    "work job case study report story news article video photo image picture "
    "way thing things part point side kind type number group team member "
    "power law rule order plan project program service system process "
    "brand new old big small good bad best worst first last next "
    "top show film movie series season episode game match play song book "
    "star fan fans sun moon water fire air spider "
    "head hand eye face body mind heart voice word words name names "
    "end start begin change move win loss lead call talk meet visit "
    "high low long short full free real true false right left "
    # Number and quantifier words. Headlines Title-Case these constantly ("One
    # Killed", "Two Held"), and none of them was in either junk set, so "One"
    # was entering the graph as an entity.
    "one two three four five six seven eight nine ten eleven twelve "
    "first second third half quarter dozen hundred thousand million billion "
    "another every each many much several few lot lots couple "
    # Bare verbs/adverbs that survive Title-Casing in headline fragments.
    "says said told get got make made take took give gave come came going "
    "here there back down over ahead amid despite across "
    # Section labels, form words and evaluation nouns that arrive Title-Cased.
    # "Test" reached the top ten. It is a cricket format too, and the cost is
    # real: "series" was already generic here, so "Test Series" is dropped as
    # all-generic. Qualified forms carrying a distinctive token still survive
    # ("Boxing Day Test", "Border-Gavaskar Test"), which is where the cricket
    # sense actually earns a node.
    "test tests result results score scores total average rate level stage "
    "list index chart table figure figures data note notes item items "
    "example examples question questions answer answers reason reasons "
    "issue issues problem problems solution options option choice choices "
    "step steps stat stats detail details fact facts source sources link links "
    "page pages section topic topics subject content summary preview review "
    "edition version format model method feature features release "
    "morning evening night afternoon weekend season period phase round "
    "user users customer customers client clients staff worker workers "
    "leader leaders official officials member members expert experts "
    "student students parent parents doctor patient patients driver "
    "record records event events meeting talks decision decisions"
).split())

_WEEKDAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
_MONTHS = {"january", "february", "march", "april", "may", "june", "july", "august",
           "september", "october", "november", "december",
           "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec"}


# Build marker + live counters, so a run can PROVE which code is executing and how
# many mentions the filter actually rejected (see backfill summary).
RESOLVER_BUILD = "entity_resolver+possessive_merge/2026-08-27"
_STATS = {"checked": 0, "filtered_out": 0}


def filter_stats() -> dict:
    return dict(_STATS)


def reset_filter_stats() -> None:
    _STATS["checked"] = 0
    _STATS["filtered_out"] = 0


# What follows the apostrophe in an English contraction. Never a name fragment, so a
# match on one of these means the "proper compound" rule must NOT fire.
_CONTRACTION_TAILS = {"ve", "re", "ll", "st", "nt", "em", "til"}


def is_valid_mention(name: str, type: str = "MISC") -> bool:
    """True if a mention should enter the entity graph. Drops temporal/numeric NER
    types, function-word junk, headline tags, weekday/month names, and ≤2-char tokens.
    Pure and deterministic (applied post-normalization); counts every decision."""
    _STATS["checked"] += 1

    def _reject() -> bool:
        _STATS["filtered_out"] += 1
        return False

    if not name or not name.strip():
        return _reject()
    if (type or "").strip().upper() in _DROP_TYPES:
        return _reject()
    norm = normalize_name(name)
    if not norm or len(norm) <= 2:            # empty or ≤2 chars (e.g. "AI", "UN")
        return _reject()
    # A curated seed entity is real by definition — never filtered, even if every
    # token is an ordinary word ("Man City" → Manchester City).
    if norm in SEED_LOOKUP:
        return True

    # A hyphenated compound of capitalised parts is a proper name by construction
    # ("Spider-Man", "Coca-Cola", "Jean-Pierre", "O'Brien") — keep it even though its
    # parts are ordinary words on their own.
    #
    # BOTH sides must be >= 2 letters AND the tail must not be a contraction suffix.
    # Without the length rule it fires on "It's"; without the suffix rule it still
    # fires on "We've" and "They're", whose tails are two letters. That is exactly
    # how "It's" ended up in the entity graph as a named entity.
    m = re.search(r"[A-Za-z]{2,}[-'’]([A-Za-z]{2,})", name.strip())
    if m and m.group(1).lower() not in _CONTRACTION_TAILS:
        return True

    tokens = norm.split(" ")
    # A name followed by a bare initial is a byline fragment, not an entity:
    # "Kevin M", "Sarah J". The distinctive first token means the all-generic
    # rule below never catches these, so they entered the graph as people who do
    # not exist. Narrow on purpose — two tokens only, second a single letter — so
    # a real name carrying an initial in the middle ("John F Kennedy") survives.
    if len(tokens) == 2 and len(tokens[1]) == 1:
        return _reject()
    # Reject when EVERY token is generic — a stopword, weekday, month, or common
    # noun. Catches "the", "Monday", "Man", and titles built entirely from common
    # words ("Brand New Day"), while keeping any name carrying at least one
    # distinctive token ("Man City", "World Bank", "Times of India").
    junk = _STOP_ENTITIES | _WEEKDAYS | _MONTHS | _COMMON_NOUNS
    # A one-letter token carries no identity. This matters for contractions, where
    # stripping the apostrophe leaves a stray letter: "It's" → "it s", "Don't" →
    # "don t". Without this the leftover "s"/"t" counts as a distinctive token and
    # the whole mention survives.
    if all(t in junk or len(t) <= 1 for t in tokens):
        return _reject()
    return True


log.info("Sherr-I resolver loaded: %s", RESOLVER_BUILD)


def resolve_key(name: str, type: str = "MISC") -> tuple[str, str, str]:
    """Pure resolution: return (norm_key, coarse_type, canonical_display).

    Applies the curated seed synonyms first (so 'TaMo' → Tata Motors), else falls
    back to deterministic normalization with the mention's own casing preserved as
    the display form. norm_key == "" means the mention is empty/unusable.
    """
    base = normalize_name(name)
    if not base:
        return "", coarse_type(type), ""
    seed = SEED_LOOKUP.get(base)
    if seed:
        cnorm, cdisplay, ctype = seed
        return cnorm, ctype, cdisplay
    return base, coarse_type(type), name.strip()


# ─── Async DB resolution (entities / entity_aliases) ──────────────────────────
async def resolve(conn, name: str, type: str = "MISC", *, create: bool = True) -> Optional[str]:
    """Resolve one mention to a canonical entity id (uuid str), creating it if new.

    Lookup order: alias table → (norm_key, type) → insert. Every path records the
    surface form as an alias so the next identical mention resolves in one hop.
    Concurrency-safe via ON CONFLICT. Returns None for empty mentions (or when
    create=False and the entity does not exist yet).
    """
    if not is_valid_mention(name, type):      # drop dates/numbers/stopwords/tags
        return None
    norm_key, ctype, display = resolve_key(name, type)
    if not norm_key:
        return None

    # 1) Known surface form / seeded synonym → straight to its entity.
    row = await conn.fetchrow(
        """
        SELECT ea.entity_id FROM entity_aliases ea
        JOIN entities e ON e.id = ea.entity_id
        WHERE ea.norm_alias = $1 AND e.type = $2
        """,
        norm_key, ctype,
    )
    if row:
        eid = row["entity_id"]
        await conn.execute(
            "UPDATE entities SET mention_count = mention_count + 1, updated_at = now() WHERE id = $1",
            eid,
        )
        await _record_alias(conn, name, ctype_entity_id=eid)
        return str(eid)

    # 2) Existing canonical entity by (norm_key, type)?  3) else create it.
    if create:
        eid = await conn.fetchval(
            """
            INSERT INTO entities (canonical_name, type, norm_key, mention_count)
            VALUES ($1, $2, $3, 1)
            ON CONFLICT (norm_key, type)
            DO UPDATE SET mention_count = entities.mention_count + 1, updated_at = now()
            RETURNING id
            """,
            display or norm_key, ctype, norm_key,
        )
    else:
        eid = await conn.fetchval(
            "SELECT id FROM entities WHERE norm_key = $1 AND type = $2", norm_key, ctype,
        )
        if eid is None:
            return None

    await _record_alias(conn, name, ctype_entity_id=eid)
    return str(eid)


async def _record_alias(conn, surface: str, ctype_entity_id) -> None:
    """Persist a surface form → entity link (best-effort, dedup on conflict)."""
    norm_alias = normalize_name(surface)
    if not norm_alias:
        return
    await conn.execute(
        """
        INSERT INTO entity_aliases (alias, norm_alias, entity_id, source)
        VALUES ($1, $2, $3, 'auto')
        ON CONFLICT (norm_alias, entity_id) DO NOTHING
        """,
        surface.strip(), norm_alias, ctype_entity_id,
    )


async def resolve_many(conn, mentions: Iterable) -> list[str]:
    """Resolve a batch of mentions in a FIXED number of queries (4), instead of
    ~4 round-trips per mention — this is what keeps a full backfill from crawling.

    Each item is a str, a (name, type) tuple, or an object/dict with .name/.type.
    Order is preserved; junk and empty mentions are dropped.
    """
    # 0) Load the document-frequency table once per process. Doing it here rather than
    # in a scheduled job means the corpus-aware filter works on the first run, without
    # depending on a nightly task being wired up.
    from app.spie.graph import entities as _entities
    try:
        await _entities.ensure_loaded(conn)
        _admits = _entities.admits
    except Exception as e:                       # never block ingestion on the filter
        log.warning("entity DF filter unavailable (%s) — shape-only filter", e)
        _admits = None

    # 1) Pure pass: filter + normalize (no I/O).
    keyed: list[tuple[str, str, str, str]] = []      # (surface, norm_key, ctype, display)
    for m in mentions:
        name, mtype = _unpack_mention(m)
        # is_valid_mention stays as a cheap pre-filter — it drops the obvious cases
        # without a dict lookup. The corpus-aware rules then decide the rest.
        if not is_valid_mention(name, mtype):
            continue
        if _admits is not None:
            ok, why = _admits(name, mtype)
            if not ok:
                _STATS["filtered_out"] += 1
                _STATS["df_rejected"] = _STATS.get("df_rejected", 0) + 1
                log.debug("entity rejected by DF filter: %s (%s)", name, why)
                continue
        nk, ct, disp = resolve_key(name, mtype)
        if nk:
            keyed.append((name, nk, ct, disp))
    if not keyed:
        return []

    # 2) One lookup for every known surface form / seeded synonym.
    norm_keys = list({k[1] for k in keyed})
    rows = await conn.fetch(
        """
        SELECT ea.norm_alias, e.type, ea.entity_id
        FROM entity_aliases ea JOIN entities e ON e.id = ea.entity_id
        WHERE ea.norm_alias = ANY($1::text[])
        """,
        norm_keys,
    )
    found: dict[tuple[str, str], Any] = {(r["norm_alias"], r["type"]): r["entity_id"] for r in rows}

    # 3) One upsert for everything still unknown (dedup within the batch first).
    missing: dict[tuple[str, str], str] = {}
    for _surface, nk, ct, disp in keyed:
        if (nk, ct) not in found:
            missing.setdefault((nk, ct), disp or nk)
    if missing:
        keys = list(missing)
        new_rows = await conn.fetch(
            """
            INSERT INTO entities (canonical_name, type, norm_key, mention_count)
            SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::int[])
            ON CONFLICT (norm_key, type)
            DO UPDATE SET mention_count = entities.mention_count + 1, updated_at = now()
            RETURNING id, norm_key, type
            """,
            [missing[k] for k in keys],          # canonical_name
            [k[1] for k in keys],                 # type
            [k[0] for k in keys],                 # norm_key
            [1] * len(keys),                      # initial mention_count
        )
        for r in new_rows:
            found[(r["norm_key"], r["type"])] = r["id"]

    # 4) Bump mention_count for the ones that already existed (single UPDATE).
    existing_counts: dict[Any, int] = {}
    for _surface, nk, ct, _disp in keyed:
        eid = found.get((nk, ct))
        if eid is not None and (nk, ct) not in missing:
            existing_counts[eid] = existing_counts.get(eid, 0) + 1
    if existing_counts:
        ids = list(existing_counts)
        await conn.execute(
            """
            UPDATE entities e SET mention_count = e.mention_count + c.n, updated_at = now()
            FROM (SELECT unnest($1::uuid[]) AS id, unnest($2::int[]) AS n) c
            WHERE e.id = c.id
            """,
            ids, [existing_counts[i] for i in ids],
        )

    # 5) One batched alias insert so future runs hit step 2 directly.
    alias_rows = {}
    for surface, nk, ct, _disp in keyed:
        eid = found.get((nk, ct))
        if eid is not None:
            alias_rows[(nk, eid)] = surface.strip()
    if alias_rows:
        pairs = list(alias_rows)
        await conn.execute(
            """
            INSERT INTO entity_aliases (alias, norm_alias, entity_id, source)
            SELECT * FROM unnest($1::text[], $2::text[], $3::uuid[], $4::text[])
            ON CONFLICT (norm_alias, entity_id) DO NOTHING
            """,
            [alias_rows[p] for p in pairs],       # alias (surface form)
            [p[0] for p in pairs],                # norm_alias
            [p[1] for p in pairs],                # entity_id
            ["auto"] * len(pairs),
        )

    # Preserve input order, drop duplicates.
    out: list[str] = []
    for _surface, nk, ct, _disp in keyed:
        eid = found.get((nk, ct))
        if eid is not None:
            s = str(eid)
            if s not in out:
                out.append(s)
    return out


def _unpack_mention(m) -> tuple[str, str]:
    if isinstance(m, str):
        return m, "MISC"
    if isinstance(m, (tuple, list)) and m:
        return (m[0], m[1] if len(m) > 1 else "MISC")
    if isinstance(m, dict):
        return m.get("name", "") or m.get("canonical", ""), m.get("type", "MISC")
    return getattr(m, "name", "") or getattr(m, "canonical", ""), getattr(m, "type", "MISC")


async def seed_aliases(conn) -> int:
    """Insert the curated seed entities + their aliases (source='seed'). Idempotent —
    re-running only fills gaps. Returns the number of seed entities ensured."""
    n = 0
    for e in _SEED:
        cdisplay = e["canonical"]
        ctype = coarse_type(e["type"]) if e["type"] in _COARSE else e["type"]
        cnorm = normalize_name(cdisplay)
        eid = await conn.fetchval(
            """
            INSERT INTO entities (canonical_name, type, norm_key, mention_count)
            VALUES ($1, $2, $3, 0)
            ON CONFLICT (norm_key, type) DO UPDATE SET updated_at = now()
            RETURNING id
            """,
            cdisplay, ctype, cnorm,
        )
        for form in [cdisplay] + e.get("aliases", []):
            nf = normalize_name(form)
            if not nf:
                continue
            await conn.execute(
                """
                INSERT INTO entity_aliases (alias, norm_alias, entity_id, source)
                VALUES ($1, $2, $3, 'seed')
                ON CONFLICT (norm_alias, entity_id) DO NOTHING
                """,
                form, nf, eid,
            )
        n += 1
    log.info("entity seed ensured: %d canonical entities", n)
    return n


async def backfill(conn, batch: int = 500) -> dict:
    """Resolve every entity mention already stored in info_objects.entities (JSONB
    [{name,type,canonical}]) through the resolver, populating entities/entity_aliases.
    Chunked to avoid long locks. Returns {objects, mentions, entities}."""
    await seed_aliases(conn)
    objects = mentions = 0
    last_id = "00000000-0000-0000-0000-000000000000"
    while True:
        rows = await conn.fetch(
            """
            SELECT id, entities FROM info_objects
            WHERE id > $1 AND entities IS NOT NULL
            ORDER BY id ASC LIMIT $2
            """,
            last_id, batch,
        )
        if not rows:
            break
        for r in rows:
            last_id = r["id"]
            objects += 1
            ents = r["entities"]
            if isinstance(ents, str):
                import json
                try:
                    ents = json.loads(ents)
                except Exception:
                    ents = []
            for ent in ents or []:
                if await resolve(conn, _unpack_mention(ent)[0], _unpack_mention(ent)[1]):
                    mentions += 1
    total = await conn.fetchval("SELECT COUNT(*) FROM entities")
    log.info("entity backfill: %d objects, %d mentions → %d entities", objects, mentions, total)
    return {"objects": objects, "mentions": mentions, "entities": total}
