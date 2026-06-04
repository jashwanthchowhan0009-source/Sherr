"""
config.py — Central configuration for SherByte.

All runtime knobs are read from environment variables (validated by Pydantic).
Nothing in the codebase should read os.getenv directly — import `settings` here.

The 9-Pillar taxonomy and the Sherr write-mode catalogue also live here because
they are referenced across the pipeline, the recommender, and the API layer.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ─── Settings ─────────────────────────────────────────────────────────────────
class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # App
    app_name: str = "SherByte API"
    app_version: str = "6.0.0"
    env: Literal["dev", "staging", "prod"] = Field("dev", alias="ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    # Database — Supabase / Postgres + pgvector
    #   DATABASE_URL is the asyncpg connection string (postgres://...).
    #   SUPABASE_URL / SUPABASE_KEY drive the supabase-py client (auth, storage).
    database_url: str = Field("", alias="DATABASE_URL")
    supabase_url: str = Field("", alias="SUPABASE_URL")
    supabase_key: str = Field("", alias="SUPABASE_KEY")
    db_pool_min: int = Field(2, alias="DB_POOL_MIN")
    db_pool_max: int = Field(10, alias="DB_POOL_MAX")

    # Redis — caching, ARQ task queue, dedup bloom set
    redis_url: str = Field("redis://localhost:6379/0", alias="REDIS_URL")

    # Auth
    jwt_secret: str = Field("sherbyte-secret-change-in-prod", alias="JWT_SECRET")
    jwt_access_ttl_min: int = Field(60, alias="JWT_ACCESS_TTL_MIN")
    jwt_refresh_ttl_days: int = Field(30, alias="JWT_REFRESH_TTL_DAYS")

    # AI providers (router: Gemini → Groq cascade)
    gemini_api_key: str = Field("", alias="GEMINI_API_KEY")
    groq_api_key: str = Field("", alias="GROQ_API_KEY")
    gemini_model: str = Field("gemini-2.5-flash", alias="GEMINI_MODEL")
    groq_model: str = Field("llama-3.3-70b-versatile", alias="GROQ_MODEL")

    # Collection
    newsapi_key: str = Field("", alias="NEWSAPI_KEY")
    collect_interval_min: int = Field(25, alias="COLLECT_INTERVAL_MIN")
    # In-process APScheduler ingest loop. Disable in prod when GitHub-Actions
    # cron / ARQ workers own ingestion to avoid double collection.
    run_scheduler: bool = Field(True, alias="RUN_SCHEDULER")
    collect_concurrency: int = Field(10, alias="COLLECT_CONCURRENCY")
    max_entries_per_feed: int = Field(36, alias="MAX_ENTRIES_PER_FEED")

    # Embeddings — MiniLM all-MiniLM-L6-v2 → 384-dim pgvector
    embed_model: str = Field("sentence-transformers/all-MiniLM-L6-v2", alias="EMBED_MODEL")
    embed_dim: int = Field(384, alias="EMBED_DIM")
    embed_batch_size: int = Field(64, alias="EMBED_BATCH_SIZE")

    # Story graph / connector
    story_link_threshold: float = Field(0.78, alias="STORY_LINK_THRESHOLD")
    story_link_window_hours: int = Field(72, alias="STORY_LINK_WINDOW_HOURS")

    # Recommender — score = α·content + β·collab + γ·freshness
    rec_alpha: float = Field(0.5, alias="REC_ALPHA")   # content weight
    rec_beta: float = Field(0.3, alias="REC_BETA")     # collaborative weight
    rec_gamma: float = Field(0.2, alias="REC_GAMMA")   # freshness weight
    mmr_lambda: float = Field(0.7, alias="MMR_LAMBDA") # relevance vs diversity
    als_factors: int = Field(64, alias="ALS_FACTORS")
    als_iterations: int = Field(15, alias="ALS_ITERATIONS")
    als_regularization: float = Field(0.05, alias="ALS_REGULARIZATION")
    freshness_halflife_hours: float = Field(12.0, alias="FRESHNESS_HALFLIFE_HOURS")

    # Watermark — SGI labelling (IT Rules 2026)
    watermark_label: str = Field(
        "Synthetically generated and edited with AI", alias="WATERMARK_LABEL"
    )
    watermark_secret: str = Field("sherr-sgi-secret", alias="WATERMARK_SECRET")

    @property
    def ai_primary(self) -> str:
        if self.gemini_api_key:
            return "gemini"
        if self.groq_api_key:
            return "groq"
        return "rule-based"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()


# ─── Taxonomy: 9 Pillars ──────────────────────────────────────────────────────
PILLARS: dict[int, dict] = {
    1: {"name": "Society & Governance", "color": "#1E88E5", "emoji": "🏛️", "slug": "society"},
    2: {"name": "Business & Economy",   "color": "#FBC02D", "emoji": "💼", "slug": "economy"},
    3: {"name": "Science & Technology", "color": "#3949AB", "emoji": "🔬", "slug": "tech"},
    4: {"name": "Arts & Culture",       "color": "#E53935", "emoji": "🎭", "slug": "arts"},
    5: {"name": "Natural World",        "color": "#43A047", "emoji": "🌿", "slug": "nature"},
    6: {"name": "Self & Well-being",    "color": "#FB8C00", "emoji": "🧘", "slug": "selfwell"},
    7: {"name": "Philosophy & Belief",  "color": "#8E24AA", "emoji": "🔮", "slug": "philo"},
    8: {"name": "Society & Lifestyle",  "color": "#00ACC1", "emoji": "✨", "slug": "lifestyle"},
    9: {"name": "Sports & Gaming",      "color": "#546E7A", "emoji": "⚽", "slug": "sports"},
}

SLUG_TO_PILLAR: dict[str, int] = {v["slug"]: k for k, v in PILLARS.items()}

# Aliases the frontend / LLM may emit → canonical pillar id
PILLAR_ALIASES: dict[str, int] = {
    "society": 1, "governance": 1, "politics": 1,
    "economy": 2, "business": 2, "finance": 2,
    "tech": 3, "science": 3, "technology": 3,
    "arts": 4, "culture": 4, "entertainment": 4,
    "nature": 5, "environment": 5, "climate": 5,
    "selfwell": 6, "wellbeing": 6, "health": 6,
    "philo": 7, "philosophy": 7, "religion": 7,
    "lifestyle": 8, "travel": 8, "food": 8,
    "sports": 9, "gaming": 9,
}

VALID_CATEGORY_SLUGS: list[str] = [p["slug"] for p in PILLARS.values()]


# ─── Sherr write modes (Narrate stage) ────────────────────────────────────────
WRITE_MODES: dict[str, dict] = {
    "ALERT":     {"max_words": 35,  "desc": "One-line breaking flash, just the fact."},
    "BRIEF":     {"max_words": 90,  "desc": "Two-sentence digest — what happened + why it matters."},
    "EXPLAINER": {"max_words": 320, "desc": "Background + context for someone new to the story."},
    "THREAD":    {"max_words": 500, "desc": "Timeline of how the story developed across events."},
    "DEEP":      {"max_words": 900, "desc": "Long-form analysis with multi-source consensus."},
}

DEFAULT_WRITE_MODE = "BRIEF"
