#!/usr/bin/env python3
"""migrate_to_neon.py — move the pruned Supabase database to Neon (free Postgres).

The Supabase quota resets on the 20th but will blow again within days, so this
prepares a lift-and-shift to Neon's free tier. IT DOES NOT RUN BY DEFAULT: with
no flags it prints the exact pg_dump / psql commands and the env-var swaps, so
you can read them before anything touches either database. Pass --run to execute.

    python scripts/migrate_to_neon.py            # print the plan + commands
    python scripts/migrate_to_neon.py --run      # dump source → restore into Neon

RUN scripts/db_cleanup.py --apply FIRST. Dumping the bloated DB and restoring it
into Neon just moves the problem; migrate the pruned one.

Env:
  SOURCE_DATABASE_URL   the current Supabase DSN   (falls back to DATABASE_URL)
  NEON_DATABASE_URL     the target Neon DSN        (create the project first)

What it does (in order):
  1. Ensures `CREATE EXTENSION vector` on Neon (pgvector is available there).
  2. pg_dump the two schemas we own — sherrbyte_app and public — from the source,
     schema + data, no owner/ACL (Neon's role differs).
  3. psql-restore that dump into Neon.
  4. The HNSW indexes come across in the dump DDL
     (info_objects.embedding, story_threads.centroid, vector_cosine_ops); step 5
     REINDEXes them so they are rebuilt against the restored rows.

After it succeeds, swap these and redeploy — NOTHING in code changes:
  • Render (the web service AND the cron_detectors / cron_ingest jobs):
      DATABASE_URL         → the Neon DSN
      SHERR_I_DATABASE_URL → the Neon DSN   (if set separately)
  • GitHub → repo Settings → Secrets and variables → Actions:
      DATABASE_URL         → the Neon DSN   (used by .github/workflows/*.yml)
  Leave UPSTASH_REDIS_URL / REDIS_URL untouched — the cache layer is unaffected.
Keep the Supabase project until Neon has served a full ingest+detector cycle;
roll back by swapping the env vars back.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile

SCHEMAS = ("sherrbyte_app", "public")


def _need(name: str, *fallbacks: str) -> str:
    val = (os.getenv(name) or "").strip()
    for fb in fallbacks:
        if not val:
            val = (os.getenv(fb) or "").strip()
    if not val:
        sys.exit(f"Set {name}" + (f" (or {', '.join(fallbacks)})" if fallbacks else ""))
    return val


def _run(cmd: list[str], **kw) -> None:
    print("  $ " + " ".join(cmd))
    subprocess.run(cmd, check=True, **kw)


def main() -> None:
    ap = argparse.ArgumentParser(description="Migrate the pruned DB to Neon.")
    ap.add_argument("--run", action="store_true",
                    help="execute the dump + restore (default: print the plan only)")
    ap.add_argument("--dump-file", default="",
                    help="where to write the dump (default: a temp file)")
    args = ap.parse_args()

    source = _need("SOURCE_DATABASE_URL", "DATABASE_URL")
    neon = _need("NEON_DATABASE_URL")

    dump = args.dump_file or os.path.join(tempfile.gettempdir(), "sherr_migrate.sql")
    schema_args: list[str] = []
    for s in SCHEMAS:
        schema_args += ["--schema", s]

    dump_cmd = ["pg_dump", source, "--no-owner", "--no-acl",
                "--format=plain", *schema_args, "--file", dump]
    ext_cmd = ["psql", neon, "-v", "ON_ERROR_STOP=1",
               "-c", "CREATE EXTENSION IF NOT EXISTS vector;"]
    restore_cmd = ["psql", neon, "-v", "ON_ERROR_STOP=1", "-f", dump]
    reindex_cmd = ["psql", neon, "-v", "ON_ERROR_STOP=1", "-c",
                   "REINDEX INDEX CONCURRENTLY idx_info_embedding_hnsw; "
                   "REINDEX INDEX CONCURRENTLY idx_threads_centroid_hnsw;"]

    print(__doc__.split("\n\n")[0])
    print("\n  PLAN:")
    print("  1. enable pgvector on Neon:")
    print("     $ " + " ".join(ext_cmd))
    print("  2. dump the two schemas from the source:")
    print("     $ " + " ".join(dump_cmd))
    print("  3. restore into Neon:")
    print("     $ " + " ".join(restore_cmd))
    print("  4. rebuild the HNSW vector indexes:")
    print("     $ " + " ".join(reindex_cmd))
    print("\n  Then swap DATABASE_URL / SHERR_I_DATABASE_URL on Render and the")
    print("  DATABASE_URL GitHub Actions secret to the Neon DSN and redeploy.")

    if not args.run:
        print("\n  (dry-run — re-run with --run to execute)")
        return

    print("\n  === RUNNING ===")
    _run(ext_cmd)
    _run(dump_cmd)
    _run(restore_cmd)
    try:
        _run(reindex_cmd)
    except subprocess.CalledProcessError:
        # CONCURRENTLY can't run in some pooled sessions; fall back to a plain
        # REINDEX (brief lock, fine on a fresh DB nobody is reading yet).
        print("  concurrent reindex failed, retrying non-concurrently…")
        _run(["psql", neon, "-v", "ON_ERROR_STOP=1", "-c",
              "REINDEX INDEX idx_info_embedding_hnsw; "
              "REINDEX INDEX idx_threads_centroid_hnsw;"])
    print(f"\n  Migration complete. Dump kept at {dump} for reference.")
    print("  Verify on Neon, then swap the env vars and redeploy.")


if __name__ == "__main__":
    main()
