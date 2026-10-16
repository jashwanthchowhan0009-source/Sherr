"""Make today's Top-N video.

    python -m video_studio.make                      # stories from DATABASE_URL
    python -m video_studio.make --input stories.json # stories from a file
    python -m video_studio.make --input s.json --stills-only   # layout check, no mp4

Writes out/<date>/: <brand>_top<N>_<date>.mp4, captions.srt, manifest.json.
Exit code 3 when there is nothing eligible to narrate (not an error, nothing made).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import audio, render, script, stories

IST = timezone(timedelta(hours=5, minutes=30))
LEAD, TAIL = 0.5, 1.6


def load_format(path: str | None) -> dict:
    base = json.loads((Path(__file__).parent / "format.json").read_text())
    if path:
        over = json.loads(Path(path).read_text())
        for k, v in over.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                base[k] = {**base[k], **v}
            else:
                base[k] = v
    return base


def srt(segs) -> str:
    def ts(x):
        ms = int(round(x * 1000))
        return f"{ms // 3600000:02d}:{ms // 60000 % 60:02d}:{ms // 1000 % 60:02d},{ms % 1000:03d}"
    out = []
    for i, sg in enumerate(segs, 1):
        out.append(f"{i}\n{ts(sg['start'])} --> {ts(sg['start'] + sg['speech_s'])}\n{sg['text']}\n")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="JSON list of story rows (else DATABASE_URL)")
    ap.add_argument("--rank", action="store_true", help="apply the eligibility/ranking to --input too")
    ap.add_argument("--format", help="JSON file overriding format.json keys")
    ap.add_argument("--count", type=int)
    ap.add_argument("--out", default="out")
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 2)
    ap.add_argument("--stills-only", action="store_true")
    a = ap.parse_args(argv)

    fmt = load_format(a.format)
    if a.count:
        fmt["count"] = a.count
    now = datetime.now(IST)

    rows = stories.from_json(a.input, fmt, a.rank) if a.input else stories.from_db(fmt)
    if not rows:
        print("[make] no eligible written stories in the window — nothing to make")
        return 3
    print(f"[make] {len(rows)} stories")

    out_dir = Path(a.out) / now.strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    segs = script.build(rows, fmt, hour=now.hour)
    audio.synthesize(segs, fmt, out_dir)

    t = LEAD
    for sg in segs:
        sg["start"] = t
        sg["end"] = t + sg["dur"]
        t = sg["end"]
    total = t + TAIL
    segs[-1]["end"] = total
    audio.mix(out_dir, [sg["start"] for sg in segs], LEAD, total, fmt)

    v = fmt["video"]
    amp = audio.envelope(out_dir / "vo.wav", LEAD, total, v["fps"])
    plan = {"amp": amp, "W": v["width"], "H": v["height"], "fps": v["fps"], "fmt": fmt, "segs": segs,
            "total": total, "lead": LEAD, "date_long": now.strftime("%A, %d %B %Y"),
            "footer": fmt["footer"].format(date=now.strftime("%d %b %Y"))}

    manifest = {"made_at": now.isoformat(), "duration_s": round(total, 2),
                "stories": [{"rank": sg["rank"], "id": sg["story"].get("id"),
                             "headline": sg["story"].get("headline"), "url": sg["story"].get("url"),
                             "source": sg["story"].get("source_name")} for sg in segs if sg["kind"] == "story"],
                "script": [sg["text"] for sg in segs]}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    (out_dir / "captions.srt").write_text(srt(segs))

    if a.stills_only:
        for i, sg in enumerate(segs):
            render.still(plan, sg["start"] + min(3.0, sg["dur"] * .7), out_dir / f"still_{i:02d}.png")
        print(f"[make] stills in {out_dir}")
        return 0

    name = f"{fmt['brand'].lower()}_top{len(rows)}_{now.strftime('%Y-%m-%d')}.mp4"
    render.render(plan, out_dir / "mix.wav", out_dir / name, workers=a.workers, crf=int(v.get("crf", 23)))
    print(f"[make] done: {out_dir / name}  ({total:.1f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
