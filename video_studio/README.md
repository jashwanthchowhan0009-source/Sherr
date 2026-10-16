# video_studio — the daily Top-10 news video

One command turns today's written, significant stories into a vertical
(1080×1920) news video with a voiceover, karaoke captions, music and **Sherr**,
the White Tiger guide whose eye glows with the voice and whose claws slash
between stories.

```
python -m video_studio.make                       # stories from DATABASE_URL
python -m video_studio.make --input stories.json  # stories from a file
python -m video_studio.make --input s.json --stills-only   # check the layout, no mp4
```

Output lands in `out/<date>/`: the mp4, `captions.srt` (upload alongside on
YouTube) and `manifest.json` (which stories, which ids, the exact script).

It runs itself every morning at 07:45 IST — `.github/workflows/top10_video.yml`
— and the video is attached to that run as an artifact.

## What gets in

The same bar as myFeed, never looser (`stories.rank`):

- **Written by us only** — headline differs from the publisher's, summary is
  not the placeholder. An unwritten row's summary can still be the publisher's
  prose, and reading that aloud is the copyright line this repo refuses.
- Last `lookback_hours` (24), `significance` ≥ myFeed's floor, highest first.
- At most `max_per_pillar` (3) per pillar, one story per thread.
- **Fewer eligible stories ⇒ a shorter video, never padding.** None ⇒ exit 3.

## What the narrator says

Every spoken word is a template from `format.json` or text the writer already
published (headline, then hook/summary). The templates give it a human rhythm —
"Kicking things off at number ten…", "Almost there…", "And the big one today…",
a pillar aside every other story — picked by story id, so a re-run says the
same thing. No model is called, so nothing is invented. A test runs every
template through the engine's SEBI blocklist.

## Changing the format

Everything is in `format.json`: brand, colours, pillar colours, count, order
(`countdown`/`ranked`), voice (`af_heart` default; any Kokoro voice) and pace,
every script template, title cards, music, mascot on/off. Pass a partial file
with `--format my.json` to override just some keys.

The mascot is `assets/tiger.png`; swap the PNG (transparent background) to
change the guide.
