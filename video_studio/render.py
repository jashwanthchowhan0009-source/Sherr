"""Frame renderer for the Top-N video: PIL frames piped into ffmpeg.

Layout (1080x1920, safe for Shorts/Reels UI):
  y 80-200    chrome: wordmark, live pill, story progress
  y 300-1380  scene content
  y 1480-1640 karaoke captions
  y 1830      footer
"""
from __future__ import annotations

import math
import subprocess
from functools import lru_cache
from multiprocessing import Pool
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont

from . import script as scriptmod

FONT_DIRS = [Path(__file__).parent / "fonts", Path("/usr/share/fonts"), Path.home() / ".fonts",
             Path.home() / ".local/share/fonts"]
FALLBACK = {"Black": "DejaVuSans-Bold.ttf", "ExtraBold": "DejaVuSans-Bold.ttf", "Bold": "DejaVuSans-Bold.ttf",
            "SemiBold": "DejaVuSans-Bold.ttf", "Medium": "DejaVuSans.ttf", "Regular": "DejaVuSans.ttf"}


@lru_cache(None)
def _find(fname: str) -> str | None:
    for d in FONT_DIRS:
        if d.exists():
            for p in d.rglob(fname):
                return str(p)
    return None


@lru_cache(None)
def F(name: str, size: int) -> ImageFont.FreeTypeFont:
    """F('InterDisplay-Black', 120). Tries .otf/.ttf, then a DejaVu fallback."""
    for ext in (".otf", ".ttf"):
        p = _find(name + ext)
        if p:
            return ImageFont.truetype(p, size)
    weight = name.split("-")[-1]
    p = _find(FALLBACK.get(weight, "DejaVuSans.ttf"))
    return ImageFont.truetype(p, size) if p else ImageFont.load_default()


def hexc(h: str) -> tuple:
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


# ── easing ──
def clamp(x, a=0.0, b=1.0): return max(a, min(b, x))
def eo(x): x = clamp(x); return 1 - (1 - x) ** 3
def eio(x): x = clamp(x); return 4 * x ** 3 if x < .5 else 1 - (-2 * x + 2) ** 3 / 2
def eback(x):
    x = clamp(x); c1 = 1.70158; c3 = c1 + 1
    return 1 + c3 * (x - 1) ** 3 + c1 * (x - 1) ** 2
def prog(t, a, d): return clamp((t - a) / d)


# ── state shared with worker processes (set by init) ──
S: dict = {}


def init(plan: dict):
    S.clear()
    S.update(plan)
    W, H = plan["W"], plan["H"]
    c = plan["fmt"]["visual"]["colors"]
    S["C"] = {k: hexc(v) for k, v in c.items()}
    S["GLOW"] = [hexc(x) for x in plan["fmt"]["visual"]["glow_colors"]]
    S["PIL"] = {k: hexc(v["color"]) for k, v in plan["fmt"]["visual"]["pillars"].items()}
    S["PNAME"] = {k: v["name"] for k, v in plan["fmt"]["visual"]["pillars"].items()}
    rng = np.random.default_rng(7)
    S["GRAIN"] = [Image.fromarray(rng.normal(0, 9, (H // 2, W // 2)).clip(-30, 30).astype(np.int8).view(np.uint8), "L")
                  for _ in range(4)]
    yy, xx = np.mgrid[0:H, 0:W]
    vig = ((xx - W / 2) / (W * .75)) ** 2 + ((yy - H / 2) / (H * .7)) ** 2
    S["VIG"] = Image.fromarray((np.clip(vig, 0, 1) ** 1.4 * 200).astype(np.uint8), "L")
    S["CAPS"] = _build_captions(plan["segs"])
    m = plan["fmt"].get("mascot", {})
    S["MASCOT"] = None
    if m.get("enabled"):
        p = Path(__file__).parent / m.get("image", "assets/tiger.png")
        if p.exists():
            img = Image.open(p).convert("RGBA")
            arr = np.asarray(img).astype(int)
            orange = (arr[..., 0] > 180) & (arr[..., 1] > 90) & (arr[..., 1] < 190) & (arr[..., 2] < 110) & (arr[..., 3] > 200)
            ys, xs = np.where(orange)
            eye = (xs.mean() / img.width, ys.mean() / img.height) if len(xs) else (.5, .5)
            S["MASCOT"] = (img, eye)


# ── helpers ──
def tw(font, s, track=0): return font.getlength(s) + track * max(0, len(s) - 1)


def text(L, xy, s, font, fill, a=1.0, track=0, anchor="la"):
    if a <= .003 or not s:
        return
    d = ImageDraw.Draw(L)
    col = fill + (int(255 * clamp(a)),)
    if not track:
        d.text(xy, s, font=font, fill=col, anchor=anchor)
        return
    x, y = xy
    w = tw(font, s, track)
    if anchor[0] == "m": x -= w / 2
    elif anchor[0] == "r": x -= w
    for ch in s:
        d.text((x, y), ch, font=font, fill=col, anchor="l" + anchor[1])
        x += font.getlength(ch) + track


def rrect(L, box, r, fill=None, outline=None, width=2, a=1.0):
    ImageDraw.Draw(L).rounded_rectangle(
        box, r, fill=fill + (int(255 * clamp(a)),) if fill else None,
        outline=outline + (int(255 * clamp(a)),) if outline else None, width=width)


def glow(L, box, r, col, a, blur=40):
    g = Image.new("RGBA", L.size, (0, 0, 0, 0))
    ImageDraw.Draw(g).rounded_rectangle(box, r, fill=col + (int(110 * clamp(a)),))
    L.alpha_composite(g.filter(ImageFilter.GaussianBlur(blur)))


def wrap(font, s, width, track=0):
    words, lines, cur = s.split(), [], ""
    for w in words:
        t = (cur + " " + w).strip()
        if tw(font, t, track) <= width or not cur:
            cur = t
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def fit_headline(s, width, max_lines=5):
    for size in (96, 88, 80, 74, 68, 62, 56, 52):
        f = F("InterDisplay-ExtraBold", size)
        lines = wrap(f, s, width)
        if len(lines) <= max_lines:
            return f, lines, int(size * 1.14)
    f = F("InterDisplay-ExtraBold", 52)
    return f, wrap(f, s, width)[:max_lines], 60


def background(t):
    W, H, C = S["W"], S["H"], S["C"]
    s = 6
    w, h = W // s, H // s
    im = Image.new("RGB", (w, h), C["bg"])
    d = ImageDraw.Draw(im)
    tint = S.get("tint")
    pos = [(.25, .22, .55, 0.), (.85, .40, .45, 1.7), (.20, .78, .40, 3.1), (.80, .88, .45, 4.4)]
    for i, (bx, by, r, ph) in enumerate(pos):
        col = S["GLOW"][i % len(S["GLOW"])]
        x = (bx + .07 * math.sin(t * .35 + ph)) * w
        y = (by + .05 * math.cos(t * .28 + ph)) * h
        rr = r * w * (1 + .08 * math.sin(t * .5 + ph))
        c = tuple(int(C["bg"][k] + (col[k] - C["bg"][k]) * .2) for k in range(3))
        d.ellipse([x - rr, y - rr, x + rr, y + rr], fill=c)
    im = im.filter(ImageFilter.GaussianBlur(28)).resize((W, H), Image.BICUBIC)
    g = Image.new("L", (W, H), 0)
    gd = ImageDraw.Draw(g)
    off = int((t * 12) % 90)
    for x in range(-90 + off, W, 90): gd.line([(x, 0), (x, H)], fill=10)
    for y in range(-90 + off, H, 90): gd.line([(0, y), (W, y)], fill=10)
    im = Image.composite(Image.new("RGB", (W, H), (60, 70, 100)), im, g)
    return Image.composite(Image.new("RGB", (W, H), (0, 0, 0)), im, S["VIG"])


def words_in(L, lines, x0, y0, t, start, font, lh, colors=None, stagger=.07, track=0):
    k = 0
    sp = font.getlength(" ")
    for li, words in enumerate(lines):
        x = x0
        for w in words:
            p = eo(prog(t, start + k * stagger, .5))
            text(L, (x, y0 + li * lh + (1 - p) * 36), w, font, (colors or {}).get(k, S["C"]["text"]), a=p, track=track)
            x += tw(font, w, track) + sp
            k += 1


def label(L, x, y, s, t, start, col):
    p = eo(prog(t, start, .45))
    text(L, (x - (1 - p) * 30, y), s, F("Inter-Bold", 30), col, a=p, track=6)
    ImageDraw.Draw(L).rectangle([x, y + 48, x + 70 * p, y + 52], fill=col + (int(255 * p),))


# ── captions ──
def _build_captions(segs):
    caps = []
    for sg in segs:
        ws = sg.get("caption", sg["text"]).split()
        if not ws:
            continue
        speech = max(.3, sg["speech_s"] - .05)
        wts = [len(w) + 3 + (6 if w[-1] in ",.?!" else 0) for w in ws]
        tot = sum(wts)
        t0 = sg["start"] + .05
        chunk = []
        for w, wt in zip(ws, wts):
            dt = speech * wt / tot
            chunk.append((w, t0, t0 + dt))
            t0 += dt
            if len(chunk) >= 4 or w[-1] in ",.?!" or sum(len(c[0]) for c in chunk) > 20:
                caps.append(chunk)
                chunk = []
        if chunk:
            caps.append(chunk)
    return caps


def captions(L, t):
    C, W = S["C"], S["W"]
    f = F("InterDisplay-ExtraBold", 64)
    caps = S["CAPS"]
    for ci, ch in enumerate(caps):
        s, e = ch[0][1], ch[-1][2]
        nxt = caps[ci + 1][0][1] if ci + 1 < len(caps) else e + .5
        if not (s - .05 <= t < min(nxt, e + .5)):
            continue
        p = eback(prog(t, s - .05, .22))
        words = [w.upper() for w, _, _ in ch]
        sp = f.getlength(" ") + 18
        wid = [f.getlength(w) for w in words]
        lines, cur, cw = [], [], 0
        for i, w in enumerate(wid):
            if cur and cw + sp + w > 920:
                lines.append(cur); cur, cw = [], 0
            cur.append(i); cw += (sp if len(cur) > 1 else 0) + w
        lines.append(cur)
        y0 = 1530 - (len(lines) - 1) * 40 + (1 - p) * 30
        for li, idx in enumerate(lines):
            x = (W - (sum(wid[i] for i in idx) + sp * (len(idx) - 1))) / 2
            for i in idx:
                y = y0 + li * 84
                if ch[i][1] <= t < ch[i][2] + .05:
                    rrect(L, (x - 12, y - 6, x + wid[i] + 12, y + 78), 14, fill=C["accent"], a=clamp(p))
                    col = (12, 12, 16)
                else:
                    text(L, (x + 3, y + 5), words[i], f, (0, 0, 0), a=.55 * clamp(p))
                    col = C["text"] if t >= ch[i][1] else (200, 204, 215)
                text(L, (x, y), words[i], f, col, a=clamp(p))
                x += wid[i] + sp
        return


# ── chrome ──
def chrome(L, t):
    C, W, fmt = S["C"], S["W"], S["fmt"]
    a = eo(prog(t, 0, .6))
    f = F("InterDisplay-Black", 40)
    text(L, (70, 120), fmt["brand"], f, C["text"], a=a, track=5, anchor="lm")
    wm = tw(f, fmt["brand"], 5)
    d = ImageDraw.Draw(L)
    d.ellipse([70 + wm + 10, 112, 70 + wm + 26, 128], fill=C["accent"] + (int(255 * a),))
    stories = [sg for sg in S["segs"] if sg["kind"] == "story"]
    cur = next((sg for sg in stories if sg["start"] <= t < sg["end"]), None)
    pill = f"TOP {len(stories)}  ·  TODAY" if cur is None else f"#{cur['rank']:02d}  ·  TOP {len(stories)}"
    pf = F("Inter-Bold", 26)
    w = tw(pf, pill, 3)
    x1 = W - 70; x0 = x1 - w - 84
    rrect(L, (x0, 92, x1, 148), 28, fill=(255, 255, 255), a=.07 * a)
    rrect(L, (x0, 92, x1, 148), 28, outline=(255, 255, 255), width=2, a=.14 * a)
    d.ellipse([x0 + 26, 112, x0 + 42, 128], fill=C["alert"] + (int(255 * a * (.55 + .45 * math.sin(t * 5))),))
    text(L, (x0 + 58, 120), pill, pf, C["text"], a=a, track=3, anchor="lm")
    n = len(stories); gap = 8; sx, ex = 70, W - 70
    sw = (ex - sx - gap * (n - 1)) / max(1, n)
    for i, sg in enumerate(stories):
        x = sx + i * (sw + gap)
        rrect(L, (x, 186, x + sw, 192), 3, fill=(255, 255, 255), a=.16 * a)
        pp = clamp((t - sg["start"]) / (sg["end"] - sg["start"])) if t >= sg["start"] else 0
        if pp > 0:
            rrect(L, (x, 186, x + max(6, sw * pp), 192), 3, fill=C["text"], a=.95 * a)
    text(L, (W / 2, 1830), S["footer"], F("Inter-Medium", 26), C["dim"], a=a, anchor="mm")



def amp_at(t):
    a = S.get("amp") or []
    i = int(t * S["fps"])
    return a[i] if 0 <= i < len(a) else 0.0


def mascot(L, cx, cy, h, t, a=1.0, intro=0.0):
    """The guide. It breathes, bobs, and its eye glows with the voice."""
    if not S.get("MASCOT") or a <= .01:
        return
    img, (ex, ey) = S["MASCOT"]
    amp = amp_at(t)
    sc = h / img.height * (1 + .035 * amp + .01 * math.sin(t * 2.2)) * (.6 + .4 * eback(intro) if intro < 1 else 1)
    w2, h2 = max(2, int(img.width * sc)), max(2, int(img.height * sc))
    m = img.resize((w2, h2), Image.LANCZOS).rotate(2.2 * math.sin(t * 1.3), Image.BICUBIC, expand=True)
    if a < 1:
        m.putalpha(m.getchannel("A").point(lambda v: int(v * clamp(a))))
    x = int(cx - m.width / 2)
    y = int(cy - m.height / 2 + 9 * math.sin(t * 2.0))
    # soft shadow-glow so the white stripes sit on the dark background
    sh = Image.new("RGBA", L.size, (0, 0, 0, 0))
    ImageDraw.Draw(sh).ellipse([cx - w2 * .55, cy - h2 * .45, cx + w2 * .55, cy + h2 * .45],
                               fill=(255, 150, 60, int(28 * a)))
    L.alpha_composite(sh.filter(ImageFilter.GaussianBlur(60)))
    L.alpha_composite(m, (x, y))
    g = Image.new("RGBA", L.size, (0, 0, 0, 0))
    gx, gy = x + ex * m.width, y + ey * m.height
    r = w2 * (.16 + .10 * amp)
    ImageDraw.Draw(g).ellipse([gx - r, gy - r * .7, gx + r, gy + r * .7],
                              fill=(255, 140, 40, int(a * (60 + 150 * amp))))
    L.alpha_composite(g.filter(ImageFilter.GaussianBlur(max(4, r * .5))))


def claw(L, p):
    """Three claw slashes tearing across the frame; p runs 0 -> 1."""
    if p <= 0 or p >= 1:
        return
    W, H = S["W"], S["H"]
    g = Image.new("RGBA", L.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(g)
    head = eio(clamp(p / .6))
    tail = eio(clamp((p - .35) / .65))
    for k in range(3):
        x0, y0 = W * .95 - k * 150, -120 + k * 40
        x1, y1 = W * .10 - k * 150, H + 120 - k * 40
        n = 28
        left, right = [], []
        for j in range(n + 1):
            u = tail + (head - tail) * j / n
            px, py = x0 + (x1 - x0) * u, y0 + (y1 - y0) * u
            wid = (34 - k * 6) * math.sin(math.pi * clamp((u - tail) / max(1e-3, head - tail))) + 2
            jag = 5 * math.sin(u * 90 + k)
            left.append((px - wid - jag, py))
            right.append((px + wid + jag, py))
        d.polygon(left + right[::-1], fill=(255, 255, 255, 235))
        d.line([((l[0] + r[0]) / 2, l[1]) for l, r in zip(left, right)], fill=(255, 150, 50, 255), width=4)
    L.alpha_composite(g.filter(ImageFilter.GaussianBlur(14)))
    L.alpha_composite(g)


# ── scenes ──
def sc_intro(L, t, s, sg):
    C, W = S["C"], S["W"]
    fmt = S["fmt"]
    n = sum(1 for x in S["segs"] if x["kind"] == "story")
    mascot(L, 820, 560, 660, t, a=eo(prog(t, s + .1, .5)), intro=prog(t, s + .1, .6))
    label(L, 80, 330, fmt["titles"]["intro_kicker"], t, s + .05, C["alert"])
    lines = [x.format(count=n) for x in fmt["titles"]["intro_title"]]
    hf = F("InterDisplay-Black", 150)
    words_in(L, [ln.split() for ln in lines], 80, 430, t, s + .15, hf, 168,
             colors={0: C["accent"], 1: C["accent"]}, stagger=.09)
    p = eo(prog(t, s + .9, .6))
    y = 430 + 168 * len(lines) + 30
    ImageDraw.Draw(L).rectangle([80, y, 80 + 920 * p, y + 3], fill=(255, 255, 255, int(60 * p)))
    text(L, (80, y + 50 + (1 - p) * 20), S["date_long"], F("Inter-Medium", 48), C["muted"], a=p)
    # one tick per story, coloured by pillar, popping in
    stories = [x for x in S["segs"] if x["kind"] == "story"]
    for i, st in enumerate(sorted(stories, key=lambda x: x["rank"])):
        q = eback(prog(t, s + 1.2 + i * .06, .4))
        col = S["PIL"].get(str(st["story"].get("pillar_id") or 1), C["accent"])
        cx = 92 + i * 52
        r = 14 * q
        ImageDraw.Draw(L).ellipse([cx - r, y + 160 - r, cx + r, y + 160 + r], fill=col + (255,))


def _art(L, x0, y0, w, h, seed, col, a):
    """Generated abstract panel — no third-party image, so nothing to license."""
    rng = np.random.default_rng(seed)
    g = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(g)
    for i in range(7):
        r = rng.uniform(.25, .9) * w
        cx, cy = rng.uniform(.3, 1.0) * w, rng.uniform(-.2, .8) * h
        d.ellipse([cx - r, cy - r, cx + r, cy + r], outline=col + (int(70 * a),), width=3)
    L.alpha_composite(g, (int(x0), int(y0)))


def sc_story(L, t, s, sg):
    C, W = S["C"], S["W"]
    st = sg["story"]
    pid = str(st.get("pillar_id") or 1)
    col = S["PIL"].get(pid, C["accent"])
    a = eo(prog(t, s, .5))
    if S.get("MASCOT"):
        mascot(L, 860, 440, 470, t, a=a)
    else:
        _art(L, 380, 250, 700, 420, int(st.get("id") or sg["rank"]), col, a)
    # giant rank numeral, gradient pillar colour → text
    num = f"{sg['rank']:02d}"
    nf = F("InterDisplay-Black", 300)
    p = eback(prog(t, s + .05, .55))
    mask = Image.new("L", L.size, 0)
    ImageDraw.Draw(mask).text((64, 600 + (1 - clamp(p)) * 60), num, font=nf, fill=int(255 * clamp(p)), anchor="ls")
    fr = ((np.arange(S["H"]) - 330) / 280).clip(0, 1)[:, None, None]
    top = np.array(col) * .65 + 255 * .35
    grad = top * (1 - fr) + np.array(col) * fr
    gim = Image.fromarray(np.repeat(grad, S["W"], axis=1).astype(np.uint8), "RGB").convert("RGBA")
    L.alpha_composite(Image.composite(gim, Image.new("RGBA", L.size, (0, 0, 0, 0)), mask))
    # pillar pill
    pf = F("Inter-Bold", 28)
    name = S["PNAME"].get(pid, "").upper()
    q = eo(prog(t, s + .2, .4))
    w = tw(pf, name, 4)
    rrect(L, (80, 640, 80 + w + 60, 696), 28, fill=col, a=.16 * q)
    rrect(L, (80, 640, 80 + w + 60, 696), 28, outline=col, width=2, a=q)
    text(L, (110, 668), name, pf, col, a=q, track=4, anchor="lm")
    # headline
    hf, lines, lh = fit_headline(scriptmod.clean_headline(st.get("headline")), W - 160)
    words_in(L, [ln.split() for ln in lines], 80, 740, t, s + .3, hf, lh, stagger=.045)
    y = 740 + lh * len(lines) + 34
    r = eo(prog(t, s + 1.0, .5))
    ImageDraw.Draw(L).rectangle([80, y, 80 + 120 * r, y + 5], fill=col + (int(255 * r),))
    room_px = 1380 - (y + 40)
    body = sg.get("display_line", "")
    for size in (40, 36, 32):
        bf = F("Inter-Medium", size)
        blines = wrap(bf, body, W - 160)
        if len(blines) * int(size * 1.35) <= room_px:
            break
    lh2 = int(size * 1.35)
    keep = max(1, room_px // lh2)
    if len(blines) > keep:
        blines = blines[:keep]
        blines[-1] = blines[-1].rstrip(",;. ") + "…"
    for i, ln in enumerate(blines):
        text(L, (80, y + 40 + i * lh2 + (1 - r) * 16), ln, bf, C["muted"], a=r)
    src = st.get("source_name") or ""
    if src:
        text(L, (80, 1400), f"via {src}", F("Inter-SemiBold", 26), C["dim"], a=r)


def sc_outro(L, t, s, sg):
    C, W = S["C"], S["W"]
    fmt = S["fmt"]
    a = eback(prog(t, s + .05, .6))
    f = F("InterDisplay-Black", 130)
    w = tw(f, fmt["brand"], 8)
    mascot(L, W / 2, 450, 480, t, a=clamp(a))
    glow(L, (W / 2 - w / 2, 700, W / 2 + w / 2, 860), 60, C["accent"], clamp(a) * .5, blur=70)
    text(L, (W / 2, 790 + (1 - clamp(a)) * 40), fmt["brand"], f, C["text"], a=clamp(a), track=8, anchor="mm")
    r = eo(prog(t, s + .4, .5))
    ImageDraw.Draw(L).rectangle([W / 2 - 60 * r, 900, W / 2 + 60 * r, 906], fill=C["accent"] + (255,))
    text(L, (W / 2, 980), fmt["tagline"], F("Inter-Medium", 42), C["muted"], a=r, anchor="mm")
    c = eback(prog(t, s + .7, .5))
    if c > 0:
        bw, bh, y0 = 420, 110, 1100
        x0 = W / 2 - bw / 2 * c
        rrect(L, (x0, y0, x0 + bw * c, y0 + bh), 55, fill=C["accent"], a=clamp(c))
        text(L, (W / 2, y0 + bh / 2), fmt["titles"]["outro_cta"], F("InterDisplay-ExtraBold", 44), (12, 12, 16),
             a=clamp((c - .5) * 2), track=4, anchor="mm")


SCENES = {"intro": sc_intro, "story": sc_story, "outro": sc_outro}


def frame(fi):
    W, H, fps = S["W"], S["H"], S["fps"]
    t = fi / fps
    bg = background(t).convert("RGBA")
    segs = S["segs"]
    for i, sg in enumerate(segs):
        s0 = sg["start"] - (S["lead"] if i == 0 else 0)
        if s0 - .01 <= t < sg["end"]:
            L = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            SCENES[sg["kind"]](L, t, sg["start"] if i else 0.0, sg)
            out = clamp((sg["end"] - t) / .28) if i < len(segs) - 1 else 1
            if out < 1:
                o = eo(out)
                L.putalpha(L.getchannel("A").point(lambda v: int(v * o)))
                L = L.transform(L.size, Image.AFFINE, (1, 0, 0, 0, 1, -(1 - out) * 60), Image.BILINEAR)
            bg.alpha_composite(L)
    if S["fmt"].get("mascot", {}).get("claw_transitions") and S.get("MASCOT"):
        for sg in segs[1:]:
            if sg["start"] - .3 <= t <= sg["start"] + .35:
                claw(bg, (t - sg["start"] + .3) / .65)
    C = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    chrome(C, t)
    captions(C, t)
    bg.alpha_composite(C)
    arr = np.asarray(bg.convert("RGB")).astype(np.int16)
    gr = np.asarray(S["GRAIN"][fi % 4].resize((W, H))).view(np.int8).astype(np.int16)
    return np.clip(arr + gr[:, :, None] // 2, 0, 255).astype(np.uint8).tobytes()


def still(plan: dict, t: float, path: Path):
    init(plan)
    Image.frombytes("RGB", (plan["W"], plan["H"]), frame(int(t * plan["fps"]))).save(path)


def render(plan: dict, audio: Path, out: Path, workers: int = 2, crf: int = 23):
    W, H, fps = plan["W"], plan["H"], plan["fps"]
    nf = int(plan["total"] * fps)
    ff = subprocess.Popen(
        ["ffmpeg", "-y", "-loglevel", "error", "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{W}x{H}",
         "-r", str(fps), "-i", "-", "-i", str(audio), "-c:v", "libx264", "-preset", "medium",
         "-crf", str(crf), "-maxrate", "9M", "-bufsize", "18M", "-pix_fmt", "yuv420p",
         "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", "-shortest", str(out)],
        stdin=subprocess.PIPE)
    with Pool(workers, initializer=init, initargs=(plan,)) as pool:
        for i, b in enumerate(pool.imap(frame, range(nf), chunksize=4)):
            ff.stdin.write(b)
            if i % (fps * 10) == 0:
                print(f"[render] {i}/{nf}", flush=True)
    ff.stdin.close()
    if ff.wait() != 0:
        raise RuntimeError("ffmpeg failed")
