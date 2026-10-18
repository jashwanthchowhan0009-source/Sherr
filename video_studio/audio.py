"""Voiceover (Kokoro, local, free) and the music/SFX mix.

Kokoro runs on CPU from two files fetched once from the kokoro-onnx GitHub
release — no API key, no per-video cost, no provider quota.
"""
from __future__ import annotations

import os
import urllib.request
from pathlib import Path

import numpy as np

MODEL_URL = "https://github.com/thewh1teagle/kokoro-onnx/releases/download/model-files-v1.0/"
MODEL_FILES = ("kokoro-v1.0.onnx", "voices-v1.0.bin")
SR = 24000


def model_dir() -> Path:
    d = Path(os.environ.get("KOKORO_DIR", Path.home() / ".cache" / "video_studio"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_models() -> Path:
    d = model_dir()
    for f in MODEL_FILES:
        p = d / f
        if not p.exists() or p.stat().st_size < 1_000_000:
            print(f"[voice] downloading {f}")
            urllib.request.urlretrieve(MODEL_URL + f, p)
    return d


def synthesize(segs: list[dict], fmt: dict, out_dir: Path) -> list[float]:
    """Speak every segment; store each segment's audio and duration on it."""
    import soundfile as sf
    from kokoro_onnx import Kokoro
    d = ensure_models()
    k = Kokoro(str(d / MODEL_FILES[0]), str(d / MODEL_FILES[1]))
    v = fmt["voice"]
    gap = np.zeros(int(SR * float(v.get("gap_s", 0.35))), dtype=np.float32)
    chunks, durs = [], []
    jit = float(v.get("speed_jitter", 0))
    for i, s in enumerate(segs):
        # a touch slower for the greeting and the sign-off, a little varied in between
        sp = float(v["speed"]) * (0.96 if s["kind"] != "story" else 1 + jit * ((i * 7 % 5) - 2) / 2)
        a, sr = k.create(s["text"], voice=v["name"], speed=sp, lang=v.get("lang", "en-us"))
        assert sr == SR
        seg = np.concatenate([a.astype(np.float32), gap])
        s["speech_s"] = len(a) / SR
        s["dur"] = len(seg) / SR
        chunks.append(seg)
        durs.append(s["dur"])
    sf.write(out_dir / "vo.wav", np.concatenate(chunks), SR)
    return durs


def _note(m): return 440 * 2 ** ((m - 69) / 12)


def music_bed(total: float, bpm: float, vol: float) -> np.ndarray:
    N = int(total * SR)
    out = np.zeros(N)
    chords = [[57, 60, 64, 69], [53, 57, 60, 65], [48, 55, 60, 64], [55, 59, 62, 67]]
    bar = 60 / bpm * 4
    for i in range(int(total / bar) + 1):
        s = int(i * bar * SR)
        e = min(N, int((i + 1) * bar * SR) + int(0.6 * SR))
        if s >= N:
            break
        tt = np.arange(e - s) / SR
        env = np.minimum(1, tt / 0.8) * np.exp(-np.maximum(0, tt - bar) * 4)
        c = chords[i % 4]
        seg = sum(np.sin(2 * np.pi * _note(m) * tt) * .5 + .25 * np.sin(2 * np.pi * _note(m) * 2.003 * tt) for m in c)
        seg = seg + .8 * np.sin(2 * np.pi * _note(c[0] - 12) * tt)
        out[s:e] += seg * env * vol
    beat = 60 / bpm
    rng = np.random.default_rng(3)
    for k in range(int(total / beat)):
        s = int(k * beat * SR)
        L = int(.25 * SR)
        if s + L > N:
            break
        tt = np.arange(L) / SR
        f = 55 + 60 * np.exp(-tt * 30)
        out[s:s + L] += np.sin(2 * np.pi * np.cumsum(f) / SR) * np.exp(-tt * 14) * vol * 2
        h, HL = int(s + beat / 2 * SR), int(.04 * SR)
        if h + HL < N:
            out[h:h + HL] += rng.standard_normal(HL) * np.exp(-np.arange(HL) / SR * 120) * vol * .25
    return np.convolve(out, np.ones(6) / 6, mode="same")


def mix(out_dir: Path, starts: list[float], lead: float, total: float, fmt: dict) -> Path:
    import soundfile as sf
    vo, sr = sf.read(out_dir / "vo.wav")
    N = int(total * SR)
    t = np.arange(N) / SR
    v = np.zeros(N)
    s0 = int(lead * SR)
    v[s0:s0 + len(vo)] = vo[: max(0, N - s0)]
    m = fmt.get("music", {})
    music = music_bed(total, float(m.get("bpm", 100)), float(m.get("volume", .05))) if m.get("enabled", True) else np.zeros(N)
    sfx = np.zeros(N)
    rng = np.random.default_rng(1)
    for st in starts:
        L, s = int(.5 * SR), max(0, int((st - .25) * SR))
        if s + L > N:
            continue
        n = np.convolve(rng.standard_normal(L), np.ones(12) / 12, mode="same")
        sfx[s:s + L] += n * np.sin(np.pi * np.arange(L) / L) ** 2 * .2
    env = np.convolve(np.abs(v), np.ones(int(.2 * SR)) / (.2 * SR), mode="same")
    duck = 1 - .55 * np.clip(env / (env.max() * .25 + 1e-9), 0, 1)
    fade = np.minimum(1, t / 1.0) * np.minimum(1, (total - t) / 1.2)
    out = v + music * duck * fade + sfx
    out = out / (np.max(np.abs(out)) + 1e-9) * .93
    sf.write(out_dir / "mix.wav", out, SR)
    return out_dir / "mix.wav"


def envelope(vo_path: Path, lead: float, total: float, fps: int) -> list[float]:
    """Per-frame voice loudness 0..1, smoothed — drives the mascot's eye and pulse."""
    import soundfile as sf
    vo, sr = sf.read(vo_path)
    n = int(total * fps)
    hop = sr / fps
    out = np.zeros(n)
    for f in range(n):
        s = int((f / fps - lead) * sr)
        if 0 <= s < len(vo):
            seg = vo[s:s + int(hop)]
            out[f] = np.sqrt(np.mean(seg ** 2)) if len(seg) else 0
    ref = np.percentile(out[out > 0], 95) if (out > 0).any() else 1
    out = np.clip(out / (ref + 1e-9), 0, 1)
    sm = np.zeros_like(out)
    for i in range(n):             # fast attack, slow release
        prev = sm[i - 1] if i else 0
        sm[i] = prev + (out[i] - prev) * (.6 if out[i] > prev else .18)
    return [round(float(x), 3) for x in sm]
