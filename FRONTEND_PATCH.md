# SherByte Frontend Patch — v5.0

Your backend now returns `isTrending` and `sentiment` on every article, plus a
proper `refined_title` and `cached_summary`. Your current `normalizeArticle`
already handles these as fallbacks, so articles will just start looking better
automatically once the AI pass runs.

The one visual upgrade worth adding is a **skeleton shimmer** so the feed
doesn't sit blank while `loadFeed()` is fetching. Two small changes below.

---

## 1. Add to your `<style>` block (paste anywhere near the bottom)

```css
/* ═══════════════════════════════════
   SKELETON SHIMMER (premium loading state)
═══════════════════════════════════ */
@keyframes shimmer {
  0%   { background-position: -400px 0; }
  100% { background-position: 400px 0; }
}

.nc-skeleton {
  padding: 14px 16px;
  border-bottom: 1px solid var(--line2);
}
.nc-skel-bar {
  background: linear-gradient(90deg,
    var(--bg2) 0%,
    var(--bg3) 50%,
    var(--bg2) 100%);
  background-size: 800px 100%;
  animation: shimmer 1.4s ease-in-out infinite;
  border-radius: 8px;
}
.nc-skel-tag   { width: 80px;  height: 14px; margin-bottom: 10px; }
.nc-skel-title { width: 92%;   height: 18px; margin-bottom: 8px; }
.nc-skel-title2{ width: 70%;   height: 18px; margin-bottom: 12px; }
.nc-skel-row   { display: flex; gap: 12px; align-items: flex-start; }
.nc-skel-text  { flex: 1; }
.nc-skel-desc  { width: 100%;  height: 12px; margin-bottom: 6px; }
.nc-skel-desc2 { width: 85%;   height: 12px; }
.nc-skel-img   { width: 86px;  height: 64px; border-radius: 12px; flex-shrink: 0; }

/* Trending stripe on the hero card */
.nc-trending-badge {
  position: absolute; top: 14px; left: 14px; z-index: 2;
  padding: 4px 10px; border-radius: 100px;
  background: linear-gradient(135deg, #FF5B5B, #FB8C00);
  color: #fff; font-size: 0.6rem; font-weight: 800;
  letter-spacing: 1px; text-transform: uppercase;
  box-shadow: 0 4px 16px rgba(255,91,91,0.4);
  animation: trendingPulse 2s ease-in-out infinite;
}
@keyframes trendingPulse {
  0%,100% { transform: scale(1); }
  50%     { transform: scale(1.05); }
}

/* Sentiment dot */
.nc-sentiment {
  display: inline-block; width: 6px; height: 6px;
  border-radius: 50%; margin-left: 6px; vertical-align: middle;
}
.nc-sentiment.positive { background: #52C17A; box-shadow: 0 0 6px #52C17A; }
.nc-sentiment.neutral  { background: var(--t3); }
.nc-sentiment.negative { background: #FF5B5B; box-shadow: 0 0 6px #FF5B5B; }
```

---

## 2. Replace your `loadFeed()` skeleton section (find `feed-loader` in your code)

In your current `loadFeed()` function you have:

```js
const loadEl = document.createElement('div');
loadEl.id = 'feed-loader';
loadEl.style.cssText = 'text-align:center;padding:20px;color:var(--t3);font-size:0.8rem';
loadEl.textContent = '⚡ Loading...';
document.getElementById('home-feed').appendChild(loadEl);
```

Replace those 5 lines with:

```js
// Skeleton shimmer (4 placeholder cards)
const loadEl = document.createElement('div');
loadEl.id = 'feed-loader';
loadEl.innerHTML = Array.from({length: 4}).map(() => `
  <div class="nc-skeleton">
    <div class="nc-skel-bar nc-skel-tag"></div>
    <div class="nc-skel-row">
      <div class="nc-skel-text">
        <div class="nc-skel-bar nc-skel-title"></div>
        <div class="nc-skel-bar nc-skel-title2"></div>
        <div class="nc-skel-bar nc-skel-desc"></div>
        <div class="nc-skel-bar nc-skel-desc2"></div>
      </div>
      <div class="nc-skel-bar nc-skel-img"></div>
    </div>
  </div>
`).join('');
document.getElementById('home-feed').appendChild(loadEl);
```

---

## 3. (Optional) Show trending badge + sentiment dot on cards

In `renderArticleCard()`, inside the Hero Card branch (the `else` block, MODE A),
find the `nc-hero-image` div and add the trending badge just before it:

```js
${a.isTrending ? `<div style="position:relative">
  <div class="nc-trending-badge">🔥 Trending</div>
</div>` : ''}
```

And on the `nc-meta` line, add a sentiment indicator after the category tag:

```js
<div class="nc-meta">
  <span class="nc-tag" style="background:${cc.bg};color:${cc.hex}">${cc.icon} ${cc.short}</span>
  ${a.sentiment ? `<span class="nc-sentiment ${a.sentiment}" title="${a.sentiment}"></span>` : ''}
</div>
```

---

## 4. API URL reminder

Your current `index.html` has:
```js
const API_URL = "https://sherr-b3z2.onrender.com";
```
If you redeploy with the new `render.yaml` service name (`sherbyte-v5`), update
this to whatever Render assigns. Or keep the existing service name in
`render.yaml` so the URL doesn't change.

---

## What you'll see once deployed

Watch your backend logs on the first collection cycle:

```
[RSS] Collected 1200 raw from 51 feeds
[DEDUP] 740 unique of 1200 raw (dropped 460 intra-batch dupes)
[DB] 612 new articles inserted
[AI] Processing 50 articles via gemini (concurrency=5)
[AI] 48/50 articles refined
  Pillar 1 [society]:   98
  Pillar 2 [economy]:   76
  Pillar 3 [tech]:      84
  ...
```

The 460 duplicates that `[DEDUP]` catches are exactly the problem the old URL-only
dedup was missing. That's where your "3 sources × same story" bloat was coming from.
