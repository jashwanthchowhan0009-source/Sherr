# NewsGlobe

A zoomable **3D world map** that surfaces **local + national news, weather, and markets**
for any place you tap. Spin the globe, zoom into a region/city (3D terrain relief),
tap **anywhere** to get news for that exact lat/lon, search any place, or jump to the
featured home city (**Anantapur**). Travel back in time with the date dial.

Built with **MapLibre GL** (globe projection + 3D terrain) — keyless basemap + terrain.

## Data sources
| Panel   | Source | Endpoint | Notes |
|---------|--------|----------|-------|
| Basemap | CARTO dark (raster) | — | Keyless, attribution shown |
| Terrain | AWS Terrain Tiles (terrarium DEM) | — | Keyless 3D relief + hillshade |
| Place   | BigDataCloud reverse-geocode | `/api/place` | Keyless; lat/lon → city/region/country |
| Search  | Open-Meteo geocoding | `/api/geocode` | Keyless forward geocode |
| Weather | Open-Meteo | `/api/weather` | Forecast ≤90d + today; ERA5 archive older |
| News    | **NewsData.io** (India, recent) → **GDELT** (global/history) | `/api/news` | India local depth needs a key (below) |
| Markets | Yahoo Finance chart | `/api/stocks` | Index history by symbol |

## Environment variables (Vercel project: NewsGlobe)
| Var | Required? | What it enables |
|-----|-----------|-----------------|
| `INDIA_NEWS_KEY` | optional but recommended | NewsData.io key → real **local Indian news** (incl. Anantapur). Without it, news falls back to GDELT (global/national only; hyperlocal mostly empty). Get a free key at newsdata.io. |

Set it in the Vercel dashboard → NewsGlobe project → Settings → Environment Variables,
then redeploy. Everything else is keyless and works out of the box.

## Develop / build
```bash
npm install
npm run dev      # http://localhost:5173 (frontend; /api/* run on Vercel — use `vercel dev`)
npm run build
```

## Deploy
Separate Vercel project, **Root Directory = `newsglobe`** (Vite preset; `api/*.mjs`
deploy as Node functions automatically).

## Notes
- The clay-diorama / hand-modelled-landmark renders are offline AI/3D art and can't be
  reproduced 1:1 in a live map; this targets the achievable **3D-terrain** look + a
  premium dark UI.
- News history depth varies: NewsData free tier is recent-only; GDELT full-text covers
  ~3 months. Weather/markets go back years/decades.
