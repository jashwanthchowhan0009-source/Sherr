# NewsGlobe

A cartoon **2.5D globe** that connects places to **real** news, weather, and market
history — then lets you **travel back in time**. Spin the planet, tap a city for its
headlines and weather, tap a market pin for index prices, and use the time dial to
rewind day / month / year.

Ported from the original single-file prototype into a proper **Vite** app with the
**exact same look and interactions**; the mock data generators are replaced with live
APIs (proxied through Vercel serverless functions so there are no CORS issues and keys/
egress stay server-side).

## Data sources
| Panel    | Source | Endpoint (proxied) | Notes |
|----------|--------|--------------------|-------|
| Weather  | [Open-Meteo](https://open-meteo.com) | `/api/weather` | Forecast API for ~last 90 days + today; ERA5 **Archive** API back to 1940. |
| News     | [GDELT DOC 2.0](https://www.gdeltproject.org/) | `/api/news` | Full-text search by place + day. GDELT's DOC index is a rolling **~3-month** window; older dates show a note. |
| Markets  | [Yahoo Finance](https://finance.yahoo.com) | `/api/stocks` | `chart` endpoint → ~30 trading-day closes up to the chosen date. |

Index symbols: BSE Sensex `^BSESN`, Nifty 50 `^NSEI`, NASDAQ `^IXIC`, Dow `^DJI`,
FTSE 100 `^FTSE`, Nikkei 225 `^N225`.

## Local development
```bash
npm install
npm run dev      # http://localhost:5173
```
> `npm run dev` serves the frontend only. The `/api/*` functions run on Vercel — use
> `vercel dev` (Vercel CLI) to exercise them locally, or just deploy a preview.

## Build
```bash
npm run build    # → dist/
npm run preview
```

## Deploy (Vercel)
This lives in the `newsglobe/` subfolder of the repo, so create a **separate** Vercel
project with **Root Directory = `newsglobe`**. The Vite preset builds `dist/` and the
`api/*.js` files deploy as Node serverless functions automatically.

```bash
cd newsglobe
vercel            # first run links/creates the project (set root dir = newsglobe)
vercel --prod
```

## Notes / future
- **Exact prototype look preserved** (cartoon globe, glass UI, time dial, past-time tint).
- Street-level / Google-Maps-style zoom is intentionally **out of scope** here — it needs
  a real map-tile renderer (MapLibre/Mapbox) and is a separate phase.
- All API calls are cached per `city + date` on the client and edge-cached on Vercel.
