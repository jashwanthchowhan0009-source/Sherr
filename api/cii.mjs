// Country Instability Index (CII) — worldmonitor-style geopolitical tension scoring.
// Combines a Global Peace Index-inspired baseline with live GDELT article counts
// for major hotspots. Scores: 0 = very stable, 100 = critical instability.
// Cache: 15 min (geopolitical scores don't need real-time freshness).

function send(res, status, obj, cache) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  if (cache) res.setHeader('Cache-Control', cache);
  res.end(JSON.stringify(obj));
}

// Baseline scores per ISO 3166-1 alpha-2. Approximate, GPI/Freedom House-derived.
const BASELINE = {
  // Very stable (0–15)
  IS:5, NO:7, DK:8, FI:9, NZ:8, AT:10, CH:8, IE:11, SG:10, NL:11,
  SE:10, JP:12, CA:14, AU:12, CZ:12, SI:12, SK:13, DE:14, LU:9, BE:17,
  // Stable (16–30)
  GB:21, FR:23, IT:21, ES:18, PT:11, GR:24, HU:22, PL:20, LT:16, LV:17,
  EE:14, CY:20, MT:12, RO:20, HR:14, BG:20, KR:25, MN:22, VN:28, CL:28,
  UY:19, US:27, CN:30, GH:27, MY:27, BN:11,
  // Moderate (31–55)
  IN:37, TW:38, AR:36, BR:40, TR:50, EG:44, MA:30, TN:37, ID:30, PH:38,
  TH:35, KH:33, LA:32, BD:40, LK:34, NP:32, ZW:50, KE:38, UG:42, TZ:30,
  MZ:44, SA:44, AE:27, QA:21, KW:24, OM:28, JO:33, DZ:37, GT:48, HN:54,
  SV:50, NI:52, PY:30, BO:36, PE:33, EC:38, CO:46, DO:37, CU:40, ZA:46,
  MX:54, VE:70, CM:47, MR:44, SN:30, CI:34, BY:53, GE:44, AM:48, AZ:51,
  KZ:28, UZ:32, TM:34, KG:37, TJ:41,
  // Elevated (56–75)
  PK:64, IR:67, IQ:63, LB:65, HT:76, ML:73, BF:71, NE:63, TD:68, NG:61,
  CD:71, CF:79, LY:71, AF:77, MM:74, ET:68, ER:61, RU:70,
  // Critical (76–100)
  UA:84, KP:81, PS:88, IL:68, SD:78, SS:83, YE:88, SO:81, SY:78,
};

// Hotspot GDELT queries: article count → score boost for listed countries.
const HOTSPOTS = [
  { q:'Ukraine Russia war military offensive', cc:['UA','RU'], w:1.8 },
  { q:'Israel Gaza Hamas war bombing airstrike', cc:['IL','PS','LB'], w:1.8 },
  { q:'Taiwan China military threat invasion', cc:['TW','CN'], w:1.4 },
  { q:'Iran nuclear sanctions IRGC military', cc:['IR'], w:1.3 },
  { q:'North Korea missile nuclear provocation', cc:['KP'], w:1.4 },
  { q:'Sudan civil war conflict Darfur fighting', cc:['SD','SS'], w:1.3 },
];

async function gdeltCount(query) {
  const url = 'https://api.gdeltproject.org/api/v2/doc/doc?' + new URLSearchParams({
    query, mode:'ArtList', maxrecords:'50', format:'json', timespan:'7d', sort:'DateDesc',
  });
  try {
    const r = await fetch(url, {
      headers:{ 'User-Agent':'NewsGlobe/2.0' },
      signal: AbortSignal.timeout(5000),
    });
    if (!r.ok) return 0;
    let j = {};
    try { j = JSON.parse(await r.text()); } catch {}
    return (j.articles || []).length;
  } catch { return 0; }
}

export default async function handler(req, res) {
  const cache = 'public, s-maxage=900, stale-while-revalidate=3600';
  try {
    const counts = await Promise.all(HOTSPOTS.map(h => gdeltCount(h.q)));
    const scores = { ...BASELINE };
    HOTSPOTS.forEach((h, i) => {
      const boost = Math.min(22, counts[i] * h.w * 0.45);
      h.cc.forEach(cc => {
        if (scores[cc] != null) scores[cc] = Math.min(98, scores[cc] + boost);
      });
    });
    for (const k of Object.keys(scores)) {
      scores[k] = Math.min(100, Math.max(0, Math.round(scores[k])));
    }
    send(res, 200, {
      countries: scores,
      tiers: { stable:[0,30], moderate:[31,55], elevated:[56,75], critical:[76,100] },
      updated: new Date().toISOString(),
      source: 'GPI baseline + GDELT live',
    }, cache);
  } catch (e) {
    const fallback = {};
    for (const [k, v] of Object.entries(BASELINE)) fallback[k] = Math.round(v);
    send(res, 200, {
      countries: fallback, updated: new Date().toISOString(),
      source: 'baseline', error: String(e),
    }, cache);
  }
}
