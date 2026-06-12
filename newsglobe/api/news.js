// Historical news by city + date via the GDELT DOC 2.0 API.
// NOTE: GDELT DOC full-text search indexes a rolling ~3-month window. For dates
// inside that window we constrain to the day; older dates fall back to the most
// relevant recent coverage for the place (and the UI explains the limitation).
export default async function handler(req, res) {
  try {
    const { q, date } = req.query || {};
    if (!q) return res.status(400).json({ error: 'q (place name) is required' });

    const today = new Date(); today.setUTCHours(0, 0, 0, 0);
    const params = new URLSearchParams({
      query: `"${q}" sourcelang:english`,
      mode: 'ArtList',
      maxrecords: '12',
      format: 'json',
      sort: 'HybridRel',
    });

    let withinWindow = true;
    if (date) {
      const d = new Date(date + 'T00:00:00Z');
      const daysBack = Math.round((today - d) / 86400000);
      if (daysBack >= 0 && daysBack <= 90) {
        const day = date.replace(/-/g, '');
        params.set('startdatetime', day + '000000');
        params.set('enddatetime', day + '235959');
        params.set('sort', 'DateDesc');
      } else if (daysBack > 90) {
        withinWindow = false;           // outside GDELT's full-text window
      }
    }

    const url = 'https://api.gdeltproject.org/api/v2/doc/doc?' + params.toString();
    const r = await fetch(url, { headers: { 'User-Agent': 'NewsGlobe/1.0 (+https://vercel.app)' } });
    const text = await r.text();

    let j = {};
    try { j = JSON.parse(text); } catch { j = { articles: [] }; }

    const seen = new Set();
    const articles = (j.articles || [])
      .map(a => ({
        h: a.title,
        s: (a.sourcecommonname || a.domain || '').replace(/^www\./, ''),
        url: a.url,
      }))
      .filter(a => a.h && !seen.has(a.h) && seen.add(a.h))
      .slice(0, 6);

    res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
    return res.status(200).json({ articles, withinWindow, source: 'gdelt' });
  } catch (e) {
    return res.status(200).json({ articles: [], error: String(e) });
  }
}
