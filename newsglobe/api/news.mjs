// Historical news by place + date via the GDELT DOC 2.0 API.
// Standard Node req/res only (no Vercel helpers).
function params(req) {
  try { return new URL(req.url, 'http://localhost').searchParams; }
  catch { return new URLSearchParams(); }
}
function send(res, status, obj, cache) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  if (cache) res.setHeader('Cache-Control', cache);
  res.end(JSON.stringify(obj));
}

export default async function handler(req, res) {
  const p = params(req);
  const q = p.get('q'), date = p.get('date');
  if (!q) return send(res, 400, { error: 'q (place name) is required' });

  try {
    const today = new Date(); today.setUTCHours(0, 0, 0, 0);
    const qp = new URLSearchParams({
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
        qp.set('startdatetime', day + '000000');
        qp.set('enddatetime', day + '235959');
        qp.set('sort', 'DateDesc');
      } else if (daysBack > 90) {
        withinWindow = false;            // outside GDELT's full-text window
      }
    }

    const url = 'https://api.gdeltproject.org/api/v2/doc/doc?' + qp.toString();
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

    return send(res, 200, { articles, withinWindow, source: 'gdelt' },
      'public, s-maxage=3600, stale-while-revalidate=86400');
  } catch (e) {
    return send(res, 200, { articles: [], error: String(e) });
  }
}
