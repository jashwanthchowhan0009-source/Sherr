// Index price history via Yahoo Finance's public chart endpoint.
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
  const symbol = p.get('symbol'), date = p.get('date');
  if (!symbol) return send(res, 400, { error: 'symbol is required' });

  try {
    const end = date ? new Date(date + 'T00:00:00Z') : new Date();
    const period2 = Math.floor(end.getTime() / 1000) + 86400;  // include the day itself
    const period1 = period2 - 60 * 86400;                      // ~60 days → 30+ trading days

    const path = `/v8/finance/chart/${encodeURIComponent(symbol)}?period1=${period1}&period2=${period2}&interval=1d`;
    let data = null;
    for (const host of ['query1.finance.yahoo.com', 'query2.finance.yahoo.com']) {
      try {
        const r = await fetch(`https://${host}${path}`, {
          headers: { 'User-Agent': 'Mozilla/5.0 (compatible; NewsGlobe/1.0)' },
        });
        if (r.ok) { data = await r.json(); break; }
      } catch { /* try next host */ }
    }

    const result = data && data.chart && data.chart.result && data.chart.result[0];
    if (!result) return send(res, 200, { value: null, series: [], error: 'no data', source: 'yahoo' });

    const closeRaw = (result.indicators && result.indicators.quote && result.indicators.quote[0] && result.indicators.quote[0].close) || [];
    const closes = closeRaw.filter(v => v != null && isFinite(v));
    const series = closes.slice(-30);
    const value = series.length ? series[series.length - 1] : null;
    const prevClose = series.length > 1 ? series[series.length - 2] : null;
    const currency = (result.meta && result.meta.currency) || '';

    return send(res, 200, { value, prevClose, series, currency, asOf: date || null, source: 'yahoo' },
      'public, s-maxage=3600, stale-while-revalidate=86400');
  } catch (e) {
    return send(res, 200, { value: null, series: [], error: String(e) });
  }
}
