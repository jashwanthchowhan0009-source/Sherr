// Index price history via Yahoo Finance's public chart endpoint.
// Server-side fetch avoids the browser CORS block. Returns ~30 trading-day
// closes up to the requested date, plus the value and previous close.
export default async function handler(req, res) {
  try {
    const { symbol, date } = req.query || {};
    if (!symbol) return res.status(400).json({ error: 'symbol is required' });

    const end = date ? new Date(date + 'T00:00:00Z') : new Date();
    const period2 = Math.floor(end.getTime() / 1000) + 86400;   // include the day itself
    const period1 = period2 - 60 * 86400;                       // ~60 days → 30+ trading days

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
    if (!result) return res.status(200).json({ value: null, series: [], error: 'no data', source: 'yahoo' });

    const closeRaw = (result.indicators && result.indicators.quote && result.indicators.quote[0] && result.indicators.quote[0].close) || [];
    const closes = closeRaw.filter(v => v != null && isFinite(v));
    const series = closes.slice(-30);
    const value = series.length ? series[series.length - 1] : null;
    const prevClose = series.length > 1 ? series[series.length - 2] : null;
    const currency = (result.meta && result.meta.currency) || '';

    res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
    return res.status(200).json({ value, prevClose, series, currency, asOf: date || null, source: 'yahoo' });
  } catch (e) {
    return res.status(200).json({ value: null, series: [], error: String(e) });
  }
}
