// Historical / current daily weather via Open-Meteo.
// Uses ONLY standard Node req/res primitives (no Vercel helper methods), so it
// can't crash on a missing res.json/req.query.
const WMO = {
  0:['☀️','Clear skies'],1:['🌤️','Mainly clear'],2:['⛅','Partly cloudy'],3:['☁️','Overcast'],
  45:['🌫️','Fog'],48:['🌫️','Rime fog'],
  51:['🌦️','Light drizzle'],53:['🌦️','Drizzle'],55:['🌦️','Heavy drizzle'],
  56:['🌧️','Freezing drizzle'],57:['🌧️','Freezing drizzle'],
  61:['🌧️','Light rain'],63:['🌧️','Rain'],65:['🌧️','Heavy rain'],
  66:['🌧️','Freezing rain'],67:['🌧️','Freezing rain'],
  71:['🌨️','Light snow'],73:['🌨️','Snow'],75:['🌨️','Heavy snow'],77:['❄️','Snow grains'],
  80:['🌦️','Rain showers'],81:['🌧️','Rain showers'],82:['⛈️','Violent showers'],
  85:['🌨️','Snow showers'],86:['❄️','Snow showers'],
  95:['⛈️','Thunderstorm'],96:['⛈️','Thunderstorm + hail'],99:['⛈️','Thunderstorm + hail'],
};
const describe = (c) => WMO[c] || ['🌍', 'Unknown'];

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
  const q = params(req);
  const lat = q.get('lat'), lon = q.get('lon'), date = q.get('date');
  if (!lat || !lon || !date) return send(res, 400, { error: 'lat, lon and date are required' });

  try {
    const today = new Date(); today.setUTCHours(0, 0, 0, 0);
    const d = new Date(date + 'T00:00:00Z');
    const daysBack = Math.round((today - d) / 86400000);

    const base = daysBack > 90
      ? 'https://archive-api.open-meteo.com/v1/archive'
      : 'https://api.open-meteo.com/v1/forecast';
    const url = `${base}?latitude=${encodeURIComponent(lat)}&longitude=${encodeURIComponent(lon)}` +
      `&start_date=${date}&end_date=${date}` +
      `&daily=temperature_2m_max,temperature_2m_min,weather_code&timezone=auto`;

    const r = await fetch(url);
    const j = await r.json();
    const dly = j.daily || {};
    const tmax = dly.temperature_2m_max && dly.temperature_2m_max[0];
    const tmin = dly.temperature_2m_min && dly.temperature_2m_min[0];
    const code = dly.weather_code && dly.weather_code[0];

    let temp = null;
    if (tmax != null && tmin != null) temp = Math.round((tmax + tmin) / 2);
    else if (tmax != null) temp = Math.round(tmax);
    else if (tmin != null) temp = Math.round(tmin);

    const [emoji, cond] = describe(code);
    return send(res, 200, { temp, emoji, cond, code: code ?? null, source: 'open-meteo' },
      'public, s-maxage=86400, stale-while-revalidate=604800');
  } catch (e) {
    return send(res, 200, { temp: null, emoji: '🌍', cond: 'Weather unavailable', error: String(e) });
  }
}
