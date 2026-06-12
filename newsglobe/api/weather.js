// Historical / current daily weather via Open-Meteo.
// Forecast API covers the last ~92 days + today; the Archive (ERA5) API covers
// 1940 → ~5 days ago. We pick whichever fits the requested date.
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
function describe(code){ return WMO[code] || ['🌍','Unknown']; }

export default async function handler(req, res) {
  try {
    const { lat, lon, date } = req.query || {};
    if (!lat || !lon || !date) return res.status(400).json({ error: 'lat, lon and date are required' });

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
    res.setHeader('Cache-Control', 'public, s-maxage=86400, stale-while-revalidate=604800');
    return res.status(200).json({ temp, emoji, cond, code: code ?? null, source: 'open-meteo' });
  } catch (e) {
    return res.status(200).json({ temp: null, emoji: '🌍', cond: 'Weather unavailable', error: String(e) });
  }
}
