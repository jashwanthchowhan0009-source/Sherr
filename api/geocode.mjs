// Forward geocode a search query -> candidate places. Open-Meteo geocoding is
// free and key-less. Raw Node I/O only.
function params(req){try{return new URL(req.url,'http://localhost').searchParams;}catch{return new URLSearchParams();}}
function send(res,status,obj,cache){res.statusCode=status;res.setHeader('Content-Type','application/json; charset=utf-8');if(cache)res.setHeader('Cache-Control',cache);res.end(JSON.stringify(obj));}

export default async function handler(req,res){
  const q=(params(req).get('q')||'').trim();
  if(!q)return send(res,400,{results:[],error:'q is required'});
  try{
    const url=`https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(q)}&count=6&language=en&format=json`;
    const r=await fetch(url);const j=await r.json();
    const results=(j.results||[]).map(p=>({
      name:p.name, region:p.admin1||'', country:p.country||'', countryCode:p.country_code||'',
      lat:p.latitude, lon:p.longitude
    }));
    return send(res,200,{results},'public, s-maxage=86400, stale-while-revalidate=604800');
  }catch(e){return send(res,200,{results:[],error:String(e)});}
}
