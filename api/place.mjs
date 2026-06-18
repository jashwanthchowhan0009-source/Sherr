// Reverse-geocode lat/lon -> place name, region (state), country.
// BigDataCloud reverse-geocode-client is free and key-less. Raw Node I/O only.
function params(req){try{return new URL(req.url,'http://localhost').searchParams;}catch{return new URLSearchParams();}}
function send(res,status,obj,cache){res.statusCode=status;res.setHeader('Content-Type','application/json; charset=utf-8');if(cache)res.setHeader('Cache-Control',cache);res.end(JSON.stringify(obj));}

export default async function handler(req,res){
  const q=params(req);const lat=q.get('lat'),lon=q.get('lon');
  if(!lat||!lon)return send(res,400,{error:'lat and lon are required'});
  try{
    const url=`https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=${encodeURIComponent(lat)}&longitude=${encodeURIComponent(lon)}&localityLanguage=en`;
    const r=await fetch(url);const j=await r.json();
    const city=j.city||j.locality||j.principalSubdivision||'';
    return send(res,200,{
      city, region:j.principalSubdivision||'', country:j.countryName||'', countryCode:j.countryCode||'',
      lat:+lat, lon:+lon, source:'bigdatacloud'
    },'public, s-maxage=86400, stale-while-revalidate=604800');
  }catch(e){return send(res,200,{city:'',region:'',country:'',countryCode:'',lat:+lat,lon:+lon,error:String(e)});}
}
