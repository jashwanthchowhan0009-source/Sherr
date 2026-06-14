// Local + national news for a place/date.
//  - For India + recent dates, use NewsData.io (env INDIA_NEWS_KEY) for real
//    local depth (incl. tier-3 cities like Anantapur).
//  - Otherwise (global, or historical, or no key) fall back to GDELT DOC 2.0.
// Raw Node I/O only (no Vercel helpers).
function params(req){try{return new URL(req.url,'http://localhost').searchParams;}catch{return new URLSearchParams();}}
function send(res,status,obj,cache){res.statusCode=status;res.setHeader('Content-Type','application/json; charset=utf-8');if(cache)res.setHeader('Cache-Control',cache);res.end(JSON.stringify(obj));}
const dedupe=(arr)=>{const seen=new Set();return arr.filter(a=>a.h&&!seen.has(a.h)&&seen.add(a.h));};

async function fromNewsData(place,region){
  const key=process.env.INDIA_NEWS_KEY;
  if(!key)return null;
  const q=[place,region].filter(Boolean).join(' ')||place;
  const url=`https://newsdata.io/api/1/news?apikey=${encodeURIComponent(key)}&country=in&language=en&q=${encodeURIComponent(q)}`;
  const r=await fetch(url);
  if(!r.ok)return null;
  const j=await r.json();
  if(j.status&&j.status!=='success')return null;
  const arts=dedupe((j.results||[]).map(a=>({h:a.title,s:a.source_id||a.source_name||'',url:a.link}))).slice(0,6);
  return arts.length?arts:null;
}

async function fromGDELT(place,date,today){
  const qp=new URLSearchParams({query:`"${place}" sourcelang:english`,mode:'ArtList',maxrecords:'12',format:'json',sort:'HybridRel'});
  if(date){
    const d=new Date(date+'T00:00:00Z');const back=Math.round((today-d)/86400000);
    if(back>=0&&back<=90){const day=date.replace(/-/g,'');qp.set('startdatetime',day+'000000');qp.set('enddatetime',day+'235959');qp.set('sort','DateDesc');}
  }
  const r=await fetch('https://api.gdeltproject.org/api/v2/doc/doc?'+qp.toString(),{headers:{'User-Agent':'NewsGlobe/2.0'}});
  const text=await r.text();let j={};try{j=JSON.parse(text);}catch{j={articles:[]};}
  return dedupe((j.articles||[]).map(a=>({h:a.title,s:(a.sourcecommonname||a.domain||'').replace(/^www\./,''),url:a.url}))).slice(0,6);
}

export default async function handler(req,res){
  const p=params(req);
  const place=(p.get('q')||'').trim();
  const region=(p.get('region')||'').trim();
  const country=(p.get('country')||'').trim();
  const date=p.get('date');
  if(!place)return send(res,400,{error:'q (place name) is required'});

  const today=new Date();today.setUTCHours(0,0,0,0);
  const isIndia=/^(in|ind|india)$/i.test(country)||/india/i.test(country);
  const recent=!date||((today-new Date(date+'T00:00:00Z'))/86400000)<=3;

  try{
    if(isIndia&&recent){
      try{const a=await fromNewsData(place,region);if(a)return send(res,200,{articles:a,source:'NewsData.io'},'public, s-maxage=900, stale-while-revalidate=86400');}catch(e){}
    }
    const g=await fromGDELT(place,date,today);
    return send(res,200,{articles:g,source:'GDELT'},'public, s-maxage=3600, stale-while-revalidate=86400');
  }catch(e){return send(res,200,{articles:[],error:String(e)});}
}
