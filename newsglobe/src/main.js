import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import './style.css';
"use strict";

/* ============ date state ============ */
const TODAY=new Date(); TODAY.setHours(0,0,0,0);
let viewDate=new Date(TODAY);
const dayIndex=d=>Math.floor(d.getTime()/86400000);
const fmt=d=>d.toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric'});
const isoKey=d=>d.toISOString().slice(0,10);
const esc=s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

/* ============ featured locations (beacons + market data) ============ */
const CITIES=[
 {id:'anantapur',name:'Anantapur',country:'India',lat:14.6819,lon:77.6006,type:'home'},
 // markets
 {id:'mumbai',name:'Mumbai',country:'India',lat:19.076,lon:72.877,type:'mkt',indices:[{n:'BSE Sensex',cur:'₹',symbol:'^BSESN'},{n:'NSE Nifty 50',cur:'₹',symbol:'^NSEI'}]},
 {id:'nyc',name:'New York',country:'USA',lat:40.713,lon:-74.006,type:'mkt',indices:[{n:'NASDAQ Comp.',cur:'$',symbol:'^IXIC'},{n:'Dow Jones',cur:'$',symbol:'^DJI'}]},
 {id:'london',name:'London',country:'UK',lat:51.507,lon:-0.128,type:'mkt',indices:[{n:'FTSE 100',cur:'£',symbol:'^FTSE'}]},
 {id:'tokyo',name:'Tokyo',country:'Japan',lat:35.690,lon:139.692,type:'mkt',indices:[{n:'Nikkei 225',cur:'¥',symbol:'^N225'}]},
 {id:'hongkong',name:'Hong Kong',country:'China',lat:22.319,lon:114.169,type:'mkt',indices:[{n:'Hang Seng',cur:'HK$',symbol:'^HSI'}]},
 {id:'frankfurt',name:'Frankfurt',country:'Germany',lat:50.110,lon:8.682,type:'mkt',indices:[{n:'DAX',cur:'€',symbol:'^GDAXI'}]},
 {id:'singapore',name:'Singapore',country:'Singapore',lat:1.352,lon:103.820,type:'mkt',indices:[{n:'Straits Times',cur:'S$',symbol:'^STI'}]},
 {id:'sydney',name:'Sydney',country:'Australia',lat:-33.868,lon:151.209,type:'mkt',indices:[{n:'ASX 200',cur:'A$',symbol:'^AXJO'}]},
 {id:'saopaulo',name:'São Paulo',country:'Brazil',lat:-23.551,lon:-46.633,type:'mkt',indices:[{n:'Bovespa',cur:'R$',symbol:'^BVSP'}]},
 // news cities
 {id:'delhi',name:'New Delhi',country:'India',lat:28.614,lon:77.209,type:'news'},
 {id:'hyd',name:'Hyderabad',country:'India',lat:17.385,lon:78.486,type:'news'},
 {id:'bengaluru',name:'Bengaluru',country:'India',lat:12.972,lon:77.595,type:'news'},
 {id:'chennai',name:'Chennai',country:'India',lat:13.083,lon:80.270,type:'news'},
 {id:'kolkata',name:'Kolkata',country:'India',lat:22.573,lon:88.364,type:'news'},
 {id:'dubai',name:'Dubai',country:'UAE',lat:25.205,lon:55.271,type:'news'},
 {id:'singaporeN',name:'Jakarta',country:'Indonesia',lat:-6.208,lon:106.846,type:'news'},
 {id:'cairo',name:'Cairo',country:'Egypt',lat:30.044,lon:31.235,type:'news'},
 {id:'lagos',name:'Lagos',country:'Nigeria',lat:6.524,lon:3.379,type:'news'},
 {id:'moscow',name:'Moscow',country:'Russia',lat:55.755,lon:37.617,type:'news'},
 {id:'la',name:'Los Angeles',country:'USA',lat:34.052,lon:-118.244,type:'news'},
];

/* ============ data layer (real APIs via /api proxies) ============ */
const cache=new Map();
function cached(key,fn){if(cache.has(key))return cache.get(key);const p=fn().catch(e=>{cache.delete(key);throw e;});cache.set(key,p);return p;}
async function jget(url){const r=await fetch(url);if(!r.ok)throw new Error('HTTP '+r.status);return r.json();}
function fetchWeather(lat,lon,iso){return cached(`wx:${lat},${lon}:${iso}`,()=>jget(`/api/weather?lat=${lat}&lon=${lon}&date=${iso}`));}
function fetchNews(loc,iso){
  const p=new URLSearchParams({q:loc.name||'',date:iso});
  if(loc.region)p.set('region',loc.region); if(loc.country)p.set('country',loc.country);
  if(loc.lat!=null)p.set('lat',loc.lat); if(loc.lon!=null)p.set('lon',loc.lon);
  return cached(`news:${loc.name}:${iso}`,()=>jget('/api/news?'+p.toString()));
}
function fetchStock(sym,iso){return cached(`stk:${sym}:${iso}`,()=>jget(`/api/stocks?symbol=${encodeURIComponent(sym)}&date=${iso}`));}
function fetchPlace(lat,lon){return cached(`place:${lat.toFixed(3)},${lon.toFixed(3)}`,()=>jget(`/api/place?lat=${lat}&lon=${lon}`));}
async function geocode(name){return jget('/api/geocode?q='+encodeURIComponent(name));}

function sparkline(series){
  const pts=(series||[]).filter(v=>typeof v==='number'&&isFinite(v));
  if(pts.length<2)return '';
  let min=Math.min(...pts),max=Math.max(...pts);const span=(max-min)||1;
  const xy=pts.map((v,i)=>`${(i/(pts.length-1)*100).toFixed(1)},${(40-((v-min)/span)*36-2).toFixed(1)}`).join(' ');
  const up=pts[pts.length-1]>=pts[0];
  return `<svg viewBox="0 0 100 42" preserveAspectRatio="none" aria-hidden="true"><polyline points="${xy}" fill="none" stroke="${up?'#34D399':'#FB7185'}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}

/* ============ MapLibre map: globe → 3D terrain (keyless sources) ============ */
const map=new maplibregl.Map({
  container:'map',
  attributionControl:{compact:true},
  maxPitch:85, minZoom:1.2, maxZoom:18.5, dragRotate:true,
  center:[78.0,18.0], zoom:1.5,
  style:{
    version:8,
    glyphs:'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
    sources:{
      base:{type:'raster',tileSize:256,maxzoom:19,attribution:'© Esri, Maxar, Earthstar Geographics',
        tiles:['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}']},
      dem:{type:'raster-dem',encoding:'terrarium',tileSize:256,maxzoom:13,
        tiles:['https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png']},
      omt:{type:'vector',url:'https://tiles.openfreemap.org/planet'}
    },
    layers:[
      // soft "paper" land base — transparent at globe zoom (space shows), opaque up close
      {id:'land',type:'background',paint:{'background-color':'#f4efe3','background-opacity':['interpolate',['linear'],['zoom'],9,0,12,1]}},
      {id:'base',type:'raster',source:'base'},
      {id:'hills',type:'hillshade',source:'dem',paint:{'hillshade-exaggeration':0.18,'hillshade-shadow-color':'#06101f','hillshade-highlight-color':'#dfeaff'}},
      {id:'buildings',type:'fill-extrusion',source:'omt','source-layer':'building',minzoom:13,
        paint:{
          'fill-extrusion-color':['interpolate',['linear'],['coalesce',['get','render_height'],10],0,'#39507e',40,'#5e86c4',150,'#86dcff'],
          'fill-extrusion-height':['interpolate',['linear'],['zoom'],13,0,14.5,['coalesce',['get','render_height'],8]],
          'fill-extrusion-base':['coalesce',['get','render_min_height'],0],
          'fill-extrusion-opacity':0.9,'fill-extrusion-vertical-gradient':true}},
      // ─── illustrated "hero city" layers (Anantapur only; hidden by default) ───
      {id:'il-water',type:'fill',source:'omt','source-layer':'water',layout:{visibility:'none'},paint:{'fill-color':'#a7d8ef'}},
      {id:'il-green',type:'fill',source:'omt','source-layer':'landcover',layout:{visibility:'none'},
        filter:['match',['get','class'],['grass','wood','scrub','farmland'],true,false],paint:{'fill-color':'#c4e3a3','fill-opacity':0.92}},
      {id:'il-park',type:'fill',source:'omt','source-layer':'park',layout:{visibility:'none'},paint:{'fill-color':'#b6dd96','fill-opacity':0.85}},
      {id:'il-road-cas',type:'line',source:'omt','source-layer':'transportation',layout:{visibility:'none','line-cap':'round','line-join':'round'},
        paint:{'line-color':'#e7ddc9','line-width':['interpolate',['linear'],['zoom'],12,2.5,18,16]}},
      {id:'il-road',type:'line',source:'omt','source-layer':'transportation',layout:{visibility:'none','line-cap':'round','line-join':'round'},
        paint:{'line-color':'#ffffff','line-width':['interpolate',['linear'],['zoom'],12,1,18,10]}},
      {id:'il-building',type:'fill-extrusion',source:'omt','source-layer':'building',minzoom:13,layout:{visibility:'none'},
        paint:{
          'fill-extrusion-color':['interpolate',['linear'],['coalesce',['get','render_height'],8],0,'#f0e0c8',24,'#eccfa9',80,'#dcae84'],
          'fill-extrusion-height':['interpolate',['linear'],['zoom'],13,0,14.5,['coalesce',['get','render_height'],7]],
          'fill-extrusion-base':['coalesce',['get','render_min_height'],0],
          'fill-extrusion-opacity':0.97,'fill-extrusion-vertical-gradient':true}}
    ]
  }
});
map.addControl(new maplibregl.NavigationControl({visualizePitch:true}),'bottom-right');
map.on('style.load',()=>{
  try{ map.setProjection({type:'globe'}); }catch(e){}
  try{ map.setSky({'sky-color':'#0a1a40','horizon-color':'#3b74d6','fog-color':'#0a1330','sky-horizon-blend':0.5,'horizon-fog-blend':0.8,'fog-ground-blend':0.25,'atmosphere-blend':['interpolate',['linear'],['zoom'],0,1,5,0.6,9,0]}); }catch(e){}
  try{ map.setTerrain({source:'dem',exaggeration:1.35}); }catch(e){}
});
/* ---- beacons as a native WebGL layer (properly occluded by the globe) ---- */
const byId=Object.fromEntries(CITIES.map(c=>[c.id,c]));
const beaconsGeo={type:'FeatureCollection',features:CITIES.map(c=>({
  type:'Feature',geometry:{type:'Point',coordinates:[c.lon,c.lat]},properties:{id:c.id,name:c.name,type:c.type}}))};
const colorExpr=['match',['get','type'],'mkt','#34D399','home','#6fd3ff','#FB923C'];

map.on('load',()=>{
  document.getElementById('gl-load')?.classList.add('gone');
  if(!map.getSource('beacons')){
    map.addSource('beacons',{type:'geojson',data:beaconsGeo});
    map.addLayer({id:'beacon-glow',type:'circle',source:'beacons',paint:{
      'circle-radius':['match',['get','type'],'home',18,13],'circle-color':colorExpr,'circle-blur':1,'circle-opacity':0.4}});
    map.addLayer({id:'beacon-core',type:'circle',source:'beacons',paint:{
      'circle-radius':['match',['get','type'],'home',5,3.5],'circle-color':'#ffffff',
      'circle-stroke-color':colorExpr,'circle-stroke-width':1.6,'circle-opacity':0.96}});
    map.addLayer({id:'beacon-label',type:'symbol',source:'beacons',layout:{
      'text-field':['get','name'],'text-font':['Open Sans Regular'],'text-size':11,
      'text-offset':[0,1.1],'text-anchor':'top','text-allow-overlap':false,
      'symbol-sort-key':['match',['get','type'],'home',0,'mkt',1,2]},
      paint:{'text-color':'#eef4ff','text-halo-color':'#04070f','text-halo-width':1.3}});
  }
  let pt=0;                                   // gentle beacon pulse (opacity)
  (function pulse(){pt+=0.04;
    if(map.getLayer('beacon-glow'))map.setPaintProperty('beacon-glow','circle-opacity',0.3+0.18*(0.5+0.5*Math.sin(pt)));
    requestAnimationFrame(pulse);})();
});
setTimeout(()=>document.getElementById('gl-load')?.classList.add('gone'),9000);
map.on('mouseenter','beacon-core',()=>{map.getCanvas().style.cursor='pointer';});
map.on('mouseleave','beacon-core',()=>{map.getCanvas().style.cursor='';});

/* fly into a city: tilt + zoom to street level so 3D buildings + terrain show */
function flyToCity(lat,lon){
  map.flyTo({center:[lon,lat],zoom:14.5,pitch:62,bearing:-12,speed:0.9,curve:1.5,essential:true});
}

/* click: a beacon → fly into that city; otherwise tap-anywhere → news */
map.on('click',async e=>{
  document.getElementById('hint')?.classList.add('gone');
  const box=[[e.point.x-9,e.point.y-9],[e.point.x+9,e.point.y+9]];
  const hits=map.queryRenderedFeatures(box,{layers:['beacon-glow','beacon-core']});
  if(hits.length){const c=byId[hits[0].properties.id];if(c){flyToCity(c.lat,c.lon);openLocation(c);return;}}
  const lat=e.lngLat.lat, lon=e.lngLat.lng;
  openLocation({name:'Locating…',country:'',lat,lon,_pending:true});
  try{
    const pl=await fetchPlace(lat,lon);
    openLocation({name:pl.city||pl.region||'This area',region:pl.region,country:pl.country,countryCode:pl.countryCode,lat,lon});
  }catch(err){ openLocation({name:'This area',lat,lon}); }
});

/* ─── Anantapur "hero city": swap the satellite 3D map for the illustrated
   style only when the view is over Anantapur (zoomed in). Everywhere else
   keeps the normal look. Still fully interactive (tap for news/weather). ─── */
const ANANTAPUR=[77.6006,14.6819];
function inAnantapur(){
  const c=map.getCenter();
  return map.getZoom()>=12 && Math.abs(c.lng-ANANTAPUR[0])<0.28 && Math.abs(c.lat-ANANTAPUR[1])<0.28;
}
let illustrated=null;
function applyCityMode(){
  const on=inAnantapur();
  if(on===illustrated)return; illustrated=on;
  const setV=(id,v)=>{ if(map.getLayer(id)) map.setLayoutProperty(id,'visibility',v); };
  ['base','hills','buildings'].forEach(id=>setV(id,on?'none':'visible'));
  ['il-water','il-green','il-park','il-road-cas','il-road','il-building'].forEach(id=>setV(id,on?'visible':'none'));
}
map.on('moveend',applyCityMode);
map.on('load',applyCityMode);

/* ============ panel ============ */
const panel=document.getElementById('panel');
let activeLoc=null, panelToken=0;
function openLocation(loc){activeLoc=loc;panel.classList.add('open');document.getElementById('hint')?.classList.add('gone');renderPanel();}
function closePanel(){panel.classList.remove('open');activeLoc=null;panelToken++;}
function head(c,d){return `<div class="phead"><div><h2>${esc(c.name)}</h2><div class="country">${esc([c.region,c.country].filter(Boolean).join(', '))}</div></div><button id="closeBtn" aria-label="Close">✕</button></div><div class="pdate">${dayIndex(TODAY)===dayIndex(d)?'Today':'⏳ '+esc(fmt(d))}</div>`;}
function bindClose(){const b=document.getElementById('closeBtn');if(b)b.onclick=closePanel;}
function loadingCard(label,lines){let s=`<div class="card"><div class="clabel">${label}</div>`;for(let i=0;i<(lines||2);i++)s+=`<div class="skl${i===0?' lg':''}"></div>`;return s+'</div>';}
function stockCard(ix,s){
  if(!s||s.value==null)return `<div class="card stock"><div class="clabel">Index</div><div class="srow"><span class="sname">${esc(ix.n)}</span><span class="sval muted">—</span></div><div class="schg muted">No market data for this date</div></div>`;
  const prev=s.prevClose,chg=(prev!=null&&prev!==0)?(s.value-prev)/prev*100:null,up=chg==null?true:chg>=0,cur=ix.cur||'';
  return `<div class="card stock"><div class="clabel">Index</div><div class="srow"><span class="sname">${esc(ix.n)}</span><span class="sval">${cur}${s.value.toLocaleString('en-IN',{maximumFractionDigits:0})}</span></div>${chg==null?'<div class="schg muted">prev. close unavailable</div>':`<div class="schg ${up?'up':'down'}">${up?'▲':'▼'} ${Math.abs(chg).toFixed(2)}% vs prev. close</div>`}${sparkline(s.series)}</div>`;
}
function weatherCard(wx){
  if(!wx||wx.temp==null)return `<div class="card"><div class="clabel">Weather</div><div class="wx"><span class="wemoji">${esc((wx&&wx.emoji)||'🌍')}</span><div><div class="wtemp muted">—</div><div class="wcond">No weather record for this date</div></div></div></div>`;
  return `<div class="card"><div class="clabel">Weather</div><div class="wx"><span class="wemoji">${esc(wx.emoji||'🌤️')}</span><div><div class="wtemp">${wx.temp}°C</div><div class="wcond">${esc(wx.cond||'')}</div></div></div></div>`;
}
function newsCard(res,d){
  const arts=(res&&res.articles)||[];
  const scope=res&&res.scope==='local'?' · Local':res&&res.scope==='national'?' · Around India':'';
  if(!arts.length)return `<div class="card news"><div class="clabel">Headlines</div><div class="muted">No headlines found for ${esc(fmt(d))} near here.<br>Try a nearby city, or a more recent date.</div></div>`;
  const items=arts.map(n=>{const t=esc(n.h||''),s=esc(n.s||'');const inner=n.url?`<a href="${esc(n.url)}" target="_blank" rel="noopener">${t}</a>`:t;return `<li>${inner}<span class="src">${s}${s?' · ':''}${esc(fmt(d))}</span></li>`;}).join('');
  return `<div class="card news"><div class="clabel">Headlines${scope}</div><ul>${items}</ul></div>`;
}
function footNote(){return `<div class="demo-note">Live · local/national news · Open-Meteo weather · Yahoo Finance markets.<br>Local depth & history vary by source.</div>`;}

async function renderPanel(){
  if(!activeLoc)return;
  const c=activeLoc, d=viewDate, iso=isoKey(d), token=++panelToken;
  let shell=head(c,d);
  if(c.indices)shell+=c.indices.map(()=>loadingCard('Index',2)).join('');
  shell+=loadingCard('Weather',1)+loadingCard('Headlines',3)+footNote();
  panel.innerHTML=shell;bindClose();
  if(c._pending)return;                       // still reverse-geocoding; will re-render

  const tasks=[fetchWeather(c.lat,c.lon,iso),fetchNews(c,iso)];
  if(c.indices)c.indices.forEach(ix=>tasks.push(fetchStock(ix.symbol,iso)));
  const settled=await Promise.allSettled(tasks);
  if(token!==panelToken||activeLoc!==c)return;
  const val=r=>r&&r.status==='fulfilled'?r.value:null;
  const wx=val(settled[0]), news=val(settled[1]);
  const stocks=c.indices?c.indices.map((ix,i)=>val(settled[2+i])):[];
  let html=head(c,d);
  if(c.indices)html+=c.indices.map((ix,i)=>stockCard(ix,stocks[i])).join('');
  html+=weatherCard(wx)+newsCard(news,d)+footNote();
  panel.innerHTML=html;bindClose();
}

/* ============ search ============ */
const sInput=document.getElementById('searchInput'), sResults=document.getElementById('searchResults');
let sTimer=null;
sInput.addEventListener('input',()=>{
  clearTimeout(sTimer);const q=sInput.value.trim();
  if(q.length<2){sResults.classList.remove('on');return;}
  sTimer=setTimeout(async()=>{
    try{
      const data=await geocode(q);
      const list=(data.results||[]).slice(0,6);
      if(!list.length){sResults.classList.remove('on');return;}
      sResults.innerHTML=list.map(r=>`<div class="sres" data-lat="${r.lat}" data-lon="${r.lon}" data-name="${esc(r.name)}" data-region="${esc(r.region||'')}" data-country="${esc(r.country||'')}">${esc(r.name)}<small>${esc([r.region,r.country].filter(Boolean).join(', '))}</small></div>`).join('');
      sResults.classList.add('on');
    }catch(e){sResults.classList.remove('on');}
  },280);
});
sResults.addEventListener('click',e=>{
  const el=e.target.closest('.sres');if(!el)return;
  const lat=+el.dataset.lat, lon=+el.dataset.lon;
  sResults.classList.remove('on');sInput.value=el.dataset.name;
  flyToCity(lat,lon);
  openLocation({name:el.dataset.name,region:el.dataset.region,country:el.dataset.country,lat,lon});
});
document.getElementById('homeBtn').onclick=()=>{
  const a=CITIES[0]; sResults.classList.remove('on');
  flyToCity(a.lat,a.lon);
  openLocation(a);
};

/* ============ time dial ============ */
const dateLabel=document.getElementById('dateLabel');
function shift(unit,amt){const d=new Date(viewDate);if(unit==='d')d.setDate(d.getDate()+amt);if(unit==='m')d.setMonth(d.getMonth()+amt);if(unit==='y')d.setFullYear(d.getFullYear()+amt);if(d>TODAY)return;viewDate=d;syncTime();}
function syncTime(){
  const back=dayIndex(TODAY)-dayIndex(viewDate);
  dateLabel.innerHTML=fmt(viewDate)+(back?`<small>${back<365?back+' day'+(back>1?'s':''):(back/365).toFixed(1)+' yrs'} back</small>`:'<small>live · today</small>');
  ['bDayFwd','bMonthFwd'].forEach(id=>document.getElementById(id).disabled=back===0);
  document.getElementById('todayBtn').disabled=back===0;
  if(activeLoc&&!activeLoc._pending)renderPanel();
}
document.getElementById('bDayBack').onclick=()=>shift('d',-1);
document.getElementById('bMonthBack').onclick=()=>shift('m',-1);
document.getElementById('bYearBack').onclick=()=>shift('y',-1);
document.getElementById('bDayFwd').onclick=()=>shift('d',1);
document.getElementById('bMonthFwd').onclick=()=>shift('m',1);
document.getElementById('todayBtn').onclick=()=>{viewDate=new Date(TODAY);syncTime()};
syncTime();
