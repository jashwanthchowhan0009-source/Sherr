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
function timeAgo(ms){const s=Math.max(0,(Date.now()-ms)/1000);if(s<60)return Math.floor(s)+'s ago';const m=s/60;if(m<60)return Math.floor(m)+'m ago';const h=m/60;if(h<24)return Math.floor(h)+'h ago';return Math.floor(h/24)+'d ago';}

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
 // geopolitical flashpoints (worldmonitor-style hotspot beacons)
 {id:'kyiv',name:'Kyiv',country:'Ukraine',lat:50.450,lon:30.523,type:'hotspot'},
 {id:'gaza',name:'Gaza City',country:'Palestine',lat:31.500,lon:34.467,type:'hotspot'},
 {id:'tehran',name:'Tehran',country:'Iran',lat:35.689,lon:51.389,type:'hotspot'},
 {id:'taipei',name:'Taipei',country:'Taiwan',lat:25.047,lon:121.517,type:'hotspot'},
 {id:'pyongyang',name:'Pyongyang',country:'N. Korea',lat:39.019,lon:125.738,type:'hotspot'},
 {id:'khartoum',name:'Khartoum',country:'Sudan',lat:15.552,lon:32.532,type:'hotspot'},
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
      {id:'base',type:'raster',source:'base'},
      {id:'hills',type:'hillshade',source:'dem',paint:{'hillshade-exaggeration':0.18,'hillshade-shadow-color':'#06101f','hillshade-highlight-color':'#dfeaff'}},
      {id:'buildings',type:'fill-extrusion',source:'omt','source-layer':'building',minzoom:13,
        paint:{
          'fill-extrusion-color':['interpolate',['linear'],['coalesce',['get','render_height'],10],0,'#39507e',40,'#5e86c4',150,'#86dcff'],
          'fill-extrusion-height':['interpolate',['linear'],['zoom'],13,0,14.5,['coalesce',['get','render_height'],8]],
          'fill-extrusion-base':['coalesce',['get','render_min_height'],0],
          'fill-extrusion-opacity':0.9,'fill-extrusion-vertical-gradient':true}}
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
const colorExpr=['match',['get','type'],'mkt','#34D399','home','#6fd3ff','hotspot','#FF4D4D','#FB923C'];

map.on('load',()=>{
  document.getElementById('gl-load')?.classList.add('gone');
  if(!map.getSource('beacons')){
    map.addSource('beacons',{type:'geojson',data:beaconsGeo});
    map.addLayer({id:'beacon-glow',type:'circle',source:'beacons',paint:{
      'circle-radius':['match',['get','type'],'home',18,'hotspot',10,13],'circle-color':colorExpr,'circle-blur':1,'circle-opacity':0.4}});
    map.addLayer({id:'beacon-core',type:'circle',source:'beacons',paint:{
      'circle-radius':['match',['get','type'],'home',5,3.5],'circle-color':'#ffffff',
      'circle-stroke-color':colorExpr,'circle-stroke-width':1.6,'circle-opacity':0.96}});
    map.addLayer({id:'beacon-label',type:'symbol',source:'beacons',layout:{
      'text-field':['get','name'],'text-font':['Open Sans Regular'],'text-size':11,
      'text-offset':[0,1.1],'text-anchor':'top','text-allow-overlap':false,
      'symbol-sort-key':['match',['get','type'],'home',0,'mkt',1,2]},
      paint:{'text-color':'#eef4ff','text-halo-color':'#04070f','text-halo-width':1.3}});
  }
  let pt=0;                                   // gentle beacon + quake pulse (opacity)
  (function pulse(){pt+=0.04;
    if(map.getLayer('beacon-glow'))map.setPaintProperty('beacon-glow','circle-opacity',0.3+0.18*(0.5+0.5*Math.sin(pt)));
    if(quakesActive&&map.getLayer('quake-glow'))map.setPaintProperty('quake-glow','circle-opacity',0.22+0.22*(0.5+0.5*Math.sin(pt*1.4)));
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
  if(quakesActive&&map.getLayer('quake-core')){
    const qh=map.queryRenderedFeatures(box,{layers:['quake-core','quake-glow']});
    if(qh.length){openQuake(qh[0].properties,qh[0].geometry.coordinates);return;}
  }
  const hits=map.queryRenderedFeatures(box,{layers:['beacon-glow','beacon-core']});
  if(hits.length){const c=byId[hits[0].properties.id];if(c){flyToCity(c.lat,c.lon);openLocation(c);return;}}
  const lat=e.lngLat.lat, lon=e.lngLat.lng;
  openLocation({name:'Locating…',country:'',lat,lon,_pending:true});
  try{
    const pl=await fetchPlace(lat,lon);
    openLocation({name:pl.city||pl.region||'This area',region:pl.region,country:pl.country,countryCode:pl.countryCode,lat,lon});
  }catch(err){ openLocation({name:'This area',lat,lon}); }
});

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
function quakeCard(q){
  const mag=q.mag!=null?(+q.mag).toFixed(1):'—';
  const sev=q.mag>=6?'down':q.mag>=4.5?'':'up';
  const depth=q.depth!=null?Math.round(q.depth)+' km deep':'';
  const when=q.time?timeAgo(q.time):'';
  const meta=[depth,when].filter(Boolean).join(' · ');
  return `<div class="card stock"><div class="clabel">Earthquake · USGS</div>`+
    `<div class="srow"><span class="sname">${esc(q.place||'Seismic event')}</span><span class="sval">M ${mag}</span></div>`+
    `<div class="schg ${sev}">${q.mag>=6?'⚠️ Major':q.mag>=4.5?'Moderate':'Minor'} quake${meta?' · '+esc(meta):''}</div>`+
    (q.url?`<div style="margin-top:8px;font-size:12px"><a href="${esc(q.url)}" target="_blank" rel="noopener">USGS event details ↗</a></div>`:'')+
    `</div>`;
}
function footNote(){
  const ciiLegend=ciiActive?'<br>🌡️ Stability: <span style="color:#30D158">■</span> Stable &nbsp;<span style="color:#FF9F0A">■</span> Moderate &nbsp;<span style="color:#f97316">■</span> Elevated &nbsp;<span style="color:#FF453A">■</span> Critical':'';
  const quakeLegend=quakesActive?'<br>🌍 Quakes (USGS · 24h): <span style="color:#fde047">●</span> M2.5 &nbsp;<span style="color:#fb923c">●</span> M4.5 &nbsp;<span style="color:#ef4444">●</span> M6+':'';
  return `<div class="demo-note">Live · news · weather · markets · GDELT signals.${ciiLegend}${quakeLegend}</div>`;
}

async function renderPanel(){
  if(!activeLoc)return;
  const c=activeLoc, d=viewDate, iso=isoKey(d), token=++panelToken;
  let shell=head(c,d);
  if(c._quake)shell+=quakeCard(c._quake);
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
  if(c._quake)html+=quakeCard(c._quake);
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

/* ============ sun & light (photographer mode) ============ */
// compact solar position (from SunCalc, MIT): date+lat+lon -> {altitude, azimuth} rad
const _rad=Math.PI/180, _dayMs=86400000, _J1970=2440588, _J2000=2451545, _e=_rad*23.4397;
function _toDays(d){return d.valueOf()/_dayMs-0.5+_J1970-_J2000;}
function _sunCoords(d){
  const M=_rad*(357.5291+0.98560028*d);
  const L=M+_rad*(1.9148*Math.sin(M)+0.02*Math.sin(2*M)+0.0003*Math.sin(3*M))+_rad*102.9372+Math.PI;
  return {dec:Math.asin(Math.sin(_e)*Math.sin(L)), ra:Math.atan2(Math.sin(L)*Math.cos(_e),Math.cos(L))};
}
function sunPosition(date,lat,lon){
  const lw=_rad*-lon, phi=_rad*lat, d=_toDays(date), c=_sunCoords(d), H=_rad*(280.16+360.9856235*d)-lw-c.ra;
  return {
    altitude:Math.asin(Math.sin(phi)*Math.sin(c.dec)+Math.cos(phi)*Math.cos(c.dec)*Math.cos(H)),
    azimuth:Math.atan2(Math.sin(H),Math.cos(H)*Math.sin(phi)-Math.tan(c.dec)*Math.cos(phi))   // from south
  };
}
const sunSlider=document.getElementById('sunslider');
let selMin=new Date().getHours()*60+new Date().getMinutes();
if(sunSlider)sunSlider.value=selMin;
const sunColor=a=>a>12?'#fff6e8':a>0?'#ffc187':a>-6?'#c98a7a':'#3a4a7a';
const sunIntensity=a=>a>0?0.35+0.25*Math.min(1,a/40):0.18;
function sunTint(a){
  if(a>12)return 'rgba(0,0,0,0)';
  if(a>0)return 'rgba(255,150,70,'+(0.22*(12-a)/12).toFixed(2)+')';
  if(a>-8)return 'rgba(150,90,120,0.30)';
  return 'rgba(8,14,40,0.50)';
}
function applySun(azDeg,altDeg){
  const polar=Math.max(2,90-altDeg), col=sunColor(altDeg), inten=sunIntensity(altDeg);
  let ok=false;
  try{ map.setLights([
    {id:'amb',type:'ambient',properties:{color:'#ffffff',intensity:altDeg>0?0.55:0.3}},
    {id:'sun',type:'directional',properties:{direction:[azDeg,polar],color:col,intensity:inten,'cast-shadows':true}}
  ]); ok=true; }catch(e){}
  if(!ok){ try{ map.setLight({anchor:'map',position:[1.5,azDeg,polar],color:col,intensity:inten}); }catch(e){} }
}
function updateSun(){
  if(!sunSlider)return;
  const c=map.getCenter(), lat=c.lat, lon=c.lng, tz=Math.round(lon/15);
  const dt=new Date(Date.UTC(viewDate.getFullYear(),viewDate.getMonth(),viewDate.getDate(),Math.floor(selMin/60)-tz,selMin%60));
  const sp=sunPosition(dt,lat,lon), altDeg=sp.altitude*180/Math.PI, azDeg=(sp.azimuth*180/Math.PI+540)%360;
  applySun(azDeg,altDeg);
  const tint=document.getElementById('suntint'); if(tint)tint.style.background=sunTint(altDeg);
  const lab=document.getElementById('sunlabel'); if(lab)lab.textContent=String(Math.floor(selMin/60)).padStart(2,'0')+':'+String(selMin%60).padStart(2,'0');
  const ph=document.getElementById('sunphase'); if(ph)ph.textContent=altDeg>0?(altDeg<6?'🌅':'☀️'):(altDeg>-8?'🌆':'🌙');
}
if(sunSlider)sunSlider.addEventListener('input',()=>{selMin=+sunSlider.value;updateSun();});
map.on('moveend',updateSun);
map.on('load',updateSun);

/* ============ Country Instability Index (worldmonitor-style heatmap) ============ */
const CII_GEO='https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson';
let ciiLoaded=false, ciiActive=false;

async function initCII(){
  if(ciiLoaded)return;
  try{
    const[ciiData,geo]=await Promise.all([
      jget('/api/cii'),
      fetch(CII_GEO).then(r=>r.json()),
    ]);
    const scores=ciiData.countries||{};
    const enriched={...geo,features:geo.features.map(f=>({
      ...f,properties:{...f.properties,cii:scores[f.properties.ISO_A2]??-1}
    }))};
    if(!map.getSource('cii'))map.addSource('cii',{type:'geojson',data:enriched});
    if(!map.getLayer('cii-fill')){
      map.addLayer({
        id:'cii-fill',type:'fill',source:'cii',
        filter:['>=',['get','cii'],0],
        layout:{visibility:'none'},
        paint:{
          'fill-color':['interpolate',['linear'],['get','cii'],
            0,'#166534', 25,'#30D158', 50,'#FF9F0A',
            70,'#f97316', 85,'#FF453A', 100,'#7f1d1d'],
          'fill-opacity':0.50,
        }
      },'beacon-glow');
    }
    ciiLoaded=true;
  }catch(e){console.warn('CII layer failed:',e);}
}

function toggleCII(){
  ciiActive=!ciiActive;
  document.getElementById('ciiBtn')?.classList.toggle('active',ciiActive);
  const show=()=>{if(map.getLayer('cii-fill'))map.setLayoutProperty('cii-fill','visibility',ciiActive?'visible':'none');};
  if(!ciiLoaded){initCII().then(show);}else{show();}
}
document.getElementById('ciiBtn')?.addEventListener('click',toggleCII);

/* ============ live earthquakes (USGS feed — keyless, CORS, past 24h M2.5+) ============ */
const QUAKE_FEED='https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson';
let quakesLoaded=false, quakesActive=false;

async function initQuakes(){
  if(quakesLoaded)return;
  try{
    const data=await jget(QUAKE_FEED);
    if(!map.getSource('quakes'))map.addSource('quakes',{type:'geojson',data});
    const before=map.getLayer('beacon-glow')?'beacon-glow':undefined;
    if(!map.getLayer('quake-glow')){
      map.addLayer({id:'quake-glow',type:'circle',source:'quakes',layout:{visibility:'none'},paint:{
        'circle-radius':['interpolate',['linear'],['get','mag'],2.5,7,5,18,7,38],
        'circle-color':['interpolate',['linear'],['get','mag'],2.5,'#fde047',4.5,'#fb923c',6,'#ef4444',8,'#7f1d1d'],
        'circle-blur':1,'circle-opacity':0.0}},before);
    }
    if(!map.getLayer('quake-core')){
      map.addLayer({id:'quake-core',type:'circle',source:'quakes',layout:{visibility:'none'},paint:{
        'circle-radius':['interpolate',['linear'],['get','mag'],2.5,2.5,5,6,7,12],
        'circle-color':['interpolate',['linear'],['get','mag'],2.5,'#fef08a',4.5,'#fb923c',6,'#ef4444',8,'#fca5a5'],
        'circle-stroke-color':'#1a0a0a','circle-stroke-width':0.8,'circle-opacity':0.95}},before);
    }
    quakesLoaded=true;
  }catch(e){console.warn('Quakes layer failed:',e);}
}
function toggleQuakes(){
  quakesActive=!quakesActive;
  document.getElementById('quakeBtn')?.classList.toggle('active',quakesActive);
  const show=()=>['quake-glow','quake-core'].forEach(id=>{if(map.getLayer(id))map.setLayoutProperty(id,'visibility',quakesActive?'visible':'none');});
  if(!quakesLoaded){initQuakes().then(show);}else{show();}
}
document.getElementById('quakeBtn')?.addEventListener('click',toggleQuakes);
function openQuake(p,coords){
  openLocation({name:'M '+(p.mag!=null?(+p.mag).toFixed(1):'?')+' earthquake',country:p.place||'',
    lat:coords[1],lon:coords[0],_quake:{mag:p.mag,place:p.place,time:+p.time,depth:coords[2],url:p.url}});
}
map.on('mouseenter','quake-core',()=>{map.getCanvas().style.cursor='pointer';});
map.on('mouseleave','quake-core',()=>{map.getCanvas().style.cursor='';});

/* ============ time dial ============ */
const dateLabel=document.getElementById('dateLabel');
function shift(unit,amt){const d=new Date(viewDate);if(unit==='d')d.setDate(d.getDate()+amt);if(unit==='m')d.setMonth(d.getMonth()+amt);if(unit==='y')d.setFullYear(d.getFullYear()+amt);if(d>TODAY)return;viewDate=d;syncTime();}
function syncTime(){
  const back=dayIndex(TODAY)-dayIndex(viewDate);
  dateLabel.innerHTML=fmt(viewDate)+(back?`<small>${back<365?back+' day'+(back>1?'s':''):(back/365).toFixed(1)+' yrs'} back</small>`:'<small>live · today</small>');
  ['bDayFwd','bMonthFwd'].forEach(id=>document.getElementById(id).disabled=back===0);
  document.getElementById('todayBtn').disabled=back===0;
  if(activeLoc&&!activeLoc._pending)renderPanel();
  if(typeof updateSun==='function')updateSun();
}
document.getElementById('bDayBack').onclick=()=>shift('d',-1);
document.getElementById('bMonthBack').onclick=()=>shift('m',-1);
document.getElementById('bYearBack').onclick=()=>shift('y',-1);
document.getElementById('bDayFwd').onclick=()=>shift('d',1);
document.getElementById('bMonthFwd').onclick=()=>shift('m',1);
document.getElementById('todayBtn').onclick=()=>{viewDate=new Date(TODAY);syncTime()};
syncTime();
