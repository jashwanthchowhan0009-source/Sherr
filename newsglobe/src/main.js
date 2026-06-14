import * as THREE from 'three';
import './style.css';
"use strict";

/* ============ seeded randomness (starfield only) ============ */
function xmur3(str){let h=1779033703^str.length;for(let i=0;i<str.length;i++){h=Math.imul(h^str.charCodeAt(i),3432918353);h=h<<13|h>>>19}return function(){h=Math.imul(h^(h>>>16),2246822507);h=Math.imul(h^(h>>>13),3266489909);return (h^=h>>>16)>>>0}}
function mulberry32(a){return function(){a|=0;a=a+0x6D2B79F5|0;let t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296}}
function rng(key){return mulberry32(xmur3(key)())}

/* ============ date state ============ */
const TODAY=new Date(); TODAY.setHours(0,0,0,0);
let viewDate=new Date(TODAY);
const dayIndex=d=>Math.floor(d.getTime()/86400000);
const fmt=d=>d.toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric'});
const isoKey=d=>d.toISOString().slice(0,10);
const esc=s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

/* ============ cities (expanded global coverage) ============ */
const CITIES=[
 // markets
 {id:'mumbai',  name:'Mumbai',    country:'India',   lat:19.076,lon:72.877, type:'mkt',
   indices:[{n:'BSE Sensex',base:84600,cur:'₹',symbol:'^BSESN'},{n:'NSE Nifty 50',base:25750,cur:'₹',symbol:'^NSEI'}]},
 {id:'nyc',     name:'New York',  country:'USA',     lat:40.713,lon:-74.006,type:'mkt',
   indices:[{n:'NASDAQ Comp.',base:21400,cur:'$',symbol:'^IXIC'},{n:'Dow Jones',base:44600,cur:'$',symbol:'^DJI'}]},
 {id:'london',  name:'London',    country:'UK',      lat:51.507,lon:-0.128, type:'mkt',
   indices:[{n:'FTSE 100',base:8420,cur:'£',symbol:'^FTSE'}]},
 {id:'tokyo',   name:'Tokyo',     country:'Japan',   lat:35.690,lon:139.692,type:'mkt',
   indices:[{n:'Nikkei 225',base:41200,cur:'¥',symbol:'^N225'}]},
 {id:'hongkong',name:'Hong Kong', country:'China',   lat:22.319,lon:114.169,type:'mkt',
   indices:[{n:'Hang Seng',base:24000,cur:'HK$',symbol:'^HSI'}]},
 {id:'shanghai',name:'Shanghai',  country:'China',   lat:31.230,lon:121.474,type:'mkt',
   indices:[{n:'SSE Composite',base:3300,cur:'¥',symbol:'000001.SS'}]},
 {id:'frankfurt',name:'Frankfurt',country:'Germany', lat:50.110,lon:8.682,  type:'mkt',
   indices:[{n:'DAX',base:20000,cur:'€',symbol:'^GDAXI'}]},
 {id:'paris',   name:'Paris',     country:'France',  lat:48.857,lon:2.352,  type:'mkt',
   indices:[{n:'CAC 40',base:7800,cur:'€',symbol:'^FCHI'}]},
 {id:'toronto', name:'Toronto',   country:'Canada',  lat:43.651,lon:-79.347,type:'mkt',
   indices:[{n:'S&P/TSX',base:24500,cur:'C$',symbol:'^GSPTSE'}]},
 {id:'seoul',   name:'Seoul',     country:'S. Korea',lat:37.566,lon:126.978,type:'mkt',
   indices:[{n:'KOSPI',base:2700,cur:'₩',symbol:'^KS11'}]},
 {id:'singapore',name:'Singapore',country:'Singapore',lat:1.352,lon:103.820,type:'mkt',
   indices:[{n:'Straits Times',base:3700,cur:'S$',symbol:'^STI'}]},
 {id:'sydney',  name:'Sydney',    country:'Australia',lat:-33.868,lon:151.209,type:'mkt',
   indices:[{n:'ASX 200',base:8300,cur:'A$',symbol:'^AXJO'}]},
 {id:'saopaulo',name:'São Paulo', country:'Brazil',  lat:-23.551,lon:-46.633,type:'mkt',
   indices:[{n:'Bovespa',base:130000,cur:'R$',symbol:'^BVSP'}]},
 // news cities
 {id:'hyd',    name:'Hyderabad', country:'India',     lat:17.385,lon:78.486, type:'news'},
 {id:'delhi',  name:'New Delhi', country:'India',     lat:28.614,lon:77.209, type:'news'},
 {id:'bengaluru',name:'Bengaluru',country:'India',    lat:12.972,lon:77.595, type:'news'},
 {id:'chennai',name:'Chennai',   country:'India',     lat:13.083,lon:80.270, type:'news'},
 {id:'dubai',  name:'Dubai',     country:'UAE',       lat:25.205,lon:55.271, type:'news'},
 {id:'jakarta',name:'Jakarta',   country:'Indonesia', lat:-6.208,lon:106.846,type:'news'},
 {id:'beijing',name:'Beijing',   country:'China',     lat:39.904,lon:116.407,type:'news'},
 {id:'bangkok',name:'Bangkok',   country:'Thailand',  lat:13.756,lon:100.501,type:'news'},
 {id:'manila', name:'Manila',    country:'Philippines',lat:14.600,lon:120.984,type:'news'},
 {id:'kl',     name:'Kuala Lumpur',country:'Malaysia',lat:3.139,lon:101.687,type:'news'},
 {id:'karachi',name:'Karachi',   country:'Pakistan',  lat:24.861,lon:67.010, type:'news'},
 {id:'dhaka',  name:'Dhaka',     country:'Bangladesh',lat:23.811,lon:90.413, type:'news'},
 {id:'cairo',  name:'Cairo',     country:'Egypt',     lat:30.044,lon:31.235, type:'news'},
 {id:'lagos',  name:'Lagos',     country:'Nigeria',   lat:6.524,lon:3.379,   type:'news'},
 {id:'nairobi',name:'Nairobi',   country:'Kenya',     lat:-1.286,lon:36.817, type:'news'},
 {id:'joburg', name:'Johannesburg',country:'S. Africa',lat:-26.205,lon:28.047,type:'news'},
 {id:'moscow', name:'Moscow',    country:'Russia',    lat:55.755,lon:37.617, type:'news'},
 {id:'istanbul',name:'Istanbul', country:'Türkiye',   lat:41.008,lon:28.978, type:'news'},
 {id:'berlin', name:'Berlin',    country:'Germany',   lat:52.520,lon:13.405, type:'news'},
 {id:'madrid', name:'Madrid',    country:'Spain',     lat:40.416,lon:-3.703, type:'news'},
 {id:'rome',   name:'Rome',      country:'Italy',     lat:41.902,lon:12.496, type:'news'},
 {id:'la',     name:'Los Angeles',country:'USA',      lat:34.052,lon:-118.244,type:'news'},
 {id:'chicago',name:'Chicago',   country:'USA',       lat:41.878,lon:-87.630,type:'news'},
 {id:'mexico', name:'Mexico City',country:'Mexico',   lat:19.432,lon:-99.133,type:'news'},
 {id:'bsas',   name:'Buenos Aires',country:'Argentina',lat:-34.603,lon:-58.381,type:'news'},
 {id:'auckland',name:'Auckland', country:'New Zealand',lat:-36.848,lon:174.763,type:'news'},
];
const byId=Object.fromEntries(CITIES.map(c=>[c.id,c]));

/* network routes (great-circle arcs between hubs) */
const ROUTES=[
 ['nyc','london'],['london','frankfurt'],['london','paris'],['london','dubai'],
 ['dubai','mumbai'],['mumbai','singapore'],['singapore','tokyo'],['singapore','hongkong'],
 ['hongkong','shanghai'],['tokyo','nyc'],['tokyo','sydney'],['nyc','la'],['nyc','saopaulo'],
 ['dubai','singapore'],['frankfurt','moscow'],['singapore','jakarta'],['saopaulo','lagos'],
 ['dubai','nairobi'],['la','tokyo'],['sydney','auckland'],['seoul','tokyo'],['toronto','nyc'],
 ['mumbai','dubai'],['istanbul','frankfurt'],['cairo','dubai'],
];

/* ============ data layer (real APIs via /api proxies) ============ */
const cache=new Map();
function cached(key,fn){if(cache.has(key))return cache.get(key);const p=fn().catch(e=>{cache.delete(key);throw e;});cache.set(key,p);return p;}
async function jget(url){const r=await fetch(url);if(!r.ok)throw new Error('HTTP '+r.status);return r.json();}
function fetchWeather(c,iso){return cached('wx:'+c.id+':'+iso,()=>jget(`/api/weather?lat=${c.lat}&lon=${c.lon}&date=${iso}`));}
function fetchNews(c,iso){return cached('news:'+c.id+':'+iso,()=>jget(`/api/news?q=${encodeURIComponent(c.name)}&country=${encodeURIComponent(c.country)}&date=${iso}`));}
function fetchStock(sym,iso){return cached('stk:'+sym+':'+iso,()=>jget(`/api/stocks?symbol=${encodeURIComponent(sym)}&date=${iso}`));}
function sparkline(series){
  const pts=(series||[]).filter(v=>typeof v==='number'&&isFinite(v));
  if(pts.length<2)return '';
  let min=Math.min(...pts),max=Math.max(...pts);const span=(max-min)||1;
  const xy=pts.map((v,i)=>`${(i/(pts.length-1)*100).toFixed(1)},${(40-((v-min)/span)*36-2).toFixed(1)}`).join(' ');
  const up=pts[pts.length-1]>=pts[0];
  return `<svg viewBox="0 0 100 42" preserveAspectRatio="none" aria-hidden="true"><polyline points="${xy}" fill="none" stroke="${up?'#34D399':'#FB7185'}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}

/* ============ THREE scene ============ */
const W=()=>innerWidth, H=()=>innerHeight;
const renderer=new THREE.WebGLRenderer({antialias:true});
renderer.setPixelRatio(Math.min(devicePixelRatio,2));
renderer.setSize(W(),H());
renderer.setClearColor(0x05070f,1);
const scene=new THREE.Scene();
const camera=new THREE.PerspectiveCamera(45,W()/H(),0.1,100);
camera.position.z=5.6;

const SUN_DIR=new THREE.Vector3(1.0,0.28,0.65).normalize();
const sun=new THREE.DirectionalLight(0xFFF6E6,0.7); sun.position.copy(SUN_DIR.clone().multiplyScalar(10)); scene.add(sun);
scene.add(new THREE.HemisphereLight(0x33507f,0x070b14,0.35));
scene.add(new THREE.AmbientLight(0x1a2336,0.5));
document.getElementById('scene').appendChild(renderer.domElement);

/* stars */
(function(){
  const g=new THREE.BufferGeometry(), n=1600, pos=new Float32Array(n*3);const r0=rng('stars');
  for(let i=0;i<n;i++){const r=30+r0()*36,t=r0()*Math.PI*2,p=Math.acos(2*r0()-1);
    pos[i*3]=r*Math.sin(p)*Math.cos(t);pos[i*3+1]=r*Math.cos(p);pos[i*3+2]=r*Math.sin(p)*Math.sin(t)}
  g.setAttribute('position',new THREE.BufferAttribute(pos,3));
  scene.add(new THREE.Points(g,new THREE.PointsMaterial({color:0xCBD5F5,size:0.13,sizeAttenuation:true,transparent:true,opacity:0.8})));
})();

const globeGroup=new THREE.Group();scene.add(globeGroup);
const R=2;

/* night-network Earth shader: dark stylized base + glowing city lights */
const earthUniforms={dayMap:{value:null},nightMap:{value:null},sunViewDir:{value:new THREE.Vector3(1,0,0)}};
const earthMat=new THREE.ShaderMaterial({uniforms:earthUniforms,
  vertexShader:`varying vec2 vUv;varying vec3 vNormal;varying vec3 vView;
    void main(){vUv=uv;vNormal=normalize(normalMatrix*normal);vec4 mv=modelViewMatrix*vec4(position,1.0);vView=normalize(-mv.xyz);gl_Position=projectionMatrix*mv;}`,
  fragmentShader:`uniform sampler2D dayMap;uniform sampler2D nightMap;uniform vec3 sunViewDir;
    varying vec2 vUv;varying vec3 vNormal;varying vec3 vView;
    void main(){
      vec3 n=normalize(vNormal);vec3 s=normalize(sunViewDir);
      float day=smoothstep(-0.10,0.50,dot(n,s));
      vec3 dc=texture2D(dayMap,vUv).rgb;
      // stylized dark navy base, continents faintly readable
      vec3 base=mix(vec3(0.02,0.05,0.11), dc*vec3(0.32,0.40,0.5), 0.55);
      base*=(0.55+0.55*day);
      // warm glowing city lights — stronger on the night side
      vec3 lights=texture2D(nightMap,vUv).rgb;
      float amt=(1.0-day*0.7);
      vec3 col=base + lights*vec3(1.5,1.1,0.55)*amt*1.7;
      // cool atmospheric fresnel rim
      float fres=pow(1.0-max(dot(n,normalize(vView)),0.0),3.0);
      col+=vec3(0.16,0.42,0.9)*fres*0.6;
      gl_FragColor=vec4(col,1.0);
    }`});
const earth=new THREE.Mesh(new THREE.SphereGeometry(R,96,96),earthMat);
globeGroup.add(earth);

/* faint cloud veil */
const cloudMat=new THREE.MeshPhongMaterial({transparent:true,opacity:0.0,depthWrite:false});
const clouds=new THREE.Mesh(new THREE.SphereGeometry(R*1.012,64,64),cloudMat);
globeGroup.add(clouds);

/* atmosphere rim glow */
globeGroup.add(new THREE.Mesh(new THREE.SphereGeometry(R*1.18,64,64),new THREE.ShaderMaterial({
  vertexShader:`varying vec3 vN;void main(){vN=normalize(normalMatrix*normal);gl_Position=projectionMatrix*modelViewMatrix*vec4(position,1.0);}`,
  fragmentShader:`varying vec3 vN;void main(){float i=pow(0.64-dot(vN,vec3(0.,0.,1.)),3.0);gl_FragColor=vec4(0.30,0.62,1.0,1.0)*i;}`,
  blending:THREE.AdditiveBlending,side:THREE.BackSide,transparent:true,depthWrite:false})));

/* soft radial glow sprite texture (for beacons + arc pulses) */
function glowTexture(){
  const s=128,c=document.createElement('canvas');c.width=c.height=s;const x=c.getContext('2d');
  const g=x.createRadialGradient(s/2,s/2,0,s/2,s/2,s/2);
  g.addColorStop(0,'rgba(255,255,255,1)');g.addColorStop(0.22,'rgba(255,255,255,0.75)');
  g.addColorStop(0.5,'rgba(255,255,255,0.22)');g.addColorStop(1,'rgba(255,255,255,0)');
  x.fillStyle=g;x.fillRect(0,0,s,s);return new THREE.CanvasTexture(c);
}
const GLOW=glowTexture();
function glowSprite(color,size,opacity){
  const s=new THREE.Sprite(new THREE.SpriteMaterial({map:GLOW,color,transparent:true,blending:THREE.AdditiveBlending,depthWrite:false,opacity}));
  s.scale.setScalar(size);return s;
}

/* lat/lon → position */
function latLonV3(lat,lon,r){
  const phi=(90-lat)*Math.PI/180, th=(lon+180)*Math.PI/180;
  return new THREE.Vector3(-r*Math.sin(phi)*Math.cos(th), r*Math.cos(phi), r*Math.sin(phi)*Math.sin(th));
}

/* ---- glowing beacon markers (replace ring pins) ---- */
const MKT=0x34D399, NEWS=0xFBBF24;
const beacons=[], pinHits=[];
CITIES.forEach(city=>{
  const col=city.type==='mkt'?MKT:NEWS;
  const pos=latLonV3(city.lat,city.lon,R*1.006);
  const grp=new THREE.Group();grp.position.copy(pos);
  const glow=glowSprite(col,0.20,0.85);
  const core=glowSprite(0xffffff,0.055,0.95);
  grp.add(glow);grp.add(core);
  globeGroup.add(grp);
  const hit=new THREE.Mesh(new THREE.SphereGeometry(0.11,8,8),new THREE.MeshBasicMaterial({visible:false}));
  hit.position.copy(pos);hit.userData={city,glow,core};globeGroup.add(hit);
  pinHits.push(hit);
  beacons.push({city,glow,core,col,base:0.20,phase:Math.random()*6.28});
});

/* ---- network arcs (great-circle, glowing, animated pulse) ---- */
const arcs=[];
ROUTES.forEach(([a,b])=>{
  const ca=byId[a],cb=byId[b];if(!ca||!cb)return;
  const p0=latLonV3(ca.lat,ca.lon,R), p1=latLonV3(cb.lat,cb.lon,R);
  const dist=p0.distanceTo(p1);
  const mid=p0.clone().add(p1).multiplyScalar(0.5).normalize().multiplyScalar(R+dist*0.38+0.12);
  const curve=new THREE.QuadraticBezierCurve3(p0,mid,p1);
  const pts=curve.getPoints(64);
  const geo=new THREE.BufferGeometry().setFromPoints(pts);
  const line=new THREE.Line(geo,new THREE.LineBasicMaterial({color:0x6fd3ff,transparent:true,opacity:0.28,blending:THREE.AdditiveBlending,depthWrite:false}));
  globeGroup.add(line);
  const dot=glowSprite(0x9fe6ff,0.07,0.95);globeGroup.add(dot);
  arcs.push({curve,dot,t:Math.random(),speed:0.0012+Math.random()*0.0016});
});

/* ---- HTML city labels ---- */
const labelLayer=document.getElementById('labels');
const labels=CITIES.map(city=>{
  const el=document.createElement('div');
  el.className='city-label'+(city.type==='mkt'?' mkt':'');
  el.textContent=city.name;
  labelLayer.appendChild(el);
  return {city,base:latLonV3(city.lat,city.lon,R*1.02),el};
});

/* ---- NASA textures (day + night) from CDN ---- */
const TEX_BASES=[
  'https://cdn.jsdelivr.net/gh/mrdoob/three.js@r128/examples/textures/planets/',
  'https://cdn.jsdelivr.net/npm/three@0.128.0/examples/textures/planets/',
];
const loader=new THREE.TextureLoader();loader.setCrossOrigin('anonymous');
const maxAniso=renderer.capabilities.getMaxAnisotropy?renderer.capabilities.getMaxAnisotropy():1;
function loadFrom(url){return new Promise((res,rej)=>loader.load(url,t=>{t.anisotropy=Math.min(maxAniso,4);res(t);},undefined,()=>rej(new Error(url))));}
async function loadTex(file){
  for(const base of TEX_BASES){try{const t=await loadFrom(base+file);console.log('[globe] texture ok:',file);return t;}catch(e){console.warn('[globe] texture failed:',base+file);}}
  console.error('[globe] ALL sources failed for',file);return null;
}
Promise.all([loadTex('earth_atmos_2048.jpg'),loadTex('earth_lights_2048.png'),loadTex('earth_clouds_1024.png')])
.then(([day,night,cloud])=>{
  if(day)earthUniforms.dayMap.value=day;
  if(night)earthUniforms.nightMap.value=night;
  if(!day){console.error('[globe] day texture unavailable — solid-blue fallback');
    earth.material=new THREE.MeshPhongMaterial({color:0x12233f,emissive:0x0a1830,shininess:10});}
  if(cloud){cloudMat.map=cloud;cloudMat.opacity=0.18;cloudMat.needsUpdate=true;}
  document.getElementById('gl-load')?.classList.add('gone');
});
setTimeout(()=>document.getElementById('gl-load')?.classList.add('gone'),8000);

/* point India toward camera on load */
globeGroup.rotation.y=-(75+180)*Math.PI/180+Math.PI/2-0.5;

/* ============ interaction (with inertia) ============ */
let dragging=false,moved=0,px=0,py=0,velY=0,tiltX=-0.18;
globeGroup.rotation.x=tiltX;
const ray=new THREE.Raycaster(), ptr=new THREE.Vector2();
const tip=document.getElementById('tip');
const hintEl=document.getElementById('hint');
let hovered=null, pinchD=0;
function setPtr(e){const t=e.touches?e.touches[0]:e;ptr.x=t.clientX/W()*2-1;ptr.y=-(t.clientY/H())*2+1;return t}
function pick(){ray.setFromCamera(ptr,camera);const i=ray.intersectObjects(pinHits);return i.length?i[0].object:null}
const el=renderer.domElement;
el.addEventListener('pointerdown',e=>{dragging=true;moved=0;px=e.clientX;py=e.clientY;velY=0});
addEventListener('pointermove',e=>{
  if(dragging){
    const dx=e.clientX-px,dy=e.clientY-py;moved+=Math.abs(dx)+Math.abs(dy);
    globeGroup.rotation.y+=dx*0.005; velY=dx*0.005;
    tiltX=Math.max(-1.1,Math.min(1.1,tiltX+dy*0.003));globeGroup.rotation.x=tiltX;
    px=e.clientX;py=e.clientY;hintEl.classList.add('gone');
  } else if(e.pointerType!=='touch'){
    setPtr(e);const h=pick();
    if(h!==hovered){
      if(hovered){hovered.userData.glow.material.opacity=0.85;}
      hovered=h;
      if(h){h.userData.glow.material.opacity=1;tip.textContent=h.userData.city.name;
        tip.style.borderColor=h.userData.city.type==='mkt'?'rgba(52,211,153,.5)':'rgba(251,191,36,.5)';tip.classList.add('show');el.style.cursor='pointer'}
      else{tip.classList.remove('show');el.style.cursor='grab'}
    }
    if(h){tip.style.left=e.clientX+'px';tip.style.top=e.clientY+'px'}
  }
});
addEventListener('pointerup',e=>{if(dragging&&moved<6){setPtr(e);const h=pick();if(h)openPanel(h.userData.city)}dragging=false;});
el.addEventListener('wheel',e=>{e.preventDefault();camera.position.z=Math.max(3.1,Math.min(9,camera.position.z+e.deltaY*0.0035))},{passive:false});
el.addEventListener('touchstart',e=>{if(e.touches.length===2){pinchD=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY)}},{passive:true});
el.addEventListener('touchmove',e=>{if(e.touches.length===2){const d=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY);camera.position.z=Math.max(3.1,Math.min(9,camera.position.z-(d-pinchD)*0.01));pinchD=d;dragging=false}},{passive:true});

/* ============ panel ============ */
const panel=document.getElementById('panel');
let activeCity=null, panelToken=0;
function openPanel(city){activeCity=city;renderPanel();panel.classList.add('open');hintEl.classList.add('gone')}
function closePanel(){panel.classList.remove('open');activeCity=null;panelToken++}
function panelHead(c,d){return `<div class="phead"><div><h2>${esc(c.name)}</h2><div class="country">${esc(c.country)}</div></div><button id="closeBtn" aria-label="Close">✕</button></div><div class="pdate">${dayIndex(TODAY)===dayIndex(d)?'Today':'⏳ '+esc(fmt(d))}</div>`;}
function bindClose(){const b=document.getElementById('closeBtn');if(b)b.onclick=closePanel;}
function loadingCard(label,lines){let s=`<div class="card"><div class="clabel">${label}</div>`;for(let i=0;i<(lines||2);i++)s+=`<div class="skl${i===0?' lg':''}"></div>`;return s+'</div>';}
function stockCardHTML(ix,s){
  if(!s||s.value==null)return `<div class="card stock"><div class="clabel">Index</div><div class="srow"><span class="sname">${esc(ix.n)}</span><span class="sval muted">—</span></div><div class="schg muted">No market data for this date</div></div>`;
  const prev=s.prevClose,chg=(prev!=null&&prev!==0)?(s.value-prev)/prev*100:null,up=chg==null?true:chg>=0,cur=ix.cur||'';
  return `<div class="card stock"><div class="clabel">Index</div><div class="srow"><span class="sname">${esc(ix.n)}</span><span class="sval">${cur}${s.value.toLocaleString('en-IN',{maximumFractionDigits:0})}</span></div>${chg==null?'<div class="schg muted">prev. close unavailable</div>':`<div class="schg ${up?'up':'down'}">${up?'▲':'▼'} ${Math.abs(chg).toFixed(2)}% vs prev. close</div>`}${sparkline(s.series)}</div>`;
}
function weatherCardHTML(wx){
  if(!wx||wx.temp==null)return `<div class="card"><div class="clabel">Weather</div><div class="wx"><span class="wemoji">${esc((wx&&wx.emoji)||'🌍')}</span><div><div class="wtemp muted">—</div><div class="wcond">No weather record for this date</div></div></div></div>`;
  return `<div class="card"><div class="clabel">Weather</div><div class="wx"><span class="wemoji">${esc(wx.emoji||'🌤️')}</span><div><div class="wtemp">${wx.temp}°C</div><div class="wcond">${esc(wx.cond||'')}</div></div></div></div>`;
}
function newsCardHTML(res,d){
  const arts=(res&&res.articles)||[];
  if(!arts.length)return `<div class="card news"><div class="clabel">Headlines</div><div class="muted">No archived headlines for ${esc(fmt(d))}.<br>GDELT full-text search covers roughly the last 3 months.</div></div>`;
  const items=arts.map(n=>{const title=esc(n.h||''),src=esc(n.s||'');const inner=n.url?`<a href="${esc(n.url)}" target="_blank" rel="noopener">${title}</a>`:title;return `<li>${inner}<span class="src">${src}${src?' · ':''}${esc(fmt(d))}</span></li>`;}).join('');
  return `<div class="card news"><div class="clabel">Headlines</div><ul>${items}</ul></div>`;
}
function footNote(){return `<div class="demo-note">Live data · GDELT (news) · Open-Meteo (weather) · Yahoo Finance (markets).<br>Historical depth varies by source.</div>`;}
async function renderPanel(){
  if(!activeCity)return;
  const c=activeCity,d=viewDate,iso=isoKey(d),token=++panelToken;
  let shell=panelHead(c,d);
  if(c.type==='mkt')shell+=c.indices.map(()=>loadingCard('Index',2)).join('');
  shell+=loadingCard('Weather',1)+loadingCard('Headlines',3)+footNote();
  panel.innerHTML=shell;bindClose();
  const tasks=[fetchWeather(c,iso),fetchNews(c,iso)];
  if(c.type==='mkt')c.indices.forEach(ix=>tasks.push(fetchStock(ix.symbol,iso)));
  const settled=await Promise.allSettled(tasks);
  if(token!==panelToken||activeCity!==c)return;
  const val=r=>r&&r.status==='fulfilled'?r.value:null;
  const wx=val(settled[0]),news=val(settled[1]);
  const stocks=c.type==='mkt'?c.indices.map((ix,i)=>val(settled[2+i])):[];
  let html=panelHead(c,d);
  if(c.type==='mkt')html+=c.indices.map((ix,i)=>stockCardHTML(ix,stocks[i])).join('');
  html+=weatherCardHTML(wx)+newsCardHTML(news,d)+footNote();
  panel.innerHTML=html;bindClose();
}

/* ============ time dial ============ */
const dateLabel=document.getElementById('dateLabel');
function shift(unit,amt){const d=new Date(viewDate);if(unit==='d')d.setDate(d.getDate()+amt);if(unit==='m')d.setMonth(d.getMonth()+amt);if(unit==='y')d.setFullYear(d.getFullYear()+amt);if(d>TODAY)return;viewDate=d;syncTime();}
function syncTime(){
  const back=dayIndex(TODAY)-dayIndex(viewDate);
  dateLabel.innerHTML=fmt(viewDate)+(back?`<small>${back<365?back+' day'+(back>1?'s':''):(back/365).toFixed(1)+' yrs'} back</small>`:'<small>live · today</small>');
  const p=Math.min(1,back/3650);
  document.getElementById('pastveil').style.opacity=p*0.9;
  document.getElementById('scene').style.filter=back?`saturate(${1-0.2*p}) sepia(${0.14*p})`:'none';
  ['bDayFwd','bMonthFwd'].forEach(id=>document.getElementById(id).disabled=back===0);
  document.getElementById('todayBtn').disabled=back===0;
  renderPanel();
}
document.getElementById('bDayBack').onclick=()=>shift('d',-1);
document.getElementById('bMonthBack').onclick=()=>shift('m',-1);
document.getElementById('bYearBack').onclick=()=>shift('y',-1);
document.getElementById('bDayFwd').onclick=()=>shift('d',1);
document.getElementById('bMonthFwd').onclick=()=>shift('m',1);
document.getElementById('todayBtn').onclick=()=>{viewDate=new Date(TODAY);syncTime()};
syncTime();

/* ============ loop ============ */
const clock=new THREE.Clock();
const reduceMotion=matchMedia('(prefers-reduced-motion: reduce)').matches;
const _sv=new THREE.Vector3(), _w=new THREE.Vector3(), _camDir=new THREE.Vector3();
function updateLabels(){
  const w=W(),h=H();
  camera.getWorldPosition(_camDir);
  for(const L of labels){
    _w.copy(L.base).applyMatrix4(globeGroup.matrixWorld);
    const facing=_w.dot(_w.clone().sub(camera.position))<0; // normal·(toCam) > 0 → front
    const ndc=_w.clone().project(camera);
    if(!facing||ndc.z>1){L.el.style.opacity='0';continue;}
    const x=(ndc.x*0.5+0.5)*w, y=(-ndc.y*0.5+0.5)*h;
    L.el.style.transform=`translate(${x.toFixed(1)}px,${y.toFixed(1)}px) translate(-50%,-150%)`;
    L.el.style.opacity=(activeCity===L.city)?'1':'0.72';
  }
}
function tick(){
  requestAnimationFrame(tick);
  const t=clock.getElapsedTime();
  if(!dragging&&!reduceMotion){globeGroup.rotation.y+=velY+0.0006;velY*=0.95;if(Math.abs(velY)<0.00002)velY=0;}
  if(!reduceMotion){
    clouds.rotation.y+=0.00035;
    beacons.forEach(b=>{const k=1+0.22*Math.sin(t*2.2+b.phase);b.glow.scale.setScalar(b.base*k);b.glow.material.opacity=(hovered&&hovered.userData.glow===b.glow?1:0.6+0.3*Math.sin(t*2.2+b.phase));});
    arcs.forEach(a=>{a.t=(a.t+a.speed)%1;a.dot.position.copy(a.curve.getPoint(a.t));a.dot.material.opacity=0.5+0.5*Math.sin(a.t*Math.PI);});
  }
  _sv.copy(SUN_DIR).transformDirection(camera.matrixWorldInverse);
  earthUniforms.sunViewDir.value.copy(_sv);
  updateLabels();
  renderer.render(scene,camera);
}
tick();
addEventListener('resize',()=>{camera.aspect=W()/H();camera.updateProjectionMatrix();renderer.setSize(W(),H())});
