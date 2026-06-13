import * as THREE from 'three';
import './style.css';
"use strict";

/* ============ seeded randomness (used only for the starfield) ============ */
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

/* ============ cities (with Yahoo Finance symbols) ============ */
const CITIES=[
 {id:'mumbai',  name:'Mumbai',    country:'India',     lat:19.076,lon:72.877, type:'mkt',
   indices:[{n:'BSE Sensex',base:84600,cur:'₹',symbol:'^BSESN'},{n:'NSE Nifty 50',base:25750,cur:'₹',symbol:'^NSEI'}]},
 {id:'hyd',     name:'Hyderabad', country:'India',     lat:17.385,lon:78.486, type:'news'},
 {id:'delhi',   name:'New Delhi', country:'India',     lat:28.614,lon:77.209, type:'news'},
 {id:'nyc',     name:'New York',  country:'USA',       lat:40.713,lon:-74.006,type:'mkt',
   indices:[{n:'NASDAQ Comp.',base:21400,cur:'$',symbol:'^IXIC'},{n:'Dow Jones',base:44600,cur:'$',symbol:'^DJI'}]},
 {id:'london',  name:'London',    country:'UK',        lat:51.507,lon:-0.128, type:'mkt',
   indices:[{n:'FTSE 100',base:8420,cur:'£',symbol:'^FTSE'}]},
 {id:'tokyo',   name:'Tokyo',     country:'Japan',     lat:35.690,lon:139.692,type:'mkt',
   indices:[{n:'Nikkei 225',base:41200,cur:'¥',symbol:'^N225'}]},
 {id:'dubai',   name:'Dubai',     country:'UAE',       lat:25.205,lon:55.271, type:'news'},
 {id:'sydney',  name:'Sydney',    country:'Australia', lat:-33.868,lon:151.209,type:'news'},
 {id:'saopaulo',name:'São Paulo', country:'Brazil',    lat:-23.551,lon:-46.633,type:'news'},
];

/* ============ data layer — real APIs via /api proxies (cached per city+date) ============ */
const cache=new Map();
function cached(key,fn){
  if(cache.has(key))return cache.get(key);
  const p=fn().catch(e=>{cache.delete(key);throw e;});
  cache.set(key,p);return p;
}
async function jget(url){const r=await fetch(url);if(!r.ok)throw new Error('HTTP '+r.status);return r.json();}
function fetchWeather(city,iso){return cached('wx:'+city.id+':'+iso,()=>jget(`/api/weather?lat=${city.lat}&lon=${city.lon}&date=${iso}`));}
function fetchNews(city,iso){return cached('news:'+city.id+':'+iso,()=>jget(`/api/news?q=${encodeURIComponent(city.name)}&country=${encodeURIComponent(city.country)}&date=${iso}`));}
function fetchStock(symbol,iso){return cached('stk:'+symbol+':'+iso,()=>jget(`/api/stocks?symbol=${encodeURIComponent(symbol)}&date=${iso}`));}

function sparkline(series){
  const pts=(series||[]).filter(v=>typeof v==='number'&&isFinite(v));
  if(pts.length<2)return '';
  let min=Math.min(...pts),max=Math.max(...pts);const span=(max-min)||1;
  const xy=pts.map((v,i)=>`${(i/(pts.length-1)*100).toFixed(1)},${(40-((v-min)/span)*36-2).toFixed(1)}`).join(' ');
  const up=pts[pts.length-1]>=pts[0];
  return `<svg viewBox="0 0 100 42" preserveAspectRatio="none" aria-hidden="true">
    <polyline points="${xy}" fill="none" stroke="${up?'#34D399':'#FB7185'}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
}

/* ============ THREE scene ============ */
const W=()=>innerWidth, H=()=>innerHeight;
const renderer=new THREE.WebGLRenderer({antialias:true});
renderer.setPixelRatio(Math.min(devicePixelRatio,2));   // capped for mobile perf
renderer.setSize(W(),H());
renderer.setClearColor(0x05070f,1);

const scene=new THREE.Scene();
const camera=new THREE.PerspectiveCamera(45,W()/H(),0.1,100);
camera.position.z=5.6;

/* premium lighting: warm key "sun" + cool fill + faint ambient */
const SUN_DIR=new THREE.Vector3(1.0,0.28,0.65).normalize();   // fixed in world space
const sun=new THREE.DirectionalLight(0xFFF6E6,1.15); sun.position.copy(SUN_DIR.clone().multiplyScalar(10)); scene.add(sun);
scene.add(new THREE.HemisphereLight(0x4a6da8,0x0a0e18,0.35));
scene.add(new THREE.AmbientLight(0x223044,0.4));

document.getElementById('scene').appendChild(renderer.domElement);

/* stars */
(function(){
  const g=new THREE.BufferGeometry(), n=1500, pos=new Float32Array(n*3);
  const r0=rng('stars');
  for(let i=0;i<n;i++){const r=30+r0()*34,t=r0()*Math.PI*2,p=Math.acos(2*r0()-1);
    pos[i*3]=r*Math.sin(p)*Math.cos(t);pos[i*3+1]=r*Math.cos(p);pos[i*3+2]=r*Math.sin(p)*Math.sin(t)}
  g.setAttribute('position',new THREE.BufferAttribute(pos,3));
  scene.add(new THREE.Points(g,new THREE.PointsMaterial({color:0xCBD5F5,size:0.14,sizeAttenuation:true,transparent:true,opacity:0.85})));
})();

const globeGroup=new THREE.Group();scene.add(globeGroup);
const R=2;

/* ---- realistic Earth: day/night terminator shader (NASA Blue Marble) ---- */
const earthUniforms={
  dayMap:{value:null}, nightMap:{value:null}, specMap:{value:null},
  sunViewDir:{value:new THREE.Vector3(1,0,0)},
};
const earthMat=new THREE.ShaderMaterial({
  uniforms:earthUniforms,
  vertexShader:`
    varying vec2 vUv; varying vec3 vNormal; varying vec3 vView;
    void main(){
      vUv=uv;
      vNormal=normalize(normalMatrix*normal);
      vec4 mv=modelViewMatrix*vec4(position,1.0);
      vView=normalize(-mv.xyz);
      gl_Position=projectionMatrix*mv;
    }`,
  fragmentShader:`
    uniform sampler2D dayMap; uniform sampler2D nightMap; uniform sampler2D specMap;
    uniform vec3 sunViewDir;
    varying vec2 vUv; varying vec3 vNormal; varying vec3 vView;
    void main(){
      vec3 n=normalize(vNormal);
      vec3 s=normalize(sunViewDir);
      float lit=dot(n,s);
      float t=smoothstep(-0.12,0.22,lit);          // soft terminator
      vec3 day=texture2D(dayMap,vUv).rgb*1.06;
      vec3 night=texture2D(nightMap,vUv).rgb*1.5;   // city lights glow
      vec3 col=mix(night,day,t);
      // specular sun-glint on oceans
      float ocean=texture2D(specMap,vUv).r;
      vec3 refl=reflect(-s,n);
      float spec=pow(max(dot(refl,normalize(vView)),0.0),20.0)*ocean*t;
      col+=vec3(0.7,0.8,0.95)*spec*0.9;
      // cool fresnel rim into the atmosphere
      float fres=pow(1.0-max(dot(n,normalize(vView)),0.0),3.0);
      col+=vec3(0.20,0.45,0.85)*fres*0.55;
      gl_FragColor=vec4(col,1.0);
    }`,
});
const earth=new THREE.Mesh(new THREE.SphereGeometry(R,96,96),earthMat);
globeGroup.add(earth);

/* clouds (lit, semi-transparent) */
const cloudMat=new THREE.MeshPhongMaterial({transparent:true,opacity:0.0,depthWrite:false});
const clouds=new THREE.Mesh(new THREE.SphereGeometry(R*1.012,64,64),cloudMat);
globeGroup.add(clouds);

/* atmosphere rim glow (additive back-side fresnel) */
globeGroup.add(new THREE.Mesh(new THREE.SphereGeometry(R*1.16,64,64),new THREE.ShaderMaterial({
  vertexShader:`varying vec3 vN;void main(){vN=normalize(normalMatrix*normal);gl_Position=projectionMatrix*modelViewMatrix*vec4(position,1.0);}`,
  fragmentShader:`varying vec3 vN;void main(){float i=pow(0.66-dot(vN,vec3(0.,0.,1.)),3.0);gl_FragColor=vec4(0.36,0.66,1.0,1.0)*i;}`,
  blending:THREE.AdditiveBlending,side:THREE.BackSide,transparent:true,depthWrite:false})));

/* ---- load NASA Blue Marble textures from CDN (public domain, CORS-enabled) ---- */
// gh path serves straight from the three.js repo tag (always contains the
// textures); npm path is a fallback. Each load is logged for debugging.
const TEX_BASES=[
  'https://cdn.jsdelivr.net/gh/mrdoob/three.js@r128/examples/textures/planets/',
  'https://cdn.jsdelivr.net/npm/three@0.128.0/examples/textures/planets/',
];
const loader=new THREE.TextureLoader();
loader.setCrossOrigin('anonymous');
const maxAniso=renderer.capabilities.getMaxAnisotropy?renderer.capabilities.getMaxAnisotropy():1;
function loadFrom(url){
  return new Promise((res,rej)=>loader.load(url,
    t=>{t.anisotropy=Math.min(maxAniso,4);res(t);},
    undefined,
    ()=>rej(new Error(url))));
}
async function loadTex(file){
  for(const base of TEX_BASES){
    try{ const t=await loadFrom(base+file); console.log('[globe] texture ok:',file,'←',base); return t; }
    catch(e){ console.warn('[globe] texture failed:',base+file); }
  }
  console.error('[globe] ALL sources failed for',file);
  return null;
}
let texturesReady=false;
Promise.all([
  loadTex('earth_atmos_2048.jpg'),    // day
  loadTex('earth_lights_2048.png'),   // night city lights
  loadTex('earth_specular_2048.jpg'), // ocean specular mask
  loadTex('earth_clouds_1024.png'),   // clouds
]).then(([day,night,spec,cloud])=>{
  if(day){earthUniforms.dayMap.value=day;}
  if(night){earthUniforms.nightMap.value=night;}
  if(spec){earthUniforms.specMap.value=spec;}
  if(!day){ // texture CDN unreachable → solid premium-blue fallback so it's never black
    console.error('[globe] day texture unavailable — using solid-blue fallback');
    earth.material=new THREE.MeshPhongMaterial({color:0x1b3a6b,emissive:0x0a1830,shininess:12});
  }
  if(cloud){cloudMat.map=cloud;cloudMat.opacity=0.85;cloudMat.needsUpdate=true;}
  texturesReady=true;
  document.getElementById('gl-load')?.classList.add('gone');
});
/* safety: hide the loader even if the CDN hangs */
setTimeout(()=>document.getElementById('gl-load')?.classList.add('gone'),8000);

/* pins */
function latLonV3(lat,lon,r){
  const phi=(90-lat)*Math.PI/180, th=(lon+180)*Math.PI/180;
  return new THREE.Vector3(-r*Math.sin(phi)*Math.cos(th), r*Math.cos(phi), r*Math.sin(phi)*Math.sin(th));
}
const pinHits=[];
CITIES.forEach(city=>{
  const col=city.type==='mkt'?0x34D399:0xFB923C;
  const g=new THREE.Group();
  const pos=latLonV3(city.lat,city.lon,R);
  g.position.copy(pos);
  g.lookAt(pos.clone().multiplyScalar(2));
  const stem=new THREE.Mesh(new THREE.CylinderGeometry(0.012,0.012,0.16,8),new THREE.MeshBasicMaterial({color:col}));
  stem.rotation.x=Math.PI/2; stem.position.z=0.08; g.add(stem);
  const head=new THREE.Mesh(new THREE.SphereGeometry(0.055,16,16),new THREE.MeshBasicMaterial({color:col}));
  head.position.z=0.17; g.add(head);
  const ring=new THREE.Mesh(new THREE.RingGeometry(0.07,0.1,32),
    new THREE.MeshBasicMaterial({color:col,transparent:true,opacity:0.8,side:THREE.DoubleSide}));
  ring.position.z=0.02; g.add(ring);
  const hit=new THREE.Mesh(new THREE.SphereGeometry(0.17,8,8),new THREE.MeshBasicMaterial({visible:false}));
  hit.position.z=0.15; hit.userData={city,head,ring}; g.add(hit);
  pinHits.push(hit);
  globeGroup.add(g);
});

/* point India toward camera on load */
globeGroup.rotation.y=-(75+180)*Math.PI/180+Math.PI/2-0.5;

/* ============ interaction (with drag inertia) ============ */
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
    globeGroup.rotation.y+=dx*0.005; velY=dx*0.005;            // capture momentum
    tiltX=Math.max(-1.1,Math.min(1.1,tiltX+dy*0.003));globeGroup.rotation.x=tiltX;
    px=e.clientX;py=e.clientY;hintEl.classList.add('gone');
  } else if(e.pointerType!=='touch'){
    setPtr(e);const h=pick();
    if(h!==hovered){
      if(hovered){hovered.userData.head.scale.setScalar(1)}
      hovered=h;
      if(h){h.userData.head.scale.setScalar(1.7);tip.textContent=h.userData.city.name;
        tip.style.borderColor=h.userData.city.type==='mkt'?'rgba(52,211,153,.5)':'rgba(251,146,60,.5)';tip.classList.add('show');
        el.style.cursor='pointer'}
      else{tip.classList.remove('show');el.style.cursor='grab'}
    }
    if(h){tip.style.left=e.clientX+'px';tip.style.top=e.clientY+'px'}
  }
});
addEventListener('pointerup',e=>{
  if(dragging&&moved<6){setPtr(e);const h=pick();if(h)openPanel(h.userData.city)}
  dragging=false;
});
el.addEventListener('wheel',e=>{e.preventDefault();camera.position.z=Math.max(3.1,Math.min(9,camera.position.z+e.deltaY*0.0035))},{passive:false});
el.addEventListener('touchstart',e=>{if(e.touches.length===2){pinchD=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY)}},{passive:true});
el.addEventListener('touchmove',e=>{if(e.touches.length===2){const d=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY);
  camera.position.z=Math.max(3.1,Math.min(9,camera.position.z-(d-pinchD)*0.01));pinchD=d;dragging=false}},{passive:true});

/* ============ panel ============ */
const panel=document.getElementById('panel');
let activeCity=null;
let panelToken=0;
function openPanel(city){activeCity=city;renderPanel();panel.classList.add('open');hintEl.classList.add('gone')}
function closePanel(){panel.classList.remove('open');activeCity=null;panelToken++}

function panelHead(c,d){
  return `<div class="phead"><div><h2>${esc(c.name)}</h2><div class="country">${esc(c.country)}</div></div>
    <button id="closeBtn" aria-label="Close">✕</button></div>
    <div class="pdate">${dayIndex(TODAY)===dayIndex(d)?'Today':'⏳ '+esc(fmt(d))}</div>`;
}
function bindClose(){const b=document.getElementById('closeBtn');if(b)b.onclick=closePanel;}
function loadingCard(label,lines){
  let s=`<div class="card"><div class="clabel">${label}</div>`;
  for(let i=0;i<(lines||2);i++)s+=`<div class="skl${i===0?' lg':''}"></div>`;
  return s+'</div>';
}
function stockCardHTML(ix,s){
  if(!s||s.value==null){
    return `<div class="card stock"><div class="clabel">Index</div>
      <div class="srow"><span class="sname">${esc(ix.n)}</span><span class="sval muted">—</span></div>
      <div class="schg muted">No market data for this date</div></div>`;
  }
  const prev=s.prevClose;
  const chg=(prev!=null&&prev!==0)?(s.value-prev)/prev*100:null;
  const up=chg==null?true:chg>=0;
  const cur=ix.cur||'';
  return `<div class="card stock"><div class="clabel">Index</div>
    <div class="srow"><span class="sname">${esc(ix.n)}</span>
      <span class="sval">${cur}${s.value.toLocaleString('en-IN',{maximumFractionDigits:0})}</span></div>
    ${chg==null?'<div class="schg muted">prev. close unavailable</div>'
      :`<div class="schg ${up?'up':'down'}">${up?'▲':'▼'} ${Math.abs(chg).toFixed(2)}% vs prev. close</div>`}
    ${sparkline(s.series)}</div>`;
}
function weatherCardHTML(wx){
  if(!wx||wx.temp==null){
    return `<div class="card"><div class="clabel">Weather</div>
      <div class="wx"><span class="wemoji">${esc((wx&&wx.emoji)||'🌍')}</span>
      <div><div class="wtemp muted">—</div><div class="wcond">No weather record for this date</div></div></div></div>`;
  }
  return `<div class="card"><div class="clabel">Weather</div>
    <div class="wx"><span class="wemoji">${esc(wx.emoji||'🌤️')}</span>
    <div><div class="wtemp">${wx.temp}°C</div><div class="wcond">${esc(wx.cond||'')}</div></div></div></div>`;
}
function newsCardHTML(res,d){
  const arts=(res&&res.articles)||[];
  if(!arts.length){
    return `<div class="card news"><div class="clabel">Headlines</div>
      <div class="muted">No archived headlines for ${esc(fmt(d))}.<br>GDELT full-text search covers roughly the last 3 months.</div></div>`;
  }
  const items=arts.map(n=>{
    const title=esc(n.h||''), src=esc(n.s||'');
    const inner=n.url?`<a href="${esc(n.url)}" target="_blank" rel="noopener">${title}</a>`:title;
    return `<li>${inner}<span class="src">${src}${src?' · ':''}${esc(fmt(d))}</span></li>`;
  }).join('');
  return `<div class="card news"><div class="clabel">Headlines</div><ul>${items}</ul></div>`;
}
function footNote(){
  return `<div class="demo-note">Live data · GDELT (news) · Open-Meteo (weather) · Yahoo Finance (markets).<br>Historical depth varies by source.</div>`;
}
async function renderPanel(){
  if(!activeCity)return;
  const c=activeCity, d=viewDate, iso=isoKey(d);
  const token=++panelToken;
  let shell=panelHead(c,d);
  if(c.type==='mkt')shell+=c.indices.map(()=>loadingCard('Index',2)).join('');
  shell+=loadingCard('Weather',1)+loadingCard('Headlines',3)+footNote();
  panel.innerHTML=shell;bindClose();

  const tasks=[fetchWeather(c,iso),fetchNews(c,iso)];
  if(c.type==='mkt')c.indices.forEach(ix=>tasks.push(fetchStock(ix.symbol,iso)));
  const settled=await Promise.allSettled(tasks);
  if(token!==panelToken||activeCity!==c)return;

  const val=r=>r&&r.status==='fulfilled'?r.value:null;
  const wx=val(settled[0]);
  const news=val(settled[1]);
  const stocks=c.type==='mkt'?c.indices.map((ix,i)=>val(settled[2+i])):[];

  let html=panelHead(c,d);
  if(c.type==='mkt')html+=c.indices.map((ix,i)=>stockCardHTML(ix,stocks[i])).join('');
  html+=weatherCardHTML(wx)+newsCardHTML(news,d)+footNote();
  panel.innerHTML=html;bindClose();
}

/* ============ time dial ============ */
const dateLabel=document.getElementById('dateLabel');
function shift(unit,amt){
  const d=new Date(viewDate);
  if(unit==='d')d.setDate(d.getDate()+amt);
  if(unit==='m')d.setMonth(d.getMonth()+amt);
  if(unit==='y')d.setFullYear(d.getFullYear()+amt);
  if(d>TODAY)return;
  viewDate=d;syncTime();
}
function syncTime(){
  const back=dayIndex(TODAY)-dayIndex(viewDate);
  dateLabel.innerHTML=fmt(viewDate)+(back?`<small>${back<365?back+' day'+(back>1?'s':''):(back/365).toFixed(1)+' yrs'} back</small>`:'<small>live · today</small>');
  const p=Math.min(1,back/3650);
  document.getElementById('pastveil').style.opacity=p*0.9;
  document.getElementById('scene').style.filter=back?`saturate(${1-0.22*p}) sepia(${0.16*p})`:'none';
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
const _sv=new THREE.Vector3();
function tick(){
  requestAnimationFrame(tick);
  const t=clock.getElapsedTime();
  // drag inertia → decays into a gentle idle spin
  if(!dragging&&!reduceMotion){ globeGroup.rotation.y += velY + 0.0006; velY *= 0.95; if(Math.abs(velY)<0.00002)velY=0; }
  if(!reduceMotion){ clouds.rotation.y += 0.00035;
    pinHits.forEach((h,i)=>{const s=1+0.25*Math.sin(t*2.4+i);h.userData.ring.scale.setScalar(s);
      h.userData.ring.material.opacity=0.85-0.35*Math.sin(t*2.4+i)});
  }
  // keep the day/night terminator fixed in world space as the globe spins
  _sv.copy(SUN_DIR).transformDirection(camera.matrixWorldInverse);
  earthUniforms.sunViewDir.value.copy(_sv);
  renderer.render(scene,camera);
}
tick();
addEventListener('resize',()=>{camera.aspect=W()/H();camera.updateProjectionMatrix();renderer.setSize(W(),H())});
