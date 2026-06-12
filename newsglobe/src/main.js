import * as THREE from 'three';
import './style.css';
"use strict";

/* ============ seeded randomness (used only for the globe's textures) ============ */
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

/* ============ cities (now with Yahoo Finance symbols) ============ */
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
async function jget(url){
  const r=await fetch(url);
  if(!r.ok)throw new Error('HTTP '+r.status);
  return r.json();
}
function fetchWeather(city,iso){
  return cached('wx:'+city.id+':'+iso,()=>jget(`/api/weather?lat=${city.lat}&lon=${city.lon}&date=${iso}`));
}
function fetchNews(city,iso){
  return cached('news:'+city.id+':'+iso,()=>jget(`/api/news?q=${encodeURIComponent(city.name)}&country=${encodeURIComponent(city.country)}&date=${iso}`));
}
function fetchStock(symbol,iso){
  return cached('stk:'+symbol+':'+iso,()=>jget(`/api/stocks?symbol=${encodeURIComponent(symbol)}&date=${iso}`));
}

/* sparkline from a real close-price series */
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
renderer.setPixelRatio(Math.min(devicePixelRatio,2));
renderer.setSize(W(),H());
renderer.setClearColor(0x0B1026,1);
document.getElementById('scene').appendChild(renderer.domElement);

const scene=new THREE.Scene();
const camera=new THREE.PerspectiveCamera(45,W()/H(),0.1,100);
camera.position.z=5.6;

scene.add(new THREE.AmbientLight(0xBFD4FF,0.85));
const sun=new THREE.DirectionalLight(0xFFF4D6,1.0); sun.position.set(4,2.5,5); scene.add(sun);

/* stars */
(function(){
  const g=new THREE.BufferGeometry(), n=1300, pos=new Float32Array(n*3);
  const r0=rng('stars');
  for(let i=0;i<n;i++){const r=28+r0()*30,t=r0()*Math.PI*2,p=Math.acos(2*r0()-1);
    pos[i*3]=r*Math.sin(p)*Math.cos(t);pos[i*3+1]=r*Math.cos(p);pos[i*3+2]=r*Math.sin(p)*Math.sin(t)}
  g.setAttribute('position',new THREE.BufferAttribute(pos,3));
  scene.add(new THREE.Points(g,new THREE.PointsMaterial({color:0xCBD5F5,size:0.16,sizeAttenuation:true,transparent:true,opacity:0.85})));
})();

/* ---- cartoon earth texture ---- */
function makeEarthTexture(){
  const w=2048,h=1024,c=document.createElement('canvas');c.width=w;c.height=h;
  const x=c.getContext('2d');
  const X=lon=>(lon+180)/360*w, Y=lat=>(90-lat)/180*h;

  // candy ocean
  const og=x.createLinearGradient(0,0,0,h);
  og.addColorStop(0,'#67D5F5');og.addColorStop(0.5,'#3AAFE8');og.addColorStop(1,'#67D5F5');
  x.fillStyle=og;x.fillRect(0,0,w,h);
  // playful wave flecks
  const wr=rng('waves');x.fillStyle='rgba(255,255,255,0.18)';
  for(let i=0;i<260;i++){const ww=8+wr()*22;x.beginPath();
    x.ellipse(wr()*w,wr()*h,ww,2.2,0,0,Math.PI*2);x.fill()}

  function blob(pts,fill,stroke){
    x.beginPath();
    const P=pts.map(p=>[X(p[0]),Y(p[1])]);
    x.moveTo((P[0][0]+P[P.length-1][0])/2,(P[0][1]+P[P.length-1][1])/2);
    for(let i=0;i<P.length;i++){const a=P[i],b=P[(i+1)%P.length];
      x.quadraticCurveTo(a[0],a[1],(a[0]+b[0])/2,(a[1]+b[1])/2)}
    x.closePath();
    x.fillStyle=fill;x.fill();
    x.lineJoin='round';x.lineWidth=10;x.strokeStyle=stroke;x.stroke();
  }
  const LAND='#5BD66E', EDGE='#2E9E55';

  blob([[-166,66],[-150,71],[-128,70],[-108,72],[-90,70],[-76,71],[-60,60],[-56,50],[-66,44],[-75,38],[-79,32],[-81,26],[-90,29],[-97,26],[-97,20],[-92,15],[-84,10],[-79,8],[-83,14],[-88,16],[-97,17],[-106,23],[-113,31],[-121,35],[-125,43],[-129,51],[-136,58],[-152,60],[-165,60]],LAND,EDGE); // N America
  blob([[-79,9],[-71,12],[-61,10],[-51,4],[-43,-3],[-35,-8],[-38,-15],[-41,-22],[-48,-28],[-54,-34],[-59,-39],[-65,-41],[-66,-47],[-69,-53],[-75,-50],[-73,-43],[-72,-34],[-70,-24],[-71,-17],[-77,-11],[-81,-4],[-80,3]],LAND,EDGE); // S America
  blob([[-17,15],[-11,25],[-6,33],[0,36],[10,37],[20,33],[31,31],[34,27],[43,12],[51,11],[46,1],[40,-6],[37,-13],[34,-21],[31,-30],[26,-34],[19,-35],[14,-29],[12,-17],[13,-9],[9,-1],[8,4],[-4,5],[-13,9]],LAND,EDGE); // Africa
  blob([[44,-13],[50,-17],[48,-25],[44,-23]],LAND,EDGE); // Madagascar
  blob([[-10,37],[-9,43],[-1,46],[0,51],[6,53],[9,55],[6,58],[12,60],[19,57],[22,56],[28,60],[24,66],[20,70],[32,70],[46,68],[62,69],[76,72],[92,73],[108,72],[124,72],[138,71],[152,69],[164,68],[177,65],[177,62],[162,60],[158,53],[148,46],[141,44],[134,43],[129,41],[126,38],[121,36],[119,29],[109,18],[106,10],[103,2],[100,8],[98,14],[94,17],[91,22],[88,21],[85,19],[81,15],[79,9],[76,8],[72,19],[68,23],[66,25],[60,25],[57,26],[52,25],[58,21],[54,16],[50,14],[45,13],[43,16],[40,21],[35,28],[35,36],[29,37],[25,37],[21,39],[16,39],[14,42],[9,44],[3,43],[-2,37],[-6,36]],LAND,EDGE); // Eurasia + India + Arabia
  blob([[114,-22],[121,-18],[129,-13],[136,-12],[142,-11],[146,-19],[153,-26],[150,-36],[145,-39],[139,-37],[134,-35],[128,-32],[122,-34],[115,-34],[112,-26]],LAND,EDGE); // Australia
  blob([[-58,76],[-50,80],[-36,81],[-24,77],[-21,71],[-29,68],[-40,65],[-46,60],[-52,64],[-54,70]],'#F2F8FF','#BFD8EE'); // Greenland (icy)
  blob([[-6,50],[-4,54],[-5,58],[-2,57],[0,52]],LAND,EDGE); // UK
  blob([[139,34],[141,37],[141,41],[143,43],[141,45],[139,42],[137,36],[133,34]],LAND,EDGE); // Japan
  blob([[96,5],[101,0],[106,-5],[101,-3],[96,2]],LAND,EDGE); // Sumatra
  blob([[109,1],[116,2],[118,-2],[112,-4],[109,-2]],LAND,EDGE); // Borneo
  blob([[131,-2],[138,-3],[146,-6],[141,-8],[133,-5]],LAND,EDGE); // New Guinea
  blob([[172,-35],[176,-38],[174,-41],[171,-38]],LAND,EDGE);
  blob([[167,-41],[172,-44],[169,-46],[166,-44]],LAND,EDGE); // NZ

  // deserts
  x.globalAlpha=0.8;x.fillStyle='#F5D778';
  [[5,22,150,46],[47,22,60,30],[133,-25,80,34]].forEach(([lo,la,rw,rh])=>{x.beginPath();x.ellipse(X(lo),Y(la),rw,rh,0,0,Math.PI*2);x.fill()});
  x.globalAlpha=1;

  // Antarctica
  x.fillStyle='#F2F8FF';x.strokeStyle='#BFD8EE';x.lineWidth=10;
  x.beginPath();x.moveTo(0,Y(-78));
  for(let lon=-180;lon<=180;lon+=12){x.quadraticCurveTo(X(lon-6),Y(-76-Math.sin(lon/27)*3),X(lon),Y(-79+Math.cos(lon/19)*2))}
  x.lineTo(w,h);x.lineTo(0,h);x.closePath();x.fill();x.stroke();

  const t=new THREE.CanvasTexture(c);t.anisotropy=4;return t;
}
function makeCloudTexture(){
  const w=1024,h=512,c=document.createElement('canvas');c.width=w;c.height=h;
  const x=c.getContext('2d'),r=rng('clouds');
  x.fillStyle='rgba(255,255,255,0.95)';
  for(let i=0;i<26;i++){
    const cx=r()*w, cy=h*0.18+r()*h*0.64, s=12+r()*20, puffs=3+Math.floor(r()*3);
    for(let p=0;p<puffs;p++){x.beginPath();x.ellipse(cx+(p-puffs/2)*s*0.9,cy+(r()-0.5)*s*0.4,s*(0.7+r()*0.5),s*0.62,0,0,Math.PI*2);x.fill()}
  }
  return new THREE.CanvasTexture(c);
}

const globeGroup=new THREE.Group();scene.add(globeGroup);
const globe=new THREE.Mesh(new THREE.SphereGeometry(2,64,64),
  new THREE.MeshPhongMaterial({map:makeEarthTexture(),shininess:8}));
globeGroup.add(globe);
const clouds=new THREE.Mesh(new THREE.SphereGeometry(2.045,48,48),
  new THREE.MeshLambertMaterial({map:makeCloudTexture(),transparent:true,opacity:0.55,depthWrite:false}));
globeGroup.add(clouds);

/* atmosphere glow */
globeGroup.add(new THREE.Mesh(new THREE.SphereGeometry(2.32,48,48),new THREE.ShaderMaterial({
  vertexShader:`varying vec3 vN;void main(){vN=normalize(normalMatrix*normal);gl_Position=projectionMatrix*modelViewMatrix*vec4(position,1.0);}`,
  fragmentShader:`varying vec3 vN;void main(){float i=pow(0.62-dot(vN,vec3(0.,0.,1.)),2.0);gl_FragColor=vec4(0.42,0.72,1.0,1.0)*i;}`,
  blending:THREE.AdditiveBlending,side:THREE.BackSide,transparent:true,depthWrite:false})));

/* pins */
function latLonV3(lat,lon,r){
  const phi=(90-lat)*Math.PI/180, th=(lon+180)*Math.PI/180;
  return new THREE.Vector3(-r*Math.sin(phi)*Math.cos(th), r*Math.cos(phi), r*Math.sin(phi)*Math.sin(th));
}
const pinHits=[];
CITIES.forEach(city=>{
  const col=city.type==='mkt'?0x34D399:0xFB923C;
  const g=new THREE.Group();
  const pos=latLonV3(city.lat,city.lon,2.0);
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

/* ============ interaction ============ */
let dragging=false,moved=0,px=0,py=0,velY=0.0016,tiltX=-0.18;
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
    globeGroup.rotation.y+=dx*0.005; velY=dx*0.00012;
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
  dragging=false;if(Math.abs(velY)<0.0004)velY=0.0012*Math.sign(velY||1);
});
el.addEventListener('wheel',e=>{e.preventDefault();camera.position.z=Math.max(3.1,Math.min(9,camera.position.z+e.deltaY*0.0035))},{passive:false});
el.addEventListener('touchstart',e=>{if(e.touches.length===2){pinchD=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY)}},{passive:true});
el.addEventListener('touchmove',e=>{if(e.touches.length===2){const d=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY);
  camera.position.z=Math.max(3.1,Math.min(9,camera.position.z-(d-pinchD)*0.01));pinchD=d;dragging=false}},{passive:true});

/* ============ panel ============ */
const panel=document.getElementById('panel');
let activeCity=null;
let panelToken=0;                                  // guards against out-of-order async renders

function openPanel(city){activeCity=city;renderPanel();panel.classList.add('open');hintEl.classList.add('gone')}
function closePanel(){panel.classList.remove('open');activeCity=null;panelToken++}

function panelHead(c,d){
  return `<div class="phead"><div><h2>${esc(c.name)}</h2><div class="country">${esc(c.country)}</div></div>
    <button id="closeBtn" aria-label="Close">✕</button></div>
    <div class="pdate">${dayIndex(TODAY)===dayIndex(d)?'Today':'⏳ '+esc(fmt(d))}</div>`;
}
function bindClose(){const b=document.getElementById('closeBtn');if(b)b.onclick=closePanel;}

function stockCardHTML(ix,s){
  if(!s||s.value==null){
    return `<div class="card stock"><div class="clabel">Index</div>
      <div class="srow"><span class="sname">${esc(ix.n)}</span><span class="sval muted">—</span></div>
      <div class="schg muted">No market data for this date</div></div>`;
  }
  const prev=s.prevClose;
  const chg=(prev!=null&&prev!==0)?(s.value-prev)/prev*100:null;
  const up=chg==null?true:chg>=0;
  const cur=ix.cur||(s.currency==='USD'?'$':s.currency==='GBP'?'£':s.currency==='JPY'?'¥':s.currency==='INR'?'₹':'');
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
    const title=esc(n.h||'');
    const src=esc(n.s||'');
    const inner=n.url?`<a href="${esc(n.url)}" target="_blank" rel="noopener">${title}</a>`:title;
    return `<li>${inner}<span class="src">${src}${src?' · ':''}${esc(fmt(d))}</span></li>`;
  }).join('');
  return `<div class="card news"><div class="clabel">Headlines</div><ul>${items}</ul></div>`;
}
function footNote(){
  return `<div class="demo-note">Live data · GDELT (news) · Open-Meteo (weather) · Yahoo Finance (markets).<br>Historical depth varies by source.</div>`;
}
function loadingCard(label,lines){
  let s=`<div class="card"><div class="clabel">${label}</div>`;
  for(let i=0;i<(lines||2);i++)s+=`<div class="skl${i===0?' lg':''}"></div>`;
  return s+'</div>';
}

async function renderPanel(){
  if(!activeCity)return;
  const c=activeCity, d=viewDate, iso=isoKey(d);
  const token=++panelToken;

  // loading shell (same layout, muted skeletons)
  let shell=panelHead(c,d);
  if(c.type==='mkt')shell+=c.indices.map(()=>loadingCard('Index',2)).join('');
  shell+=loadingCard('Weather',1)+loadingCard('Headlines',3)+footNote();
  panel.innerHTML=shell;bindClose();

  // fire all requests in parallel
  const tasks=[fetchWeather(c,iso),fetchNews(c,iso)];
  if(c.type==='mkt')c.indices.forEach(ix=>tasks.push(fetchStock(ix.symbol,iso)));
  const settled=await Promise.allSettled(tasks);
  if(token!==panelToken||activeCity!==c)return;          // superseded by a newer open/date change

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
  document.getElementById('scene').style.filter=back?`saturate(${1-0.25*p}) sepia(${0.18*p})`:'none';
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
function tick(){
  requestAnimationFrame(tick);
  const t=clock.getElapsedTime();
  if(!dragging&&!reduceMotion){globeGroup.rotation.y+=velY||0.0012}
  if(!reduceMotion){clouds.rotation.y+=0.0004;
    pinHits.forEach((h,i)=>{const s=1+0.25*Math.sin(t*2.4+i);h.userData.ring.scale.setScalar(s);
      h.userData.ring.material.opacity=0.85-0.35*Math.sin(t*2.4+i)})}
  renderer.render(scene,camera);
}
tick();
addEventListener('resize',()=>{camera.aspect=W()/H();camera.updateProjectionMatrix();renderer.setSize(W(),H())});
