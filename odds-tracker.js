/**
 * ai_tracker.js — ScorePop Adaptive Tracker v3.2
 * ═══════════════════════════════════════════════════════════════════
 * v3.2 değişiklikleri (v3.1 üzerine):
 *
 *  [FIX-9] Öğrenme uyumsuzluğu (Bug #2):
 *    • learnFromMatch() artık önce pendingSignals[fid].stateKey'i kullanıyor.
 *    • Sinyal anındaki key ile öğrenme key'i garantili eşleşiyor.
 *    • Fallback: pending yoksa son snapshot._stateKey (önceki davranış).
 *
 *  [FIX-10] X/X birikimi / yeni outcome sıfır başlangıç sorunu (Bug #3):
 *    • predict() içinde Laplace smoothing eklendi: (cnt+1)/(total+N).
 *    • N = FOCUS_RESULTS.length = 9 → her outcome için +1 prior.
 *    • Eski memory silinmeden X/X avantajı hemen kırılır.
 *    • baseProb 1/N (≈0.111) ile tutarlı; predictWithSimilarity güncellendi.
 *    • Mevcut counts'a dokunulmadı — eski veriler korunuyor.
 *
 *  v3.1'den korunanlar: FIX-7 (ht_ft yoksa ×0.70), FIX-8 (1/X + 2/X)
 *  ve tüm v3.0 özellikleri aynı çalışır.
 */
'use strict';

const https      = require('https');
const fs         = require('fs');
const nodemailer = require('nodemailer');
const { createClient } = require('@supabase/supabase-js');

// ── Config ────────────────────────────────────────────────────────────
const INTERVAL_MS         = parseInt(process.env.INTERVAL_MS          || '300000');
const MAX_RUNTIME_MS      = parseInt(process.env.MAX_RUNTIME_MS       || '17100000');
const LOOKAHEAD_H         = parseInt(process.env.LOOKAHEAD_HOURS      || '8');
const CACHE_FILE          = process.env.CACHE_FILE   || 'tracker_cache.json';
const FIRED_FILE          = process.env.FIRED_FILE   || 'fired_alerts.json';
const MEMORY_FILE         = process.env.MEMORY_FILE  || 'learned_memory.json';
const DRY_RUN             = process.env.DRY_RUN === 'true';
const BOOTSTRAP_THRESHOLD = parseInt(process.env.BOOTSTRAP_THRESHOLD  || '20');
const SIGNAL_WINDOW_H     = parseFloat(process.env.SIGNAL_WINDOW_H    || '0.5');
const ACCURACY_MIN_SAMPLES= parseInt(process.env.ACCURACY_MIN_SAMPLES || '10');
const ACCURACY_PENALTY_THR= parseFloat(process.env.ACCURACY_PENALTY_THR || '0.20');
const ACCURACY_BOOST_THR  = parseFloat(process.env.ACCURACY_BOOST_THR   || '0.45');
const MIN_SIGNALS         = 2;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const sb = (SUPABASE_URL && SUPABASE_KEY) ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ── State ─────────────────────────────────────────────────────────────
const matchCache = new Map();
let firedAlerts  = {};
let memory       = {
  patterns:       {},
  signalAccuracy: {},
  pendingSignals: {},
  version:        3,
  totalLearned:   0,
};
let cycleCount = 0;
const startTime = Date.now();

// [FIX-8] 1/X ve 2/X eklendi (HT ev/dep kazanır ama FT beraberlik)
const FOCUS_RESULTS = ['1/1', '2/1', '1/X', '2/X', 'X/X', 'X/2', 'X/1', '2/2', '1/2'];

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 0 — DOĞRULUK MOTORU (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
function resolvePendingSignal(fid, actualResult) {
  const pending = memory.pendingSignals[fid];
  if (!pending) return;
  const { topSignal, tier } = pending;
  if (!memory.signalAccuracy[topSignal])
    memory.signalAccuracy[topSignal] = { fired: 0, correct: 0 };
  const acc = memory.signalAccuracy[topSignal];
  if (topSignal === actualResult) acc.correct++;
  const accuracy = acc.fired > 0 ? (acc.correct / acc.fired) : 0;
  console.log(
    `  [Accuracy] ${topSignal} (${tier}) → Tahmin: ${topSignal} | Gerçek: ${actualResult}` +
    ` | ${topSignal === actualResult ? '✅ DOĞRU' : '❌ YANLIŞ'}` +
    ` | Genel: %${(accuracy * 100).toFixed(1)} (${acc.correct}/${acc.fired})`
  );
  delete memory.pendingSignals[fid];
}

function getAccuracyMultiplier(signalType) {
  const acc = memory.signalAccuracy[signalType];
  if (!acc || acc.fired < ACCURACY_MIN_SAMPLES)
    return { multiplier: 1.0, label: 'yetersiz_örnek', accuracy: null };
  const accuracy = acc.correct / acc.fired;
  if (accuracy >= ACCURACY_BOOST_THR)
    return { multiplier: 1.4, label: `🟢 %${(accuracy*100).toFixed(0)} doğru`, accuracy };
  if (accuracy <= ACCURACY_PENALTY_THR)
    return { multiplier: 0.0, label: `🔴 %${(accuracy*100).toFixed(0)} doğru — bastırıldı`, accuracy };
  return { multiplier: 1.0, label: `🟡 %${(accuracy*100).toFixed(0)} doğru`, accuracy };
}

function logAccuracyReport() {
  const entries = Object.entries(memory.signalAccuracy)
    .filter(([, v]) => v.fired >= 3)
    .map(([type, v]) => ({ type, fired: v.fired, correct: v.correct, accuracy: v.correct / v.fired }))
    .sort((a, b) => b.accuracy - a.accuracy);
  if (entries.length === 0) { console.log('[Accuracy] Henüz yeterli sinyal verisi yok.'); return; }
  console.log('\n' + '─'.repeat(55));
  console.log('  📈 SİNYAL DOĞRULUK RAPORU');
  console.log('─'.repeat(55));
  for (const e of entries) {
    const bar   = '█'.repeat(Math.round(e.accuracy * 20));
    const empty = '░'.repeat(20 - Math.round(e.accuracy * 20));
    const rating = e.accuracy >= ACCURACY_BOOST_THR ? '✅ BOOST' :
                   e.accuracy <= ACCURACY_PENALTY_THR ? '❌ BASTIR' : '⚡ NORMAL';
    console.log(`  ${e.type.padEnd(5)} ${bar}${empty} %${(e.accuracy*100).toFixed(1).padStart(5)} (${e.correct}/${e.fired}) ${rating}`);
  }
  const tf = entries.reduce((s,e) => s+e.fired, 0);
  const tc = entries.reduce((s,e) => s+e.correct, 0);
  console.log('─'.repeat(55));
  console.log(`  TOPLAM: ${tc}/${tf} doğru (%${(tc/tf*100).toFixed(1)})`);
  console.log('─'.repeat(55) + '\n');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 1 — CACHE & MEMORY
// ════════════════════════════════════════════════════════════════════
function loadCache() {
  if (fs.existsSync(CACHE_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
      for (const [fid, val] of Object.entries(data.matchCache || {})) matchCache.set(fid, val);
      console.log(`[Cache] ${matchCache.size} maç yüklendi`);
    } catch (e) { console.warn('[Cache] Yüklenemedi:', e.message); }
  }
  if (fs.existsSync(FIRED_FILE)) {
    try { firedAlerts = JSON.parse(fs.readFileSync(FIRED_FILE, 'utf8')); } catch { firedAlerts = {}; }
  }
  if (fs.existsSync(MEMORY_FILE)) {
    try {
      const loaded = JSON.parse(fs.readFileSync(MEMORY_FILE, 'utf8'));
      if (!loaded.patterns) loaded.patterns = {};
      memory = {
        patterns:       loaded.patterns       || {},
        signalAccuracy: loaded.signalAccuracy || {},
        pendingSignals: loaded.pendingSignals || {},
        version:        3,
        totalLearned:   loaded.totalLearned   || 0,
      };
      console.log(
        `[Memory] ${Object.keys(memory.patterns).length} pattern` +
        ` | ${memory.totalLearned} öğrenme` +
        ` | ${Object.keys(memory.signalAccuracy).length} doğruluk kaydı` +
        ` | ${Object.keys(memory.pendingSignals).length} bekleyen sinyal`
      );
    } catch (e) { console.warn('[Memory] Yüklenemedi:', e.message); }
  }
}

function saveCache() {
  const obj = { savedAt: new Date().toISOString(), matchCache: {} };
  for (const [fid, val] of matchCache.entries()) obj.matchCache[fid] = val;
  fs.writeFileSync(CACHE_FILE,  JSON.stringify(obj,    null, 2));
  fs.writeFileSync(FIRED_FILE,  JSON.stringify(firedAlerts, null, 2));
  fs.writeFileSync(MEMORY_FILE, JSON.stringify(memory, null, 2));
  pushToGit();
}

function pushToGit() {
  const { execSync } = require('child_process');
  try {
    execSync('git config user.email "scorepop@bot.com"', { stdio: 'pipe' });
    execSync('git config user.name "ScorePop Bot"',      { stdio: 'pipe' });
    execSync('git add learned_memory.json tracker_cache.json fired_alerts.json', { stdio: 'pipe' });
    const staged = execSync('git diff --cached --name-only', { stdio: 'pipe' }).toString().trim();
    if (!staged) { console.log('[Git] ⏩ Değişiklik yok.'); return; }
    const msg = `chore: memory update ${new Date().toISOString().slice(0,16).replace('T',' ')}`;
    execSync(`git commit -m "${msg}"`, { stdio: 'pipe' });
    execSync('git pull --rebase --autostash origin main', { stdio: 'pipe' });
    execSync('git push origin main',           { stdio: 'pipe' });
    console.log('[Git] ✅ Push başarılı.');
  } catch (e) {
    if (e.stderr) console.warn('[Git] STDERR:', e.stderr.toString().trim());
    console.warn('[Git] Hata:', e.message);
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 2 — HTTP + NESİNE
// ════════════════════════════════════════════════════════════════════
function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'Accept':          'application/json',
        'Accept-Encoding': 'identity',
        'Referer':         'https://www.nesine.com/',
        'Origin':          'https://www.nesine.com',
        'User-Agent':      'Mozilla/5.0 (compatible; ScorePop/3.2)',
      }
    }, res => {
      let buf = '';
      res.on('data', d => buf += d);
      res.on('end', () => {
        try { resolve(JSON.parse(buf)); }
        catch (e) { reject(new Error(`JSON parse: ${e.message}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 3 — TAKIM EŞLEŞTİRME (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
const TEAM_ALIASES = {
  'not forest':'nottingham forest','cry. palace':'crystal palace',
  'r wien amt':'rapid wien','w bregenz':'schwarz weiss b','rb bragantino':'bragantino',
  'new york rb':'ny red bulls','fc midtjylland':'midtjylland',
  'pacos de ferreira':'p ferreira','seattle s':'seattle sounders',
  'st louis':'s louis city','gabala':'kabala','rz pellets wac':'wolfsberger',
  'sw bregenz':'schwarz weiss b','fc zurich':'zurih',
  'future fc':'modern sport club','the new saints':'tns','vancouver':'v whitecaps',
};
function norm(s) {
  return (s||'').toLowerCase()
    .replace(/ğ/g,'g').replace(/ü/g,'u').replace(/ş/g,'s')
    .replace(/ı/g,'i').replace(/ö/g,'o').replace(/ç/g,'c')
    .replace(/[^a-z0-9]/g,' ').replace(/\s+/g,' ').trim();
}
function normA(s) { const n=norm(s); return TEAM_ALIASES[n]||n; }
function tokenSim(a,b) {
  const ta=new Set(norm(a).split(' ').filter(x=>x.length>1));
  const tb=new Set(norm(b).split(' ').filter(x=>x.length>1));
  if(!ta.size||!tb.size) return 0;
  let hit=0;
  for(const t of ta){
    if(tb.has(t)){hit++;continue;}
    for(const u of tb){if(t.startsWith(u)||u.startsWith(t)){hit+=0.7;break;}}
  }
  return hit/Math.max(ta.size,tb.size);
}
function findBestMatch(home,away,events) {
  const TH=0.35,MIN=0.20,ONE=0.65,CROSS=0.25;
  let bN=null,bNS=TH-0.01;
  for(const ev of events){
    const hs=tokenSim(normA(home),norm(ev.HN));
    const as=tokenSim(normA(away),norm(ev.AN));
    const avg=(hs+as)/2;
    if(hs>=MIN&&as>=MIN&&avg>bNS){bNS=avg;bN=ev;}
  }
  if(bN) return {ev:bN,score:bNS};
  let bC=null,bCS=-1;
  for(const ev of events){
    const combos=[
      {s:tokenSim(normA(home),norm(ev.HN)),c:tokenSim(normA(away),norm(ev.AN))},
      {s:tokenSim(normA(away),norm(ev.HN)),c:tokenSim(normA(home),norm(ev.AN))},
    ];
    for(const {s,c} of combos){
      if(s>=ONE&&c>=CROSS){
        const conf=(s+c)/2;
        if(conf>=TH&&conf>bCS){bCS=conf;bC=ev;}
      }
    }
  }
  return bC?{ev:bC,score:bCS}:null;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 4 — MARKET PARSE (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
function parseMarkets(maArr) {
  const m={};
  if(!Array.isArray(maArr)) return m;
  for(const x of maArr){
    const id=x.MTID, oca=x.OCA||[];
    const g=n=>{const o=oca.find(x=>x.N===n);return o?+o.O:0;};
    if(id===1 &&oca.length===3) m['1x2']            ={home:g(1),draw:g(2),away:g(3)};
    if(id===7 &&oca.length===3) m['ht_1x2']         ={home:g(1),draw:g(2),away:g(3)};
    if(id===9 &&oca.length===3) m['2h_1x2']         ={home:g(1),draw:g(2),away:g(3)};
    if(id===5 &&oca.length===9) m['ht_ft']          ={
      '1/1':g(1),'1/X':g(2),'1/2':g(3),
      'X/1':g(4),'X/X':g(5),'X/2':g(6),
      '2/1':g(7),'2/X':g(8),'2/2':g(9),
    };
    if(id===12&&oca.length===2) m['ou25']           ={under:g(1),over:g(2)};
    if(id===11&&oca.length===2) m['ou15']           ={under:g(1),over:g(2)};
    if(id===13&&oca.length===2) m['ou35']           ={under:g(1),over:g(2)};
    if(id===38&&oca.length===2) m['btts']           ={yes:g(1),no:g(2)};
    if(id===48&&oca.length===3) m['more_goals_half']={first:g(1),equal:g(2),second:g(3)};
    if(id===3 &&oca.length===3) m['dc']             ={'1x':g(1),'12':g(2),'x2':g(3)};
  }
  return m;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 5 — DELTA HESABI (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
function calcDelta(prev,curr) {
  const ch={};
  const keys=['1x2','ht_1x2','2h_1x2','ht_ft','ou25','ou15','ou35','btts','more_goals_half'];
  for(const k of keys){
    ch[k]={};
    const p=prev[k]||{},c=curr[k]||{};
    for(const sub of Object.keys({...p,...c})){
      const pv=p[sub],cv=c[sub];
      ch[k][sub]=(pv&&cv&&pv!==cv)?+(cv-pv).toFixed(3):0;
    }
  }
  return ch;
}
function ftGroups(changes) {
  const s=k=>changes?.ht_ft?.[k]||0;
  return {ev_ft:s('1/1')+s('2/1')+s('X/1'),dep_ft:s('1/2')+s('2/2')+s('X/2'),bera:s('1/X')+s('2/X')+s('X/X')};
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 6 — ÖZELLİK ÇIKARIMI & DURUM KODU
// ════════════════════════════════════════════════════════════════════

// [FIX-7] null → 'none' (önce son label'dı → yanlış state key üretiyordu)
function bucket(val, thresholds, labels) {
  if (val === null || val === undefined) return 'none';
  for (let i = 0; i < thresholds.length; i++) if (val <= thresholds[i]) return labels[i];
  return labels[labels.length - 1];
}

function extractFeatures(markets, changes, cumCache, snapshots) {
  const mk = (k,s) => markets?.[k]?.[s] ?? null;
  const { ev_ft, dep_ft } = ftGroups(changes || {});

  const ms1    = mk('1x2','home');
  const ms2    = mk('1x2','away');
  const iy1    = mk('ht_1x2','home');
  const iy2    = mk('ht_1x2','away');
  const sy1    = mk('2h_1x2','home');
  const iyms21 = mk('ht_ft','2/1');
  const iyms22 = mk('ht_ft','2/2');
  const iyms11 = mk('ht_ft','1/1');
  const iyms12 = mk('ht_ft','1/2');
  const au25o  = mk('ou25','over');
  const bttsY  = mk('btts','yes');
  const dcg2h  = mk('more_goals_half','second');

  // [FIX-7] ht_ft varlığını kontrol et
  const hasHtFt = !!(markets?.ht_ft);

  const f = {
    ms1_bucket:    bucket(ms1,    [1.30,1.60,2.50], ['vlow','low','med','high']),
    ms2_bucket:    bucket(ms2,    [1.80,3.00],       ['low','med','high']),
    iy1_bucket:    bucket(iy1,    [1.70,2.50],       ['low','med','high']),
    iy2_bucket:    bucket(iy2,    [3.50,5.00],       ['low','med','high']),
    sy1_bucket:    bucket(sy1,    [1.70,2.50],       ['low','med','high']),
    iyms21_bucket: bucket(iyms21, [10,22,35],        ['vlow','low','med','high']),
    iyms22_bucket: bucket(iyms22, [3,8,15],          ['vlow','low','med','high']),
    iyms11_bucket: bucket(iyms11, [3,6,12],          ['vlow','low','med','high']),
    iyms12_bucket: bucket(iyms12, [10,20,35],        ['vlow','low','med','high']),
    au25o_bucket:  bucket(au25o,  [1.50,2.00,2.80],  ['low','med','high','vhigh']),
    btts_bucket:   bucket(bttsY,  [1.50,2.00],       ['low','med','high']),
    ev_ft_sign:    ev_ft  < -1 ? 'neg' : ev_ft  > 1 ? 'pos' : 'flat',
    dep_ft_sign:   dep_ft < -1 ? 'neg' : dep_ft > 1 ? 'pos' : 'flat',
  };

  const recent = (snapshots||[]).slice(-3);
  let evMomentum='flat', depMomentum='flat';
  if(recent.length >= 2) {
    const evVals  = recent.map(s=>{const {ev_ft:e}=ftGroups(s.changes||{});return e;});
    const depVals = recent.map(s=>{const {dep_ft:d}=ftGroups(s.changes||{});return d;});
    const evSlope  = evVals[evVals.length-1]  - evVals[0];
    const depSlope = depVals[depVals.length-1]- depVals[0];
    evMomentum  = evSlope  < -1.5 ? 'falling' : evSlope  > 1.5 ? 'rising' : 'stable';
    depMomentum = depSlope < -1.5 ? 'falling' : depSlope > 1.5 ? 'rising' : 'stable';
  }
  f.ev_momentum  = evMomentum;
  f.dep_momentum = depMomentum;
  f.div_ev_to_dep = (f.ev_ft_sign==='neg'&&f.dep_ft_sign==='pos')?'yes':'no';
  f.div_strong_ev = (f.ev_ft_sign==='neg'&&f.dep_ft_sign==='neg')?'yes':'no';

  return {
    raw: { ms1,ms2,iy1,iy2,sy1,iyms21,iyms22,iyms11,iyms12,au25o,bttsY,dcg2h,ev_ft,dep_ft },
    buckets: f,
    hasHtFt,  // [FIX-7]
  };
}

// [FIX-7] ht_ft yoksa iyms boyutları 'na' marker alır
function generateStateKey(features) {
  const b = features.buckets;
  const iyms21Key = (b.iyms21_bucket !== 'none') ? `iyms21_${b.iyms21_bucket}` : 'iyms21_na';
  const iy2Key    = (b.iy2_bucket    !== 'none') ? `iy2_${b.iy2_bucket}`       : 'iy2_na';
  const au25Key   = (b.au25o_bucket  !== 'none') ? `au25_${b.au25o_bucket}`    : 'au25_na';

  return [
    `ms1_${b.ms1_bucket}`,
    iy2Key,
    `evft_${b.ev_ft_sign}`,
    `depft_${b.dep_ft_sign}`,
    iyms21Key,
    `evmom_${b.ev_momentum}`,
    `depmon_${b.dep_momentum}`,
    `div_${b.div_ev_to_dep}`,
    `ms2_${b.ms2_bucket}`,
    au25Key,
  ].join('|');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 7 — ÖĞRENME MOTORU
// ════════════════════════════════════════════════════════════════════

// [FIX-9] Sinyal anındaki stateKey ile öğrenme key'ini eşleştir.
// Önce pendingSignals[fid].stateKey'e bak — signal ateşlendiğinde
// markFired() orada saklıyor. Eğer pending yoksa (bootstrap vs.) son
// snapshot._stateKey'e düş (önceki v3.1 davranışı, aynı maç).
function learnFromMatch(fixtureId, actualHtFt) {
  const match = matchCache.get(fixtureId);
  if (!match || !match.snapshots || match.snapshots.length === 0) return;
  if (!FOCUS_RESULTS.includes(actualHtFt)) return;

  // [FIX-9] Sinyal anındaki key öncelikli
  const pending = memory.pendingSignals[fixtureId];
  let key;
  if (pending?.stateKey) {
    key = pending.stateKey;
    console.log(`[Learn] fixture=${fixtureId} → pending stateKey kullanıldı`);
  } else {
    const lastSnap = match.snapshots[match.snapshots.length - 1];
    if (!lastSnap._stateKey) {
      console.warn(`[Learn] fixture=${fixtureId} _stateKey yok, öğrenme atlandı`);
      return;
    }
    key = lastSnap._stateKey;
  }

  if (!memory.patterns[key]) memory.patterns[key] = {};
  if (!memory.patterns[key][actualHtFt])
    memory.patterns[key][actualHtFt] = { count: 0, firstSeen: new Date().toISOString() };
  memory.patterns[key][actualHtFt].count++;
  memory.totalLearned++;
  console.log(`[Learn] "${key}" → ${actualHtFt} (sayı: ${memory.patterns[key][actualHtFt].count})`);
}

// [FIX-10] Laplace smoothing: (count+1) / (total + N)
// N = FOCUS_RESULTS.length = 9
// Her outcome için +1 prior → eski memory'de 0'lı yeni outcome'lar
// (1/X, 2/X) artık X/X'in birikmiş count'una karşı şansını korur.
// baseProb 1/N ile tutarlı; eski veriler silinmez.
function predict(stateKey) {
  const N       = FOCUS_RESULTS.length;          // 9
  const basePrb = 1 / N;                          // ≈ 0.111
  const pattern = memory.patterns[stateKey];
  const result  = {};

  // Varsayılan: veri yokken uniform prior
  for (const r of FOCUS_RESULTS)
    result[r] = { prob: +basePrb.toFixed(3), lift: 1.0, count: 0, confidence: 'none' };

  if (!pattern) return result;

  // Ham gözlem toplamı (Laplace öncesi)
  let total = 0;
  for (const r of FOCUS_RESULTS) total += (pattern[r]?.count || 0);
  if (total < 2) return result;

  for (const r of FOCUS_RESULTS) {
    const cnt  = pattern[r]?.count || 0;
    // [FIX-10] Laplace smoothing
    const prob = (cnt + 1) / (total + N);
    const lift = prob / basePrb;
    let confidence = 'low';
    if (total >= 10 && prob >= 0.30) confidence = 'high';
    else if (total >= 5  && prob >= 0.20) confidence = 'medium';
    result[r] = { prob: +prob.toFixed(3), lift: +lift.toFixed(2), count: cnt, total, confidence };
  }
  return result;
}

function predictWithSimilarity(stateKey) {
  const N         = FOCUS_RESULTS.length; // 9
  const basePrb   = 1 / N;               // [FIX-10] predict() ile tutarlı
  const direct    = predict(stateKey);
  const directTotal = direct['2/1'].total || 0;
  if (directTotal >= 5) return direct;

  const neighbors  = [];
  const targetParts = stateKey.split('|');
  for (const k of Object.keys(memory.patterns)) {
    const parts = k.split('|');
    let diff = 0;
    for (let i = 0; i < parts.length; i++) if (parts[i] !== targetParts[i]) diff++;
    if (diff === 1) {
      const total = FOCUS_RESULTS.reduce((s,r) => s+(memory.patterns[k][r]?.count||0), 0);
      if (total >= 3) neighbors.push({ key: k, total });
    }
  }
  if (neighbors.length === 0) return direct;

  const blended  = {};
  let weightSum  = directTotal;
  for (const r of FOCUS_RESULTS)
    blended[r] = { prob: direct[r].prob * directTotal, count: direct[r].count };

  for (const n of neighbors) {
    const nPred = predict(n.key);
    const w     = n.total * 0.5;
    weightSum  += w;
    for (const r of FOCUS_RESULTS) {
      blended[r].prob  += nPred[r].prob * w;
      blended[r].count += nPred[r].count;
    }
  }

  const final = {};
  for (const r of FOCUS_RESULTS) {
    const prob = weightSum > 0 ? blended[r].prob / weightSum : 0;
    final[r] = {
      prob: +prob.toFixed(3), lift: +(prob / basePrb).toFixed(2),  // [FIX-10]
      count: blended[r].count, total: Math.round(weightSum),
      confidence: (weightSum >= 8 && prob >= 0.25) ? 'medium' : 'low',
    };
  }
  return final;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 8 — AKILLI SİNYAL MOTORU (v3.1 - hasHtFt entegreli)
// ════════════════════════════════════════════════════════════════════
function evaluateSmartSignals(markets, changes, cumCache, snapshots) {
  const features  = extractFeatures(markets, changes, cumCache, snapshots);
  const stateKey  = generateStateKey(features);
  const raw       = features.raw;
  const b         = features.buckets;
  const hasHtFt   = features.hasHtFt; // [FIX-7]

  if (snapshots.length > 0) snapshots[snapshots.length - 1]._stateKey = stateKey;

  const predictions = predictWithSimilarity(stateKey);
  const signals     = [];

  const trendStrength =
    (b.ev_momentum === 'falling' && b.dep_momentum === 'rising') ? 'strong_reversal' :
    (b.ev_momentum === 'falling')                                 ? 'ev_dominant'     :
    (b.dep_momentum === 'falling')                                ? 'dep_dominant'    : 'neutral';

  const hasMemory = memory.totalLearned >= BOOTSTRAP_THRESHOLD;

  // Bootstrap sinyaller
  if (!hasMemory) {
    const evCum  = cumCache.ev_ft_cum  || 0;
    const depCum = cumCache.dep_ft_cum || 0;
    if (raw.iyms21 && raw.iyms21 <= 12 && evCum <= -4)
      signals.push({ type:'2/1', tier:'STANDART', rule:`[BOOTSTRAP] İYMS21=${raw.iyms21} ev_cum=${evCum}`, prec:5.0, lift:1.5, effectiveLift:1.5, prob:0.15, stateKey, trendStrength:'bootstrap', histCount:0, accLabel:'bootstrap' });
    if (raw.ms1 && raw.ms1 <= 1.50 && evCum <= -3)
      signals.push({ type:'1/1', tier:'STANDART', rule:`[BOOTSTRAP] MS1=${raw.ms1} ev_cum=${evCum}`, prec:5.0, lift:1.4, effectiveLift:1.4, prob:0.14, stateKey, trendStrength:'bootstrap', histCount:0, accLabel:'bootstrap' });
    if (raw.iyms22 && raw.iyms22 <= 6 && depCum <= -3)
      signals.push({ type:'2/2', tier:'STANDART', rule:`[BOOTSTRAP] İYMS22=${raw.iyms22} dep_cum=${depCum}`, prec:5.0, lift:1.4, effectiveLift:1.4, prob:0.14, stateKey, trendStrength:'bootstrap', histCount:0, accLabel:'bootstrap' });
    if (raw.iyms12 && raw.iyms12 <= 12 && depCum <= -4)
      signals.push({ type:'1/2', tier:'STANDART', rule:`[BOOTSTRAP] İYMS12=${raw.iyms12} dep_cum=${depCum}`, prec:5.0, lift:1.4, effectiveLift:1.4, prob:0.14, stateKey, trendStrength:'bootstrap', histCount:0, accLabel:'bootstrap' });
  }

  // Pattern + Accuracy tabanlı sinyaller
  for (const outcome of FOCUS_RESULTS) {
    const p = predictions[outcome];
    if (p.lift < 1.20) continue;

    const { multiplier, label: accLabel, accuracy } = getAccuracyMultiplier(outcome);
    if (multiplier === 0.0) {
      console.log(`[AccFilter] ${outcome} bastırıldı (${accLabel})`);
      continue;
    }

    let tier      = 'STANDART';
    let rule      = `State: ${stateKey.substring(0, 60)}...`;
    let precision = p.prob * 10;
    let liftVal   = p.lift;

    let effectiveLift = +(liftVal * multiplier).toFixed(2);

    if (effectiveLift >= 2.50 && p.confidence === 'high') {
      tier = 'ELITE'; precision = Math.min(9.5, p.prob * 10 + 2);
    } else if (effectiveLift >= 2.00 && (p.confidence === 'high' || p.confidence === 'medium')) {
      tier = 'PREMIER'; precision = Math.min(8.5, p.prob * 10 + 1);
    }
    if (multiplier > 1.0) {
      if (tier === 'STANDART') tier = 'PREMIER';
      else if (tier === 'PREMIER') tier = 'ELITE';
      precision = Math.min(9.8, precision + 0.5);
    }

    if (outcome === '2/1' && trendStrength === 'strong_reversal') { precision += 0.8; rule = 'Reversal + ' + rule; }
    if (outcome === '1/1' && trendStrength === 'ev_dominant')      { precision += 0.6; rule = 'Ev dom + ' + rule; }
    if (outcome === '2/2' && trendStrength === 'dep_dominant')     { precision += 0.6; rule = 'Dep dom + ' + rule; }
    if (outcome === '2/1' && raw.ev_ft <= -3 && raw.dep_ft >= 2)  precision += 0.5;
    if (outcome === '1/1' && raw.ev_ft <= -2 && raw.dep_ft >= 1)  precision += 0.4;
    if (outcome === '2/2' && raw.dep_ft <= -2)                    precision += 0.4;

    const minProb = (p.total >= 10) ? 0.18 : (p.total >= 5) ? 0.22 : 0.28;
    if (p.prob < minProb) continue;

    // [FIX-7] ht_ft yoksa lift cezası + tier düşürme + uyarı
    let htFtNote = '';
    if (!hasHtFt) {
      liftVal      = +(liftVal      * 0.70).toFixed(2);
      effectiveLift= +(effectiveLift* 0.70).toFixed(2);
      htFtNote     = ' ⚠IY/MS-YOK(oran.eksik)';
      if (tier === 'ELITE')   tier = 'PREMIER';  // bir kademe düşür
      if (tier === 'PREMIER') tier = 'STANDART';
    }

    signals.push({
      type: outcome, tier,
      rule: `${rule} | hist=${p.count}/${p.total}${htFtNote}`,
      prec: +precision.toFixed(2),
      lift: liftVal, effectiveLift,
      prob: p.prob, stateKey, trendStrength,
      histCount: p.count, accLabel, accuracy,
      hasHtFt, // [FIX-7]
    });
  }

  const tierW = { ELITE: 3, PREMIER: 2, STANDART: 1 };
  signals.sort((a,c) => (tierW[c.tier]||0)-(tierW[a.tier]||0) || c.effectiveLift-a.effectiveLift);

  return { signals, features, predictions, stateKey, hasHtFt }; // [FIX-7]
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 9 — YEREL YORUM (hasHtFt uyarısı eklendi) [FIX-7]
// ════════════════════════════════════════════════════════════════════
function generateLocalInterpretation(matchData) {
  const { signals, features, predictions, stateKey, hasHtFt } = matchData;
  if (signals.length === 0) return null;

  const top = signals[0];
  const f   = features.buckets;
  const r   = features.raw;

  let mkt = '';
  if (f.div_ev_to_dep === 'yes')      mkt = 'Piyasada ev/dep FT reversal baskısı var. ';
  else if (f.div_strong_ev === 'yes') mkt = 'Her iki yarıda ev güçleniyor. ';
  else                                mkt = 'Piyasa hareketi karışık yönlü. ';
  if (f.ev_momentum  === 'falling') mkt += 'Ev FT oranları düşüyor (para girişi). ';
  if (f.dep_momentum === 'rising')  mkt += 'Dep FT oranları yükseliyor (para çıkışı). ';

  const hist = predictions[top.type] || {};
  let note = '';
  if      ((hist.total||0) >= 10)             note = `Bu pattern ${hist.total} kez tekrarlandı, ${hist.count} kez ${top.type} geldi (%${((hist.prob||0)*100).toFixed(0)}).`;
  else if ((hist.total||0) >= 3)              note = `Sınırlı örnek (${hist.total}) ama eğilim ${top.type} yönünde.`;
  else if (top.trendStrength === 'bootstrap') note = `[Bootstrap] Kural tabanlı sinyal — hafıza: ${memory.totalLearned}/${BOOTSTRAP_THRESHOLD}.`;
  else                                        note = 'Yeni pattern, temkinli olun.';

  let accNote = '';
  if (top.accuracy !== null && top.accuracy !== undefined) {
    const acc = memory.signalAccuracy[top.type] || {};
    accNote = `\n📏 Geçmiş Doğruluk: %${(top.accuracy*100).toFixed(1)} (${acc.correct}/${acc.fired} ateşlendi) — ${top.accLabel}`;
  } else if (top.trendStrength !== 'bootstrap') {
    accNote = `\n📏 Doğruluk: Henüz yeterli veri yok (<${ACCURACY_MIN_SAMPLES} ateşleme)`;
  }

  // [FIX-7] ht_ft uyarısı
  const htFtWarning = (hasHtFt === false)
    ? '\n⚠️  IY/MS oranı yok — IY/MS tabanlı tahminler %30 cezalı, temkinli olun.'
    : '';

  return (
    `📊 DURUM: ${stateKey.substring(0, 55)}...\n` +
    `${mkt}\n` +
    `🎯 TAHMİN: ${top.type} | ${top.tier} | Lift: ${top.lift}x (efektif: ${top.effectiveLift}x) | Olas: %${((top.prob||0)*100).toFixed(1)}\n` +
    `📚 ${note}${accNote}${htFtWarning}\n` +
    `⚡ Trend: ${top.trendStrength} | İYMS21: ${r.iyms21||'N/A'} | MS1: ${r.ms1||'?'}`
  );
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 10 — SİNYAL LOGGER (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
function logSignals(matchesWithSignals, cycleNo) {
  const now       = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
  const tierColor = { ELITE: '💎', PREMIER: '🥇', STANDART: '📊' };
  console.log('\n' + '▓'.repeat(60));
  console.log(`  SİNYAL RAPORU — Döngü #${cycleNo} | ${now}`);
  console.log('▓'.repeat(60));
  for (const m of matchesWithSignals) {
    const top = m.signals[0];
    const htFtTag = m.hasHtFt === false ? ' [⚠ IY/MS YOK]' : '';
    console.log(`\n${tierColor[top.tier]||'⚪'} ${m.name}${htFtTag}`);
    console.log(`   ⏰ ${m.h2k<0?'Başladı':m.h2k<1?Math.round(m.h2k*60)+' dk sonra':m.h2k.toFixed(1)+' saat sonra'}`);
    console.log(`   📈 Ev kümülâtif: ${m.ev_ft_cum.toFixed(2)} | Dep: ${m.dep_ft_cum.toFixed(2)}`);
    for (const s of m.signals.slice(0, 3)) {
      const accStr = s.accuracy!==null&&s.accuracy!==undefined
        ? ` | Doğruluk: %${(s.accuracy*100).toFixed(0)} (${s.accLabel})`
        : ` | Doğruluk: veri bekleniyor`;
      console.log(
        `   ${tierColor[s.tier]} [${s.tier}] ${s.type}` +
        ` | Lift: ${s.lift}x → Efektif: ${s.effectiveLift}x` +
        ` | Olas: %${(s.prob*100).toFixed(1)}${accStr}`
      );
      console.log(`      ↳ ${s.rule}`);
    }
    if (m.interpretation) {
      console.log('   ─────────────────────────────────');
      for (const line of m.interpretation.split('\n')) if(line.trim()) console.log('   '+line);
    }
  }
  console.log('\n' + '▓'.repeat(60) + '\n');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 10b — ALERT HELPERS (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
function alreadyFired(fid, label) {
  return (firedAlerts[fid] || []).includes(label);
}
function markFired(fid, label, signalData) {
  if (!firedAlerts[fid]) firedAlerts[fid] = [];
  if (!firedAlerts[fid].includes(label)) firedAlerts[fid].push(label);
  if (signalData) {
    memory.pendingSignals[fid] = {
      predictedAt: new Date().toISOString(),
      stateKey:    signalData.stateKey,
      topSignal:   signalData.type,
      tier:        signalData.tier,
      lift:        signalData.lift,
      effectiveLift: signalData.effectiveLift,
      prob:        signalData.prob,
      signalLabel: label,
    };
    if (!memory.signalAccuracy[signalData.type])
      memory.signalAccuracy[signalData.type] = { fired: 0, correct: 0 };
    memory.signalAccuracy[signalData.type].fired++;
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 11 — MAÇ LİSTESİ (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
async function loadFixtures() {
  if (!sb) { console.warn('[Fixtures] Supabase bağlantısı yok'); return []; }
  try {
    const { data, error } = await sb.from('future_matches').select('*');
    if (error) { console.error('[Fixtures] Hata:', error.message); return []; }
    if (!data || data.length === 0) { console.log('[Fixtures] Maç bulunamadı.'); return []; }
    return data.map(r => {
      let parsed = {};
      try { parsed = typeof r.data === 'string' ? JSON.parse(r.data) : (r.data || {}); } catch { parsed = {}; }
      const kickoff = parsed?.fixture?.date || r.date || r.kickoff || null;
      return {
        fixture_id: String(r.fixture_id || r.id),
        home_team:  parsed?.teams?.home?.name || r.home_team || '',
        away_team:  parsed?.teams?.away?.name || r.away_team || '',
        kickoff,
      };
    }).filter(r => r.home_team && r.away_team);
  } catch (e) { console.error('[Fixtures] Hata:', e.message); return []; }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 12 — CANLI SKOR & ÖĞRENME (v3.0'dan korundu)
// ════════════════════════════════════════════════════════════════════
function calcHtFtResult(htHome, htAway, ftHome, ftAway) {
  if (htHome == null || ftHome == null) return null;
  const ht = htHome > htAway ? '1' : htHome < htAway ? '2' : 'X';
  const ft = ftHome > ftAway ? '1' : ftHome < ftAway ? '2' : 'X';
  return `${ht}/${ft}`;
}

async function syncLiveMatches() {
  if (!sb) return;
  let liveRows;
  try {
    const { data, error } = await sb
      .from('live_matches')
      .select('fixture_id, status_short, home_score, away_score')
      .in('status_short', ['1H','HT','2H','FT']);
    if (error) { console.error('[Live] Supabase hata:', error.message); return; }
    liveRows = data || [];
  } catch (e) { console.error('[Live] Hata:', e.message); return; }

  console.log(`[Live] ${liveRows.length} aktif/biten maç`);
  if (liveRows.length > 0) console.log('[Live] Örnek:', JSON.stringify(liveRows[0]));
  let matchedLive=0, learnedCount=0, resolvedCount=0;

  for (const row of liveRows) {
    const fid    = String(row.fixture_id);
    const status = row.status_short;
    const hScore = row.home_score ?? null;
    const aScore = row.away_score ?? null;
    matchedLive++;
    const match    = matchCache.get(fid);
    if (!match) continue;
    const prevLive = match.liveData || {};
    if (prevLive.status === status && status !== 'FT') continue;
    let htHome = prevLive.htHome ?? null;
    let htAway = prevLive.htAway ?? null;
    let ftHome=null, ftAway=null, htFtResult=null;

    if (status === 'HT') {
      htHome = hScore; htAway = aScore;
      console.log(`  ⏸ HT: ${match.name} | İY ${htHome}-${htAway}`);
    } else if (status === 'FT' && prevLive.status !== 'FT') {
      ftHome = hScore; ftAway = aScore;
      htFtResult = calcHtFtResult(htHome, htAway, ftHome, ftAway);
      console.log(
        `  🏁 FT: ${match.name}` +
        ` | İY: ${htHome??'?'}-${htAway??'?'}` +
        ` → MS: ${ftHome}-${ftAway}` +
        ` | HT/FT: ${htFtResult||'hesaplanamadı'}`
      );
      if (htFtResult) {
        resolvePendingSignal(fid, htFtResult);
        resolvedCount++;
        learnFromMatch(fid, htFtResult);
        learnedCount++;
      } else {
        console.warn(`  ⚠️ ${match.name}: HT skoru yok — öğrenme/çözümleme atlandı`);
      }
    }
    match.liveData = { status, htHome, htAway, ftHome, ftAway, htFtResult };
    matchCache.set(fid, match);
  }
  if (liveRows.length > 0)
    console.log(`[Live] Eşleşme: ${matchedLive}/${liveRows.length} | Öğrenilen: ${learnedCount} | Çözümlenen: ${resolvedCount}`);
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 13 — ANA DÖNGÜ (v3.0'dan korundu, hasHtFt eklendi)
// ════════════════════════════════════════════════════════════════════
function hoursToKickoff(ko) {
  if (!ko) return 999;
  try {
    const hasTimezone = /Z|[+-]\d{2}:?\d{2}$/.test(String(ko));
    const utcMs = hasTimezone
      ? new Date(ko).getTime()
      : new Date(ko).getTime() - 3 * 3600000;
    return (utcMs - Date.now()) / 3600000;
  } catch { return 999; }
}

async function runCycle() {
  cycleCount++;
  const elapsed       = Math.round((Date.now() - startTime) / 60000);
  const bootstrapMode = memory.totalLearned < BOOTSTRAP_THRESHOLD;
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`[Tracker] Döngü #${cycleCount} | ${new Date().toISOString()} | +${elapsed}dk`);
  console.log(`[Memory]  ${Object.keys(memory.patterns).length} pattern | ${memory.totalLearned} öğrenme${bootstrapMode?` | ⚡ BOOTSTRAP (${memory.totalLearned}/${BOOTSTRAP_THRESHOLD})`:''}`);
  console.log(`[Accuracy] ${Object.keys(memory.signalAccuracy).length} tip | ${Object.keys(memory.pendingSignals).length} bekleyen`);
  console.log(`[Config]  Sinyal penceresi: ≤${SIGNAL_WINDOW_H*60}dk | Lookahead: ${LOOKAHEAD_H}sa`);
  console.log('═'.repeat(60));

  if (cycleCount % 10 === 1) logAccuracyReport();

  let nesineData;
  try {
    nesineData = await fetchJSON('https://cdnbulten.nesine.com/api/bulten/getprebultenfull');
  } catch (e) { console.error('[Nesine] Hata:', e.message); return; }
  const events = (nesineData?.sg?.EA || []).filter(e => e.TYPE === 1);
  console.log(`[Nesine] ${events.length} event`);

  const fixtures = await loadFixtures();
  if (fixtures.length === 0) { console.warn('[Fixtures] Boş'); return; }

  const matchesWithSignals = [];
  let matchedCount = 0;

  for (const fix of fixtures) {
    const h2k = hoursToKickoff(fix.kickoff);
    if (h2k > LOOKAHEAD_H || h2k < -2.5) continue;
    const result = findBestMatch(fix.home_team, fix.away_team, events);
    if (!result) continue;
    matchedCount++;
    const currMarkets = parseMarkets(result.ev.MA);
    if (!Object.keys(currMarkets).length) continue;

    const fid     = fix.fixture_id;
    const prev    = matchCache.get(fid);
    const changes = prev?.latestMarkets ? calcDelta(prev.latestMarkets, currMarkets) : {};
    const { ev_ft, dep_ft } = ftGroups(changes);
    const ev_ft_cum  = (prev?.ev_ft_cum  || 0) + (ev_ft  < 0 ? ev_ft  : 0);
    const dep_ft_cum = (prev?.dep_ft_cum || 0) + (dep_ft < 0 ? dep_ft : 0);

    const snapshots = prev?.snapshots || [];
    snapshots.push({ time: new Date().toISOString(), markets: currMarkets, changes, ev_ft, dep_ft });
    if (snapshots.length > 10) snapshots.shift();

    const { signals, features, predictions, stateKey, hasHtFt } = evaluateSmartSignals(
      currMarkets, changes, { ev_ft_cum, dep_ft_cum }, snapshots
    );

    matchCache.set(fid, {
      name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, latestMarkets: currMarkets,
      ev_ft_cum, dep_ft_cum, snapshots,
      liveData: prev?.liveData || {},
    });

    if (h2k > SIGNAL_WINDOW_H) {
      const minLeft = Math.round(h2k * 60);
      console.log(`[⏳ Bekle] ${fix.home_team} vs ${fix.away_team} | ${minLeft} dk kaldı`);
      continue;
    }

    if (signals.length === 0) continue;
    const hasHighTier = signals.some(s => s.tier === 'ELITE' || s.tier === 'PREMIER');
    const isBootstrap = signals.every(s => s.trendStrength === 'bootstrap');
    if (!hasHighTier && signals.length < MIN_SIGNALS && !isBootstrap) continue;

    const topSignal = signals[0];
    const topLabel  = `${topSignal.type}_${topSignal.tier}`;
    if (alreadyFired(fid, topLabel)) continue;

    const interpretation = generateLocalInterpretation({
      signals, features, predictions, stateKey,
      hasHtFt, // [FIX-7]
    });

    matchesWithSignals.push({
      fid, name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, h2k,
      signals, features, interpretation,
      ev_ft_cum, dep_ft_cum,
      hasHtFt, // [FIX-7]
    });
  }

  console.log(`[Tracker] Eşleşen: ${matchedCount} | Yeni sinyal: ${matchesWithSignals.length}`);

  if (matchesWithSignals.length === 0) {
    await syncLiveMatches(); saveCache(); return;
  }

  logSignals(matchesWithSignals, cycleCount);

  for (const m of matchesWithSignals) {
    const top      = m.signals[0];
    const topLabel = `${top.type}_${top.tier}`;
    markFired(m.fid, topLabel, top);
  }

  await syncLiveMatches();
  saveCache();
}

// ════════════════════════════════════════════════════════════════════
// MAIN
// ════════════════════════════════════════════════════════════════════
async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║  ScorePop Adaptive v3.2 — Self-Evaluating Market Engine  ║');
  console.log(`║  Döngü: ${Math.round(INTERVAL_MS/60000)}dk | Süre: ${Math.round(MAX_RUNTIME_MS/3600000)}sa | DryRun: ${String(DRY_RUN).padEnd(6)}║`);
  console.log(`║  Bootstrap eşiği   : ${String(BOOTSTRAP_THRESHOLD).padEnd(36)}║`);
  console.log(`║  Sinyal penceresi  : ≤${String(Math.round(SIGNAL_WINDOW_H*60)+'dk').padEnd(35)}║`);
  console.log(`║  Accuracy min örnek: ${String(ACCURACY_MIN_SAMPLES).padEnd(36)}║`);
  console.log(`║  Penalty / Boost   : <%${String((ACCURACY_PENALTY_THR*100).toFixed(0)).padEnd(9)} / >%${String((ACCURACY_BOOST_THR*100).toFixed(0)).padEnd(22)}║`);
  console.log(`║  v3.1 FIX-7: ht_ft eksik → lift×0.70 + tier düşer      ║`);
  console.log(`║  v3.1 FIX-8: FOCUS_RESULTS 1/X ve 2/X eklendi (9 sonuç)║`);
  console.log(`║  v3.2 FIX-9: learnFromMatch → pending stateKey öncelikli ║`);
  console.log(`║  v3.2 FIX-10: Laplace smoothing (cnt+1)/(total+9)        ║`);
  console.log('╚══════════════════════════════════════════════════════════╝');

  loadCache();

  const deadline = startTime + MAX_RUNTIME_MS;
  while (Date.now() < deadline) {
    const cycleStart = Date.now();
    try { await runCycle(); }
    catch (e) { console.error('[Tracker] Döngü hatası:', e.message, e.stack); }
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    const wait    = Math.max(0, INTERVAL_MS - (Date.now() - cycleStart));
    const waitMin = Math.round(wait / 60000 * 10) / 10;
    console.log(`\n[Tracker] Sonraki: ${waitMin}dk | Kalan: ${Math.round(remaining/60000)}dk`);
    if (wait > 0 && remaining > wait) await new Promise(r => setTimeout(r, wait));
  }

  logAccuracyReport();  
  saveCache();

  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║  OTURUM TAMAMLANDI                                       ║');
  console.log(`║  Döngü         : ${String(cycleCount).padEnd(40)}║`);
  console.log(`║  İzlenen       : ${String(matchCache.size).padEnd(40)}║`);
  console.log(`║  Pattern       : ${String(Object.keys(memory.patterns).length).padEnd(40)}║`);
  console.log(`║  Öğrenilen     : ${String(memory.totalLearned).padEnd(40)}║`);
  console.log(`║  Sinyal Tipi   : ${String(Object.keys(memory.signalAccuracy).length).padEnd(40)}║`);
  console.log(`║  Bekleyen      : ${String(Object.keys(memory.pendingSignals).length).padEnd(40)}║`);
  console.log('╚══════════════════════════════════════════════════════════╝');
}

process.on('SIGINT', () => {
  console.log('\n[Sistem] 🛑 Kapanma — kaydediliyor...');
  logAccuracyReport(); saveCache();
  console.log('[Sistem] ✅ Kaydedildi.'); process.exit(0);
});
process.on('SIGTERM', () => { saveCache(); process.exit(0); });

main().catch(e => { console.error('[FATAL]', e); process.exit(1); });
