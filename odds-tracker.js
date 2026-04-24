/**
 * ai_tracker.js — ScorePop Adaptive Tracker v3.4
 * ═══════════════════════════════════════════════════════════════════
 * v3.4 değişiklikleri (v3.3 üzerine):
 *
 *  [FIX-A] syncLiveMatches — FT retry kilidi kaldırıldı:
 *    • prevLive.status === 'FT' ama ftHome null ise bir sonraki döngüde
 *      tekrar denenir. Önceki "tek sefer" koşulu öğrenmeyi tamamen durduruyordu.
 *    • ftHome / ftAway önceki başarılı değer korunur, null gelirse güncellenmez.
 *
 *  [FIX-B] AccReset mekanizması kaldırıldı:
 *    • Her loadCache'de düşük doğruluklu sinyalleri sıfırlayan blok silindi.
 *    • fired/correct birikimi artık kesilmiyor. Sadece uyarı loglanıyor.
 *    • recent[] dizisi yoksa tüm entry'lerde başlatılıyor.
 *
 *  [FIX-C] Eski format pendingSignals temizlendi:
 *    • 10-boyutlu eski stateKey formatındaki bekleyen sinyaller loadCache'de
 *      silinir. Bunlar çözümlenemez, birikmelerinin anlamı yok.
 *
 *  [FIX-D] learnFromMatch eski format stateKey'i atlar:
 *    • 6-boyut + iyms22d kontrolü — eski snapshot'lar yeni pattern'e karışmaz.
 *
 *  [FIX-E] markFired'da recent[] başlatma:
 *    • signalAccuracy entry'si oluşturulurken recent:[] eklendi.
 *
 *  [FIX-F] Supabase ht_home_score / ht_away_score desteği:
 *    • live_matches sorgusuna ht sütunları eklendi (mevcut değilse null gelir,
 *      prevLive.htHome fallback'i devreye girer — geriye dönük uyumlu).
 *
 *  v3.3'ten korunanlar: FIX-11..15 ve tüm v3.0 özellikleri.
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
const SIGNAL_WINDOW_H     = parseFloat(process.env.SIGNAL_WINDOW_H    || '0.75'); // 45 dk
const ACCURACY_MIN_SAMPLES= parseInt(process.env.ACCURACY_MIN_SAMPLES || '10');
const ACCURACY_PENALTY_THR= parseFloat(process.env.ACCURACY_PENALTY_THR || '0.20');
const ACCURACY_BOOST_THR  = parseFloat(process.env.ACCURACY_BOOST_THR   || '0.45');
const MIN_SIGNALS         = 1;

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
  marketStats:    {},
  version:        3,
  totalLearned:   0,
};
let cycleCount = 0;
const startTime = Date.now();

const FOCUS_RESULTS = ['1/1', '2/1', '1/X', '2/X', 'X/X', 'X/2', 'X/1', '2/2', '1/2'];

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 0 — DOĞRULUK MOTORU
// ════════════════════════════════════════════════════════════════════
const EXPLORE_RATE = 0.20; // Bastırılan sinyallerin %20'si keşif için geçer

function resolvePendingSignal(fid, actualResult) {
  const pending = memory.pendingSignals[fid];
  if (!pending) return;
  const { topSignal, tier } = pending;
  if (!memory.signalAccuracy[topSignal])
    memory.signalAccuracy[topSignal] = { fired: 0, correct: 0, recent: [] };
  const acc = memory.signalAccuracy[topSignal];
  const isCorrect = topSignal === actualResult;
  if (isCorrect) acc.correct++;

  // Son 20 sonucu tut — eski verilerden daha hızlı adapte olur
  if (!acc.recent) acc.recent = [];
  acc.recent.push(isCorrect);
  if (acc.recent.length > 20) acc.recent.shift();

  const accuracy  = acc.fired > 0 ? (acc.correct / acc.fired) : 0;
  const recentAcc = acc.recent.length > 0
    ? acc.recent.filter(Boolean).length / acc.recent.length : null;
  console.log(
    `  [Accuracy] ${topSignal} (${tier}) → ${isCorrect ? '✅ DOĞRU' : '❌ YANLIŞ'}` +
    ` | Genel: %${(accuracy*100).toFixed(1)} (${acc.correct}/${acc.fired})` +
    ` | Son20: %${recentAcc !== null ? (recentAcc*100).toFixed(1) : 'N/A'}`
  );
  delete memory.pendingSignals[fid];
}

function getAccuracyMultiplier(signalType) {
  const acc = memory.signalAccuracy[signalType];
  if (!acc || acc.fired < ACCURACY_MIN_SAMPLES)
    return { multiplier: 1.0, label: 'yetersiz_örnek', accuracy: null };

  // Son 20 varsa onu kullan — piyasa koşulları değişebilir
  const recent   = acc.recent || [];
  const accuracy = recent.length >= 5
    ? recent.filter(Boolean).length / recent.length
    : acc.correct / acc.fired;

  if (accuracy >= ACCURACY_BOOST_THR)
    return { multiplier: 1.4, label: `🟢 %${(accuracy*100).toFixed(0)} doğru`, accuracy };

  if (accuracy <= ACCURACY_PENALTY_THR) {
    // Tamamen kapatma yerine %20 ihtimalle keşif yap
    if (Math.random() < EXPLORE_RATE) {
      console.log(`[AccFilter] ${signalType} KEŞİF modu (%${(accuracy*100).toFixed(0)} doğru)`);
      return { multiplier: 0.6, label: '🔵 keşif', accuracy, isExplore: true };
    }
    return { multiplier: 0.0, label: `🔴 %${(accuracy*100).toFixed(0)} doğru — bastırıldı`, accuracy };
  }

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
        marketStats:    loaded.marketStats    || {},
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

  // [FIX-B] AccReset KALDIRILDI — sıfırlama öğrenmeyi öldürüyordu.
  // getAccuracyMultiplier zaten düşük doğruluklu sinyalleri bastırıyor;
  // fired/correct birikmesi KESILMEMALI. Sadece uyarı ver.
  for (const [type, acc] of Object.entries(memory.signalAccuracy)) {
    if (!acc.recent) acc.recent = [];
    if (acc.fired >= ACCURACY_MIN_SAMPLES) {
      const accuracy = acc.correct / acc.fired;
      if (accuracy <= ACCURACY_PENALTY_THR) {
        console.log(`[AccWarn] ${type} doğruluk düşük: %${(accuracy*100).toFixed(0)} (${acc.correct}/${acc.fired}) — bastırılacak ama sıfırlanmayacak`);
      }
    }
  }

  // [FIX-C] Eski format pendingSignals temizle (10-boyutlu eski stateKey formatı)
  let stalePending = 0;
  for (const [fid, p] of Object.entries(memory.pendingSignals)) {
    const sk = p.stateKey || '';
    const parts = sk.split('|');
    if (parts.length !== 6 || !sk.includes('iyms22d')) {
      delete memory.pendingSignals[fid];
      stalePending++;
    }
  }
  if (stalePending > 0)
    console.log(`[Fix-C] ${stalePending} eski format pendingSignal temizlendi.`);
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
    // Takılı rebase/merge varsa temizle
    if (fs.existsSync('.git/rebase-merge') || fs.existsSync('.git/rebase-apply')) {
      console.warn('[Git] ⚠️ Takılı rebase tespit edildi, abort yapılıyor...');
      try { execSync('git rebase --abort', { stdio: 'pipe' }); } catch {}
    }
    if (fs.existsSync('.git/MERGE_HEAD')) {
      try { execSync('git merge --abort', { stdio: 'pipe' }); } catch {}
    }

    execSync('git config user.email "scorepop@bot.com"', { stdio: 'pipe' });
    execSync('git config user.name "ScorePop Bot"',      { stdio: 'pipe' });
    execSync('git add learned_memory.json tracker_cache.json fired_alerts.json', { stdio: 'pipe' });
    const staged = execSync('git diff --cached --name-only', { stdio: 'pipe' }).toString().trim();
    if (!staged) { console.log('[Git] ⏩ Değişiklik yok.'); return; }
    const msg = `chore: memory update ${new Date().toISOString().slice(0,16).replace('T',' ')}`;
    execSync(`git commit -m "${msg}"`, { stdio: 'pipe' });
    execSync('git pull --rebase --autostash origin main', { stdio: 'pipe' });
    execSync('git push origin main', { stdio: 'pipe' });
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
        'User-Agent':      'Mozilla/5.0 (compatible; ScorePop/3.3)',
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
// BÖLÜM 3 — TAKIM EŞLEŞTİRME
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
// BÖLÜM 4 — MARKET PARSE
// [FIX-15] Yeni marketler eklendi: ou45, ht_ou15, ht_ou05, dc_2h
// ⚠️  MTID numaraları Nesine API'ye göre değişebilir.
//     console.log('[Markets] IDs:', maArr.map(x=>x.MTID)) ile doğrula.
// ════════════════════════════════════════════════════════════════════
function parseMarkets(maArr) {
  const m={};
  if(!Array.isArray(maArr)) return m;
  for(const x of maArr){
    const id=x.MTID, oca=x.OCA||[];
    const g=n=>{const o=oca.find(x=>x.N===n);return o?+o.O:0;};
    if(id===1  &&oca.length===3) m['1x2']            ={home:g(1),draw:g(2),away:g(3)};
    if(id===7  &&oca.length===3) m['ht_1x2']         ={home:g(1),draw:g(2),away:g(3)};
    if(id===9  &&oca.length===3) m['2h_1x2']         ={home:g(1),draw:g(2),away:g(3)};
    if(id===5  &&oca.length===9) m['ht_ft']          ={
      '1/1':g(1),'1/X':g(2),'1/2':g(3),
      'X/1':g(4),'X/X':g(5),'X/2':g(6),
      '2/1':g(7),'2/X':g(8),'2/2':g(9),
    };
    if(id===12 &&oca.length===2) m['ou25']           ={under:g(1),over:g(2)};
    if(id===11 &&oca.length===2) m['ou15']           ={under:g(1),over:g(2)};
    if(id===13 &&oca.length===2) m['ou35']           ={under:g(1),over:g(2)};
    if(id===38 &&oca.length===2) m['btts']           ={yes:g(1),no:g(2)};
    if(id===48 &&oca.length===3) m['more_goals_half']={first:g(1),equal:g(2),second:g(3)};
    if(id===3  &&oca.length===3) m['dc']             ={'1x':g(1),'12':g(2),'x2':g(3)};
    // [FIX-15] Yeni marketler — MTID değerlerini Nesine API'den doğrula:
    if(id===2  &&oca.length===2) m['ou45']           ={under:g(1),over:g(2)};
    if(id===14 &&oca.length===2) m['ht_ou15']        ={under:g(1),over:g(2)};
    if(id===30 &&oca.length===2) m['ht_ou05']        ={under:g(1),over:g(2)};
    if(id===10 &&oca.length===3) m['dc_2h']          ={'1x':g(1),'12':g(2),'x2':g(3)};
  }
  return m;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 5 — DELTA HESABI
// ════════════════════════════════════════════════════════════════════
function calcDelta(prev,curr) {
  const ch={};
  const keys=['1x2','ht_1x2','2h_1x2','ht_ft','ou25','ou15','ou35','ou45','btts','more_goals_half','ht_ou15','ht_ou05','dc','dc_2h'];
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

function bucket(val, thresholds, labels) {
  if (val === null || val === undefined) return 'none';
  for (let i = 0; i < thresholds.length; i++) if (val <= thresholds[i]) return labels[i];
  return labels[labels.length - 1];
}

// ────────────────────────────────────────────────────────────────────
// Dinamik Quantile Sistemi
// ────────────────────────────────────────────────────────────────────

function recordMarketValue(marketKey, value) {
  if (value == null) return;
  if (!memory.marketStats[marketKey])
    memory.marketStats[marketKey] = { values: [], q10: null, q25: null, q50: null, q75: null, q90: null };
  const stat = memory.marketStats[marketKey];
  stat.values.push(value);
  if (stat.values.length % 50 === 0) {
    const sorted = [...stat.values].sort((a, b) => a - b);
    const n = sorted.length;
    stat.q10 = sorted[Math.floor(n * 0.10)];
    stat.q25 = sorted[Math.floor(n * 0.25)];
    stat.q50 = sorted[Math.floor(n * 0.50)];
    stat.q75 = sorted[Math.floor(n * 0.75)];
    stat.q90 = sorted[Math.floor(n * 0.90)];
    console.log(
      `[Stats] ${marketKey} (${n} gözlem):` +
      ` q10=${stat.q10} q25=${stat.q25} q50=${stat.q50}` +
      ` q75=${stat.q75} q90=${stat.q90}`
    );
  }
}

function dynamicBucket(value, marketKey, fallbackThresholds, fallbackLabels) {
  if (value == null) return 'none';
  const stat = memory.marketStats?.[marketKey];
  // 100 gözlem dolana kadar insan tanımlı sabit threshold kullan
  if (!stat?.q25 || stat.values.length < 100)
    return bucket(value, fallbackThresholds, fallbackLabels);
  // Yeterli veri varsa veriden öğrenilmiş quantile sınırlarını kullan
  if (value <= stat.q10) return 'vlow';
  if (value <= stat.q25) return 'low';
  if (value <= stat.q75) return 'med';
  if (value <= stat.q90) return 'high';
  return 'vhigh';
}

function analyzeFeatureImportance() {
  const importance = {};
  for (const [stateKey, outcomes] of Object.entries(memory.patterns)) {
    const parts  = stateKey.split('|');
    const total  = Object.values(outcomes).reduce((s, v) => s + (v.count || 0), 0);
    if (total < 3) continue;
    const maxCnt = Math.max(...Object.values(outcomes).map(v => v.count || 0));
    const lift   = total > 0 ? (maxCnt / total) / (1 / FOCUS_RESULTS.length) : 1;
    for (const part of parts) {
      if (!importance[part]) importance[part] = { totalLift: 0, count: 0 };
      importance[part].totalLift += lift;
      importance[part].count++;
    }
  }
  const ranked = Object.entries(importance)
    .map(([k, v]) => ({ feature: k, avgLift: +(v.totalLift / v.count).toFixed(2) }))
    .sort((a, b) => b.avgLift - a.avgLift)
    .slice(0, 20);
  console.log('\n' + '─'.repeat(55));
  console.log('  🔬 FEATURE IMPORTANCE (öğrenilmiş)');
  console.log('─'.repeat(55));
  for (const r of ranked)
    console.log(`  ${r.feature.padEnd(35)} lift: ${r.avgLift}x`);
  console.log('─'.repeat(55) + '\n');
  return ranked;
}

// ────────────────────────────────────────────────────────────────────
// [FIX-12] calcFromOpen — TAM VERSİYON
// Tüm marketlerin açılıştan % değişimini hesaplar.
// Negatif değer = oran düştü = o sonuca para girdi.
// [FIX-13] extractFeatures'dan ÖNCE tanımlandı (hoisting bağımlılığı yok).
// ────────────────────────────────────────────────────────────────────
function calcFromOpen(openingMarkets, currMarkets) {
  const result = {};
  const o = openingMarkets || {};
  const c = currMarkets    || {};

  // Yardımcı: % değişim, null-safe
  const pct = (open, curr) =>
    (open && curr && open !== 0) ? +((curr - open) / open).toFixed(3) : null;

  // ── 1X2 ──────────────────────────────────────────────────────────
  result.ms1_drop  = pct(o['1x2']?.home, c['1x2']?.home);
  result.ms2_drop  = pct(o['1x2']?.away, c['1x2']?.away);
  result.msx_drop  = pct(o['1x2']?.draw, c['1x2']?.draw);

  // ── HT 1X2 (İY) ──────────────────────────────────────────────────
  result.iy1_drop  = pct(o['ht_1x2']?.home, c['ht_1x2']?.home);
  result.iy2_drop  = pct(o['ht_1x2']?.away, c['ht_1x2']?.away);
  result.iyx_drop  = pct(o['ht_1x2']?.draw, c['ht_1x2']?.draw);

  // ── 2H 1X2 (2Y) ──────────────────────────────────────────────────
  result.sy1_drop  = pct(o['2h_1x2']?.home, c['2h_1x2']?.home);
  result.sy2_drop  = pct(o['2h_1x2']?.away, c['2h_1x2']?.away);
  result.syx_drop  = pct(o['2h_1x2']?.draw, c['2h_1x2']?.draw);

  // ── IY/MS (HT/FT) 9 sonuç ────────────────────────────────────────
  result.iyms11_drop = pct(o['ht_ft']?.['1/1'], c['ht_ft']?.['1/1']);
  result.iyms22_drop = pct(o['ht_ft']?.['2/2'], c['ht_ft']?.['2/2']);
  result.iyms21_drop = pct(o['ht_ft']?.['2/1'], c['ht_ft']?.['2/1']);
  result.iyms12_drop = pct(o['ht_ft']?.['1/2'], c['ht_ft']?.['1/2']);
  result.iymsxx_drop = pct(o['ht_ft']?.['X/X'], c['ht_ft']?.['X/X']);
  result.iymsx1_drop = pct(o['ht_ft']?.['X/1'], c['ht_ft']?.['X/1']);
  result.iymsx2_drop = pct(o['ht_ft']?.['X/2'], c['ht_ft']?.['X/2']);
  result.iyms1x_drop = pct(o['ht_ft']?.['1/X'], c['ht_ft']?.['1/X']);
  result.iyms2x_drop = pct(o['ht_ft']?.['2/X'], c['ht_ft']?.['2/X']);

  // ── Alt/Üst FT ───────────────────────────────────────────────────
  result.ou15o_drop  = pct(o['ou15']?.over,  c['ou15']?.over);
  result.ou15u_drop  = pct(o['ou15']?.under, c['ou15']?.under);
  result.ou25o_drop  = pct(o['ou25']?.over,  c['ou25']?.over);
  result.ou25u_drop  = pct(o['ou25']?.under, c['ou25']?.under);
  result.ou35o_drop  = pct(o['ou35']?.over,  c['ou35']?.over);
  result.ou35u_drop  = pct(o['ou35']?.under, c['ou35']?.under);
  result.ou45o_drop  = pct(o['ou45']?.over,  c['ou45']?.over);
  result.ou45u_drop  = pct(o['ou45']?.under, c['ou45']?.under);

  // ── Alt/Üst IY ───────────────────────────────────────────────────
  result.htou15o_drop = pct(o['ht_ou15']?.over,  c['ht_ou15']?.over);
  result.htou15u_drop = pct(o['ht_ou15']?.under, c['ht_ou15']?.under);
  result.htou05o_drop = pct(o['ht_ou05']?.over,  c['ht_ou05']?.over);
  result.htou05u_drop = pct(o['ht_ou05']?.under, c['ht_ou05']?.under);

  // ── KGV (BTTS) ───────────────────────────────────────────────────
  result.bttsy_drop  = pct(o['btts']?.yes, c['btts']?.yes);
  result.bttsn_drop  = pct(o['btts']?.no,  c['btts']?.no);

  // ── Çifte Şans FT ────────────────────────────────────────────────
  result.dc1x_drop   = pct(o['dc']?.['1x'], c['dc']?.['1x']);
  result.dc12_drop   = pct(o['dc']?.['12'], c['dc']?.['12']);
  result.dcx2_drop   = pct(o['dc']?.['x2'], c['dc']?.['x2']);

  // ── Çifte Şans 2Y ────────────────────────────────────────────────
  result.dc2h1x_drop = pct(o['dc_2h']?.['1x'], c['dc_2h']?.['1x']);
  result.dc2h12_drop = pct(o['dc_2h']?.['12'], c['dc_2h']?.['12']);
  result.dc2hx2_drop = pct(o['dc_2h']?.['x2'], c['dc_2h']?.['x2']);

  // ── Yarı Daha Gollü ──────────────────────────────────────────────
  result.mgh1_drop   = pct(o['more_goals_half']?.first,  c['more_goals_half']?.first);
  result.mgh2_drop   = pct(o['more_goals_half']?.second, c['more_goals_half']?.second);

  return result;
}

// ────────────────────────────────────────────────────────────────────
// extractFeatures
// [FIX-11] recordMarketValue çağrıları const f={} DIŞINDA ve ÖNCE
// [FIX-14] f objesine tüm drop bucket'ları eklendi
// ────────────────────────────────────────────────────────────────────
function extractFeatures(markets, changes, cumCache, snapshots, openingMarkets) {
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

  const hasHtFt = !!(markets?.ht_ft);

  // [FIX-11] Gözlemleri kaydet — const f={} DIŞINDA ve ÖNCE
  recordMarketValue('ms1',    ms1);
  recordMarketValue('ms2',    ms2);
  recordMarketValue('iy1',    iy1);
  recordMarketValue('iy2',    iy2);
  recordMarketValue('sy1',    sy1);
  recordMarketValue('iyms21', iyms21);
  recordMarketValue('iyms22', iyms22);
  recordMarketValue('iyms11', iyms11);
  recordMarketValue('iyms12', iyms12);
  recordMarketValue('au25o',  au25o);
  recordMarketValue('bttsY',  bttsY);

  // Açılıştan değişimler
  const drops = calcFromOpen(openingMarkets, markets);

  // Threshold grupları
  const mainThr = [-0.20, -0.10, -0.05];
  const mainLbl = ['heavy','mod','light','flat'];
  const htftThr = [-0.50, -0.30, -0.10];
  const htftLbl = ['heavy','mod','light','flat'];
  const ouThr   = [-0.25, -0.12, -0.05];
  const ouLbl   = ['heavy','mod','light','flat'];

  // [FIX-11] Obje literal — sadece property tanımları, statement YOK
  const f = {
    // ── Anlık oran bucket'ları (dynamicBucket: 100 gözlem sonrası otomatik geçiş) ──
    ms1_bucket:    dynamicBucket(ms1,    'ms1',    [1.30,1.60,2.50], ['vlow','low','med','high']),
    ms2_bucket:    dynamicBucket(ms2,    'ms2',    [1.80,3.00],       ['low','med','high']),
    iy1_bucket:    dynamicBucket(iy1,    'iy1',    [1.70,2.50],       ['low','med','high']),
    iy2_bucket:    dynamicBucket(iy2,    'iy2',    [3.50,5.00],       ['low','med','high']),
    sy1_bucket:    dynamicBucket(sy1,    'sy1',    [1.70,2.50],       ['low','med','high']),
    iyms21_bucket: dynamicBucket(iyms21, 'iyms21', [10,22,35],        ['vlow','low','med','high']),
    iyms22_bucket: dynamicBucket(iyms22, 'iyms22', [3,8,15],          ['vlow','low','med','high']),
    iyms11_bucket: dynamicBucket(iyms11, 'iyms11', [3,6,12],          ['vlow','low','med','high']),
    iyms12_bucket: dynamicBucket(iyms12, 'iyms12', [10,20,35],        ['vlow','low','med','high']),
    au25o_bucket:  dynamicBucket(au25o,  'au25o',  [1.50,2.00,2.80],  ['low','med','high','vhigh']),
    btts_bucket:   dynamicBucket(bttsY,  'bttsY',  [1.50,2.00],       ['low','med','high']),

    // ── Delta sinyalleri ──────────────────────────────────────────
    ev_ft_sign:    ev_ft  < -1 ? 'neg' : ev_ft  > 1 ? 'pos' : 'flat',
    dep_ft_sign:   dep_ft < -1 ? 'neg' : dep_ft > 1 ? 'pos' : 'flat',

    // [FIX-14] Açılıştan % değişim bucket'ları — TAM VERSİYON
    // 1X2
    ms1_drop:      bucket(drops.ms1_drop,     mainThr, mainLbl),
    ms2_drop:      bucket(drops.ms2_drop,     mainThr, mainLbl),
    msx_drop:      bucket(drops.msx_drop,     mainThr, mainLbl),
    // IY 1X2
    iy1_drop:      bucket(drops.iy1_drop,     mainThr, mainLbl),
    iy2_drop:      bucket(drops.iy2_drop,     mainThr, mainLbl),
    iyx_drop:      bucket(drops.iyx_drop,     mainThr, mainLbl),
    // 2Y 1X2
    sy1_drop:      bucket(drops.sy1_drop,     mainThr, mainLbl),
    sy2_drop:      bucket(drops.sy2_drop,     mainThr, mainLbl),
    // IY/MS 9 sonuç
    iyms11_drop:   bucket(drops.iyms11_drop,  htftThr, htftLbl),
    iyms22_drop:   bucket(drops.iyms22_drop,  htftThr, htftLbl),
    iyms21_drop:   bucket(drops.iyms21_drop,  htftThr, htftLbl),
    iyms12_drop:   bucket(drops.iyms12_drop,  htftThr, htftLbl),
    iymsxx_drop:   bucket(drops.iymsxx_drop,  htftThr, htftLbl),
    iymsx1_drop:   bucket(drops.iymsx1_drop,  htftThr, htftLbl),
    iymsx2_drop:   bucket(drops.iymsx2_drop,  htftThr, htftLbl),
    iyms1x_drop:   bucket(drops.iyms1x_drop,  htftThr, htftLbl),
    iyms2x_drop:   bucket(drops.iyms2x_drop,  htftThr, htftLbl),
    // Alt/Üst FT
    ou15o_drop:    bucket(drops.ou15o_drop,   ouThr, ouLbl),
    ou25o_drop:    bucket(drops.ou25o_drop,   ouThr, ouLbl),
    ou25u_drop:    bucket(drops.ou25u_drop,   ouThr, ouLbl),
    ou35o_drop:    bucket(drops.ou35o_drop,   ouThr, ouLbl),
    ou45o_drop:    bucket(drops.ou45o_drop,   ouThr, ouLbl),
    // Alt/Üst IY
    htou15o_drop:  bucket(drops.htou15o_drop, ouThr, ouLbl),
    htou05o_drop:  bucket(drops.htou05o_drop, ouThr, ouLbl),
    // KGV
    bttsy_drop:    bucket(drops.bttsy_drop,   ouThr, ouLbl),
    bttsn_drop:    bucket(drops.bttsn_drop,   ouThr, ouLbl),
    // Çifte Şans
    dc1x_drop:     bucket(drops.dc1x_drop,    mainThr, mainLbl),
    dc12_drop:     bucket(drops.dc12_drop,    mainThr, mainLbl),
    dcx2_drop:     bucket(drops.dcx2_drop,    mainThr, mainLbl),
    // Yarı Daha Gollü
    mgh2_drop:     bucket(drops.mgh2_drop,    ouThr, ouLbl),
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
    hasHtFt,
  };
}

function generateStateKey(features) {
  const b = features.buckets;

  // Feature importance sıralamasına göre en ayırt edici 6 boyut seçildi.
  // Çıkarılanlar: evft_sign, depft_sign, evmom, depmon, div_ev_to_dep
  //   → hepsi 4.44x, birbirini tekrar eden "hareketsiz piyasa" etiketleri
  // Çıkarılanlar: ms1d, ms2d, ou25od
  //   → büyük çoğunluğu 'none'/'flat', ayırt edici değil
  // Kalan: 6 boyut → 4^6 ≈ 4096 kombinasyon → 351 öğrenme ile ~12 maç/pattern

  const iyms21Key = (b.iyms21_bucket !== 'none') ? `iyms21_${b.iyms21_bucket}` : 'iyms21_na';
  const iy2Key    = (b.iy2_bucket    !== 'none') ? `iy2_${b.iy2_bucket}`       : 'iy2_na';
  const au25Key   = (b.au25o_bucket  !== 'none') ? `au25_${b.au25o_bucket}`    : 'au25_na';

  return [
    `ms1_${b.ms1_bucket}`,      // Ev MS gücü         (lift: 4.61x)
    iy2Key,                      // Dep IY gücü        (lift: 4.8x)
    iyms21Key,                   // Dep IY/MS favori   (lift: 6x) ← EN KRİTİK
    `ms2_${b.ms2_bucket}`,      // Dep MS gücü        (lift: 4.61x)
    au25Key,                     // Gol beklentisi     (lift: 6x)
    `iyms22d_${b.iyms22_drop}`, // 2/2 açılış hareketi (lift: 6x)
  ].join('|');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 7 — ÖĞRENME MOTORU
// ════════════════════════════════════════════════════════════════════
function learnFromMatch(fixtureId, actualHtFt) {
  const match = matchCache.get(fixtureId);
  if (!match || !match.snapshots || match.snapshots.length === 0) return;
  if (!FOCUS_RESULTS.includes(actualHtFt)) return;

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

  // [FIX-D] Eski format stateKey'leri atla — yeni kod bunları bulamaz
  const keyParts = key.split('|');
  if (keyParts.length !== 6 || !key.includes('iyms22d')) {
    console.warn(`[Learn] fixture=${fixtureId} eski format stateKey → atlandı (${key.substring(0,50)}...)`);
    return;
  }

  if (!memory.patterns[key]) memory.patterns[key] = {};
  if (!memory.patterns[key][actualHtFt])
    memory.patterns[key][actualHtFt] = { count: 0, firstSeen: new Date().toISOString() };
  memory.patterns[key][actualHtFt].count++;
  memory.totalLearned++;
  console.log(`[Learn] "${key}" → ${actualHtFt} (sayı: ${memory.patterns[key][actualHtFt].count})`);
}

function predict(stateKey) {
  const N       = FOCUS_RESULTS.length;
  const basePrb = 1 / N;
  const pattern = memory.patterns[stateKey];
  const result  = {};

  for (const r of FOCUS_RESULTS)
    result[r] = { prob: +basePrb.toFixed(3), lift: 1.0, count: 0, confidence: 'none' };

  if (!pattern) return result;

  let total = 0;
  for (const r of FOCUS_RESULTS) total += (pattern[r]?.count || 0);
  if (total < 2) return result;

  for (const r of FOCUS_RESULTS) {
    const cnt  = pattern[r]?.count || 0;
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
  const N         = FOCUS_RESULTS.length;
  const basePrb   = 1 / N;
  const direct    = predict(stateKey);
  // [FIX-16] Sadece 2/1'e bakmak yerine tüm outcome'ların max total'ını al.
  // Eğer pattern'de 2/1 hiç görülmediyse total=0 ama 1/1 için 8 örnek
  // olsa bile komşuya düşüyordu — artık en dolu outcome'a bakılıyor.
  const directTotal = Math.max(...FOCUS_RESULTS.map(r => direct[r].total || 0));
  if (directTotal >= 5) return direct;

  const neighbors   = [];
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
      prob: +prob.toFixed(3), lift: +(prob / basePrb).toFixed(2),
      count: blended[r].count, total: Math.round(weightSum),
      confidence: (weightSum >= 8 && prob >= 0.25) ? 'medium' : 'low',
    };
  }
  return final;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 8 — AKILLI SİNYAL MOTORU
// ════════════════════════════════════════════════════════════════════
function evaluateSmartSignals(markets, changes, cumCache, snapshots, openingMarkets) {
  const features  = extractFeatures(markets, changes, cumCache, snapshots, openingMarkets);
  const stateKey  = generateStateKey(features);
  const raw       = features.raw;
  const b         = features.buckets;
  const hasHtFt   = features.hasHtFt;

  if (snapshots.length > 0) snapshots[snapshots.length - 1]._stateKey = stateKey;

  const predictions = predictWithSimilarity(stateKey);
  const signals     = [];

  const trendStrength =
    (b.ev_momentum === 'falling' && b.dep_momentum === 'rising') ? 'strong_reversal' :
    (b.ev_momentum === 'falling')                                 ? 'ev_dominant'     :
    (b.dep_momentum === 'falling')                                ? 'dep_dominant'    : 'neutral';

  const hasMemory = memory.totalLearned >= BOOTSTRAP_THRESHOLD;

  // ── BOOTSTRAP KATMANI ─────────────────────────────────────────────
  // Kural tabanlı sinyaller — hem hafıza dolmadan önce hem sonra çalışır.
  // Pattern motoru aynı outcome için zaten sinyal ürettiyse bootstrap eklenmez.
  // Eşikler 831 gerçek Nesine maçından ölçülen dağılıma göre kalibre edildi:
  //   IYMS21 min=15.6 q10=20.5 q25=22.6 q50=26.0 q75=30.5  → eski <=18 hiç tetiklenmiyordu
  //   IYMS22 min=1.2  q10=2.9  q25=3.5  q50=4.7  q75=6.8   → eski <=8 nadiren tetikleniyordu
  //   ev/depCum <=−2 sadece %7 maçta oluşuyor               → −0.5 makul hareket eşiği
  {
    const evCum  = cumCache.ev_ft_cum  || 0;
    const depCum = cumCache.dep_ft_cum || 0;

    const bsPush = (type, rule, prec, lift, prob) => {
      // Aynı outcome pattern motorundan geldiyse bootstrap ekleme
      if (signals.some(s => s.type === type && s.trendStrength !== 'bootstrap')) return;
      signals.push({
        type, tier: 'STANDART', rule: `[BOOTSTRAP] ${rule}`,
        prec, lift, effectiveLift: lift, prob,
        stateKey, trendStrength: 'bootstrap',
        histCount: 0, accLabel: 'bootstrap', accuracy: null, hasHtFt,
      });
    };

    // 2/1 → dep IY+FT baskın: IYMS21 düşük oran (<=30, q50 altı) + dep para girişi
    if (raw.iyms21 && raw.iyms21 <= 30 && depCum <= -0.5)
      bsPush('2/1', `İYMS21=${raw.iyms21?.toFixed(1)} dep_cum=${depCum.toFixed(2)}`, 5.0, 1.5, 0.15);

    // X/1 → IY dep, FT ev dönüşü: IYMS21 düşük + ev para girişi
    if (raw.iyms21 && raw.iyms21 <= 30 && evCum <= -0.5)
      bsPush('X/1', `İYMS21=${raw.iyms21?.toFixed(1)} ev_cum=${evCum.toFixed(2)} (IY dep→FT ev)`, 5.0, 1.4, 0.14);

    // 1/1 → ev her iki yarı baskın: MS1 düşük oran + ev para girişi
    if (raw.ms1 && raw.ms1 <= 1.60 && evCum <= -0.5)
      bsPush('1/1', `MS1=${raw.ms1?.toFixed(2)} ev_cum=${evCum.toFixed(2)}`, 5.0, 1.4, 0.14);

    // 2/2 → dep her iki yarı baskın: IYMS22 düşük oran (<=10, q90 altı) + dep para girişi
    if (raw.iyms22 && raw.iyms22 <= 10 && depCum <= -0.5)
      bsPush('2/2', `İYMS22=${raw.iyms22?.toFixed(1)} dep_cum=${depCum.toFixed(2)}`, 5.0, 1.4, 0.14);

    // 1/2 → ev IY, dep FT: IYMS12 düşük oran + dep para girişi
    if (raw.iyms12 && raw.iyms12 <= 30 && depCum <= -0.5)
      bsPush('1/2', `İYMS12=${raw.iyms12?.toFixed(1)} dep_cum=${depCum.toFixed(2)}`, 5.0, 1.4, 0.14);

    // X/X → her iki yarı berabere: oran hareketi yok + MS berabere düşük
    if (raw.ms1 && raw.ms1 >= 2.00 && raw.ms2 && raw.ms2 >= 2.00 && Math.abs(evCum) <= 0.2 && Math.abs(depCum) <= 0.2)
      bsPush('X/X', `MS1=${raw.ms1?.toFixed(2)} MS2=${raw.ms2?.toFixed(2)} hareket=düşük`, 4.5, 1.3, 0.12);
  }

  for (const outcome of FOCUS_RESULTS) {
    const p = predictions[outcome];
    if (p.lift < 1.10) continue;

    const { multiplier, label: accLabel, accuracy } = getAccuracyMultiplier(outcome);
    if (multiplier === 0.0) {
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

    const minProb = (p.total >= 20) ? 0.18
                  : (p.total >= 10) ? 0.20
                  : (p.total >= 5)  ? 0.22
                  : 0.11; // az örnekte lift>=1.10 yeterliyse geç
    if (p.prob < minProb) continue;

    let htFtNote = '';
    if (!hasHtFt) {
      liftVal      = +(liftVal      * 0.70).toFixed(2);
      effectiveLift= +(effectiveLift* 0.70).toFixed(2);
      htFtNote     = ' ⚠IY/MS-YOK(oran.eksik)';
      if (tier === 'ELITE')   tier = 'PREMIER';
      if (tier === 'PREMIER') tier = 'STANDART';
    }

    signals.push({
      type: outcome, tier,
      rule: `${rule} | hist=${p.count}/${p.total}${htFtNote}`,
      prec: +precision.toFixed(2),
      lift: liftVal, effectiveLift,
      prob: p.prob, stateKey, trendStrength,
      histCount: p.count, accLabel, accuracy,
      hasHtFt,
    });
  }

  const tierW = { ELITE: 3, PREMIER: 2, STANDART: 1 };
  signals.sort((a,c) => (tierW[c.tier]||0)-(tierW[a.tier]||0) || c.effectiveLift-a.effectiveLift);

  return { signals, features, predictions, stateKey, hasHtFt };
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 9 — YEREL YORUM
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

  const htFtWarning = (hasHtFt === false)
    ? '\n⚠️  IY/MS oranı yok — IY/MS tabanlı tahminler %30 cezalı, temkinli olun.'
    : '';

  // Drop özeti — açılıştan hareket varsa ekle
  const dropNotes = [];
  if (f.iyms22_drop === 'heavy') dropNotes.push('IY/MS 2/2 açılıştan ağır düştü');
  if (f.iyms21_drop === 'heavy') dropNotes.push('IY/MS 2/1 açılıştan ağır düştü');
  if (f.ms1_drop    === 'heavy') dropNotes.push('MS1 açılıştan ağır düştü');
  if (f.ou25o_drop  === 'heavy') dropNotes.push('2.5 üst açılıştan ağır düştü');
  const dropNote = dropNotes.length > 0 ? `\n📉 Açılış Hareketi: ${dropNotes.join(' | ')}` : '';

  return (
    `📊 DURUM: ${stateKey.substring(0, 55)}...\n` +
    `${mkt}\n` +
    `🎯 TAHMİN: ${top.type} | ${top.tier} | Lift: ${top.lift}x (efektif: ${top.effectiveLift}x) | Olas: %${((top.prob||0)*100).toFixed(1)}\n` +
    `📚 ${note}${accNote}${htFtWarning}${dropNote}\n` +
    `⚡ Trend: ${top.trendStrength} | İYMS21: ${r.iyms21||'N/A'} | MS1: ${r.ms1||'?'}`
  );
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 10 — SİNYAL LOGGER
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
// BÖLÜM 10b — ALERT HELPERS
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
      // [FIX-E] recent[] her zaman başlatılsın
      memory.signalAccuracy[signalData.type] = { fired: 0, correct: 0, recent: [] };
    if (!memory.signalAccuracy[signalData.type].recent)
      memory.signalAccuracy[signalData.type].recent = [];
    memory.signalAccuracy[signalData.type].fired++;
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 11 — MAÇ LİSTESİ
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
// BÖLÜM 12 — CANLI SKOR & ÖĞRENME
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
    // [FIX-F] ht_home_score / ht_away_score sütunları varsa çek (yoksa graceful devam)
    const { data, error } = await sb
      .from('live_matches')
      .select('fixture_id, status_short, home_score, away_score, ht_home_score, ht_away_score')
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
    const hScore  = row.home_score    ?? null;
    const aScore  = row.away_score    ?? null;
    // [FIX-F] Supabase'de ht sütunları varsa FT anında da HT skoru bilinir
    const htScore = row.ht_home_score ?? null;
    const atScore = row.ht_away_score ?? null;
    matchedLive++;
    const match    = matchCache.get(fid);
    if (!match) continue;
    const prevLive = match.liveData || {};
    // [FIX-A] FT'de ftHome null geldiyse bir sonraki döngüde tekrar dene (retry guard)
    const ftNeedsRetry = status === 'FT' && prevLive.status === 'FT' && !prevLive.ftHome == null;
    if (prevLive.status === status && status !== 'FT') continue;
    if (prevLive.status === 'FT' && !ftNeedsRetry) continue;

    // [FIX-F] htScore Supabase'den geldiyse prevLive'ı ezebilir
    let htHome = htScore ?? prevLive.htHome ?? null;
    let htAway = atScore ?? prevLive.htAway ?? null;
    let ftHome = prevLive.ftHome ?? null;
    let ftAway = prevLive.ftAway ?? null;
    let htFtResult = prevLive.htFtResult ?? null;

    if (status === 'HT') {
      htHome = hScore; htAway = aScore;
      console.log(`  ⏸ HT: ${match.name} | İY ${htHome}-${htAway}`);
    } else if (status === 'FT') {
      // [FIX-A] hScore/aScore doluysa güncelle (her retry'da dene)
      if (hScore != null) { ftHome = hScore; ftAway = aScore; }
      htFtResult = calcHtFtResult(htHome, htAway, ftHome, ftAway);
      const retryTag = ftNeedsRetry ? ' [RETRY]' : '';
      console.log(
        `  🏁 FT${retryTag}: ${match.name}` +
        ` | İY: ${htHome??'?'}-${htAway??'?'}` +
        ` → MS: ${ftHome??'?'}-${ftAway??'?'}` +
        ` | HT/FT: ${htFtResult||'hesaplanamadı'}`
      );
      if (htFtResult) {
        if (!prevLive.htFtResult) {
        resolvePendingSignal(fid, htFtResult);
        resolvedCount++;
        learnFromMatch(fid, htFtResult);
        learnedCount++;
      } else {
        console.warn(`  ⚠️ ${match.name}: skor eksik — sonraki döngüde tekrar denenecek`);
      }
     }
    }
    match.liveData = { status, htHome, htAway, ftHome, ftAway, htFtResult };
    matchCache.set(fid, match);
  }
  if (liveRows.length > 0)
    console.log(`[Live] Eşleşme: ${matchedLive}/${liveRows.length} | Öğrenilen: ${learnedCount} | Çözümlenen: ${resolvedCount}`);
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 13 — ANA DÖNGÜ
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

  if (cycleCount % 10 === 1) {
    logAccuracyReport();
    if (memory.totalLearned >= 10) analyzeFeatureImportance();
  }

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
      currMarkets, changes, { ev_ft_cum, dep_ft_cum }, snapshots,
      prev?.openingMarkets
    );

    matchCache.set(fid, {
      name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, latestMarkets: currMarkets,
      ev_ft_cum, dep_ft_cum, snapshots,
      liveData: prev?.liveData || {},
      // İlk döngüde açılış kaydedilir, sonraki döngülerde korunur:
      openingMarkets: prev?.openingMarkets || currMarkets,
    });

    if (h2k > SIGNAL_WINDOW_H) {
      const minLeft = Math.round(h2k * 60);
      continue;
    }

    if (signals.length === 0) {
      console.log(`[❌ F1-SinyalYok] ${fix.home_team} vs ${fix.away_team} | learned=${memory.totalLearned}`);
      continue;
    }
    const hasHighTier = signals.some(s => s.tier === 'ELITE' || s.tier === 'PREMIER');
    const isBootstrap = signals.every(s => s.trendStrength === 'bootstrap');
    if (!hasHighTier && signals.length < MIN_SIGNALS && !isBootstrap) {
      console.log(
        `[❌ F2-TierFiltre] ${fix.home_team} vs ${fix.away_team}` +
        ` | ${signals.map(s => s.type+'('+s.tier+')').join(', ')}` +
        ` | highTier=${hasHighTier} bootstrap=${isBootstrap}`
      );
      continue;
    }

    const topSignal = signals[0];
    const topLabel  = `${topSignal.type}_${topSignal.tier}`;
    if (alreadyFired(fid, topLabel)) {
      console.log(`[❌ F3-ZatenAteşlendi] ${fix.home_team} vs ${fix.away_team} | ${topLabel}`);
      continue;
    }

    const interpretation = generateLocalInterpretation({
      signals, features, predictions, stateKey, hasHtFt,
    });

    matchesWithSignals.push({
      fid, name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, h2k,
      signals, features, interpretation,
      ev_ft_cum, dep_ft_cum,
      hasHtFt,
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
  console.log('║  ScorePop Adaptive v3.4 — Self-Evaluating Market Engine  ║');
  console.log(`║  Döngü: ${Math.round(INTERVAL_MS/60000)}dk | Süre: ${Math.round(MAX_RUNTIME_MS/3600000)}sa | DryRun: ${String(DRY_RUN).padEnd(6)}║`);
  console.log(`║  Bootstrap eşiği   : ${String(BOOTSTRAP_THRESHOLD).padEnd(36)}║`);
  console.log(`║  Sinyal penceresi  : ≤${String(Math.round(SIGNAL_WINDOW_H*60)+'dk').padEnd(35)}║`);
  console.log(`║  Accuracy min örnek: ${String(ACCURACY_MIN_SAMPLES).padEnd(36)}║`);
  console.log(`║  Penalty / Boost   : <%${String((ACCURACY_PENALTY_THR*100).toFixed(0)).padEnd(9)} / >%${String((ACCURACY_BOOST_THR*100).toFixed(0)).padEnd(22)}║`);
  console.log(`║  v3.4 FIX-A: FT retry kilidi kaldırıldı                 ║`);
  console.log(`║  v3.4 FIX-B: AccReset kaldırıldı, recent[] başlatıldı   ║`);
  console.log(`║  v3.4 FIX-C: Eski pendingSignals migrasyonu              ║`);
  console.log(`║  v3.4 FIX-D: learnFromMatch eski stateKey koruması       ║`);
  console.log(`║  v3.4 FIX-F: Supabase ht_home/away_score desteği        ║`);
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
