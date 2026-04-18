/**
 * ai_tracker.js — ScorePop Adaptive Tracker v2
 * ═══════════════════════════════════════════════════════════════════
 * Nesine'den oran çeker → Özellik vektörü çıkarır → Durum kodu üretir →
 * Tarihsel hafızaya sorar → Olasılık/Lift hesaplar → Sinyal üretir.
 * Maç bittiğinde gerçek sonucu hafızaya işleyerek kendi kendini günceller.
 *
 * Ortam değişkenleri:
 * SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_TO
 * INTERVAL_MS (5dk), MAX_RUNTIME_MS (4.75sa), LOOKAHEAD_HOURS (8)
 * CACHE_FILE          — tracker_cache.json
 * FIRED_FILE          — fired_alerts.json
 * MEMORY_FILE         — learned_memory.json (YENİ: Öğrenme veritabanı)
 * DRY_RUN
 * SUPABASE_URL, SUPABASE_KEY
 */
'use strict';

const https    = require('https');
const fs       = require('fs');
const nodemailer = require('nodemailer');
const { createClient } = require('@supabase/supabase-js');

// ── Config ──────────────────────────────────────────────────────────
const INTERVAL_MS    = parseInt(process.env.INTERVAL_MS    || '300000');
const MAX_RUNTIME_MS = parseInt(process.env.MAX_RUNTIME_MS || '17100000');
const LOOKAHEAD_H    = parseInt(process.env.LOOKAHEAD_HOURS || '8');
const CACHE_FILE     = process.env.CACHE_FILE  || 'tracker_cache.json';
const FIRED_FILE     = process.env.FIRED_FILE  || 'fired_alerts.json';
const MEMORY_FILE    = process.env.MEMORY_FILE || 'learned_memory.json';
const DRY_RUN        = process.env.DRY_RUN === 'true';
const MIN_SIGNALS    = 2;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const sb = (SUPABASE_URL && SUPABASE_KEY) ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ── State ────────────────────────────────────────────────────────────
const matchCache = new Map();   // fixture_id → match state
let firedAlerts  = {};          // fixture_id → [labels]
let memory       = { patterns: {}, version: 2, totalLearned: 0 }; // YENİ
let cycleCount   = 0;
const startTime  = Date.now();

const HTFT_RESULTS = ['1/1','1/X','1/2','X/1','X/X','X/2','2/1','2/X','2/2'];
const FOCUS_RESULTS = ['1/1','2/1','2/2','1/2']; // Odaklandığımız ana senaryolar

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 1 — CACHE & MEMORY (Disk ↔ Bellek)
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
      memory = JSON.parse(fs.readFileSync(MEMORY_FILE, 'utf8'));
      if (!memory.patterns) memory = { patterns: {}, version: 2, totalLearned: memory.totalLearned || 0 };
      console.log(`[Memory] ${Object.keys(memory.patterns).length} pattern yüklendi | Toplam öğrenilen: ${memory.totalLearned}`);
    } catch (e) { console.warn('[Memory] Yüklenemedi:', e.message); }
  }
}

function saveCache() {
  const obj = { savedAt: new Date().toISOString(), matchCache: {} };
  for (const [fid, val] of matchCache.entries()) obj.matchCache[fid] = val;
  fs.writeFileSync(CACHE_FILE, JSON.stringify(obj, null, 2));
  fs.writeFileSync(FIRED_FILE, JSON.stringify(firedAlerts, null, 2));
  fs.writeFileSync(MEMORY_FILE, JSON.stringify(memory, null, 2));
}

function alreadyFired(fid, label) { return (firedAlerts[fid] || []).includes(label); }
function markFired(fid, label) {
  firedAlerts[fid] = firedAlerts[fid] || [];
  if (!firedAlerts[fid].includes(label)) firedAlerts[fid].push(label);
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 2 — HTTP + NESİNE
// ════════════════════════════════════════════════════════════════════
function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'Accept': 'application/json', 'Accept-Encoding': 'identity',
        'Referer': 'https://www.nesine.com/', 'Origin': 'https://www.nesine.com',
        'User-Agent': 'Mozilla/5.0 (compatible; ScorePop/2.0)',
      }
    }, res => {
      let buf = '';
      res.on('data', d => buf += d);
      res.on('end', () => { try { resolve(JSON.parse(buf)); } catch (e) { reject(new Error(`JSON parse: ${e.message}`)); } });
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
  'r wien amt':'rapid wien','w bregenz':'schwarz weiss b',
  'rb bragantino':'bragantino','new york rb':'ny red bulls',
  'fc midtjylland':'midtjylland','pacos de ferreira':'p ferreira',
};
function norm(s) {
  return (s || '').toLowerCase()
    .replace(/ğ/g,'g').replace(/ü/g,'u').replace(/ş/g,'s')
    .replace(/ı/g,'i').replace(/ö/g,'o').replace(/ç/g,'c')
    .replace(/[^a-z0-9]/g,' ').replace(/\s+/g,' ').trim();
}
function normA(s) { const n = norm(s); return TEAM_ALIASES[n] || n; }
function tokenSim(a, b) {
  const ta = new Set(norm(a).split(' ').filter(x => x.length > 1));
  const tb = new Set(norm(b).split(' ').filter(x => x.length > 1));
  if (!ta.size || !tb.size) return 0;
  let hit = 0;
  for (const t of ta) {
    if (tb.has(t)) { hit++; continue; }
    for (const u of tb) { if (t.startsWith(u)||u.startsWith(t)) { hit += 0.7; break; } }
  }
  return hit / Math.max(ta.size, tb.size);
}
function findBestMatch(home, away, events) {
  const THRESHOLD = 0.35;
  let best = null, bestScore = THRESHOLD - 0.01;
  for (const ev of events) {
    const hs  = tokenSim(normA(home), norm(ev.HN));
    const as_ = tokenSim(normA(away), norm(ev.AN));
    const avg = (hs + as_) / 2;
    if (hs >= 0.20 && as_ >= 0.20 && avg > bestScore) { bestScore = avg; best = ev; }
  }
  return best ? { ev: best, score: bestScore } : null;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 4 — MARKET PARSE
// ════════════════════════════════════════════════════════════════════
function parseMarkets(maArr) {
  const m = {};
  if (!Array.isArray(maArr)) return m;
  for (const x of maArr) {
    const id = x.MTID; const oca = x.OCA || [];
    const g = n => { const o = oca.find(x => x.N === n); return o ? +o.O : 0; };
    if (id === 1  && oca.length === 3) m['1x2']    = { home:g(1), draw:g(2), away:g(3) };
    if (id === 7  && oca.length === 3) m['ht_1x2'] = { home:g(1), draw:g(2), away:g(3) };
    if (id === 9  && oca.length === 3) m['2h_1x2'] = { home:g(1), draw:g(2), away:g(3) };
    if (id === 5  && oca.length === 9) m['ht_ft']  = {
      '1/1':g(1),'1/X':g(2),'1/2':g(3),
      'X/1':g(4),'X/X':g(5),'X/2':g(6),
      '2/1':g(7),'2/X':g(8),'2/2':g(9),
    };
    if (id === 12 && oca.length === 2) m['ou25']   = { under:g(1), over:g(2) };
    if (id === 11 && oca.length === 2) m['ou15']   = { under:g(1), over:g(2) };
    if (id === 13 && oca.length === 2) m['ou35']   = { under:g(1), over:g(2) };
    if (id === 38 && oca.length === 2) m['btts']   = { yes:g(1), no:g(2) };
    if (id === 48 && oca.length === 3) m['more_goals_half'] = { first:g(1), equal:g(2), second:g(3) };
    if (id === 3  && oca.length === 3) m['dc']     = { '1x':g(1), '12':g(2), 'x2':g(3) };
  }
  return m;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 5 — DELTA HESABI
// ════════════════════════════════════════════════════════════════════
function calcDelta(prev, curr) {
  const ch = {};
  const keys = ['1x2','ht_1x2','2h_1x2','ht_ft','ou25','ou15','ou35','btts','more_goals_half'];
  for (const k of keys) {
    ch[k] = {}; const p = prev[k] || {}, c = curr[k] || {};
    for (const sub of Object.keys({...p,...c})) {
      const pv = p[sub], cv = c[sub];
      if (pv && cv && pv !== cv) ch[k][sub] = +(cv - pv).toFixed(3);
      else ch[k][sub] = 0;
    }
  }
  return ch;
}

function ftGroups(changes) {
  const s = k => changes?.ht_ft?.[k] || 0;
  return { ev_ft: s('1/1')+s('2/1')+s('X/1'), dep_ft: s('1/2')+s('2/2')+s('X/2'), bera: s('1/X')+s('2/X')+s('X/X') };
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 6 — ÖZELLİK ÇIKARIMI & DURUM KODU (YENİ: AI yerine)
// ════════════════════════════════════════════════════════════════════

/**
 * Sürekli sayıları kategorilere ayırır. Böylece benzer piyasa
 * koşulları tek bir "durum" altında toplanır.
 */
function bucket(val, thresholds, labels) {
  for (let i = 0; i < thresholds.length; i++) if (val <= thresholds[i]) return labels[i];
  return labels[labels.length - 1];
}

function extractFeatures(markets, changes, cumCache, snapshots) {
  const mk = (k, s) => markets?.[k]?.[s] ?? null;
  const ch = (k, s) => changes?.[k]?.[s] ?? null;
  const { ev_ft, dep_ft } = ftGroups(changes || {});

  // Ham değerler
  const ms1   = mk('1x2','home');
  const ms2   = mk('1x2','away');
  const iy1   = mk('ht_1x2','home');
  const iy2   = mk('ht_1x2','away');
  const sy1   = mk('2h_1x2','home');
  const iyms21= mk('ht_ft','2/1');
  const iyms22= mk('ht_ft','2/2');
  const iyms11= mk('ht_ft','1/1');
  const iyms12= mk('ht_ft','1/2');
  const au25o = mk('ou25','over');
  const bttsY = mk('btts','yes');
  const dcg2h = mk('more_goals_half','second');

  // Kategoriler (Bucket'lar)
  const f = {
    ms1_bucket:   bucket(ms1,   [1.30, 1.60, 2.50], ['vlow','low','med','high']),
    ms2_bucket:   bucket(ms2,   [1.80, 3.00],       ['low','med','high']),
    iy1_bucket:   bucket(iy1,   [1.70, 2.50],       ['low','med','high']),
    iy2_bucket:   bucket(iy2,   [3.50, 5.00],       ['low','med','high']),
    sy1_bucket:   bucket(sy1,   [1.70, 2.50],       ['low','med','high']),
    iyms21_bucket:bucket(iyms21,[10, 22, 35],       ['vlow','low','med','high']),
    iyms22_bucket:bucket(iyms22,[3, 8, 15],         ['vlow','low','med','high']),
    iyms11_bucket:bucket(iyms11,[3, 6, 12],         ['vlow','low','med','high']),
    iyms12_bucket:bucket(iyms12,[10, 20, 35],       ['vlow','low','med','high']),
    au25o_bucket: bucket(au25o, [1.50, 2.00, 2.80], ['low','med','high','vhigh']),
    btts_bucket:  bucket(bttsY, [1.50, 2.00],       ['low','med','high']),
    ev_ft_sign:   ev_ft  < -1 ? 'neg' : ev_ft  > 1 ? 'pos' : 'flat',
    dep_ft_sign:  dep_ft < -1 ? 'neg' : dep_ft > 1 ? 'pos' : 'flat',
  };

  // Trend / Momentum (son 3 snapshot üzerinden)
  const recent = (snapshots || []).slice(-3);
  let evMomentum = 'flat';
  let depMomentum = 'flat';
  if (recent.length >= 2) {
    const evVals = recent.map(s => {
      const { ev_ft: eft } = ftGroups(s.changes || {});
      return eft;
    });
    const depVals = recent.map(s => {
      const { dep_ft: dft } = ftGroups(s.changes || {});
      return dft;
    });
    const evSlope = evVals[evVals.length-1] - evVals[0];
    const depSlope = depVals[depVals.length-1] - depVals[0];
    evMomentum  = evSlope  < -1.5 ? 'falling' : evSlope  > 1.5 ? 'rising' : 'stable';
    depMomentum = depSlope < -1.5 ? 'falling' : depSlope > 1.5 ? 'rising' : 'stable';
  }

  f.ev_momentum = evMomentum;
  f.dep_momentum = depMomentum;

  // Divergence flag'leri
  f.div_ev_to_dep = (f.ev_ft_sign === 'neg' && f.dep_ft_sign === 'pos') ? 'yes' : 'no';
  f.div_strong_ev = (f.ev_ft_sign === 'neg' && f.dep_ft_sign === 'neg') ? 'yes' : 'no';

  return { raw: { ms1,ms2,iy1,iy2,sy1,iyms21,iyms22,iyms11,iyms12,au25o,bttsY,dcg2h, ev_ft, dep_ft }, buckets: f };
}

/**
 * Özellikleri önem sırasına göre birleştirip tekil bir string üretir.
 * Öğrenme hafızası bu string üzerinden indekslenir.
 */
function generateStateKey(features) {
  const b = features.buckets;
  // En ayırt edici özellikler önde
  return [
    `ms1_${b.ms1_bucket}`,
    `iy2_${b.iy2_bucket}`,
    `evft_${b.ev_ft_sign}`,
    `depft_${b.dep_ft_sign}`,
    `iyms21_${b.iyms21_bucket}`,
    `evmom_${b.ev_momentum}`,
    `depmon_${b.dep_momentum}`,
    `div_${b.div_ev_to_dep}`,
    `ms2_${b.ms2_bucket}`,
    `au25_${b.au25o_bucket}`,
  ].join('|');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 7 — ÖĞRENME MOTORU (YENİ: Kalp)
// ════════════════════════════════════════════════════════════════════

/**
 * Bir maç bittiğinde çağrılır. Maçın son snapshot'larındaki durum
 * kodlarını bulur ve gerçekleşen HT/FT sonucunun sayacını artırır.
 */
function learnFromMatch(fixtureId, actualHtFt) {
  const match = matchCache.get(fixtureId);
  if (!match || !match.snapshots || match.snapshots.length === 0) return;
  if (!FOCUS_RESULTS.includes(actualHtFt)) return; // Sadece odak sonuçları öğren

  // Daha verimli yaklaşım: Cache'e kaydederken stateKey'i de snapshot'a ekle
  // Ama şimdilik son snapshot'ın stateKey'ini kullanalım (maç kapandığında son durum en temsilcisi)
  const lastSnap = match.snapshots[match.snapshots.length - 1];
  if (!lastSnap._stateKey) return; // Eğer eski formatta ise

  const key = lastSnap._stateKey;
  if (!memory.patterns[key]) memory.patterns[key] = {};
  if (!memory.patterns[key][actualHtFt]) memory.patterns[key][actualHtFt] = { count: 0, firstSeen: new Date().toISOString() };

  memory.patterns[key][actualHtFt].count++;
  memory.totalLearned++;
  console.log(`[Learn] "${key}" → ${actualHtFt} (yeni sayı: ${memory.patterns[key][actualHtFt].count})`);
}

/**
 * Mevcut durum için her bir HT/FT sonucunun olasılığını ve
 * "lift" değerini (piyasa ortalamasına göre aşım) döndürür.
 */
function predict(stateKey) {
  const pattern = memory.patterns[stateKey];
  const result = {};

  for (const r of FOCUS_RESULTS) result[r] = { prob: 0.05, lift: 1.0, count: 0, confidence: 'none' };

  if (!pattern) return result; // Hiç görülmemiş durum → nötr, düşük olasılık

  let total = 0;
  for (const r of FOCUS_RESULTS) total += (pattern[r]?.count || 0);
  if (total < 2) return result; // Yetersiz örnek

  for (const r of FOCUS_RESULTS) {
    const cnt = pattern[r]?.count || 0;
    const prob = cnt / total;
    // Lift: Bu durumda r'nin olma olasılığı / Genel olarak r'nin olma olasılığı (~0.11 varsayım)
    const baseProb = 0.11; // 1/9 yaklaşık
    const lift = prob / baseProb;
    let confidence = 'low';
    if (total >= 10 && prob >= 0.30) confidence = 'high';
    else if (total >= 5 && prob >= 0.20) confidence = 'medium';

    result[r] = { prob: +prob.toFixed(3), lift: +lift.toFixed(2), count: cnt, total, confidence };
  }
  return result;
}

/**
 * İki durum kodu arasındaki "benzerliği" hesaplar.
 * Aynı kategoride olmayan fakat yakın bucket'larda olan durumları
 * birbirine bağlayarak soğuk başlangıç (cold start) sorununu hafifletir.
 */
function predictWithSimilarity(stateKey, currentFeatures) {
  const direct = predict(stateKey);
  // Eğer direkt tahmin yeterli örnek içeriyorsa onu kullan
  const directTotal = direct['2/1'].total || 0;
  if (directTotal >= 5) return direct;

  // Yakın komşu arama: Sadece 1 özellik farklı olan patternleri bul ve ağırlıklı ortalama al
  const neighbors = [];
  const keys = Object.keys(memory.patterns);
  const targetParts = stateKey.split('|');

  for (const k of keys) {
    const parts = k.split('|');
    let diff = 0;
    for (let i = 0; i < parts.length; i++) if (parts[i] !== targetParts[i]) diff++;
    if (diff === 1) {
      const total = FOCUS_RESULTS.reduce((sum, r) => sum + (memory.patterns[k][r]?.count || 0), 0);
      if (total >= 3) neighbors.push({ key: k, total, diff });
    }
  }

  if (neighbors.length === 0) return direct;

  // Komşu tahminlerini ağırlıklı olarak karıştır
  const blended = {};
  let weightSum = directTotal; // Direkt gözlemin ağırlığı

  for (const r of FOCUS_RESULTS) blended[r] = { prob: direct[r].prob * directTotal, count: direct[r].count };

  for (const n of neighbors) {
    const nPred = predict(n.key);
    const w = n.total * 0.5; // Komşuların ağırlığı yarım
    weightSum += w;
    for (const r of FOCUS_RESULTS) {
      blended[r].prob += nPred[r].prob * w;
      blended[r].count += nPred[r].count;
    }
  }

  const final = {};
  for (const r of FOCUS_RESULTS) {
    const prob = weightSum > 0 ? blended[r].prob / weightSum : 0;
    const baseProb = 0.11;
    final[r] = {
      prob: +prob.toFixed(3),
      lift: +(prob / baseProb).toFixed(2),
      count: blended[r].count,
      total: Math.round(weightSum),
      confidence: (weightSum >= 8 && prob >= 0.25) ? 'medium' : 'low'
    };
  }
  return final;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 8 — AKILLI SİNYAL MOTORU (Manuel kuralların yerine)
// ════════════════════════════════════════════════════════════════════
function evaluateSmartSignals(markets, changes, cumCache, snapshots) {
  const features = extractFeatures(markets, changes, cumCache, snapshots);
  const stateKey = generateStateKey(features);
  const raw = features.raw;

  // Snapshot'a stateKey'i de ekleyelim (öğrenme için gerekli)
  if (snapshots.length > 0) snapshots[snapshots.length - 1]._stateKey = stateKey;

  const predictions = predictWithSimilarity(stateKey, features);
  const signals = [];

  // Momentum Analizi (Trend gücü)
  const b = features.buckets;
  const trendStrength = (b.ev_momentum === 'falling' && b.dep_momentum === 'rising') ? 'strong_reversal' :
                        (b.ev_momentum === 'falling') ? 'ev_dominant' :
                        (b.dep_momentum === 'falling') ? 'dep_dominant' : 'neutral';

  // Her olası sonuç için değerlendirme
  for (const outcome of FOCUS_RESULTS) {
    const p = predictions[outcome];
    if (p.lift < 1.20) continue; // Lift çok düşükse sinyal üretme

    let tier = 'STANDART';
    let rule = `State: ${stateKey.substring(0, 60)}...`;
    let precision = p.prob * 10; // 0-10 skala

    // Lift ve confidence'a göre tier belirle
    if (p.lift >= 2.50 && p.confidence === 'high') {
      tier = 'ELITE';
      precision = Math.min(9.5, p.prob * 10 + 2);
    } else if (p.lift >= 2.00 && (p.confidence === 'high' || p.confidence === 'medium')) {
      tier = 'PREMIER';
      precision = Math.min(8.5, p.prob * 10 + 1);
    } else if (p.lift >= 1.60) {
      tier = 'STANDART';
      precision = p.prob * 10;
    }

    // Trend gücü bonusu
    if (outcome === '2/1' && trendStrength === 'strong_reversal') { precision += 0.8; rule = 'Reversal momentum + ' + rule; }
    if (outcome === '1/1' && trendStrength === 'ev_dominant') { precision += 0.6; rule = 'Ev dominance + ' + rule; }
    if (outcome === '2/2' && trendStrength === 'dep_dominant') { precision += 0.6; rule = 'Dep dominance + ' + rule; }

    // Kümülatif delta ile cross-check
    const { ev_ft, dep_ft } = ftGroups(changes || {});
    if (outcome === '2/1' && raw.ev_ft <= -3 && raw.dep_ft >= 2) precision += 0.5;
    if (outcome === '1/1' && raw.ev_ft <= -2 && raw.dep_ft >= 1) precision += 0.4;
    if (outcome === '2/2' && raw.dep_ft <= -2) precision += 0.4;

    // Olasılık threshold'u (yeterli örnek yoksa daha konservatif)
    const minProb = (p.total >= 10) ? 0.18 : (p.total >= 5) ? 0.22 : 0.28;
    if (p.prob < minProb) continue;

    signals.push({
      type: outcome,
      tier,
      rule: `${rule} | hist=${p.count}/${p.total}`,
      prec: +precision.toFixed(2),
      lift: p.lift,
      prob: p.prob,
      stateKey,
      trendStrength
    });
  }

  // Tier ve lift sıralaması
  const tierW = { ELITE: 3, PREMIER: 2, STANDART: 1 };
  signals.sort((a, b) => (tierW[b.tier] || 0) - (tierW[a.tier] || 0) || b.lift - a.lift);

  return { signals, features, predictions, stateKey };
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 9 — YORUM ÜRETİCİ (Yerel, AI'sız)
// ════════════════════════════════════════════════════════════════════
function generateLocalInterpretation(matchData) {
  const { signals, features, predictions, stateKey } = matchData;
  if (signals.length === 0) return null;

  const top = signals[0];
  const f = features.buckets;
  const r = features.raw;

  // Piyasa yönü analizi (doğal dil)
  let marketNarrative = '';
  if (f.div_ev_to_dep === 'yes') marketNarrative = `Piyasada ev/dep FT reversal baskısı var. `;
  else if (f.div_strong_ev === 'yes') marketNarrative = `Her iki yarıda ev güçleniyor. `;
  else marketNarrative = `Piyasa hareketi karışık yönlü. `;

  if (f.ev_momentum === 'falling') marketNarrative += `Ev FT oranları düşüyor (para girişi). `;
  if (f.dep_momentum === 'rising') marketNarrative += `Dep FT oranları yükseliyor (para çıkışı). `;

  // Tarihsel performans özeti
  const hist = predictions[top.type];
  let historicalNote = '';
  if (hist.total >= 10) historicalNote = `Bu pattern geçmişte ${hist.total} kez tekrarlandı, ${hist.count} kez ${top.type} geldi (%${(hist.prob*100).toFixed(0)}).`;
  else if (hist.total >= 3) historicalNote = `Sınırlı örnek (${hist.total}) ama eğilim ${top.type} yönünde.`;
  else historicalNote = `Yeni pattern, temkinli olun.`;

  return `
📊 DURUM KODU: ${stateKey.substring(0, 55)}...
${marketNarrative}
🎯 TAHMİN: ${top.type} | Güven: ${top.tier} | Lift: ${top.lift}x | Olasılık: %${(top.prob*100).toFixed(1)}
📚 ${historicalNote}
⚡ Trend: ${top.trendStrength} | Son İYMS21: ${r.iyms21 || '?'} | MS1: ${r.ms1 || '?'}
`.trim();
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 10 — E-POSTA (AI bloğu yerine Local Insight)
// ════════════════════════════════════════════════════════════════════
function createTransport() {
  return nodemailer.createTransport({
    host: process.env.SMTP_HOST || 'smtp.gmail.com',
    port: parseInt(process.env.SMTP_PORT || '587'),
    secure: process.env.SMTP_PORT === '465',
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  });
}

function buildEmailHTML(matchesWithSignals, cycleNo) {
  const now = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
  const tierColor = { ELITE: '#c0392b', PREMIER: '#e67e22', STANDART: '#2980b9' };
  const typeEmoji = { '1/1': '🟡', '2/2': '🟣', '2/1': '🟢', '1/2': '🔵' };

  const matchBlocks = matchesWithSignals.map(m => {
    const top = m.signals[0];
    const allSigs = m.signals.slice(0, 3).map(s =>
      `<span style="display:inline-block;background:${tierColor[s.tier]};color:#fff;padding:3px 8px;border-radius:4px;font-size:12px;margin:2px;">
        ${s.type} %${(s.prob*100).toFixed(0)} (lift ${s.lift}x)
      </span>`
    ).join(' ');

    const localInsight = m.interpretation
      ? `<div style="background:#f8f9fa;border-left:3px solid ${tierColor[top.tier]};padding:10px;margin-top:8px;font-size:12px;color:#444;white-space:pre-wrap;">${m.interpretation.replace(/\n/g, '<br>')}</div>`
      : '';

    const feat = m.features?.buckets;
    const momentumBars = `
      <div style="margin-top:6px;font-size:11px;color:#666;">
        <span style="color:${feat.ev_momentum==='falling'?'#e74c3c':'#27ae60'}">● Ev FT: ${feat.ev_ft_sign} (${feat.ev_momentum})</span> &nbsp;|&nbsp;
        <span style="color:${feat.dep_momentum==='rising'?'#e74c3c':'#27ae60'}">● Dep FT: ${feat.dep_ft_sign} (${feat.dep_momentum})</span>
      </div>`;

    return `
    <div style="border:1px solid #ddd;border-radius:8px;padding:14px;margin-bottom:14px;background:#fff;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div style="font-weight:bold;font-size:15px;color:#2c3e50;">${typeEmoji[top.type] || '⚪'} ${m.name}</div>
        <div style="font-size:12px;color:#7f8c8d;">${m.h2k < 1 ? '⏳ Başladı / Başlayacak' : '🔜 ' + m.h2k.toFixed(1) + ' saat'}</div>
      </div>
      <div style="margin:8px 0;">${allSigs}</div>
      ${momentumBars}
      <div style="margin-top:6px;font-size:11px;color:#555;">
        Hafıza: ${top.histCount || 0} örnek | Durum: <code style="background:#ecf0f1;padding:2px 4px;border-radius:3px;">${top.stateKey?.substring(0,40)}...</code>
      </div>
      ${localInsight}
    </div>`;
  }).join('');

  // Öğrenme istatistiği
  const memStats = Object.values(memory.patterns).reduce((acc, p) => {
    const t = FOCUS_RESULTS.reduce((s, r) => s + (p[r]?.count || 0), 0);
    if (t > 0) { acc.patterns++; acc.totalSamples += t; }
    return acc;
  }, { patterns: 0, totalSamples: 0 });

  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>ScorePop Adaptive Alarm</title></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;max-width:800px;margin:0 auto;padding:16px;background:#f5f6fa;color:#2c3e50;">
  <div style="background:linear-gradient(135deg,#0f0c29,#302b63,#24243e);color:#fff;padding:22px;border-radius:10px;margin-bottom:18px;">
    <h1 style="margin:0;font-size:20px;">🧠 ScorePop Adaptive — Piyasa Öğrenme Motoru</h1>
    <p style="margin:6px 0 0;opacity:.85;font-size:13px;">Döngü #${cycleNo} | ${now} | Hafızada ${memStats.patterns} pattern, ${memStats.totalSamples} örnek</p>
  </div>

  <div style="background:#fff3cd;border-left:4px solid #f0ad4e;padding:12px;margin-bottom:16px;font-size:12px;border-radius:4px;">
    💡 Bu sinyaller <b>tamamen yerel öğrenme verilerine</b> dayanır. Claude/ChatGPT kullanılmaz. Her sonuç sistemin hafızasını güçlendirir.
  </div>

  <p style="color:#7f8c8d;font-size:13px;margin-bottom:12px;">
    <b>${matchesWithSignals.length}</b> maçta güçlü istatistiksel sapma tespit edildi.
  </p>

  ${matchBlocks}

  <div style="background:#ffeaa7;padding:12px;border-radius:6px;font-size:11px;margin-top:8px;">
    ⚠️ Sistem geçmiş oran hareketleri ile sonuçları eşleştirerek olasılık üretir. Yüksek lift = piyasa ortalamasına göre aşım.
    Her zaman kendi değerlendirmenizi yapın.
  </div>
  <p style="font-size:11px;color:#bdc3c7;margin-top:10px;text-align:right;">ScorePop Adaptive v2 | Self-Learning Engine</p>
</body>
</html>`;
}

async function sendEmail(subject, html) {
  const to = process.env.MAIL_TO || '';
  if (!to) { console.warn('[Mail] MAIL_TO tanımlı değil'); return false; }
  if (DRY_RUN) { console.log(`[DRY_RUN] Mail atılmadı: ${subject}`); return true; }

  try {
    const info = await createTransport().sendMail({
      from: `"ScorePop AI" <${process.env.SMTP_USER}>`,
      to, subject, html,
    });
    console.log(`[Mail] ✅ Gönderildi → ${to} (${info.messageId})`);
    return true;
  } catch (e) { console.error('[Mail] Hata:', e.message); return false; }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 11 — MAÇ LİSTESİ (Supabase'den)
// ════════════════════════════════════════════════════════════════════
async function loadFixtures() {
  if (!sb) {
    console.warn('[Fixtures] Supabase bağlantısı yok, devam edilemiyor.');
    return [];
  }

  try {
    console.log('[Fixtures] Supabase "fixture_matches" tablosu kontrol ediliyor...');
    const { data, error } = await sb
      .from('fixture_matches')
      .select('*');

    if (error) {
      console.error('[Fixtures] Supabase Okuma Hatası:', error.message);
      return [];
    }

    if (!data || data.length === 0) {
      console.log('[Fixtures] Veritabanında izlenecek maç bulunamadı.');
      return [];
    }

    return data.map(r => ({
      fixture_id: String(r.fixture_id || r.id),
      home_team:  r.home_team || r.data?.teams?.home?.name || '',
      away_team:  r.away_team || r.data?.teams?.away?.name || '',
      kickoff:    r.date || r.kickoff || null,
    })).filter(r => r.home_team && r.away_team);

  } catch (e) {
    console.error('[Fixtures] Beklenmeyen Hata:', e.message);
    return [];
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 12 — CANLI SKOR & ÖĞRENME DÖNGÜSÜ (Kritik)
// ════════════════════════════════════════════════════════════════════
function calcHtFtResult(htHome, htAway, ftHome, ftAway) {
  if (htHome === null || ftHome === null) return null;
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
  if (liveRows.length === 0) return;

  for (const row of liveRows) {
    const fid = String(row.fixture_id);
    if (!matchCache.has(fid)) continue;
    const match = matchCache.get(fid);
    const prevLive = match.liveData || {};

    if (prevLive.status === row.status_short && row.status_short !== 'FT') continue;

    let { htHome, htAway, ftHome, ftAway, htFtResult } = prevLive;

    if (row.status_short === 'HT') {
      htHome = row.home_score; htAway = row.away_score;
      console.log(`  ⏸ HT: ${match.name} | İY ${htHome}-${htAway}`);
    } else if (row.status_short === 'FT' && prevLive.status !== 'FT') {
      ftHome = row.home_score; ftAway = row.away_score;
      htFtResult = calcHtFtResult(htHome, htAway, ftHome, ftAway);
      console.log(`  🏁 FT: ${match.name} | ${htHome}-${htAway} → ${htFtResult} | Öğrenme başlatılıyor...`);
      // 🧠 ÖĞRENME ANI
      if (htFtResult) {
        learnFromMatch(fid, htFtResult);
      }
    }

    match.liveData = { status: row.status_short, htHome, htAway, ftHome, ftAway, htFtResult };
    matchCache.set(fid, match);
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 13 — ANA DÖNGÜ
// ════════════════════════════════════════════════════════════════════
function hoursToKickoff(ko) {
  if (!ko) return 999;
  try { return (new Date(ko) - Date.now()) / 3600000; } catch { return 999; }
}

async function runCycle() {
  cycleCount++;
  const elapsed = Math.round((Date.now() - startTime) / 60000);
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`[Tracker] Döngü #${cycleCount} | ${new Date().toISOString()} | +${elapsed}dk`);
  console.log(`[Memory]  ${Object.keys(memory.patterns).length} pattern | ${memory.totalLearned} toplam öğrenme`);
  console.log('═'.repeat(60));

  // Nesine verisi
  let nesineData;
  try {
    nesineData = await fetchJSON('https://cdnbulten.nesine.com/api/bulten/getprebultenfull');
  } catch (e) { console.error('[Nesine] Hata:', e.message); return; }
  const events = (nesineData?.sg?.EA || []).filter(e => e.TYPE === 1);
  console.log(`[Nesine] ${events.length} event`);

  // Fixture'ları oku (Artık Supabase'den çekiyoruz ve await ekledik)
  const fixtures = await loadFixtures();
  if (fixtures.length === 0) { console.warn('[Fixtures] Boş'); return; }

  const matchesWithSignals = [];
  let matchedCount = 0;

  for (const fix of fixtures) {
    const h2k = hoursToKickoff(fix.kickoff);
    if (h2k > LOOKAHEAD_H || h2k < 0) continue;

    const result = findBestMatch(fix.home_team, fix.away_team, events);
    if (!result) continue;
    matchedCount++;
    const { ev: best } = result;
    const currMarkets = parseMarkets(best.MA);
    if (!Object.keys(currMarkets).length) continue;

    const fid = fix.fixture_id;
    const prev = matchCache.get(fid);
    const changes = prev?.latestMarkets ? calcDelta(prev.latestMarkets, currMarkets) : {};
    const { ev_ft, dep_ft } = ftGroups(changes);

    const ev_ft_cum  = (prev?.ev_ft_cum  || 0) + (ev_ft  < 0 ? ev_ft  : 0);
    const dep_ft_cum = (prev?.dep_ft_cum || 0) + (dep_ft < 0 ? dep_ft : 0);

    const snapshots = prev?.snapshots || [];
    snapshots.push({ time: new Date().toISOString(), markets: currMarkets, changes, ev_ft, dep_ft });
    if (snapshots.length > 10) snapshots.shift();

    // Akıllı sinyal motoru
    const { signals, features, predictions, stateKey } = evaluateSmartSignals(
      currMarkets, changes, { ev_ft_cum, dep_ft_cum }, snapshots
    );

    // Snapshot'a stateKey ekle (öğrenme için)
    if (snapshots.length > 0) snapshots[snapshots.length - 1]._stateKey = stateKey;

    const liveData = prev?.liveData || {};
    matchCache.set(fid, {
      name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, latestMarkets: currMarkets,
      ev_ft_cum, dep_ft_cum, snapshots, liveData,
    });

    if (signals.length === 0) continue;

    // En az ELITE/PREMIER olmalı veya 2+ sinyal
    const hasHighTier = signals.some(s => s.tier === 'ELITE' || s.tier === 'PREMIER');
    if (!hasHighTier && signals.length < MIN_SIGNALS) continue;

    const topLabel = `${signals[0].type}_${signals[0].tier}`;
    if (alreadyFired(fid, topLabel)) continue;

    // Yerel yorum üret
    const interpretation = generateLocalInterpretation({ signals, features, predictions, stateKey });

    matchesWithSignals.push({
      fid, name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, h2k,
      signals, features, interpretation,
      ev_ft_cum, dep_ft_cum,
    });
  }

  console.log(`[Tracker] Eşleşen: ${matchedCount} | Sinyel (yeni): ${matchesWithSignals.length}`);

  if (matchesWithSignals.length === 0) {
    await syncLiveMatches();
    saveCache();
    return;
  }

  // E-posta
  const eliteCount = matchesWithSignals.filter(m => m.signals[0].tier === 'ELITE').length;
  const subject = eliteCount > 0
    ? `💎 ScorePop Adaptive [${eliteCount} ELITE] — ${matchesWithSignals.map(m=>m.signals[0].type).join(', ')}`
    : `🧠 ScorePop Adaptive — ${matchesWithSignals.length} Maç Sinyali`;

  const html = buildEmailHTML(matchesWithSignals, cycleCount);
  const sent = await sendEmail(subject, html);

  if (sent) {
    for (const m of matchesWithSignals) markFired(m.fid, `${m.signals[0].type}_${m.signals[0].tier}`);
  }

  await syncLiveMatches();
  saveCache();
}

// ════════════════════════════════════════════════════════════════════
// MAIN
// ════════════════════════════════════════════════════════════════════
async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║  ScorePop Adaptive v2 — Self-Learning Market Engine      ║');
  console.log(`║  Döngü: ${Math.round(INTERVAL_MS/60000)}dk | Süre: ${Math.round(MAX_RUNTIME_MS/3600000)}sa | DryRun: ${DRY_RUN}  ║`);
  console.log('╚══════════════════════════════════════════════════════════╝');

  loadCache();

  const deadline = startTime + MAX_RUNTIME_MS;
  while (Date.now() < deadline) {
    const cycleStart = Date.now();
    try { await runCycle(); }
    catch (e) { console.error('[Tracker] Döngü hatası:', e.message, e.stack); }

    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    const wait = Math.max(0, INTERVAL_MS - (Date.now() - cycleStart));
    const waitMin = Math.round(wait / 60000 * 10) / 10;
    console.log(`\n[Tracker] Sonraki: ${waitMin}dk | Kalan: ${Math.round(remaining/60000)}dk`);
    if (wait > 0 && remaining > wait) await new Promise(r => setTimeout(r, wait));
  }

  saveCache();
  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║  OTURUM TAMAMLANDI                                       ║');
  console.log(`║  Döngü: ${String(cycleCount).padEnd(47)}║`);
  console.log(`║  İzlenen: ${String(matchCache.size).padEnd(46)}║`);
  console.log(`║  Öğrenilen Pattern: ${String(Object.keys(memory.patterns).length).padEnd(36)}║`);
  console.log('╚══════════════════════════════════════════════════════════╝');
}

main().catch(e => { console.error('[FATAL]', e); process.exit(1); });
