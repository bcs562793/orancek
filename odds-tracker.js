/**
 * ai_tracker.js — ScorePop Adaptive Tracker v3.0
 * ═══════════════════════════════════════════════════════════════════
 * Nesine'den oran çeker → Özellik vektörü çıkarır → Durum kodu üretir →
 * Tarihsel hafızaya sorar → Olasılık/Lift hesaplar → Sinyal üretir.
 * Maç bittiğinde gerçek sonucu hafızaya işler + tahmin doğruluk kaydını günceller.
 *
 * ── v3.0 YENİLİKLERİ ────────────────────────────────────────────────
 *  [NEW-1] SİNYAL KAYDI (memory.pendingSignals):
 *          Her ateşlenen sinyal fixture_id bazında hafızaya alınır.
 *          Kaydedilen: stateKey, topSignal, tier, lift, prob, predictedAt
 *
 *  [NEW-2] SONUÇ ÇÖZÜMLEME (resolvePendingSignal):
 *          FT geldiğinde pendingSignals[fid] açılır, tahmin vs gerçek karşılaştırılır.
 *          memory.signalAccuracy[type] → { fired, correct, accuracy } güncellenir.
 *
 *  [NEW-3] DİNAMİK TİER AYARLAMASI (getAccuracyMultiplier):
 *          Sinyal tipi için doğruluk oranı hesaplanır.
 *          >%45 ve ≥10 örnek → BOOST (+1 tier)
 *          <%20 ve ≥10 örnek → PENALTY (sinyal bastırılır)
 *          Orta → nötr
 *
 *  [NEW-4] HASSAS SINYAL FİLTRESİ:
 *          Lift × accuracy_multiplier < 1.5 olan sinyaller düşürülür.
 *          Bootstrap modunda accuracy filtresi devre dışı (yeterli veri yok).
 *
 *  [NEW-5] ZENGİNLEŞTİRİLMİŞ LOG:
 *          Her sinyal yanında gerçekçi doğruluk oranı gösterilir.
 *          "Bu sinyal tipi: 15 ateşlendi, 7 doğru (%47)" formatı.
 *
 *  [NEW-6] ACCURACY RAPORU (logAccuracyReport):
 *          Her 10 döngüde bir tam doğruluk raporu basılır.
 *
 * ── v2.3'ten korunan düzeltmeler ────────────────────────────────────
 *  [FIX-5] hoursToKickoff: Timezone bilgisi olmayan tarihler UTC+3 kabul edilir.
 *  [FIX-6] Sinyal zamanlaması: ≤SIGNAL_WINDOW_H saat kala ateşlenir.
 *  [FIX-3] HT skoru: '1H'→'HT' geçişinde yakalanır, özel DB sütunu yok.
 *  [FIX-2] Bootstrap: 20 maç öğrenene kadar kural tabanlı sinyal.
 *  [FIX-4] histCount alanı signals nesnesine eklendi.
 *
 * Ortam değişkenleri:
 *   SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_TO
 *   INTERVAL_MS (5dk), MAX_RUNTIME_MS (4.75sa), LOOKAHEAD_HOURS (8)
 *   CACHE_FILE, FIRED_FILE, MEMORY_FILE
 *   DRY_RUN
 *   SUPABASE_URL, SUPABASE_KEY
 *   BOOTSTRAP_THRESHOLD — Kaç maç öğrenene kadar bootstrap (varsayılan 20)
 *   SIGNAL_WINDOW_H     — Maça kaç saat kalana kadar sinyal ateşlenmez (varsayılan 0.5)
 *   ACCURACY_MIN_SAMPLES — Accuracy filtresinin devreye gireceği min örnek sayısı (varsayılan 10)
 *   ACCURACY_PENALTY_THR — Bu oranın altı → sinyal bastır (varsayılan 0.20)
 *   ACCURACY_BOOST_THR   — Bu oranın üstü → tier yükselt (varsayılan 0.45)
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
  signalAccuracy: {},   // [NEW-1] { "X/2": { fired, correct }, ... }
  pendingSignals: {},   // [NEW-1] { "fixture_id": { predictedAt, stateKey, topSignal, ... } }
  version:        3,
  totalLearned:   0,
};
let cycleCount   = 0;
const startTime  = Date.now();

const FOCUS_RESULTS = ['1/1', '2/1', 'X/X', 'X/2', 'X/1', '2/2', '1/2'];

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 0 — DOĞRULUK MOTORU [NEW]
// ══════════════════════════════════════════════════════════════════════

/**
 * [NEW-2] Biten maçta tahmin vs gerçek karşılaştırması.
 * pendingSignals[fid] varsa, actualResult ile karşılaştırır ve
 * memory.signalAccuracy'yi günceller.
 */
function resolvePendingSignal(fid, actualResult) {
  const pending = memory.pendingSignals[fid];
  if (!pending) return;

  const { topSignal, tier, stateKey, predictedAt } = pending;

  if (!memory.signalAccuracy[topSignal]) {
    memory.signalAccuracy[topSignal] = { fired: 0, correct: 0 };
  }

  const acc = memory.signalAccuracy[topSignal];
  // fired zaten markFired'da artırılıyor; burada sadece correct'ı güncelle
  const isCorrect = (topSignal === actualResult);
  if (isCorrect) acc.correct++;

  const accuracy = acc.fired > 0 ? (acc.correct / acc.fired) : 0;

  console.log(
    `  [Accuracy] ${topSignal} (${tier}) → Tahmin: ${topSignal} | Gerçek: ${actualResult}` +
    ` | ${isCorrect ? '✅ DOĞRU' : '❌ YANLIŞ'}` +
    ` | Genel Doğruluk: %${(accuracy * 100).toFixed(1)} (${acc.correct}/${acc.fired})`
  );

  // Çözümlendi, pending'den çıkar
  delete memory.pendingSignals[fid];
}

/**
 * [NEW-3] Sinyal tipinin geçmiş doğruluk oranına göre çarpan döndürür.
 * Yeterli örnek yoksa nötr (1.0) döner.
 * @returns { multiplier: number, label: string, accuracy: number|null }
 */
function getAccuracyMultiplier(signalType) {
  const acc = memory.signalAccuracy[signalType];
  if (!acc || acc.fired < ACCURACY_MIN_SAMPLES) {
    return { multiplier: 1.0, label: 'yetersiz_örnek', accuracy: null };
  }

  const accuracy = acc.correct / acc.fired;

  if (accuracy >= ACCURACY_BOOST_THR) {
    return { multiplier: 1.4, label: `🟢 %${(accuracy*100).toFixed(0)} doğru`, accuracy };
  } else if (accuracy <= ACCURACY_PENALTY_THR) {
    return { multiplier: 0.0, label: `🔴 %${(accuracy*100).toFixed(0)} doğru — bastırıldı`, accuracy };
  } else {
    return { multiplier: 1.0, label: `🟡 %${(accuracy*100).toFixed(0)} doğru`, accuracy };
  }
}

/**
 * [NEW-6] Tüm sinyal tiplerine ait doğruluk raporunu konsola basar.
 */
function logAccuracyReport() {
  const entries = Object.entries(memory.signalAccuracy)
    .filter(([, v]) => v.fired >= 3)
    .map(([type, v]) => ({ type, fired: v.fired, correct: v.correct, accuracy: v.correct / v.fired }))
    .sort((a, b) => b.accuracy - a.accuracy);

  if (entries.length === 0) {
    console.log('[Accuracy] Henüz yeterli sinyal verisi yok.');
    return;
  }

  console.log('\n' + '─'.repeat(55));
  console.log('  📈 SİNYAL DOĞRULUK RAPORU');
  console.log('─'.repeat(55));
  for (const e of entries) {
    const bar    = '█'.repeat(Math.round(e.accuracy * 20));
    const empty  = '░'.repeat(20 - Math.round(e.accuracy * 20));
    const rating = e.accuracy >= ACCURACY_BOOST_THR  ? '✅ BOOST' :
                   e.accuracy <= ACCURACY_PENALTY_THR ? '❌ BASTIR' : '⚡ NORMAL';
    console.log(
      `  ${e.type.padEnd(5)} ${bar}${empty} %${(e.accuracy*100).toFixed(1).padStart(5)} ` +
      `(${e.correct}/${e.fired}) ${rating}`
    );
  }

  // Genel başarı
  const totalFired   = entries.reduce((s, e) => s + e.fired, 0);
  const totalCorrect = entries.reduce((s, e) => s + e.correct, 0);
  console.log('─'.repeat(55));
  console.log(`  TOPLAM: ${totalCorrect}/${totalFired} doğru (%${(totalCorrect/totalFired*100).toFixed(1)})`);
  console.log('─'.repeat(55) + '\n');
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 1 — CACHE & MEMORY
// ══════════════════════════════════════════════════════════════════════
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
      // v2→v3 migration: yeni alanları koru
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
    // ← Pull + rebase ekle
    execSync('git pull --rebase origin main', { stdio: 'pipe' });
    execSync('git push origin main',           { stdio: 'pipe' });
    console.log('[Git] ✅ Push başarılı.');
  } catch (e) {
    if (e.stderr) console.warn('[Git] STDERR:', e.stderr.toString().trim());
    console.warn('[Git] Hata:', e.message);
  }
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 2 — HTTP + NESİNE
// ══════════════════════════════════════════════════════════════════════
function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'Accept':          'application/json',
        'Accept-Encoding': 'identity',
        'Referer':         'https://www.nesine.com/',
        'Origin':          'https://www.nesine.com',
        'User-Agent':      'Mozilla/5.0 (compatible; ScorePop/3.0)',
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

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 3 — TAKIM EŞLEŞTİRME
// ══════════════════════════════════════════════════════════════════════
const TEAM_ALIASES = {
  'not forest':        'nottingham forest',
  'cry. palace':       'crystal palace',
  'r wien amt':        'rapid wien',
  'w bregenz':         'schwarz weiss b',
  'rb bragantino':     'bragantino',
  'new york rb':       'ny red bulls',
  'fc midtjylland':    'midtjylland',
  'pacos de ferreira': 'p ferreira',
  'seattle s':         'seattle sounders',
  'st louis':          's louis city',
  'gabala':            'kabala',
  'rz pellets wac':    'wolfsberger',
  'sw bregenz':        'schwarz weiss b',
  'fc zurich':         'zurih',
  'future fc':         'modern sport club',
  'the new saints':    'tns',
  'vancouver':         'v whitecaps',
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
    for (const u of tb) { if (t.startsWith(u) || u.startsWith(t)) { hit += 0.7; break; } }
  }
  return hit / Math.max(ta.size, tb.size);
}

function findBestMatch(home, away, events) {
  const THRESHOLD    = 0.35;
  const MIN_PER_TEAM = 0.20;
  const ONE_SIDE_HIGH= 0.65;
  const CROSS_MIN    = 0.25;

  let bestNormal = null, bestNormalScore = THRESHOLD - 0.01;
  for (const ev of events) {
    const hs  = tokenSim(normA(home), norm(ev.HN));
    const as_ = tokenSim(normA(away), norm(ev.AN));
    const avg = (hs + as_) / 2;
    if (hs >= MIN_PER_TEAM && as_ >= MIN_PER_TEAM && avg > bestNormalScore) {
      bestNormalScore = avg;
      bestNormal = ev;
    }
  }
  if (bestNormal) return { ev: bestNormal, score: bestNormalScore };

  let bestCross = null, bestCrossScore = -1;
  for (const ev of events) {
    const combos = [
      { s: tokenSim(normA(home), norm(ev.HN)), c: tokenSim(normA(away), norm(ev.AN)) },
      { s: tokenSim(normA(away), norm(ev.HN)), c: tokenSim(normA(home), norm(ev.AN)) },
    ];
    for (const { s, c } of combos) {
      if (s >= ONE_SIDE_HIGH && c >= CROSS_MIN) {
        const confidence = (s + c) / 2;
        if (confidence >= THRESHOLD && confidence > bestCrossScore) {
          bestCrossScore = confidence;
          bestCross = ev;
        }
      }
    }
  }
  return bestCross ? { ev: bestCross, score: bestCrossScore } : null;
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 4 — MARKET PARSE
// ══════════════════════════════════════════════════════════════════════
function parseMarkets(maArr) {
  const m = {};
  if (!Array.isArray(maArr)) return m;
  for (const x of maArr) {
    const id  = x.MTID;
    const oca = x.OCA || [];
    const g   = n => { const o = oca.find(x => x.N === n); return o ? +o.O : 0; };

    if (id === 1  && oca.length === 3) m['1x2']             = { home: g(1), draw: g(2), away: g(3) };
    if (id === 7  && oca.length === 3) m['ht_1x2']          = { home: g(1), draw: g(2), away: g(3) };
    if (id === 9  && oca.length === 3) m['2h_1x2']          = { home: g(1), draw: g(2), away: g(3) };
    if (id === 5  && oca.length === 9) m['ht_ft']           = {
      '1/1': g(1), '1/X': g(2), '1/2': g(3),
      'X/1': g(4), 'X/X': g(5), 'X/2': g(6),
      '2/1': g(7), '2/X': g(8), '2/2': g(9),
    };
    if (id === 12 && oca.length === 2) m['ou25']            = { under: g(1), over: g(2) };
    if (id === 11 && oca.length === 2) m['ou15']            = { under: g(1), over: g(2) };
    if (id === 13 && oca.length === 2) m['ou35']            = { under: g(1), over: g(2) };
    if (id === 38 && oca.length === 2) m['btts']            = { yes: g(1), no: g(2) };
    if (id === 48 && oca.length === 3) m['more_goals_half'] = { first: g(1), equal: g(2), second: g(3) };
    if (id === 3  && oca.length === 3) m['dc']              = { '1x': g(1), '12': g(2), 'x2': g(3) };
  }
  return m;
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 5 — DELTA HESABI
// ══════════════════════════════════════════════════════════════════════
function calcDelta(prev, curr) {
  const ch   = {};
  const keys = ['1x2','ht_1x2','2h_1x2','ht_ft','ou25','ou15','ou35','btts','more_goals_half'];
  for (const k of keys) {
    ch[k] = {};
    const p = prev[k] || {}, c = curr[k] || {};
    for (const sub of Object.keys({ ...p, ...c })) {
      const pv = p[sub], cv = c[sub];
      ch[k][sub] = (pv && cv && pv !== cv) ? +(cv - pv).toFixed(3) : 0;
    }
  }
  return ch;
}

function ftGroups(changes) {
  const s = k => changes?.ht_ft?.[k] || 0;
  return {
    ev_ft:  s('1/1') + s('2/1') + s('X/1'),
    dep_ft: s('1/2') + s('2/2') + s('X/2'),
    bera:   s('1/X') + s('2/X') + s('X/X'),
  };
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 6 — ÖZELLİK ÇIKARIMI & DURUM KODU
// ══════════════════════════════════════════════════════════════════════
function bucket(val, thresholds, labels) {
  if (val === null || val === undefined) return labels[labels.length - 1];
  for (let i = 0; i < thresholds.length; i++) if (val <= thresholds[i]) return labels[i];
  return labels[labels.length - 1];
}

function extractFeatures(markets, changes, cumCache, snapshots) {
  const mk = (k, s) => markets?.[k]?.[s] ?? null;
  const { ev_ft, dep_ft } = ftGroups(changes || {});

  const ms1    = mk('1x2', 'home');
  const ms2    = mk('1x2', 'away');
  const iy1    = mk('ht_1x2', 'home');
  const iy2    = mk('ht_1x2', 'away');
  const sy1    = mk('2h_1x2', 'home');
  const iyms21 = mk('ht_ft', '2/1');
  const iyms22 = mk('ht_ft', '2/2');
  const iyms11 = mk('ht_ft', '1/1');
  const iyms12 = mk('ht_ft', '1/2');
  const au25o  = mk('ou25', 'over');
  const bttsY  = mk('btts', 'yes');
  const dcg2h  = mk('more_goals_half', 'second');

  const f = {
    ms1_bucket:    bucket(ms1,    [1.30, 1.60, 2.50], ['vlow','low','med','high']),
    ms2_bucket:    bucket(ms2,    [1.80, 3.00],        ['low','med','high']),
    iy1_bucket:    bucket(iy1,    [1.70, 2.50],        ['low','med','high']),
    iy2_bucket:    bucket(iy2,    [3.50, 5.00],        ['low','med','high']),
    sy1_bucket:    bucket(sy1,    [1.70, 2.50],        ['low','med','high']),
    iyms21_bucket: bucket(iyms21, [10, 22, 35],        ['vlow','low','med','high']),
    iyms22_bucket: bucket(iyms22, [3, 8, 15],          ['vlow','low','med','high']),
    iyms11_bucket: bucket(iyms11, [3, 6, 12],          ['vlow','low','med','high']),
    iyms12_bucket: bucket(iyms12, [10, 20, 35],        ['vlow','low','med','high']),
    au25o_bucket:  bucket(au25o,  [1.50, 2.00, 2.80],  ['low','med','high','vhigh']),
    btts_bucket:   bucket(bttsY,  [1.50, 2.00],        ['low','med','high']),
    ev_ft_sign:    ev_ft  < -1 ? 'neg' : ev_ft  > 1 ? 'pos' : 'flat',
    dep_ft_sign:   dep_ft < -1 ? 'neg' : dep_ft > 1 ? 'pos' : 'flat',
  };

  const recent = (snapshots || []).slice(-3);
  let evMomentum = 'flat', depMomentum = 'flat';
  if (recent.length >= 2) {
    const evVals  = recent.map(s => { const { ev_ft: e }  = ftGroups(s.changes || {}); return e; });
    const depVals = recent.map(s => { const { dep_ft: d } = ftGroups(s.changes || {}); return d; });
    const evSlope  = evVals[evVals.length - 1]   - evVals[0];
    const depSlope = depVals[depVals.length - 1] - depVals[0];
    evMomentum  = evSlope  < -1.5 ? 'falling' : evSlope  > 1.5 ? 'rising' : 'stable';
    depMomentum = depSlope < -1.5 ? 'falling' : depSlope > 1.5 ? 'rising' : 'stable';
  }

  f.ev_momentum   = evMomentum;
  f.dep_momentum  = depMomentum;
  f.div_ev_to_dep = (f.ev_ft_sign === 'neg' && f.dep_ft_sign === 'pos') ? 'yes' : 'no';
  f.div_strong_ev = (f.ev_ft_sign === 'neg' && f.dep_ft_sign === 'neg') ? 'yes' : 'no';

  return {
    raw: { ms1, ms2, iy1, iy2, sy1, iyms21, iyms22, iyms11, iyms12, au25o, bttsY, dcg2h, ev_ft, dep_ft },
    buckets: f,
  };
}

function generateStateKey(features) {
  const b = features.buckets;
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

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 7 — ÖĞRENME MOTORU
// ══════════════════════════════════════════════════════════════════════
function learnFromMatch(fixtureId, actualHtFt) {
  const match = matchCache.get(fixtureId);
  if (!match || !match.snapshots || match.snapshots.length === 0) return;
  if (!FOCUS_RESULTS.includes(actualHtFt)) return;

  const lastSnap = match.snapshots[match.snapshots.length - 1];
  if (!lastSnap._stateKey) {
    console.warn(`[Learn] fixture=${fixtureId} _stateKey yok, öğrenme atlandı`);
    return;
  }

  const key = lastSnap._stateKey;
  if (!memory.patterns[key]) memory.patterns[key] = {};
  if (!memory.patterns[key][actualHtFt]) {
    memory.patterns[key][actualHtFt] = { count: 0, firstSeen: new Date().toISOString() };
  }
  memory.patterns[key][actualHtFt].count++;
  memory.totalLearned++;
  console.log(`[Learn] "${key}" → ${actualHtFt} (sayı: ${memory.patterns[key][actualHtFt].count})`);
}

function predict(stateKey) {
  const pattern = memory.patterns[stateKey];
  const result  = {};
  for (const r of FOCUS_RESULTS) result[r] = { prob: 0.05, lift: 1.0, count: 0, confidence: 'none' };
  if (!pattern) return result;

  let total = 0;
  for (const r of FOCUS_RESULTS) total += (pattern[r]?.count || 0);
  if (total < 2) return result;

  const baseProb = 0.11;
  for (const r of FOCUS_RESULTS) {
    const cnt  = pattern[r]?.count || 0;
    const prob = cnt / total;
    const lift = prob / baseProb;
    let confidence = 'low';
    if (total >= 10 && prob >= 0.30) confidence = 'high';
    else if (total >= 5 && prob >= 0.20) confidence = 'medium';
    result[r] = { prob: +prob.toFixed(3), lift: +lift.toFixed(2), count: cnt, total, confidence };
  }
  return result;
}

function predictWithSimilarity(stateKey) {
  const direct      = predict(stateKey);
  const directTotal = direct['2/1'].total || 0;
  if (directTotal >= 5) return direct;

  const neighbors   = [];
  const targetParts = stateKey.split('|');
  for (const k of Object.keys(memory.patterns)) {
    const parts = k.split('|');
    let diff = 0;
    for (let i = 0; i < parts.length; i++) if (parts[i] !== targetParts[i]) diff++;
    if (diff === 1) {
      const total = FOCUS_RESULTS.reduce((s, r) => s + (memory.patterns[k][r]?.count || 0), 0);
      if (total >= 3) neighbors.push({ key: k, total });
    }
  }
  if (neighbors.length === 0) return direct;

  const blended   = {};
  let   weightSum = directTotal;
  for (const r of FOCUS_RESULTS) blended[r] = { prob: direct[r].prob * directTotal, count: direct[r].count };

  for (const n of neighbors) {
    const nPred = predict(n.key);
    const w     = n.total * 0.5;
    weightSum  += w;
    for (const r of FOCUS_RESULTS) {
      blended[r].prob  += nPred[r].prob * w;
      blended[r].count += nPred[r].count;
    }
  }

  const final    = {};
  const baseProb = 0.11;
  for (const r of FOCUS_RESULTS) {
    const prob = weightSum > 0 ? blended[r].prob / weightSum : 0;
    final[r]   = {
      prob:       +prob.toFixed(3),
      lift:       +(prob / baseProb).toFixed(2),
      count:      blended[r].count,
      total:      Math.round(weightSum),
      confidence: (weightSum >= 8 && prob >= 0.25) ? 'medium' : 'low',
    };
  }
  return final;
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 8 — AKILLI SİNYAL MOTORU (v3.0 - Accuracy entegreli)
// ══════════════════════════════════════════════════════════════════════
function evaluateSmartSignals(markets, changes, cumCache, snapshots) {
  const features = extractFeatures(markets, changes, cumCache, snapshots);
  const stateKey = generateStateKey(features);
  const raw      = features.raw;
  const b        = features.buckets;

  if (snapshots.length > 0) snapshots[snapshots.length - 1]._stateKey = stateKey;

  const predictions = predictWithSimilarity(stateKey);
  const signals     = [];

  const trendStrength =
    (b.ev_momentum === 'falling' && b.dep_momentum === 'rising') ? 'strong_reversal' :
    (b.ev_momentum === 'falling')                                 ? 'ev_dominant'     :
    (b.dep_momentum === 'falling')                                ? 'dep_dominant'    : 'neutral';

  const hasMemory = memory.totalLearned >= BOOTSTRAP_THRESHOLD;

  // ── [FIX-2] Bootstrap sinyaller ─────────────────────────────────
  if (!hasMemory) {
    const evCum  = cumCache.ev_ft_cum  || 0;
    const depCum = cumCache.dep_ft_cum || 0;

    if (raw.iyms21 && raw.iyms21 <= 12 && evCum <= -4)
      signals.push({ type: '2/1', tier: 'STANDART', rule: `[BOOTSTRAP] İYMS21=${raw.iyms21} ev_cum=${evCum}`, prec: 5.0, lift: 1.5, prob: 0.15, stateKey, trendStrength: 'bootstrap', histCount: 0, accLabel: 'bootstrap' });

    if (raw.ms1 && raw.ms1 <= 1.50 && evCum <= -3)
      signals.push({ type: '1/1', tier: 'STANDART', rule: `[BOOTSTRAP] MS1=${raw.ms1} ev_cum=${evCum}`, prec: 5.0, lift: 1.4, prob: 0.14, stateKey, trendStrength: 'bootstrap', histCount: 0, accLabel: 'bootstrap' });

    if (raw.iyms22 && raw.iyms22 <= 6 && depCum <= -3)
      signals.push({ type: '2/2', tier: 'STANDART', rule: `[BOOTSTRAP] İYMS22=${raw.iyms22} dep_cum=${depCum}`, prec: 5.0, lift: 1.4, prob: 0.14, stateKey, trendStrength: 'bootstrap', histCount: 0, accLabel: 'bootstrap' });

    if (raw.iyms12 && raw.iyms12 <= 12 && depCum <= -4)
      signals.push({ type: '1/2', tier: 'STANDART', rule: `[BOOTSTRAP] İYMS12=${raw.iyms12} dep_cum=${depCum}`, prec: 5.0, lift: 1.4, prob: 0.14, stateKey, trendStrength: 'bootstrap', histCount: 0, accLabel: 'bootstrap' });
  }

  // ── Pattern + Accuracy tabanlı sinyaller [NEW-3][NEW-4] ──────────
  for (const outcome of FOCUS_RESULTS) {
    const p = predictions[outcome];
    if (p.lift < 1.20) continue;

    // [NEW-3] Doğruluk çarpanını al
    const { multiplier, label: accLabel, accuracy } = getAccuracyMultiplier(outcome);

    // [NEW-4] Bastırma: accuracy düşükse sinyali çıkarma
    if (multiplier === 0.0) {
      console.log(`[AccFilter] ${outcome} bastırıldı (${accLabel})`);
      continue;
    }

    let tier      = 'STANDART';
    let rule      = `State: ${stateKey.substring(0, 60)}...`;
    let precision = p.prob * 10;

    // Accuracy boost: lift × multiplier ile hesapla
    const effectiveLift = +(p.lift * multiplier).toFixed(2);

    if (effectiveLift >= 2.50 && p.confidence === 'high') {
      tier = 'ELITE'; precision = Math.min(9.5, p.prob * 10 + 2);
    } else if (effectiveLift >= 2.00 && (p.confidence === 'high' || p.confidence === 'medium')) {
      tier = 'PREMIER'; precision = Math.min(8.5, p.prob * 10 + 1);
    }

    // Accuracy boost varsa tier'ı bir kademe yükselt
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

    signals.push({
      type:         outcome,
      tier,
      rule:         `${rule} | hist=${p.count}/${p.total}`,
      prec:         +precision.toFixed(2),
      lift:         p.lift,
      effectiveLift,
      prob:         p.prob,
      stateKey,
      trendStrength,
      histCount:    p.count,
      accLabel,
      accuracy,     // null veya 0.0–1.0
    });
  }

  const tierW = { ELITE: 3, PREMIER: 2, STANDART: 1 };
  signals.sort((a, c) => (tierW[c.tier] || 0) - (tierW[a.tier] || 0) || c.effectiveLift - a.effectiveLift);

  return { signals, features, predictions, stateKey };
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 9 — YEREL YORUM
// ══════════════════════════════════════════════════════════════════════
function generateLocalInterpretation(matchData) {
  const { signals, features, predictions, stateKey } = matchData;
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

  const hist = predictions[top.type];
  let note = '';
  if      (hist.total >= 10)                    note = `Bu pattern ${hist.total} kez tekrarlandı, ${hist.count} kez ${top.type} geldi (%${(hist.prob*100).toFixed(0)}).`;
  else if (hist.total >= 3)                     note = `Sınırlı örnek (${hist.total}) ama eğilim ${top.type} yönünde.`;
  else if (top.trendStrength === 'bootstrap')   note = `[Bootstrap] Kural tabanlı sinyal — hafıza: ${memory.totalLearned}/${BOOTSTRAP_THRESHOLD}.`;
  else                                          note = 'Yeni pattern, temkinli olun.';

  // [NEW-5] Accuracy notu
  let accNote = '';
  if (top.accuracy !== null && top.accuracy !== undefined) {
    const acc = memory.signalAccuracy[top.type] || {};
    accNote = `\n📏 Geçmiş Doğruluk: %${(top.accuracy*100).toFixed(1)} (${acc.correct}/${acc.fired} ateşlendi) — ${top.accLabel}`;
  } else if (top.trendStrength !== 'bootstrap') {
    accNote = `\n📏 Doğruluk: Henüz yeterli veri yok (<${ACCURACY_MIN_SAMPLES} ateşleme)`;
  }

  return (
    `📊 DURUM: ${stateKey.substring(0, 55)}...\n` +
    `${mkt}\n` +
    `🎯 TAHMİN: ${top.type} | ${top.tier} | Lift: ${top.lift}x (efektif: ${top.effectiveLift}x) | Olas: %${(top.prob*100).toFixed(1)}\n` +
    `📚 ${note}${accNote}\n` +
    `⚡ Trend: ${top.trendStrength} | İYMS21: ${r.iyms21||'?'} | MS1: ${r.ms1||'?'}`
  );
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 10 — SİNYAL LOGGER [NEW-5]
// ══════════════════════════════════════════════════════════════════════
function logSignals(matchesWithSignals, cycleNo) {
  const now       = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
  const tierColor = { ELITE: '💎', PREMIER: '🥇', STANDART: '📊' };

  console.log('\n' + '▓'.repeat(60));
  console.log(`  SİNYAL RAPORU — Döngü #${cycleNo} | ${now}`);
  console.log('▓'.repeat(60));

  for (const m of matchesWithSignals) {
    const top = m.signals[0];
    console.log(`\n${tierColor[top.tier] || '⚪'} ${m.name}`);
    console.log(`   ⏰ Başlangıç: ${m.h2k < 0 ? 'Başladı' : m.h2k < 1 ? Math.round(m.h2k * 60) + ' dk sonra' : m.h2k.toFixed(1) + ' saat sonra'}`);
    console.log(`   📈 Ev kümülâtif: ${m.ev_ft_cum.toFixed(2)} | Dep: ${m.dep_ft_cum.toFixed(2)}`);

    for (const s of m.signals.slice(0, 3)) {
      const accStr = s.accuracy !== null && s.accuracy !== undefined
        ? ` | Doğruluk: %${(s.accuracy*100).toFixed(0)} (${s.accLabel})`
        : ` | Doğruluk: veri bekleniyor`;
      console.log(
        `   ${tierColor[s.tier]} [${s.tier}] ${s.type}` +
        ` | Lift: ${s.lift}x → Efektif: ${s.effectiveLift}x` +
        ` | Olas: %${(s.prob * 100).toFixed(1)}${accStr}`
      );
      console.log(`      ↳ ${s.rule}`);
    }

    if (m.interpretation) {
      console.log('   ─────────────────────────────────');
      for (const line of m.interpretation.split('\n')) {
        if (line.trim()) console.log('   ' + line);
      }
    }
  }

  console.log('\n' + '▓'.repeat(60) + '\n');
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 10b — ALERT HELPERS [NEW-1 entegreli]
// ══════════════════════════════════════════════════════════════════════
function alreadyFired(fixtureId, signalLabel) {
  if (!firedAlerts[fixtureId]) return false;
  return firedAlerts[fixtureId].includes(signalLabel);
}

/**
 * [NEW-1] Sinyali ateşlenmiş olarak işaretle VE pending kaydı oluştur.
 * Böylece FT geldiğinde tahmin vs gerçek karşılaştırılabilir.
 */
function markFired(fixtureId, signalLabel, signalData) {
  if (!firedAlerts[fixtureId]) firedAlerts[fixtureId] = [];
  if (!firedAlerts[fixtureId].includes(signalLabel)) {
    firedAlerts[fixtureId].push(signalLabel);
  }

  // [NEW-1] Sinyal kaydını hafızaya al
  if (signalData) {
    memory.pendingSignals[fixtureId] = {
      predictedAt: new Date().toISOString(),
      stateKey:    signalData.stateKey,
      topSignal:   signalData.type,
      tier:        signalData.tier,
      lift:        signalData.lift,
      effectiveLift: signalData.effectiveLift,
      prob:        signalData.prob,
      signalLabel,
    };

    // fired sayacını artır
    if (!memory.signalAccuracy[signalData.type]) {
      memory.signalAccuracy[signalData.type] = { fired: 0, correct: 0 };
    }
    memory.signalAccuracy[signalData.type].fired++;
  }
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 11 — MAÇ LİSTESİ
// ══════════════════════════════════════════════════════════════════════
async function loadFixtures() {
  if (!sb) { console.warn('[Fixtures] Supabase bağlantısı yok'); return []; }
  try {
    const { data, error } = await sb.from('future_matches').select('*');
    if (error) { console.error('[Fixtures] Hata:', error.message); return []; }
    if (!data || data.length === 0) { console.log('[Fixtures] Maç bulunamadı.'); return []; }
    return data.map(r => ({
      fixture_id: String(r.fixture_id || r.id),
      home_team:  r.home_team || r.data?.teams?.home?.name || '',
      away_team:  r.away_team || r.data?.teams?.away?.name || '',
      kickoff:    r.date || r.kickoff || null,
    })).filter(r => r.home_team && r.away_team);
  } catch (e) { console.error('[Fixtures] Hata:', e.message); return []; }
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 12 — CANLI SKOR & ÖĞRENME [FIX-3 + NEW-2]
// ══════════════════════════════════════════════════════════════════════
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
      .in('status_short', ['1H', 'HT', '2H', 'FT']);
    if (error) { console.error('[Live] Supabase hata:', error.message); return; }
    liveRows = data || [];
  } catch (e) { console.error('[Live] Hata:', e.message); return; }

  console.log(`[Live] ${liveRows.length} aktif/biten maç`);
  if (liveRows.length > 0) console.log('[Live] Örnek:', JSON.stringify(liveRows[0]));

  let matchedLive = 0, learnedCount = 0, resolvedCount = 0;

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
    let ftHome = null, ftAway = null, htFtResult = null;

    if (status === 'HT') {
      htHome = hScore;
      htAway = aScore;
      console.log(`  ⏸ HT: ${match.name} | İY ${htHome}-${htAway}`);

    } else if (status === 'FT' && prevLive.status !== 'FT') {
      ftHome = hScore;
      ftAway = aScore;
      htFtResult = calcHtFtResult(htHome, htAway, ftHome, ftAway);

      console.log(
        `  🏁 FT: ${match.name}` +
        ` | İY: ${htHome ?? '?'}-${htAway ?? '?'}` +
        ` → MS: ${ftHome}-${ftAway}` +
        ` | HT/FT: ${htFtResult || 'hesaplanamadı'}`
      );

      if (htFtResult) {
        // [NEW-2] Önce pending sinyali çöz (tahmin vs gerçek)
        resolvePendingSignal(fid, htFtResult);
        resolvedCount++;

        // Sonra pattern hafızasını güncelle
        learnFromMatch(fid, htFtResult);
        learnedCount++;
      } else {
        console.warn(`  ⚠️ ${match.name}: HT skoru yok — öğrenme/çözümleme atlandı`);
      }
    }

    match.liveData = { status, htHome, htAway, ftHome, ftAway, htFtResult };
    matchCache.set(fid, match);
  }

  if (liveRows.length > 0) {
    console.log(`[Live] Eşleşme: ${matchedLive}/${liveRows.length} | Öğrenilen: ${learnedCount} | Çözümlenen: ${resolvedCount}`);
  }
}

// ══════════════════════════════════════════════════════════════════════
// BÖLÜM 13 — ANA DÖNGÜ
// ══════════════════════════════════════════════════════════════════════

/** [FIX-5] Timezone düzeltmesi: timezone bilgisi yoksa UTC+3 kabul edilir. */
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
  console.log(`[Memory]  ${Object.keys(memory.patterns).length} pattern | ${memory.totalLearned} öğrenme${bootstrapMode ? ` | ⚡ BOOTSTRAP (${memory.totalLearned}/${BOOTSTRAP_THRESHOLD})` : ''}`);
  console.log(`[Accuracy] ${Object.keys(memory.signalAccuracy).length} tip izleniyor | ${Object.keys(memory.pendingSignals).length} bekleyen sinyal`);
  console.log(`[Config]  Sinyal penceresi: ≤${SIGNAL_WINDOW_H * 60} dk | Lookahead: ${LOOKAHEAD_H} sa`);
  console.log('═'.repeat(60));

  // [NEW-6] Her 10 döngüde bir doğruluk raporu
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

    const { signals, features, predictions, stateKey } = evaluateSmartSignals(
      currMarkets, changes, { ev_ft_cum, dep_ft_cum }, snapshots
    );

    const liveData = prev?.liveData || {};
    matchCache.set(fid, {
      name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, latestMarkets: currMarkets,
      ev_ft_cum, dep_ft_cum, snapshots, liveData,
    });

    // [FIX-6] Sinyal zamanlaması
    if (h2k > SIGNAL_WINDOW_H) {
      const minLeft = Math.round(h2k * 60);
      console.log(`[⏳ Bekle] ${fix.home_team} vs ${fix.away_team} | ${minLeft} dk kaldı`);
      continue;
    }

    if (signals.length === 0) continue;

    const hasHighTier = signals.some(s => s.tier === 'ELITE' || s.tier === 'PREMIER');
    const isBootstrap = signals.every(s => s.trendStrength === 'bootstrap');
    if (!hasHighTier && signals.length < MIN_SIGNALS && !isBootstrap) continue;

    const topSignal   = signals[0];
    const topLabel    = `${topSignal.type}_${topSignal.tier}`;
    if (alreadyFired(fid, topLabel)) continue;

    const interpretation = generateLocalInterpretation({ signals, features, predictions, stateKey });
    matchesWithSignals.push({
      fid, name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, h2k,
      signals, features, interpretation,
      ev_ft_cum, dep_ft_cum,
    });
  }

  console.log(`[Tracker] Eşleşen: ${matchedCount} | Yeni sinyal: ${matchesWithSignals.length}`);

  if (matchesWithSignals.length === 0) {
    await syncLiveMatches();
    saveCache();
    return;
  }

  logSignals(matchesWithSignals, cycleCount);

  // [NEW-1] Sinyalleri ateşle + pending kaydını oluştur
  for (const m of matchesWithSignals) {
    const topSignal = m.signals[0];
    const topLabel  = `${topSignal.type}_${topSignal.tier}`;
    markFired(m.fid, topLabel, topSignal);
  }

  await syncLiveMatches();
  saveCache();
}

// ══════════════════════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════════════════════
async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║  ScorePop Adaptive v3.0 — Self-Evaluating Market Engine  ║');
  console.log(`║  Döngü: ${Math.round(INTERVAL_MS/60000)}dk | Süre: ${Math.round(MAX_RUNTIME_MS/3600000)}sa | DryRun: ${String(DRY_RUN).padEnd(6)}║`);
  console.log(`║  Bootstrap eşiği   : ${String(BOOTSTRAP_THRESHOLD).padEnd(36)}║`);
  console.log(`║  Sinyal penceresi  : ≤${String(Math.round(SIGNAL_WINDOW_H * 60) + ' dk').padEnd(35)}║`);
  console.log(`║  Accuracy min örnek: ${String(ACCURACY_MIN_SAMPLES).padEnd(36)}║`);
  console.log(`║  Penalty eşiği     : <%${String((ACCURACY_PENALTY_THR*100).toFixed(0)).padEnd(33)}║`);
  console.log(`║  Boost eşiği       : >%${String((ACCURACY_BOOST_THR*100).toFixed(0)).padEnd(33)}║`);
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
    console.log(`\n[Tracker] Sonraki: ${waitMin}dk | Kalan: ${Math.round(remaining / 60000)}dk`);
    if (wait > 0 && remaining > wait) await new Promise(r => setTimeout(r, wait));
  }

  // Final raporu
  logAccuracyReport();
  saveCache();

  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║  OTURUM TAMAMLANDI                                       ║');
  console.log(`║  Döngü         : ${String(cycleCount).padEnd(40)}║`);
  console.log(`║  İzlenen       : ${String(matchCache.size).padEnd(40)}║`);
  console.log(`║  Pattern       : ${String(Object.keys(memory.patterns).length).padEnd(40)}║`);
  console.log(`║  Öğrenilen     : ${String(memory.totalLearned).padEnd(40)}║`);
  console.log(`║  Sinyal Tipi   : ${String(Object.keys(memory.signalAccuracy).length).padEnd(40)}║`);
  console.log(`║  Bekleyen Sinyal: ${String(Object.keys(memory.pendingSignals).length).padEnd(39)}║`);
  console.log('╚══════════════════════════════════════════════════════════╝');
}

process.on('SIGINT', () => {
  console.log('\n[Sistem] 🛑 Kapanma sinyali alındı — kaydediliyor...');
  logAccuracyReport();
  saveCache();
  console.log('[Sistem] ✅ Veriler kurtarıldı. Kapanıyor.');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n[Sistem] 🛑 SIGTERM alındı — kaydediliyor...');
  saveCache();
  process.exit(0);
});

main().catch(e => { console.error('[FATAL]', e); process.exit(1); });
