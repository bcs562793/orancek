/**
 * ai_tracker.js — ScorePop Adaptive Tracker v3.7
 * ═══════════════════════════════════════════════════════════════════
 * v3.7 değişiklikleri (v3.6-fixed üzerine — kullanıcı analiz bulgularına göre):
 *
 * [V37-1] Market Direction Engine (YENİ BÖLÜM 0.7):
 *   • analyzeMarketDirection(): açılış→kapanış yön analizi
 *   • detectLastMinuteMovements(): maç öncesi son 15dk büyük hareketler
 *   • learnMarketDirectionOutcome(): yön→MS+OU sonuç öğrenme
 *   • getMarketDirectionPrediction(): yön pattern'inden tahmin
 *   • memory.marketDirectionPatterns kalıcı kaydı
 *
 * [V37-2] UNDER Sinyali (kritik eksik düzeltme):
 *   • OU25_UNDER sinyal üretimi eklendi (ou25.under < 1.60 veya öğrenilmiş)
 *   • OU35_UNDER sinyal üretimi eklendi
 *   • resolvePendingSignal: OU25_UNDER / OU35_UNDER doğruluk kontrolü eklendi
 *   • learnFromMatch: UNDER25 / UNDER35 pattern kaydı eklendi
 *
 * [V37-3] Trivial Sinyal Filtresi:
 *   • ms1 < TRIVIAL_ODDS_THR (1.22) → MS_1 sinyali üretilmez
 *   • ms2 < TRIVIAL_ODDS_THR → MS_2 sinyali üretilmez
 *   • Bu eşiğin altında öğrenilmiş accuracy >= 0.80 ise izin verilir
 *
 * [V37-4] Closing Shock (Son-Dakika Hareket) Tespiti:
 *   • Son 15dk içinde %10+ hareket → rapid-fire guard bypass
 *   • Shock snapshot'lar isShock=true ile işaretlenir
 *   • Closing shock olan maçlarda effectiveLift +%20 bonus
 *   • logSignals'a ⚡ kapanış şoku etiketi eklendi
 *
 * [V37-5] Market Yönü → StateKey Entegrasyonu:
 *   • nohtft stateKey: ms1 açılış yönü (8. parça) eklendi
 *   • "ms1dir_falling / stable / rising" bucket
 *   • loadCache: eski 7-parçalı key'ler migration ile temizlenir
 *
 * [V37-6] İş Akışı Öğrenmesi:
 *   • learnFromMatch: market yönü öğrenmesi (learnMarketDirectionOutcome)
 *   • Market yönü → sonuç matrisi kaydedilir (ms1dir × ou25dir × result)
 *   • generateLocalInterpretation: yön analizi + kapanış hareketi notu eklendi
 *
 * v3.6-fixed'ten korunanlar: FIX-P0..P3 tüm düzeltmeler, FIX-A..F, NEW-1..5
 */
'use strict';

const https      = require('https');
const fs         = require('fs');
const nodemailer = require('nodemailer');
const { createClient } = require('@supabase/supabase-js');

// ── Config ────────────────────────────────────────────────────────────
const INTERVAL_MS          = parseInt(process.env.INTERVAL_MS          || '300000');
const MAX_RUNTIME_MS       = parseInt(process.env.MAX_RUNTIME_MS       || '17100000');
const LOOKAHEAD_H          = parseInt(process.env.LOOKAHEAD_HOURS      || '8');
const CACHE_FILE           = process.env.CACHE_FILE   || 'tracker_cache.json';
const FIRED_FILE           = process.env.FIRED_FILE   || 'fired_alerts.json';
const MEMORY_FILE          = process.env.MEMORY_FILE  || 'learned_memory.json';
const DRY_RUN              = process.env.DRY_RUN === 'true';
const BOOTSTRAP_THRESHOLD  = parseInt(process.env.BOOTSTRAP_THRESHOLD  || '20');
const SIGNAL_WINDOW_H      = parseFloat(process.env.SIGNAL_WINDOW_H    || '0.75');
const ACCURACY_MIN_SAMPLES = parseInt(process.env.ACCURACY_MIN_SAMPLES || '10');
const ACCURACY_PENALTY_THR = parseFloat(process.env.ACCURACY_PENALTY_THR || '0.20');
const ACCURACY_BOOST_THR   = parseFloat(process.env.ACCURACY_BOOST_THR   || '0.45');
const MIN_SIGNALS          = 1;
const MAX_MATCH_HISTORY    = 500;

// [FIX-P0-2] Rapid-fire guard
const MIN_SNAP_INTERVAL_MS = 4 * 60 * 1000; // 4 dakika

// [FIX-P1-2] Pending signal maks bekleme
const MAX_PENDING_AGE_H    = 8;

// [FIX-P2-2] OU eşik seviyeleri
const OU25_STRONG_THR = 1.35;
const OU25_WEAK_THR   = 1.50;
const OU35_STRONG_THR = 1.35;
const OU35_WEAK_THR   = 1.50;

// [V37-3] Trivial sinyal eşiği — bu altında MS_1/MS_2 üretilmez
const TRIVIAL_ODDS_THR = 1.30;

// [V37-4] Closing shock eşiği — son 15dk %10+ hareket
const CLOSING_SHOCK_THR = 0.10;

// [V37-1] Market yön eşikleri
const DIR_STRONG_THR = 0.08; // %8+ hareket = "strong"
const DIR_NORMAL_THR = 0.03; // %3-8 = "normal"

// [V37-2] UNDER sinyal koşulları
const UNDER_ODDS_THR   = 1.45; // ou25.under bu altında → under sinyali
const UNDER_OVER_MIN   = 1.80; // ou25.over bu üstünde → under sinyali
const UNDER_LEARNED_THR = 0.55; // öğrenilmiş prob bu üstünde → under sinyali

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const sb = (SUPABASE_URL && SUPABASE_KEY) ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ── State ─────────────────────────────────────────────────────────────
const matchCache = new Map();
let firedAlerts  = {};
let memory       = {
  patterns:               {},
  signalAccuracy:         {},
  pendingSignals:         {},
  marketStats:            {},
  trapPatterns:           {},
  matchHistory:           [],
  marketDirectionPatterns: {}, // [V37-1] YENİ
  version:        5,
  totalLearned:   0,
};
let cycleCount = 0;
const startTime = Date.now();

const FOCUS_RESULTS = ['1/1', '2/1', '1/X', '2/X', 'X/X', 'X/2', 'X/1', '2/2', '1/2'];

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 0 — DOĞRULUK MOTORU
// ════════════════════════════════════════════════════════════════════
const EXPLORE_RATE = 0.0;

function resolvePendingSignal(fid, actualResult, scoreData = null) {
  const pending = memory.pendingSignals[fid];
  if (!pending) return;
  const { topSignal, tier } = pending;
  if (!memory.signalAccuracy[topSignal])
    memory.signalAccuracy[topSignal] = { fired: 0, correct: 0, recent: [] };
  const acc = memory.signalAccuracy[topSignal];

  let isCorrect;
  if (topSignal.startsWith('MS_')) {
    const msPart   = topSignal.split('_')[1];
    const actualMs = actualResult.split('/')[1];
    isCorrect = msPart === actualMs;
  } else if (topSignal === 'OU25_OVER') {
    if (scoreData?.ftHome != null && scoreData?.ftAway != null) {
      isCorrect = (scoreData.ftHome + scoreData.ftAway) > 2.5;
    } else { isCorrect = null; }
  } else if (topSignal === 'OU35_OVER') {
    if (scoreData?.ftHome != null && scoreData?.ftAway != null) {
      isCorrect = (scoreData.ftHome + scoreData.ftAway) > 3.5;
    } else { isCorrect = null; }
  // [V37-2] UNDER doğruluk kontrolü
  } else if (topSignal === 'OU25_UNDER') {
    if (scoreData?.ftHome != null && scoreData?.ftAway != null) {
      isCorrect = (scoreData.ftHome + scoreData.ftAway) <= 2.5;
    } else { isCorrect = null; }
  } else if (topSignal === 'OU35_UNDER') {
    if (scoreData?.ftHome != null && scoreData?.ftAway != null) {
      isCorrect = (scoreData.ftHome + scoreData.ftAway) <= 3.5;
    } else { isCorrect = null; }
  } else {
    isCorrect = topSignal === actualResult;
  }

  if (isCorrect === null) {
    delete memory.pendingSignals[fid];
    return;
  }

  if (isCorrect) acc.correct++;
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

  if (!isCorrect && pending.fingerprint) {
    recordTrapPattern(pending.fingerprint, topSignal, actualResult);
  }

  delete memory.pendingSignals[fid];
}

function getAccuracyMultiplier(signalType) {
  const acc = memory.signalAccuracy[signalType];
  if (!acc || acc.fired < ACCURACY_MIN_SAMPLES)
    return { multiplier: 1.0, label: 'yetersiz_örnek', accuracy: null };

  const recent   = acc.recent || [];
  const accuracy = recent.length >= 5
    ? recent.filter(Boolean).length / recent.length
    : acc.correct / acc.fired;

  if (accuracy >= ACCURACY_BOOST_THR)
    return { multiplier: 1.4, label: `🟢 %${(accuracy*100).toFixed(0)} doğru`, accuracy };

  if (accuracy <= ACCURACY_PENALTY_THR) {
    if (Math.random() < EXPLORE_RATE)
      return { multiplier: 0.6, label: '🔵 keşif', accuracy, isExplore: true };
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
    const bar    = '█'.repeat(Math.round(e.accuracy * 20));
    const empty  = '░'.repeat(20 - Math.round(e.accuracy * 20));
    const rating = e.accuracy >= ACCURACY_BOOST_THR ? '✅ BOOST' :
                   e.accuracy <= ACCURACY_PENALTY_THR ? '❌ BASTIR' : '⚡ NORMAL';
    console.log(`  ${e.type.padEnd(12)} ${bar}${empty} %${(e.accuracy*100).toFixed(1).padStart(5)} (${e.correct}/${e.fired}) ${rating}`);
  }
  const tf = entries.reduce((s, e) => s + e.fired, 0);
  const tc = entries.reduce((s, e) => s + e.correct, 0);
  console.log('─'.repeat(55));
  console.log(`  TOPLAM: ${tc}/${tf} doğru (%${(tc/tf*100).toFixed(1)})`);
  console.log('─'.repeat(55) + '\n');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 0.5 — FİNGERPRİNT, TRAJECTORY, TUZAK & BENZERLİK MOTORU
// ════════════════════════════════════════════════════════════════════

function calcTrajectory(v0, v45, v15) {
  if (!v0 || !v45 || !v15) return 'unknown';
  const chg1 = (v45 - v0)  / v0;
  const chg2 = (v15 - v45) / v45;
  const THR  = 0.04;

  const falling1 = chg1 < -THR;
  const rising1  = chg1 >  THR;
  const falling2 = chg2 < -THR;
  const rising2  = chg2 >  THR;

  if (falling1 && falling2) return 'falling_steady';
  if (falling1 && rising2)  return 'falling_then_up';
  if (rising1  && falling2) return 'rising_then_down';
  if (rising1  && rising2)  return 'rising_steady';
  if (falling1)             return 'falling_flat';
  if (rising1)              return 'rising_flat';
  return 'flat';
}

function getSnapshotNearMinutes(snapshots, kickoff, minutesBefore) {
  if (!kickoff || !snapshots || !snapshots.length) return null;
  const hasTimezone = /Z|[+-]\d{2}:?\d{2}$/.test(String(kickoff));
  const kickoffMs   = hasTimezone
    ? new Date(kickoff).getTime()
    : new Date(kickoff).getTime() - 3 * 3600000;
  const targetMs = kickoffMs - minutesBefore * 60000;
  let best = null, bestDiff = Infinity;
  for (const s of snapshots) {
    const diff = Math.abs(new Date(s.time).getTime() - targetMs);
    if (diff < bestDiff) { bestDiff = diff; best = s; }
  }
  return bestDiff < 30 * 60000 ? best : null;
}

function buildFingerprint(snapshots, kickoff, openingMarkets, closingMarkets) {
  const tLast   = snapshots && snapshots.length ? snapshots[snapshots.length - 1] : null;
  const t45snap = getSnapshotNearMinutes(snapshots, kickoff, 45);
  const t15snap = getSnapshotNearMinutes(snapshots, kickoff, 15);

  const t0     = openingMarkets             || {};
  const tClose = closingMarkets             || tLast?.markets || {};
  const t45    = t45snap?.markets           || tClose;
  const t15    = t15snap?.markets           || tClose;

  const getVal = (m, k, s) => m[k]?.[s] ?? null;

  const MARKET_KEYS = [
    ['ms1',    '1x2',    'home'],
    ['ms2',    '1x2',    'away'],
    ['msx',    '1x2',    'draw'],
    ['iyms21', 'ht_ft',  '2/1'],
    ['iyms22', 'ht_ft',  '2/2'],
    ['iyms11', 'ht_ft',  '1/1'],
    ['iyms12', 'ht_ft',  '1/2'],
    ['ou25o',  'ou25',   'over'],
    ['ou25u',  'ou25',   'under'],
    ['iy1',    'ht_1x2', 'home'],
    ['iy2',    'ht_1x2', 'away'],
    ['bttsY',  'btts',   'yes'],
  ];

  const trajectories = {};
  for (const [key, market, sub] of MARKET_KEYS) {
    trajectories[key] = calcTrajectory(
      getVal(t0, market, sub),
      getVal(t45, market, sub),
      getVal(t15, market, sub)
    );
  }

  const t0vals = {};
  for (const [key, market, sub] of MARKET_KEYS)
    t0vals[key] = getVal(t0, market, sub);

  return { trajectories, t0vals, builtAt: new Date().toISOString() };
}

function buildTrapKey(fingerprint, predictedOutcome) {
  const t = fingerprint?.trajectories || {};
  return [
    `pred_${predictedOutcome}`,
    `ms1_${t.ms1    || 'unk'}`,
    `iy21_${t.iyms21 || 'unk'}`,
    `ou25_${t.ou25o  || 'unk'}`,
  ].join('|');
}

function recordTrapPattern(fingerprint, predictedOutcome, actualResult) {
  if (!memory.trapPatterns) memory.trapPatterns = {};
  const key = buildTrapKey(fingerprint, predictedOutcome);
  if (!memory.trapPatterns[key])
    memory.trapPatterns[key] = { count: 0, outcomes: {}, firstSeen: new Date().toISOString() };
  memory.trapPatterns[key].count++;
  memory.trapPatterns[key].outcomes[actualResult] =
    (memory.trapPatterns[key].outcomes[actualResult] || 0) + 1;
  console.log(`  [Trap] Pattern kaydedildi: ${key} → gerçek: ${actualResult} (${memory.trapPatterns[key].count}x)`);
}

function detectTrapRisk(fingerprint, predictedOutcome) {
  if (!fingerprint || !memory.trapPatterns) return { risk: 0, reason: null };
  const key  = buildTrapKey(fingerprint, predictedOutcome);
  const trap = memory.trapPatterns[key];
  if (!trap || trap.count < 3) return { risk: 0, reason: null };

  const risk = Math.min(0.85, trap.count / 10);
  const topActual = Object.entries(trap.outcomes)
    .sort((a, b) => b[1] - a[1])[0];

  return {
    risk,
    reason: `⚠️ Tuzak riski: bu pattern ${trap.count}x yanlış tahmindi (gerçek: ${topActual?.[0]} ${topActual?.[1]}x)`,
    trapKey: key,
  };
}

function buildOddsVector(markets) {
  const norm = (v, min, max) =>
    (v != null && max > min) ? Math.min(1, Math.max(0, (v - min) / (max - min))) : 0.5;
  return [
    norm(markets['1x2']?.home,        1.1,  5.0),
    norm(markets['1x2']?.away,        1.1,  8.0),
    norm(markets['1x2']?.draw,        2.0,  6.0),
    norm(markets['ht_ft']?.['2/1'],  10.0, 50.0),
    norm(markets['ht_ft']?.['2/2'],   1.5, 20.0),
    norm(markets['ht_ft']?.['1/1'],   1.5, 15.0),
    norm(markets['ou25']?.over,       1.2,  4.0),
    norm(markets['ht_1x2']?.away,     2.0, 10.0),
  ];
}

function cosineSim(a, b) {
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na  += a[i] * a[i];
    nb  += b[i] * b[i];
  }
  return (na > 0 && nb > 0) ? dot / (Math.sqrt(na) * Math.sqrt(nb)) : 0;
}

function findSimilarMatches(currMarkets, topN = 8) {
  const history = memory.matchHistory || [];
  if (history.length < 3) return [];
  const currVec = buildOddsVector(currMarkets);
  return history
    .filter(h => h.actualResult && h.oddsVec)
    .map(h => ({ ...h, sim: cosineSim(currVec, h.oddsVec) }))
    .sort((a, b) => b.sim - a.sim)
    .slice(0, topN)
    .filter(h => h.sim >= 0.90);
}

function findDecisiveMarket(similarMatches, predictedOutcome) {
  if (!similarMatches || similarMatches.length < 3) return null;

  const correct   = similarMatches.filter(m => m.actualResult === predictedOutcome);
  const incorrect = similarMatches.filter(m => m.actualResult !== predictedOutcome);
  if (!correct.length || !incorrect.length) return null;

  const MARKETS = ['ms1', 'ms2', 'iyms21', 'iyms22', 'ou25o', 'iy1', 'iy2'];
  let bestMarket = null, bestDiff = 0;

  for (const mkt of MARKETS) {
    const corrTraj   = correct.map(m => m.trajectories?.[mkt] || 'unknown');
    const incorrTraj = incorrect.map(m => m.trajectories?.[mkt] || 'unknown');
    const corrRate   = corrTraj.filter(t => t === 'falling_steady').length / correct.length;
    const incorrRate = incorrTraj.filter(t => t === 'falling_steady').length / incorrect.length;
    const diff = Math.abs(corrRate - incorrRate);

    if (diff > bestDiff) {
      bestDiff = diff;
      bestMarket = {
        market: mkt,
        correctFallingRate:   +corrRate.toFixed(2),
        incorrectFallingRate: +incorrRate.toFixed(2),
        diff: +diff.toFixed(2),
        label: corrRate > incorrRate
          ? `${mkt} steady düşüşü → doğru tahmin işareti`
          : `${mkt} steady düşüşü → yanlış tahmin işareti (tuzak)`,
      };
    }
  }

  return bestDiff >= 0.30 ? bestMarket : null;
}

function recordMatchHistory(fixtureId, stateKey, actualResult, markets, fingerprint) {
  if (!memory.matchHistory) memory.matchHistory = [];
  memory.matchHistory.push({
    fid:          fixtureId,
    stateKey,
    actualResult,
    oddsVec:      buildOddsVector(markets || {}),
    trajectories: fingerprint?.trajectories || null,
    learnedAt:    new Date().toISOString(),
  });
  if (memory.matchHistory.length > MAX_MATCH_HISTORY) memory.matchHistory.shift();
}

function logTrapReport() {
  const traps = Object.entries(memory.trapPatterns || {})
    .filter(([, v]) => v.count >= 3)
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 10);
  if (!traps.length) return;
  console.log('\n' + '─'.repeat(55));
  console.log('  🪤 TUZAK PATTERN RAPORU');
  console.log('─'.repeat(55));
  for (const [key, val] of traps) {
    const outcomes = Object.entries(val.outcomes).map(([k, v]) => `${k}:${v}x`).join(' ');
    console.log(`  [${val.count}x] ${key.substring(0, 50)}`);
    console.log(`         Gerçek sonuçlar: ${outcomes}`);
  }
  console.log('─'.repeat(55) + '\n');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 0.6 — CACHE MİGRASYON [FIX-P0-1 + V37-5]
// ════════════════════════════════════════════════════════════════════

/**
 * Geçersiz stateKey formatlarını temizle.
 * htft = 6 parça (iyms22d içerir)
 * nohtft = 7 veya 8 parça (nohtft ile başlar)
 * Eski 10-parçalı veya başka formatlardakiler silinir.
 */
function migrateStaleStateKeys() {
  let count = 0;
  for (const [, match] of matchCache.entries()) {
    for (const snap of (match.snapshots || [])) {
      if (!snap._stateKey) continue;
      const parts = snap._stateKey.split('|');
      const isValidHtFt   = parts.length === 6 && snap._stateKey.includes('iyms22d');
      // [V37-5] nohtft artık 7 veya 8 parça (ms1dir eklendi)
      const isValidNoHtFt = (parts.length === 7 || parts.length === 8) && snap._stateKey.startsWith('nohtft');
      if (!isValidHtFt && !isValidNoHtFt) {
        delete snap._stateKey;
        count++;
      }
    }
  }
  if (count > 0)
    console.log(`[Migration] ${count} eski format _stateKey temizlendi — yeniden hesaplanacak`);
  else
    console.log(`[Migration] Tüm _stateKey'ler geçerli formatta`);
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 0.7 — MARKET YÖN ANALİZİ [V37-1]
// ════════════════════════════════════════════════════════════════════

/**
 * Açılış → kapanış yön analizi.
 * Her market için: strong_down | down | stable | up | strong_up
 *
 * Yorum:
 *   ms1 down  = ms1 oranı düştü = Eve para girdi = ev favori güçlendi
 *   ms1 up    = ms1 oranı yükseldi = Evden para çıktı = ev zayıfladı
 *   ou25o down = 2.5 üst oranı düştü = Üste para girdi = gol beklentisi arttı
 *   ou25o up   = 2.5 üst oranı yükseldi = Üsten para çıktı = gol beklentisi azaldı (UNDER)
 */
function analyzeMarketDirection(openingMarkets, closingMarkets) {
  if (!openingMarkets || !closingMarkets) return {};

  const dirs = {};
  const PAIRS = [
    ['1x2',    ['home', 'away', 'draw']],
    ['ht_1x2', ['home', 'away', 'draw']],
    ['ou25',   ['over', 'under']],
    ['ou35',   ['over', 'under']],
    ['btts',   ['yes', 'no']],
    ['ht_ft',  ['1/1', '2/2', '2/1', '1/2', 'X/X']],
  ];

  for (const [mkt, subs] of PAIRS) {
    dirs[mkt] = {};
    const o = openingMarkets[mkt] || {};
    const c = closingMarkets[mkt] || {};
    for (const sub of subs) {
      const ov = o[sub], cv = c[sub];
      if (!ov || !cv || ov === 0) { dirs[mkt][sub] = 'unknown'; continue; }
      const pct = (cv - ov) / ov;
      if      (pct <= -DIR_STRONG_THR) dirs[mkt][sub] = 'strong_down';
      else if (pct <= -DIR_NORMAL_THR) dirs[mkt][sub] = 'down';
      else if (pct >=  DIR_STRONG_THR) dirs[mkt][sub] = 'strong_up';
      else if (pct >=  DIR_NORMAL_THR) dirs[mkt][sub] = 'up';
      else                              dirs[mkt][sub] = 'stable';
    }
  }
  return dirs;
}

/**
 * ms1 açılış oranından yön bucket üretir (nohtft stateKey için).
 * Açılış oranı yoksa 'unknown' döner.
 */
function getMs1DirBucket(openingMarkets, closingMarkets) {
  const open  = openingMarkets?.['1x2']?.home;
  const close = closingMarkets?.['1x2']?.home;
  if (!open || !close) return 'unknown';
  const pct = (close - open) / open;
  if (pct <= -DIR_STRONG_THR) return 'falling';  // Eve güçlü para girdi
  if (pct <= -DIR_NORMAL_THR) return 'falling';
  if (pct >=  DIR_NORMAL_THR) return 'rising';   // Evden para çıktı
  return 'stable';
}

/**
 * Son 15 dakika içindeki büyük oran hareketlerini tespit eder.
 * Returning: [{ market, sub, fromVal, toVal, pctChange, direction }] | null
 */
function detectLastMinuteMovements(snapshots, kickoff) {
  if (!kickoff || !snapshots || snapshots.length < 2) return null;

  const closing = getSnapshotNearMinutes(snapshots, kickoff, 10);
  const prior   = getSnapshotNearMinutes(snapshots, kickoff, 45);
  if (!closing || !prior) return null;

  // Aynı snapshot ise hareket yok
  if (closing.time === prior.time) return null;

  const closeMkts = closing.markets || {};
  const priorMkts = prior.markets || {};
  const movements = [];

  const CHECK_PAIRS = [
    ['1x2',    ['home', 'away', 'draw']],
    ['ou25',   ['over', 'under']],
    ['ou35',   ['over', 'under']],
    ['btts',   ['yes']],
    ['ht_1x2', ['home', 'away']],
  ];

  for (const [mkt, subs] of CHECK_PAIRS) {
    for (const sub of subs) {
      const fromVal = priorMkts[mkt]?.[sub];
      const toVal   = closeMkts[mkt]?.[sub];
      if (!fromVal || !toVal || fromVal === 0) continue;
      const pct = (toVal - fromVal) / fromVal;
      if (Math.abs(pct) >= CLOSING_SHOCK_THR) {
        movements.push({
          market:    mkt,
          sub,
          fromVal:   +fromVal.toFixed(2),
          toVal:     +toVal.toFixed(2),
          pctChange: +(pct * 100).toFixed(1),
          direction: pct < 0 ? 'down' : 'up',
        });
      }
    }
  }

  return movements.length > 0 ? movements : null;
}

/**
 * Market yönü → MS + OU sonuç öğrenmesi.
 * Key: "dir|ms1_{dir}|ms2_{dir}|ou25o_{dir}"
 * Değer: { count, ms:{'1':n,'X':n,'2':n}, ou:{OVER25:n,UNDER25:n,OVER35:n,UNDER35:n} }
 */
function learnMarketDirectionOutcome(fixtureId, actualHtFt, scoreData) {
  const match = matchCache.get(fixtureId);
  if (!match) return;

  const directions = analyzeMarketDirection(match.openingMarkets, match.closingMarkets);
  if (!Object.keys(directions).length) return;

  const ms1Dir  = directions['1x2']?.home   || 'unknown';
  const ms2Dir  = directions['1x2']?.away   || 'unknown';
  const ou25Dir = directions['ou25']?.over  || 'unknown';

  // Yön anahtarı
  const dirKey = `dir|ms1_${ms1Dir}|ms2_${ms2Dir}|ou25o_${ou25Dir}`;

  if (!memory.marketDirectionPatterns) memory.marketDirectionPatterns = {};
  if (!memory.marketDirectionPatterns[dirKey])
    memory.marketDirectionPatterns[dirKey] = {
      count: 0,
      ms:   { '1': 0, 'X': 0, '2': 0 },
      ou:   { OVER25: 0, UNDER25: 0, OVER35: 0, UNDER35: 0 },
      firstSeen: new Date().toISOString(),
    };

  const pat = memory.marketDirectionPatterns[dirKey];
  pat.count++;

  // MS sonucu (1, X, 2)
  const msResult = actualHtFt.split('/')[1];
  if (pat.ms[msResult] !== undefined) pat.ms[msResult]++;

  // OU sonucu
  if (scoreData?.ftHome != null && scoreData?.ftAway != null) {
    const goals = scoreData.ftHome + scoreData.ftAway;
    if (goals > 2.5) pat.ou.OVER25++;  else pat.ou.UNDER25++;
    if (goals > 3.5) pat.ou.OVER35++;  else pat.ou.UNDER35++;
  }

  console.log(`  [DirLearn] ${dirKey} → MS:${msResult} (${pat.count}. kayıt)`);
}

/**
 * Market yönü pattern'inden MS ve OU tahmini üretir.
 * Yeterli veri yoksa null döner.
 */
function getMarketDirectionPrediction(openingMarkets, closingMarkets) {
  if (!openingMarkets || !closingMarkets) return null;

  const directions = analyzeMarketDirection(openingMarkets, closingMarkets);
  const ms1Dir  = directions['1x2']?.home   || 'unknown';
  const ms2Dir  = directions['1x2']?.away   || 'unknown';
  const ou25Dir = directions['ou25']?.over  || 'unknown';

  const dirKey = `dir|ms1_${ms1Dir}|ms2_${ms2Dir}|ou25o_${ou25Dir}`;
  const pat    = memory.marketDirectionPatterns?.[dirKey];
  if (!pat || pat.count < 3) return null;

  const msTotal = (pat.ms['1'] || 0) + (pat.ms['X'] || 0) + (pat.ms['2'] || 0);
  const msBest  = Object.entries(pat.ms).sort((a, b) => b[1] - a[1])[0];

  const ouTotal      = (pat.ou?.OVER25 || 0) + (pat.ou?.UNDER25 || 0);
  const ouOverProb   = ouTotal > 0 ? (pat.ou.OVER25  || 0) / ouTotal : 0.5;
  const ouUnderProb  = ouTotal > 0 ? (pat.ou.UNDER25 || 0) / ouTotal : 0.5;

  const ou35Total    = (pat.ou?.OVER35 || 0) + (pat.ou?.UNDER35 || 0);
  const ou35OverProb = ou35Total > 0 ? (pat.ou.OVER35 || 0) / ou35Total : 0.5;

  return {
    dirKey,
    directions,
    msBest:       msBest?.[0],
    msProb:       msTotal > 0 ? msBest[1] / msTotal : 0,
    msCount:      pat.count,
    ouOverProb,
    ouUnderProb,
    ou35OverProb,
    ou35UnderProb: ou35Total > 0 ? (pat.ou.UNDER35 || 0) / ou35Total : 0.5,
    count:        pat.count,
  };
}

/**
 * Market yön özeti için insan-okunabilir etiket.
 * Eve para girdi / Depoya para girdi / Üste para girdi / Alta para girdi
 */
function describeMarketDirection(directions) {
  const notes = [];
  const ms1d = directions?.['1x2']?.home;
  const ms2d = directions?.['1x2']?.away;
  const ou25d = directions?.['ou25']?.over;

  if (ms1d === 'strong_down' || ms1d === 'down')
    notes.push('Eve para girdi (ms1 düştü)');
  else if (ms1d === 'strong_up' || ms1d === 'up')
    notes.push('Evden para çıktı (ms1 yükseldi)');

  if (ms2d === 'strong_down' || ms2d === 'down')
    notes.push('Depo güçlendi (ms2 düştü)');
  else if (ms2d === 'strong_up' || ms2d === 'up')
    notes.push('Depo zayıfladı (ms2 yükseldi)');

  if (ou25d === 'strong_down' || ou25d === 'down')
    notes.push('Üste para girdi (ou25 over düştü)');
  else if (ou25d === 'strong_up' || ou25d === 'up')
    notes.push('Alta para girdi (ou25 over yükseldi → UNDER)');

  return notes.length > 0 ? notes.join(' | ') : 'Piyasa hareketsiz kaldı';
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
        patterns:                loaded.patterns                || {},
        signalAccuracy:          loaded.signalAccuracy          || {},
        pendingSignals:          loaded.pendingSignals          || {},
        marketStats:             loaded.marketStats             || {},
        trapPatterns:            loaded.trapPatterns            || {},
        matchHistory:            loaded.matchHistory            || [],
        marketDirectionPatterns: loaded.marketDirectionPatterns || {}, // [V37-1]
        version:        5,
        totalLearned:   loaded.totalLearned || 0,
      };
      console.log(
        `[Memory] ${Object.keys(memory.patterns).length} pattern` +
        ` | ${memory.totalLearned} öğrenme` +
        ` | ${Object.keys(memory.signalAccuracy).length} doğruluk kaydı` +
        ` | ${Object.keys(memory.pendingSignals).length} bekleyen sinyal` +
        ` | ${Object.keys(memory.trapPatterns).length} tuzak pattern` +
        ` | ${memory.matchHistory.length} maç geçmişi` +
        ` | ${Object.keys(memory.marketDirectionPatterns).length} yön pattern`
      );
    } catch (e) { console.warn('[Memory] Yüklenemedi:', e.message); }
  }

  migrateStaleStateKeys();

  for (const [type, acc] of Object.entries(memory.signalAccuracy)) {
    if (!acc.recent) acc.recent = [];
    if (acc.fired >= ACCURACY_MIN_SAMPLES) {
      const accuracy = acc.correct / acc.fired;
      if (accuracy <= ACCURACY_PENALTY_THR)
        console.log(`[AccWarn] ${type} doğruluk düşük: %${(accuracy*100).toFixed(0)} — bastırılacak`);
    }
  }

  // [FIX-C] v3.7: nohtft 7 veya 8 parça
  let stalePending = 0;
  for (const [fid, p] of Object.entries(memory.pendingSignals)) {
    const sk    = p.stateKey || '';
    const parts = sk.split('|');
    const isHtFtFormat   = parts.length === 6 && sk.includes('iyms22d');
    const isNoHtFtFormat = (parts.length === 7 || parts.length === 8) && sk.startsWith('nohtft');
    if (!isHtFtFormat && !isNoHtFtFormat) {
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
  fs.writeFileSync(CACHE_FILE,  JSON.stringify(obj,        null, 2));
  fs.writeFileSync(FIRED_FILE,  JSON.stringify(firedAlerts, null, 2));
  fs.writeFileSync(MEMORY_FILE, JSON.stringify(memory,      null, 2));
  pushToGit();
}

function pushToGit() {
  const { execSync } = require('child_process');
  try {
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
    const msg = `chore: memory update ${new Date().toISOString().slice(0, 16).replace('T', ' ')}`;
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
        'User-Agent':      'Mozilla/5.0 (compatible; ScorePop/3.7)',
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
  const TH=0.35, MIN=0.20, ONE=0.65, CROSS=0.25;
  let bN=null, bNS=TH-0.01;
  for (const ev of events) {
    const hs  = tokenSim(normA(home), norm(ev.HN));
    const as  = tokenSim(normA(away), norm(ev.AN));
    const avg = (hs + as) / 2;
    if (hs >= MIN && as >= MIN && avg > bNS) { bNS = avg; bN = ev; }
  }
  if (bN) return { ev: bN, score: bNS };
  let bC=null, bCS=-1;
  for (const ev of events) {
    const combos = [
      { s: tokenSim(normA(home), norm(ev.HN)), c: tokenSim(normA(away), norm(ev.AN)) },
      { s: tokenSim(normA(away), norm(ev.HN)), c: tokenSim(normA(home), norm(ev.AN)) },
    ];
    for (const { s, c } of combos) {
      if (s >= ONE && c >= CROSS) {
        const conf = (s + c) / 2;
        if (conf >= TH && conf > bCS) { bCS = conf; bC = ev; }
      }
    }
  }
  return bC ? { ev: bC, score: bCS } : null;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 4 — MARKET PARSE
// ════════════════════════════════════════════════════════════════════
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
    if (id === 2  && oca.length === 2) m['ou45']            = { under: g(1), over: g(2) };
    if (id === 14 && oca.length === 2) m['ht_ou15']         = { under: g(1), over: g(2) };
    if (id === 30 && oca.length === 2) m['ht_ou05']         = { under: g(1), over: g(2) };
    if (id === 10 && oca.length === 3) m['dc_2h']           = { '1x': g(1), '12': g(2), 'x2': g(3) };
  }
  return m;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 5 — DELTA HESABI
// ════════════════════════════════════════════════════════════════════
function calcDelta(prev, curr) {
  const ch   = {};
  const keys = ['1x2','ht_1x2','2h_1x2','ht_ft','ou25','ou15','ou35','ou45','btts','more_goals_half','ht_ou15','ht_ou05','dc','dc_2h'];
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

function calcMoneyFlow(markets, changes) {
  const htft = changes?.ht_ft || {};
  const htft_ev  = (htft['1/1'] || 0) + (htft['2/1'] || 0) + (htft['X/1'] || 0);
  const htft_dep = (htft['1/2'] || 0) + (htft['2/2'] || 0) + (htft['X/2'] || 0);
  const bera     = (htft['1/X'] || 0) + (htft['2/X'] || 0) + (htft['X/X'] || 0);
  const hasHtFtSignal = Object.values(htft).some(v => v !== 0);

  const ms1d = changes?.['1x2']?.home    || 0;
  const ms2d = changes?.['1x2']?.away    || 0;
  const iy1d = changes?.['ht_1x2']?.home || 0;
  const iy2d = changes?.['ht_1x2']?.away || 0;
  const SCALE = 15;

  const ev_proxy  = (ms1d + iy1d * 0.5) * SCALE;
  const dep_proxy = (ms2d + iy2d * 0.5) * SCALE;

  return {
    ev_ft:  hasHtFtSignal ? htft_ev  + ev_proxy  * 0.2 : ev_proxy,
    dep_ft: hasHtFtSignal ? htft_dep + dep_proxy * 0.2 : dep_proxy,
    bera,
  };
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 6 — ÖZELLİK ÇIKARIMI & DURUM KODU
// ════════════════════════════════════════════════════════════════════
function bucket(val, thresholds, labels) {
  if (val === null || val === undefined) return 'none';
  for (let i = 0; i < thresholds.length; i++) if (val <= thresholds[i]) return labels[i];
  return labels[labels.length - 1];
}

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
  }
}

function dynamicBucket(value, marketKey, fallbackThresholds, fallbackLabels) {
  if (value == null) return 'none';
  const stat = memory.marketStats?.[marketKey];
  if (!stat?.q25 || stat.values.length < 100)
    return bucket(value, fallbackThresholds, fallbackLabels);
  if (value <= stat.q10) return 'vlow';
  if (value <= stat.q25) return 'low';
  if (value <= stat.q75) return 'med';
  if (value <= stat.q90) return 'high';
  return 'vhigh';
}

function analyzeFeatureImportance() {
  const importance = {};
  for (const [stateKey, outcomes] of Object.entries(memory.patterns)) {
    const parts = stateKey.split('|');
    const total = Object.values(outcomes).reduce((s, v) => s + (v.count || 0), 0);
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

  // [V37-1] Market yön pattern raporu
  const topDirPats = Object.entries(memory.marketDirectionPatterns || {})
    .filter(([, v]) => v.count >= 3)
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 8);
  if (topDirPats.length > 0) {
    console.log('  📐 MARKET YÖN PATTERN RAPORU');
    console.log('─'.repeat(55));
    for (const [key, val] of topDirPats) {
      const msTotal = (val.ms['1']||0) + (val.ms['X']||0) + (val.ms['2']||0);
      const ouTotal = (val.ou?.OVER25||0) + (val.ou?.UNDER25||0);
      const msStr   = msTotal > 0
        ? `MS: 1=%${((val.ms['1']||0)/msTotal*100).toFixed(0)} X=%${((val.ms['X']||0)/msTotal*100).toFixed(0)} 2=%${((val.ms['2']||0)/msTotal*100).toFixed(0)}`
        : 'MS: -';
      const ouStr   = ouTotal > 0
        ? `OU: üst=%${((val.ou.OVER25||0)/ouTotal*100).toFixed(0)} alt=%${((val.ou.UNDER25||0)/ouTotal*100).toFixed(0)}`
        : 'OU: -';
      console.log(`  [${val.count}x] ${key.replace('dir|','').substring(0, 40)}`);
      console.log(`        ${msStr} | ${ouStr}`);
    }
    console.log('─'.repeat(55) + '\n');
  }

  return ranked;
}

function calcFromOpen(openingMarkets, currMarkets) {
  const result = {};
  const o = openingMarkets || {};
  const c = currMarkets    || {};
  const pct = (open, curr) =>
    (open && curr && open !== 0) ? +((curr - open) / open).toFixed(3) : null;

  result.ms1_drop     = pct(o['1x2']?.home,            c['1x2']?.home);
  result.ms2_drop     = pct(o['1x2']?.away,            c['1x2']?.away);
  result.msx_drop     = pct(o['1x2']?.draw,            c['1x2']?.draw);
  result.iy1_drop     = pct(o['ht_1x2']?.home,         c['ht_1x2']?.home);
  result.iy2_drop     = pct(o['ht_1x2']?.away,         c['ht_1x2']?.away);
  result.iyx_drop     = pct(o['ht_1x2']?.draw,         c['ht_1x2']?.draw);
  result.sy1_drop     = pct(o['2h_1x2']?.home,         c['2h_1x2']?.home);
  result.sy2_drop     = pct(o['2h_1x2']?.away,         c['2h_1x2']?.away);
  result.syx_drop     = pct(o['2h_1x2']?.draw,         c['2h_1x2']?.draw);
  result.iyms11_drop  = pct(o['ht_ft']?.['1/1'],       c['ht_ft']?.['1/1']);
  result.iyms22_drop  = pct(o['ht_ft']?.['2/2'],       c['ht_ft']?.['2/2']);
  result.iyms21_drop  = pct(o['ht_ft']?.['2/1'],       c['ht_ft']?.['2/1']);
  result.iyms12_drop  = pct(o['ht_ft']?.['1/2'],       c['ht_ft']?.['1/2']);
  result.iymsxx_drop  = pct(o['ht_ft']?.['X/X'],       c['ht_ft']?.['X/X']);
  result.iymsx1_drop  = pct(o['ht_ft']?.['X/1'],       c['ht_ft']?.['X/1']);
  result.iymsx2_drop  = pct(o['ht_ft']?.['X/2'],       c['ht_ft']?.['X/2']);
  result.iyms1x_drop  = pct(o['ht_ft']?.['1/X'],       c['ht_ft']?.['1/X']);
  result.iyms2x_drop  = pct(o['ht_ft']?.['2/X'],       c['ht_ft']?.['2/X']);
  result.ou15o_drop   = pct(o['ou15']?.over,            c['ou15']?.over);
  result.ou15u_drop   = pct(o['ou15']?.under,           c['ou15']?.under);
  result.ou25o_drop   = pct(o['ou25']?.over,            c['ou25']?.over);
  result.ou25u_drop   = pct(o['ou25']?.under,           c['ou25']?.under);
  result.ou35o_drop   = pct(o['ou35']?.over,            c['ou35']?.over);
  result.ou35u_drop   = pct(o['ou35']?.under,           c['ou35']?.under);
  result.ou45o_drop   = pct(o['ou45']?.over,            c['ou45']?.over);
  result.ou45u_drop   = pct(o['ou45']?.under,           c['ou45']?.under);
  result.htou15o_drop = pct(o['ht_ou15']?.over,         c['ht_ou15']?.over);
  result.htou15u_drop = pct(o['ht_ou15']?.under,        c['ht_ou15']?.under);
  result.htou05o_drop = pct(o['ht_ou05']?.over,         c['ht_ou05']?.over);
  result.htou05u_drop = pct(o['ht_ou05']?.under,        c['ht_ou05']?.under);
  result.bttsy_drop   = pct(o['btts']?.yes,             c['btts']?.yes);
  result.bttsn_drop   = pct(o['btts']?.no,              c['btts']?.no);
  result.dc1x_drop    = pct(o['dc']?.['1x'],            c['dc']?.['1x']);
  result.dc12_drop    = pct(o['dc']?.['12'],             c['dc']?.['12']);
  result.dcx2_drop    = pct(o['dc']?.['x2'],            c['dc']?.['x2']);
  result.dc2h1x_drop  = pct(o['dc_2h']?.['1x'],         c['dc_2h']?.['1x']);
  result.dc2h12_drop  = pct(o['dc_2h']?.['12'],          c['dc_2h']?.['12']);
  result.dc2hx2_drop  = pct(o['dc_2h']?.['x2'],         c['dc_2h']?.['x2']);
  result.mgh1_drop    = pct(o['more_goals_half']?.first,  c['more_goals_half']?.first);
  result.mgh2_drop    = pct(o['more_goals_half']?.second, c['more_goals_half']?.second);
  return result;
}

function extractFeatures(markets, changes, cumCache, snapshots, openingMarkets) {
  const mk = (k, s) => markets?.[k]?.[s] ?? null;

  const { ev_ft, dep_ft } = calcMoneyFlow(markets, changes || {});

  const ms1    = mk('1x2',    'home');
  const ms2    = mk('1x2',    'away');
  const iy1    = mk('ht_1x2', 'home');
  const iy2    = mk('ht_1x2', 'away');
  const sy1    = mk('2h_1x2', 'home');
  const iyms21 = mk('ht_ft',  '2/1');
  const iyms22 = mk('ht_ft',  '2/2');
  const iyms11 = mk('ht_ft',  '1/1');
  const iyms12 = mk('ht_ft',  '1/2');
  const au25o  = mk('ou25',   'over');
  const bttsY  = mk('btts',   'yes');
  const dcg2h  = mk('more_goals_half', 'second');
  const hasHtFt = !!(markets?.ht_ft);

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

  const drops   = calcFromOpen(openingMarkets, markets);
  const mainThr = [-0.20, -0.10, -0.05];
  const mainLbl = ['heavy', 'mod', 'light', 'flat'];
  const htftThr = [-0.50, -0.30, -0.10];
  const htftLbl = ['heavy', 'mod', 'light', 'flat'];
  const ouThr   = [-0.25, -0.12, -0.05];
  const ouLbl   = ['heavy', 'mod', 'light', 'flat'];

  const f = {
    ms1_bucket:    dynamicBucket(ms1,    'ms1',    [1.30, 1.60, 2.50], ['vlow', 'low', 'med', 'high']),
    ms2_bucket:    dynamicBucket(ms2,    'ms2',    [1.80, 3.00],        ['low', 'med', 'high']),
    iy1_bucket:    dynamicBucket(iy1,    'iy1',    [1.70, 2.50],        ['low', 'med', 'high']),
    iy2_bucket:    dynamicBucket(iy2,    'iy2',    [3.50, 5.00],        ['low', 'med', 'high']),
    sy1_bucket:    dynamicBucket(sy1,    'sy1',    [1.70, 2.50],        ['low', 'med', 'high']),
    iyms21_bucket: dynamicBucket(iyms21, 'iyms21', [10, 22, 35],        ['vlow', 'low', 'med', 'high']),
    iyms22_bucket: dynamicBucket(iyms22, 'iyms22', [3, 8, 15],          ['vlow', 'low', 'med', 'high']),
    iyms11_bucket: dynamicBucket(iyms11, 'iyms11', [3, 6, 12],          ['vlow', 'low', 'med', 'high']),
    iyms12_bucket: dynamicBucket(iyms12, 'iyms12', [10, 20, 35],        ['vlow', 'low', 'med', 'high']),
    au25o_bucket:  dynamicBucket(au25o,  'au25o',  [1.50, 2.00, 2.80],  ['low', 'med', 'high', 'vhigh']),
    btts_bucket:   dynamicBucket(bttsY,  'bttsY',  [1.50, 2.00],        ['low', 'med', 'high']),
    ev_ft_sign:    ev_ft  < -1 ? 'neg' : ev_ft  > 1 ? 'pos' : 'flat',
    dep_ft_sign:   dep_ft < -1 ? 'neg' : dep_ft > 1 ? 'pos' : 'flat',
    ms1_drop:      bucket(drops.ms1_drop,     mainThr, mainLbl),
    ms2_drop:      bucket(drops.ms2_drop,     mainThr, mainLbl),
    msx_drop:      bucket(drops.msx_drop,     mainThr, mainLbl),
    iy1_drop:      bucket(drops.iy1_drop,     mainThr, mainLbl),
    iy2_drop:      bucket(drops.iy2_drop,     mainThr, mainLbl),
    iyx_drop:      bucket(drops.iyx_drop,     mainThr, mainLbl),
    sy1_drop:      bucket(drops.sy1_drop,     mainThr, mainLbl),
    sy2_drop:      bucket(drops.sy2_drop,     mainThr, mainLbl),
    iyms11_drop:   bucket(drops.iyms11_drop,  htftThr, htftLbl),
    iyms22_drop:   bucket(drops.iyms22_drop,  htftThr, htftLbl),
    iyms21_drop:   bucket(drops.iyms21_drop,  htftThr, htftLbl),
    iyms12_drop:   bucket(drops.iyms12_drop,  htftThr, htftLbl),
    iymsxx_drop:   bucket(drops.iymsxx_drop,  htftThr, htftLbl),
    iymsx1_drop:   bucket(drops.iymsx1_drop,  htftThr, htftLbl),
    iymsx2_drop:   bucket(drops.iymsx2_drop,  htftThr, htftLbl),
    iyms1x_drop:   bucket(drops.iyms1x_drop,  htftThr, htftLbl),
    iyms2x_drop:   bucket(drops.iyms2x_drop,  htftThr, htftLbl),
    ou15o_drop:    bucket(drops.ou15o_drop,   ouThr, ouLbl),
    ou25o_drop:    bucket(drops.ou25o_drop,   ouThr, ouLbl),
    ou25u_drop:    bucket(drops.ou25u_drop,   ouThr, ouLbl),
    ou35o_drop:    bucket(drops.ou35o_drop,   ouThr, ouLbl),
    ou45o_drop:    bucket(drops.ou45o_drop,   ouThr, ouLbl),
    htou15o_drop:  bucket(drops.htou15o_drop, ouThr, ouLbl),
    htou05o_drop:  bucket(drops.htou05o_drop, ouThr, ouLbl),
    bttsy_drop:    bucket(drops.bttsy_drop,   ouThr, ouLbl),
    bttsn_drop:    bucket(drops.bttsn_drop,   ouThr, ouLbl),
    dc1x_drop:     bucket(drops.dc1x_drop,    mainThr, mainLbl),
    dc12_drop:     bucket(drops.dc12_drop,    mainThr, mainLbl),
    dcx2_drop:     bucket(drops.dcx2_drop,    mainThr, mainLbl),
    mgh2_drop:     bucket(drops.mgh2_drop,    ouThr, ouLbl),
  };

  const recent = (snapshots || []).slice(-3);
  let evMomentum = 'flat', depMomentum = 'flat';
  if (recent.length >= 2) {
    const evVals  = recent.map(s => { const { ev_ft: e }  = calcMoneyFlow(s.markets || {}, s.changes || {}); return e; });
    const depVals = recent.map(s => { const { dep_ft: d } = calcMoneyFlow(s.markets || {}, s.changes || {}); return d; });
    const evSlope  = evVals[evVals.length - 1]   - evVals[0];
    const depSlope = depVals[depVals.length - 1] - depVals[0];
    evMomentum  = evSlope  < -1.5 ? 'falling' : evSlope  > 1.5 ? 'rising' : 'stable';
    depMomentum = depSlope < -1.5 ? 'falling' : depSlope > 1.5 ? 'rising' : 'stable';
  }
  f.ev_momentum  = evMomentum;
  f.dep_momentum = depMomentum;
  f.div_ev_to_dep = (f.ev_ft_sign === 'neg' && f.dep_ft_sign === 'pos') ? 'yes' : 'no';
  f.div_strong_ev = (f.ev_ft_sign === 'neg' && f.dep_ft_sign === 'neg') ? 'yes' : 'no';

  return {
    raw: { ms1, ms2, iy1, iy2, sy1, iyms21, iyms22, iyms11, iyms12, au25o, bttsY, dcg2h, ev_ft, dep_ft },
    buckets: f,
    hasHtFt,
  };
}

/**
 * [V37-5] generateStateKey — nohtft artık 8 parça: ms1dir eklendi.
 * Açılış yönü, hangi yöne para aktığını gösterir:
 *   falling = eve para girdi, stable = hareketsiz, rising = evden para çıktı
 */
function generateStateKey(features, openingMarkets, closingMarkets) {
  const b = features.buckets;

  // IY/MS varsa: 6-parçalı htft formatı
  if (features.hasHtFt) {
    return [
      `ms1_${b.ms1_bucket}`,
      `iy2_${b.iy2_bucket}`,
      `iyms21_${b.iyms21_bucket}`,
      `ms2_${b.ms2_bucket}`,
      `au25_${b.au25o_bucket}`,
      `iyms22d_${b.iyms22_drop}`,
    ].join('|');
  }

  // [V37-5] nohtft: 8-parçalı format — ms1dir eklendi
  const ms1ms2ratio = features.raw.ms1 && features.raw.ms2
    ? (features.raw.ms1 < features.raw.ms2 ? 'ev_fav'
      : features.raw.ms1 > features.raw.ms2 * 1.5 ? 'dep_fav' : 'dengeli')
    : 'unknown';

  const ms1DirBucket = getMs1DirBucket(openingMarkets, closingMarkets);

  return [
    `nohtft`,
    `ms1_${b.ms1_bucket}`,
    `ms2_${b.ms2_bucket}`,
    `au25_${b.au25o_bucket}`,
    `btts_${b.btts_bucket}`,
    `ratio_${ms1ms2ratio}`,
    `iy1_${b.iy1_bucket}`,
    `ms1dir_${ms1DirBucket}`, // [V37-5] Açılış yönü
  ].join('|');
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 7 — ÖĞRENME MOTORU
// ════════════════════════════════════════════════════════════════════
function learnFromMatch(fixtureId, actualHtFt, scoreData = null) {
  const match = matchCache.get(fixtureId);
  if (!match || !match.snapshots || match.snapshots.length === 0) return;
  if (!FOCUS_RESULTS.includes(actualHtFt)) return;

  if (match.learned) {
    console.log(`[Learn] fixture=${fixtureId} zaten öğrenildi, atlandı.`);
    return;
  }

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

  // [V37-5] nohtft artık 7 veya 8 parça
  const keyParts    = key.split('|');
  const isHtFtKey   = keyParts.length === 6 && key.includes('iyms22d');
  const isNoHtFtKey = (keyParts.length === 7 || keyParts.length === 8) && key.startsWith('nohtft');
  if (!isHtFtKey && !isNoHtFtKey) {
    console.warn(`[Learn] fixture=${fixtureId} tanınmayan stateKey formatı → atlandı: ${key}`);
    return;
  }

  if (!memory.patterns[key]) memory.patterns[key] = {};
  if (!memory.patterns[key][actualHtFt])
    memory.patterns[key][actualHtFt] = { count: 0, firstSeen: new Date().toISOString() };
  memory.patterns[key][actualHtFt].count++;
  memory.totalLearned++;
  match.learned = true;
  matchCache.set(fixtureId, match);
  console.log(`[Learn] "${key}" → ${actualHtFt} (sayı: ${memory.patterns[key][actualHtFt].count})`);

  if (isNoHtFtKey) {
    const msResult = actualHtFt.split('/')[1];
    const msKey    = key + '|MS_RESULT';
    if (!memory.patterns[msKey]) memory.patterns[msKey] = {};
    if (!memory.patterns[msKey][msResult])
      memory.patterns[msKey][msResult] = { count: 0, firstSeen: new Date().toISOString() };
    memory.patterns[msKey][msResult].count++;
    console.log(`[Learn-MS] "${msKey}" → ${msResult}`);
  }

  // OU öğrenmesi
  if (isNoHtFtKey && scoreData?.ftHome != null && scoreData?.ftAway != null) {
    const totalGoals = scoreData.ftHome + scoreData.ftAway;

    // [V37-2] UNDER da öğrenilsin
    const ou25Result = totalGoals > 2.5 ? 'OVER25' : 'UNDER25';
    const ou25Key    = key + '|OU25_RESULT';
    if (!memory.patterns[ou25Key]) memory.patterns[ou25Key] = {};
    if (!memory.patterns[ou25Key][ou25Result])
      memory.patterns[ou25Key][ou25Result] = { count: 0, firstSeen: new Date().toISOString() };
    memory.patterns[ou25Key][ou25Result].count++;
    console.log(`[Learn-OU25] "${ou25Key}" → ${ou25Result} (${totalGoals} gol)`);

    const ou35Result = totalGoals > 3.5 ? 'OVER35' : 'UNDER35';
    const ou35Key    = key + '|OU35_RESULT';
    if (!memory.patterns[ou35Key]) memory.patterns[ou35Key] = {};
    if (!memory.patterns[ou35Key][ou35Result])
      memory.patterns[ou35Key][ou35Result] = { count: 0, firstSeen: new Date().toISOString() };
    memory.patterns[ou35Key][ou35Result].count++;
    console.log(`[Learn-OU35] "${ou35Key}" → ${ou35Result} (${totalGoals} gol)`);
  }

  // [V37-6] Market yönü öğrenmesi
  learnMarketDirectionOutcome(fixtureId, actualHtFt, scoreData);

  recordMatchHistory(
    fixtureId,
    key,
    actualHtFt,
    match.latestMarkets || {},
    match.fingerprint   || null
  );
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
    else if (total >= 5 && prob >= 0.20) confidence = 'medium';
    result[r] = { prob: +prob.toFixed(3), lift: +lift.toFixed(2), count: cnt, total, confidence };
  }
  return result;
}

function predictWithSimilarity(stateKey) {
  const N         = FOCUS_RESULTS.length;
  const basePrb   = 1 / N;
  const direct    = predict(stateKey);
  const directTotal = Math.max(...FOCUS_RESULTS.map(r => direct[r].total || 0));
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

  const blended = {};
  let weightSum = directTotal;
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
function evaluateSmartSignals(markets, changes, cumCache, snapshots, openingMarkets, fingerprint) {
  const features  = extractFeatures(markets, changes, cumCache, snapshots, openingMarkets);
  // [V37-5] generateStateKey artık openingMarkets/closingMarkets alıyor
  const stateKey  = generateStateKey(features, openingMarkets, markets);
  const raw       = features.raw;
  const b         = features.buckets;
  const hasHtFt   = features.hasHtFt;

  if (snapshots.length > 0) snapshots[snapshots.length - 1]._stateKey = stateKey;

  const predictions    = predictWithSimilarity(stateKey);
  const similarMatches = findSimilarMatches(markets);
  const signals        = [];

  // [V37-1] Market yönü tahmini
  const dirPred = getMarketDirectionPrediction(openingMarkets, markets);

  // [V37-4] Kapanış şoku tespiti
  const closingShocks = detectLastMinuteMovements(snapshots, null); // kickoff olmadan yakın snapshot karşılaştırması
  const hasClosingShock = closingShocks && closingShocks.length > 0;

  const trendStrength =
    (b.ev_momentum === 'falling' && b.dep_momentum === 'rising') ? 'strong_reversal' :
    (b.ev_momentum === 'falling')                                 ? 'ev_dominant'     :
    (b.dep_momentum === 'falling')                                ? 'dep_dominant'    : 'neutral';

  // ── BOOTSTRAP KATMANI ──────────────────────────────────────────
  {
    const evCum  = cumCache.ev_ft_cum  || 0;
    const depCum = cumCache.dep_ft_cum || 0;

    const bsPush = (type, rule, prec, lift, prob) => {
      if (signals.some(s => s.type === type && s.trendStrength !== 'bootstrap')) return;
      signals.push({
        type, tier: 'STANDART', rule: `[BOOTSTRAP] ${rule}`,
        prec, lift, effectiveLift: lift, prob,
        stateKey, trendStrength: 'bootstrap',
        histCount: 0, accLabel: 'bootstrap', accuracy: null, hasHtFt,
        trapRisk: 0, similarCount: 0,
      });
    };

    if (raw.iyms21 && raw.iyms21 <= 22 && depCum <= -1.5)
      bsPush('2/1', `İYMS21=${raw.iyms21?.toFixed(1)} dep_cum=${depCum.toFixed(2)}`, 5.0, 1.5, 0.15);
    if (raw.iyms21 && raw.iyms21 <= 22 && evCum <= -1.5)
      bsPush('X/1', `İYMS21=${raw.iyms21?.toFixed(1)} ev_cum=${evCum.toFixed(2)}`, 5.0, 1.4, 0.14);
    if (raw.ms1 && raw.ms1 <= 1.55 && evCum <= -0.5)
      bsPush('1/1', `MS1=${raw.ms1?.toFixed(2)} ev_cum=${evCum.toFixed(2)}`, 5.0, 1.4, 0.14);
    if (raw.iyms22 && raw.iyms22 <= 5 && depCum <= -0.5)
      bsPush('2/2', `İYMS22=${raw.iyms22?.toFixed(1)} dep_cum=${depCum.toFixed(2)}`, 5.0, 1.4, 0.14);
    if (raw.iyms12 && raw.iyms12 <= 22 && depCum <= -0.5)
      bsPush('1/2', `İYMS12=${raw.iyms12?.toFixed(1)} dep_cum=${depCum.toFixed(2)}`, 5.0, 1.4, 0.14);
    if (raw.ms1 && raw.ms1 >= 2.00 && raw.ms2 && raw.ms2 >= 2.00 && Math.abs(evCum) <= 0.2 && Math.abs(depCum) <= 0.2)
      bsPush('X/X', `MS1=${raw.ms1?.toFixed(2)} MS2=${raw.ms2?.toFixed(2)} hareket=düşük`, 4.5, 1.3, 0.12);
  }

  for (const outcome of FOCUS_RESULTS) {
    const p = predictions[outcome];
    if (p.lift < 1.10) continue;

    const { multiplier, label: accLabel, accuracy } = getAccuracyMultiplier(outcome);
    if (multiplier === 0.0) continue;

    const trapInfo = detectTrapRisk(fingerprint, outcome);
    const decisive = findDecisiveMarket(similarMatches, outcome);

    let tier          = 'STANDART';
    let rule          = `State: ${stateKey.substring(0, 60)}...`;
    let precision     = p.prob * 10;
    let liftVal       = p.lift;
    let effectiveLift = +(liftVal * multiplier).toFixed(2);

    if (trapInfo.risk >= 0.5) {
      effectiveLift = +(effectiveLift * (1 - trapInfo.risk * 0.6)).toFixed(2);
      precision     = Math.max(1, precision - trapInfo.risk * 2);
      rule          = `⚠️Tuzak(${(trapInfo.risk*100).toFixed(0)}%) + ${rule}`;
    }

    if (decisive && decisive.correctFallingRate > decisive.incorrectFallingRate) {
      const trajectoryOk = fingerprint?.trajectories?.[decisive.market] === 'falling_steady';
      if (trajectoryOk) {
        effectiveLift = +(effectiveLift * 1.15).toFixed(2);
        rule          = `✅BelirleyiciMkt(${decisive.market}) + ${rule}`;
      }
    }

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
                  : (p.total >= 5)  ? 0.22 : 0.11;
    if (p.prob < minProb) continue;

    let htFtNote = '';
    if (!hasHtFt) {
      liftVal       = +(liftVal       * 0.70).toFixed(2);
      effectiveLift = +(effectiveLift * 0.70).toFixed(2);
      htFtNote      = ' ⚠IY/MS-YOK(oran.eksik)';
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
      trapRisk:       trapInfo.risk,
      trapReason:     trapInfo.reason,
      similarCount:   similarMatches.length,
      decisiveMarket: decisive,
    });
  }

  // ── nohtft sinyal filtresi ─────────────────────────────────────
  let finalSignals = signals;
  if (!hasHtFt) {
    const msProbs  = { '1': 0, 'X': 0, '2': 0 };
    const msCounts = { '1': 0, 'X': 0, '2': 0 };
    for (const s of signals) {
      const msPart = s.type.split('/')[0];
      if (msProbs[msPart] !== undefined) {
        msProbs[msPart]  += s.prob;
        msCounts[msPart] += s.histCount || 0;
      }
    }
    const msSignals = [];
    for (const [ms, prob] of Object.entries(msProbs)) {
      if (prob < 0.15) continue;
      const lift = prob / (1 / 3);
      if (lift < 1.10) continue;
      const { multiplier, label: accLabel, accuracy } = getAccuracyMultiplier('MS_' + ms);
      if (multiplier === 0.0) continue;

      // [V37-3] Trivial sinyal filtresi
      // Çok düşük oran = zaten herkes biliyor, ML değer katmıyor
      const isTrivialMs1 = ms === '1' && raw.ms1 && raw.ms1 < TRIVIAL_ODDS_THR;
      const isTrivialMs2 = ms === '2' && raw.ms2 && raw.ms2 < TRIVIAL_ODDS_THR;
      if (isTrivialMs1 || isTrivialMs2) {
        // Yalnızca çok güçlü öğrenilmiş doğrulukla geçer
        const trivialAcc = accuracy ?? 0;
        if (trivialAcc < 0.80) {
          console.log(`[V37-3] MS_${ms} trivial eşikte (oran=${ms==='1'?raw.ms1?.toFixed(2):raw.ms2?.toFixed(2)}<${TRIVIAL_ODDS_THR}) — sinyal bastırıldı`);
          continue;
        }
        console.log(`[V37-3] MS_${ms} trivial ama doğruluk yüksek (%${(trivialAcc*100).toFixed(0)}) — izin verildi`);
      }

      let effectiveLift = +(lift * multiplier).toFixed(2);
      let tier = lift >= 1.5 ? 'PREMIER' : 'STANDART';

      // [V37-4] Kapanış şoku varsa lift bonus
      if (hasClosingShock) {
        effectiveLift = +(effectiveLift * 1.20).toFixed(2);
        if (tier === 'STANDART') tier = 'PREMIER';
      }

      // [V37-1] Yön tahmini uyumlu mu?
      let dirNote = '';
      if (dirPred && dirPred.msBest === ms && dirPred.msProb > 0.55 && dirPred.count >= 5) {
        effectiveLift = +(effectiveLift * 1.10).toFixed(2);
        dirNote = ` | 📐yön:${ms}(%${(dirPred.msProb*100).toFixed(0)},${dirPred.count}x)`;
      }

      msSignals.push({
        type:          'MS_' + ms,
        tier,
        rule:          `[nohtft-MS] Agregat prob=${prob.toFixed(2)} | stateKey=${stateKey.substring(0, 40)}${dirNote}`,
        prec:          +(prob * 10).toFixed(2),
        lift:          +lift.toFixed(2),
        effectiveLift,
        prob:          +prob.toFixed(3),
        stateKey, trendStrength,
        histCount: msCounts[ms], accLabel, accuracy,
        hasHtFt: false,
        trapRisk: 0, similarCount: similarMatches.length,
        decisiveMarket: null,
        closingShock: hasClosingShock,
      });
    }

    // ────────────────────────────────────────────────────────────
    // OU25 SİNYALLERİ [V37-2: UNDER eklendi]
    // ────────────────────────────────────────────────────────────
    const ou25o = features.raw.au25o;
    const bttsY = features.raw.bttsY;
    const ou25Available = !!(markets?.ou25?.over);
    const ou25u = markets?.['ou25']?.under ?? null;

    // OU25 pattern öğrenilmiş istatistikleri
    const ou25PatternKey = stateKey + '|OU25_RESULT';
    const ou25Pat        = memory.patterns[ou25PatternKey];
    let ou25OverProb = null, ou25UnderProb = null;
    if (ou25Pat) {
      const over  = ou25Pat['OVER25']?.count  || 0;
      const under = ou25Pat['UNDER25']?.count || 0;
      const total = over + under;
      if (total >= 3) {
        ou25OverProb  = over  / total;
        ou25UnderProb = under / total;
      }
    }

    // OU35 pattern
    const ou35PatternKey = stateKey + '|OU35_RESULT';
    const ou35Pat        = memory.patterns[ou35PatternKey];
    let ou35OverProb = null, ou35UnderProb = null;
    if (ou35Pat) {
      const over  = ou35Pat['OVER35']?.count  || 0;
      const under = ou35Pat['UNDER35']?.count || 0;
      const total = over + under;
      if (total >= 3) {
        ou35OverProb  = over  / total;
        ou35UnderProb = under / total;
      }
    }

    if (ou25Available && ou25o) {
      // ── OU25 OVER sinyali ──────────────────────────────────────
      if (ou25o < OU25_STRONG_THR) {
        msSignals.push({
          type: 'OU25_OVER', tier: 'PREMIER',
          rule: `[nohtft-OU25-GÜÇLÜ] ou25.over=${ou25o?.toFixed(2)} (<${OU25_STRONG_THR})`,
          prec: 7.5, lift: 1.8, effectiveLift: 1.8,
          prob:      ou25OverProb ?? 0.68,
          stateKey, trendStrength: 'bootstrap',
          histCount: ou25Pat?.['OVER25']?.count || 0,
          accLabel:  ou25OverProb != null ? `öğrenilmiş(%${(ou25OverProb*100).toFixed(0)})` : 'bootstrap',
          accuracy:  ou25OverProb,
          hasHtFt: false, trapRisk: 0, similarCount: 0, decisiveMarket: null,
        });
      } else if (ou25o < OU25_WEAK_THR && bttsY && bttsY < 1.70) {
        msSignals.push({
          type: 'OU25_OVER', tier: 'STANDART',
          rule: `[nohtft-OU25-ZAYIF] ou25.over=${ou25o?.toFixed(2)} + btts.yes=${bttsY?.toFixed(2)} (btts<1.70 koşulu)`,
          prec: 5.5, lift: 1.4, effectiveLift: 1.4,
          prob:      ou25OverProb ?? 0.55,
          stateKey, trendStrength: 'bootstrap',
          histCount: ou25Pat?.['OVER25']?.count || 0,
          accLabel:  ou25OverProb != null ? `öğrenilmiş(%${(ou25OverProb*100).toFixed(0)})` : 'bootstrap',
          accuracy:  ou25OverProb,
          hasHtFt: false, trapRisk: 0, similarCount: 0, decisiveMarket: null,
        });
      }

      // ── OU25 UNDER sinyali [V37-2] ─────────────────────────────
      // Piyasa UNDER'ı fiyatlıyor: under oranı düşük + over oranı yüksek
      const marketExpectsUnder = ou25u && ou25u < UNDER_ODDS_THR && ou25o > UNDER_OVER_MIN;
      // Öğrenilmiş pattern UNDER'ı söylüyor
      const learnedExpectsUnder = ou25UnderProb !== null && ou25UnderProb > UNDER_LEARNED_THR;
      // Market yönü: over oranı yükseldiyse = paralar under'a gitti
      const dirExpectsUnder = dirPred && dirPred.ouUnderProb > 0.60 && dirPred.count >= 5;

      if (marketExpectsUnder || learnedExpectsUnder || dirExpectsUnder) {
        const underProb   = ou25UnderProb ?? (dirPred?.ouUnderProb ?? 0.55);
        const underLift   = +(underProb / 0.5).toFixed(2);
        const underTier   = underProb > 0.65 || (marketExpectsUnder && learnedExpectsUnder) ? 'PREMIER' : 'STANDART';
        const underRules  = [
          marketExpectsUnder    ? `ou25.under=${ou25u?.toFixed(2)} ou25.over=${ou25o?.toFixed(2)}` : '',
          learnedExpectsUnder   ? `öğrenilmiş=%${(ou25UnderProb*100).toFixed(0)} (${ou25Pat?.['UNDER25']?.count||0}x)` : '',
          dirExpectsUnder       ? `yön-UNDER=%${(dirPred.ouUnderProb*100).toFixed(0)} (${dirPred.count}x)` : '',
        ].filter(Boolean).join(' | ');

        msSignals.push({
          type: 'OU25_UNDER', tier: underTier,
          rule: `[nohtft-OU25-UNDER] ${underRules}`,
          prec: underProb > 0.65 ? 7.0 : 5.5,
          lift: underLift, effectiveLift: underLift,
          prob: +underProb.toFixed(3),
          stateKey, trendStrength: 'bootstrap',
          histCount: ou25Pat?.['UNDER25']?.count || 0,
          accLabel: ou25UnderProb != null ? `öğrenilmiş(%${(ou25UnderProb*100).toFixed(0)})` : 'bootstrap',
          accuracy: ou25UnderProb,
          hasHtFt: false, trapRisk: 0, similarCount: 0, decisiveMarket: null,
        });
      }
    }

    // ── OU35 sinyalleri [V37-2: UNDER eklendi] ────────────────────
    const ou35  = markets?.['ou35']?.over  ?? null;
    const ou35u = markets?.['ou35']?.under ?? null;
    const ou35Available = !!(markets?.ou35?.over);

    if (ou35Available && ou35) {
      // OU35 OVER
      if (ou35 < OU35_STRONG_THR) {
        msSignals.push({
          type: 'OU35_OVER', tier: 'PREMIER',
          rule: `[nohtft-OU35-GÜÇLÜ] ou35.over=${ou35?.toFixed(2)} (<${OU35_STRONG_THR})`,
          prec: 7.5, lift: 1.8, effectiveLift: 1.8,
          prob:      ou35OverProb ?? 0.68,
          stateKey, trendStrength: 'bootstrap',
          histCount: ou35Pat?.['OVER35']?.count || 0,
          accLabel:  ou35OverProb != null ? `öğrenilmiş(%${(ou35OverProb*100).toFixed(0)})` : 'bootstrap',
          accuracy:  ou35OverProb,
          hasHtFt: false, trapRisk: 0, similarCount: 0, decisiveMarket: null,
        });
      } else if (ou35 < OU35_WEAK_THR && bttsY && bttsY < 1.70) {
        msSignals.push({
          type: 'OU35_OVER', tier: 'STANDART',
          rule: `[nohtft-OU35-ZAYIF] ou35.over=${ou35?.toFixed(2)} + btts.yes=${bttsY?.toFixed(2)}`,
          prec: 5.5, lift: 1.4, effectiveLift: 1.4,
          prob:      ou35OverProb ?? 0.55,
          stateKey, trendStrength: 'bootstrap',
          histCount: ou35Pat?.['OVER35']?.count || 0,
          accLabel:  ou35OverProb != null ? `öğrenilmiş(%${(ou35OverProb*100).toFixed(0)})` : 'bootstrap',
          accuracy:  ou35OverProb,
          hasHtFt: false, trapRisk: 0, similarCount: 0, decisiveMarket: null,
        });
      }

      // OU35 UNDER [V37-2]
      const ou35MarketExpectsUnder = ou35u && ou35u < UNDER_ODDS_THR && ou35 > UNDER_OVER_MIN;
      const ou35LearnedExpectsUnder = ou35UnderProb !== null && ou35UnderProb > UNDER_LEARNED_THR;
      const ou35DirExpectsUnder = dirPred && dirPred.ou35UnderProb && dirPred.ou35UnderProb > 0.60;

      if (ou35MarketExpectsUnder || ou35LearnedExpectsUnder || ou35DirExpectsUnder) {
        const u35Prob = ou35UnderProb ?? (dirPred?.ou35UnderProb ?? 0.55);
        const u35Lift = +(u35Prob / 0.5).toFixed(2);
        msSignals.push({
          type: 'OU35_UNDER', tier: u35Prob > 0.65 ? 'PREMIER' : 'STANDART',
          rule: `[nohtft-OU35-UNDER] ou35.under=${ou35u?.toFixed(2)} ou35.over=${ou35?.toFixed(2)}`,
          prec: u35Prob > 0.65 ? 7.0 : 5.5,
          lift: u35Lift, effectiveLift: u35Lift,
          prob: +u35Prob.toFixed(3),
          stateKey, trendStrength: 'bootstrap',
          histCount: ou35Pat?.['UNDER35']?.count || 0,
          accLabel: ou35UnderProb != null ? `öğrenilmiş(%${(ou35UnderProb*100).toFixed(0)})` : 'bootstrap',
          accuracy: ou35UnderProb,
          hasHtFt: false, trapRisk: 0, similarCount: 0, decisiveMarket: null,
        });
      }
    }

    finalSignals = msSignals;
  }

  const tierW = { ELITE: 3, PREMIER: 2, STANDART: 1 };
  finalSignals.sort((a, c) => (tierW[c.tier] || 0) - (tierW[a.tier] || 0) || c.effectiveLift - a.effectiveLift);

  return {
    signals: finalSignals, features, predictions, stateKey, hasHtFt, similarMatches,
    dirPred, closingShocks,
  };
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 9 — YEREL YORUM
// ════════════════════════════════════════════════════════════════════
function generateLocalInterpretation(matchData) {
  const { signals, features, predictions, stateKey, hasHtFt, dirPred, closingShocks } = matchData;
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

  // [V37-1] Market yön notu
  let dirNote = '';
  if (dirPred && dirPred.count >= 3) {
    const dirs = dirPred.directions;
    dirNote = `\n📐 Piyasa Yönü: ${describeMarketDirection(dirs)}`;
    if (dirPred.msBest && dirPred.msProb > 0.50)
      dirNote += ` → MS_${dirPred.msBest} beklentisi %${(dirPred.msProb*100).toFixed(0)} (${dirPred.count}x geçmiş)`;
    if (dirPred.ouUnderProb > 0.55)
      dirNote += ` | UNDER beklentisi %${(dirPred.ouUnderProb*100).toFixed(0)}`;
    else if (dirPred.ouOverProb > 0.55)
      dirNote += ` | OVER beklentisi %${(dirPred.ouOverProb*100).toFixed(0)}`;
  }

  // [V37-4] Kapanış şoku notu
  let shockNote = '';
  if (closingShocks && closingShocks.length > 0) {
    const shockDesc = closingShocks.map(s =>
      `${s.market}.${s.sub}: ${s.fromVal}→${s.toVal} (${s.pctChange > 0 ? '+' : ''}${s.pctChange}%)`
    ).join(', ');
    shockNote = `\n⚡ KAPANIŞ ŞOKU: ${shockDesc}`;
  }

  const hist = predictions[top.type] || {};
  let note = '';
  if ((hist.total || 0) >= 10)             note = `Bu pattern ${hist.total} kez tekrarlandı, ${hist.count} kez ${top.type} geldi (%${((hist.prob || 0)*100).toFixed(0)}).`;
  else if ((hist.total || 0) >= 3)         note = `Sınırlı örnek (${hist.total}) ama eğilim ${top.type} yönünde.`;
  else if (top.trendStrength === 'bootstrap') note = `[Bootstrap] Kural tabanlı sinyal — hafıza: ${memory.totalLearned}/${BOOTSTRAP_THRESHOLD}.`;
  else                                     note = 'Yeni pattern, temkinli olun.';

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

  const dropNotes = [];
  if (f.iyms22_drop === 'heavy') dropNotes.push('IY/MS 2/2 açılıştan ağır düştü');
  if (f.iyms21_drop === 'heavy') dropNotes.push('IY/MS 2/1 açılıştan ağır düştü');
  if (f.ms1_drop    === 'heavy') dropNotes.push('MS1 açılıştan ağır düştü');
  if (f.ou25o_drop  === 'heavy') dropNotes.push('2.5 üst açılıştan ağır düştü');
  const dropNote = dropNotes.length > 0 ? `\n📉 Açılış Hareketi: ${dropNotes.join(' | ')}` : '';

  const trapNote = (top.trapRisk >= 0.3)
    ? `\n🪤 ${top.trapReason} (risk: %${(top.trapRisk*100).toFixed(0)})`
    : '';

  const simNote = top.similarCount > 0
    ? `\n🔍 Benzer maç geçmişi: ${top.similarCount} eşleşme bulundu`
    : '';

  const decNote = top.decisiveMarket
    ? `\n🔑 Belirleyici market: ${top.decisiveMarket.label} (fark: ${top.decisiveMarket.diff})`
    : '';

  return (
    `📊 DURUM: ${stateKey.substring(0, 55)}...\n` +
    `${mkt}\n` +
    `🎯 TAHMİN: ${top.type} | ${top.tier} | Lift: ${top.lift}x (efektif: ${top.effectiveLift}x) | Olas: %${((top.prob || 0)*100).toFixed(1)}\n` +
    `📚 ${note}${accNote}${htFtWarning}${dropNote}${trapNote}${simNote}${decNote}${dirNote}${shockNote}\n` +
    `⚡ Trend: ${top.trendStrength} | İYMS21: ${r.iyms21 || 'N/A'} | MS1: ${r.ms1 || '?'}`
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
    const top     = m.signals[0];
    const htFtTag = m.hasHtFt === false ? ' [⚠ IY/MS YOK]' : '';
    const trapTag = top.trapRisk >= 0.5 ? ` [🪤 TUZAK %${(top.trapRisk*100).toFixed(0)}]` : '';
    // [V37-4] Kapanış şoku etiketi
    const shockTag = (m.closingShocks && m.closingShocks.length > 0) ? ' [⚡ KAPANIŞ-ŞOKU]' : '';
    console.log(`\n${tierColor[top.tier] || '⚪'} ${m.name}${htFtTag}${trapTag}${shockTag}`);
    console.log(`   ⏰ ${m.h2k < 0 ? 'Başladı' : m.h2k < 1 ? Math.round(m.h2k * 60) + ' dk sonra' : m.h2k.toFixed(1) + ' saat sonra'}`);
    console.log(`   📈 Ev kümülâtif: ${m.ev_ft_cum.toFixed(2)} | Dep: ${m.dep_ft_cum.toFixed(2)}`);
    if (top.similarCount > 0)
      console.log(`   🔍 Benzer geçmiş maç: ${top.similarCount} | Belirleyici: ${top.decisiveMarket?.market || 'yok'}`);

    // [V37-1] Market yönü özeti
    if (m.dirPred && m.dirPred.count >= 3) {
      const dirs = m.dirPred.directions;
      const ms1d = dirs?.['1x2']?.home || '?';
      const ou25d = dirs?.['ou25']?.over || '?';
      console.log(`   📐 Piyasa Yönü: ms1=${ms1d} | ou25.over=${ou25d} | geçmiş=${m.dirPred.count}x`);
    }

    for (const s of m.signals.slice(0, 3)) {
      const accStr = s.accuracy !== null && s.accuracy !== undefined
        ? ` | Doğruluk: %${(s.accuracy*100).toFixed(0)} (${s.accLabel})`
        : ` | Doğruluk: veri bekleniyor`;
      const shockStr = s.closingShock ? ' ⚡' : '';
      console.log(
        `   ${tierColor[s.tier]} [${s.tier}] ${s.type}${shockStr}` +
        ` | Lift: ${s.lift}x → Efektif: ${s.effectiveLift}x` +
        ` | Olas: %${(s.prob*100).toFixed(1)}${accStr}`
      );
      console.log(`      ↳ ${s.rule}`);
    }
    if (m.interpretation) {
      console.log('   ─────────────────────────────────');
      for (const line of m.interpretation.split('\n')) if (line.trim()) console.log('   ' + line);
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
    const matchFingerprint = matchCache.get(fid)?.fingerprint || null;
    memory.pendingSignals[fid] = {
      predictedAt:   new Date().toISOString(),
      stateKey:      signalData.stateKey,
      topSignal:     signalData.type,
      tier:          signalData.tier,
      lift:          signalData.lift,
      effectiveLift: signalData.effectiveLift,
      prob:          signalData.prob,
      signalLabel:   label,
      fingerprint:   matchFingerprint,
    };
    if (!memory.signalAccuracy[signalData.type])
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

  // [FIX-P1-2] Staleness timeout
  const nowMs = Date.now();
  let expiredCount = 0;
  for (const [fid, pending] of Object.entries(memory.pendingSignals)) {
    if (!pending.predictedAt) continue;
    const ageH = (nowMs - new Date(pending.predictedAt).getTime()) / 3600000;
    if (ageH > MAX_PENDING_AGE_H) {
      console.warn(`[PendingTimeout] fixture=${fid} ${ageH.toFixed(1)}sa beklendi — iptal (sinyal: ${pending.topSignal})`);
      delete memory.pendingSignals[fid];
      expiredCount++;
    }
  }
  if (expiredCount > 0)
    console.log(`[PendingTimeout] ${expiredCount} eskimiş pending sinyal iptal edildi`);

  let liveRows;
  try {
    const { data, error } = await sb
      .from('live_matches')
      .select('fixture_id, status_short, home_score, away_score, ht_home_score, ht_away_score')
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
    const hScore  = row.home_score    ?? null;
    const aScore  = row.away_score    ?? null;
    const htScore = row.ht_home_score ?? null;
    const atScore = row.ht_away_score ?? null;
    matchedLive++;
    const match    = matchCache.get(fid);
    if (!match) continue;
    const prevLive = match.liveData || {};
    const ftNeedsRetry = status === 'FT' && prevLive.status === 'FT' && prevLive.ftHome == null;
    if (prevLive.status === status && status !== 'FT') continue;
    if (prevLive.status === 'FT' && !ftNeedsRetry) continue;

    let htHome     = htScore ?? prevLive.htHome ?? null;
    let htAway     = atScore ?? prevLive.htAway ?? null;
    let ftHome     = prevLive.ftHome ?? null;
    let ftAway     = prevLive.ftAway ?? null;
    let htFtResult = prevLive.htFtResult ?? null;

    if (status === 'HT') {
      htHome = hScore; htAway = aScore;
      console.log(`  ⏸ HT: ${match.name} | İY ${htHome}-${htAway}`);
    } else if (status === 'FT') {
      if (hScore != null) { ftHome = hScore; ftAway = aScore; }
      htFtResult = calcHtFtResult(htHome, htAway, ftHome, ftAway);
      const retryTag = ftNeedsRetry ? ' [RETRY]' : '';
      console.log(
        `  🏁 FT${retryTag}: ${match.name}` +
        ` | İY: ${htHome ?? '?'}-${htAway ?? '?'}` +
        ` → MS: ${ftHome ?? '?'}-${ftAway ?? '?'}` +
        ` | HT/FT: ${htFtResult || 'hesaplanamadı'}`
      );
      if (htFtResult) {
        if (!prevLive.htFtResult) {
          resolvePendingSignal(fid, htFtResult, { ftHome, ftAway });
          resolvedCount++;
          learnFromMatch(fid, htFtResult, { ftHome, ftAway });
          learnedCount++;
        }
      } else {
        console.warn(`  ⚠️ ${match.name}: skor eksik — sonraki döngüde tekrar denenecek`);
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
  console.log(`[Memory]  ${Object.keys(memory.patterns).length} pattern | ${memory.totalLearned} öğrenme${bootstrapMode ? ` | ⚡ BOOTSTRAP (${memory.totalLearned}/${BOOTSTRAP_THRESHOLD})` : ''}`);
  console.log(`[Accuracy] ${Object.keys(memory.signalAccuracy).length} tip | ${Object.keys(memory.pendingSignals).length} bekleyen`);
  console.log(`[Trap/Sim] ${Object.keys(memory.trapPatterns).length} tuzak | ${memory.matchHistory.length} geçmiş | ${Object.keys(memory.marketDirectionPatterns).length} yön pattern`);
  console.log(`[Config]  Sinyal penceresi: ≤${SIGNAL_WINDOW_H*60}dk | Lookahead: ${LOOKAHEAD_H}sa | Trivial eşik: <${TRIVIAL_ODDS_THR}`);
  console.log('═'.repeat(60));

  if (cycleCount % 10 === 1) {
    logAccuracyReport();
    logTrapReport();
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

    const fid      = fix.fixture_id;
    const prev     = matchCache.get(fid);
    const changes  = prev?.latestMarkets ? calcDelta(prev.latestMarkets, currMarkets) : {};

    const { ev_ft, dep_ft } = calcMoneyFlow(currMarkets, changes);
    const ev_ft_cum  = (prev?.ev_ft_cum  || 0) + (ev_ft  < 0 ? ev_ft  : 0);
    const dep_ft_cum = (prev?.dep_ft_cum || 0) + (dep_ft < 0 ? dep_ft : 0);

    const snapshots = prev?.snapshots || [];

    // [FIX-P0-2 + V37-4] Rapid-fire guard — kapanış şoku override
    const lastSnapTime      = snapshots.length > 0
      ? new Date(snapshots[snapshots.length - 1].time).getTime()
      : 0;
    const timeSinceLastSnap = Date.now() - lastSnapTime;

    // Kapanış şoku tespiti (rapid-fire bypass için)
    let isShock = false;
    if (prev?.latestMarkets && timeSinceLastSnap < MIN_SNAP_INTERVAL_MS) {
      const shockPairs = [
        ['1x2', 'home'], ['1x2', 'away'], ['ou25', 'over'], ['ou25', 'under'],
      ];
      for (const [mkt, sub] of shockPairs) {
        const prevVal = prev.latestMarkets?.[mkt]?.[sub];
        const currVal = currMarkets?.[mkt]?.[sub];
        if (prevVal && currVal && prevVal !== 0) {
          if (Math.abs((currVal - prevVal) / prevVal) >= CLOSING_SHOCK_THR) {
            isShock = true;
            console.log(`[V37-4] ⚡ KAPANIŞ ŞOKU: ${fix.home_team} vs ${fix.away_team} | ${mkt}.${sub}: ${prevVal?.toFixed(2)}→${currVal?.toFixed(2)}`);
            break;
          }
        }
      }
    }

    if (timeSinceLastSnap >= MIN_SNAP_INTERVAL_MS || isShock) {
      snapshots.push({
        time: new Date().toISOString(),
        markets: currMarkets, changes, ev_ft, dep_ft,
        isShock,
      });
      if (snapshots.length > 10) snapshots.shift();
    } else {
      if (snapshots.length > 0) {
        const lastSnap = snapshots[snapshots.length - 1];
        snapshots[snapshots.length - 1] = {
          ...lastSnap, markets: currMarkets, changes, ev_ft, dep_ft,
        };
      } else {
        snapshots.push({ time: new Date().toISOString(), markets: currMarkets, changes, ev_ft, dep_ft });
      }
    }

    const fingerprint = buildFingerprint(
      snapshots,
      fix.kickoff,
      prev?.openingMarkets || currMarkets,
      prev?.closingMarkets || currMarkets,
    );

    const { signals, features, predictions, stateKey, hasHtFt, similarMatches, dirPred, closingShocks } =
      evaluateSmartSignals(
        currMarkets, changes, { ev_ft_cum, dep_ft_cum }, snapshots,
        prev?.openingMarkets,
        fingerprint
      );

    matchCache.set(fid, {
      name:           `${fix.home_team} vs ${fix.away_team}`,
      kickoff:        fix.kickoff,
      latestMarkets:  currMarkets,
      ev_ft_cum, dep_ft_cum,
      snapshots,
      liveData:       prev?.liveData        || {},
      openingMarkets: prev?.openingMarkets  || currMarkets,
      closingMarkets: currMarkets,
      learned:        prev?.learned         || false,
      fingerprint,
    });

    if (h2k > SIGNAL_WINDOW_H) continue;
    if (signals.length === 0) continue;

    const hasHighTier = signals.some(s => s.tier === 'ELITE' || s.tier === 'PREMIER');
    const isBootstrap = signals.every(s => s.trendStrength === 'bootstrap');
    if (!hasHighTier && signals.length < MIN_SIGNALS && !isBootstrap) continue;

    const topSignal = signals[0];
    const topLabel  = `${topSignal.type}_${topSignal.tier}`;
    if (alreadyFired(fid, topLabel)) continue;

    const interpretation = generateLocalInterpretation({
      signals, features, predictions, stateKey, hasHtFt,
      dirPred, closingShocks, // [V37-1/V37-4]
    });

    matchesWithSignals.push({
      fid, name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff, h2k,
      signals, features, interpretation,
      ev_ft_cum, dep_ft_cum,
      hasHtFt, similarMatches,
      dirPred, closingShocks, // [V37-1/V37-4]
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
  console.log('║  ScorePop Adaptive v3.7 — Market Direction + UNDER Fix  ║');
  console.log(`║  Döngü: ${Math.round(INTERVAL_MS/60000)}dk | Süre: ${Math.round(MAX_RUNTIME_MS/3600000)}sa | DryRun: ${String(DRY_RUN).padEnd(6)}║`);
  console.log(`║  Bootstrap eşiği   : ${String(BOOTSTRAP_THRESHOLD).padEnd(36)}║`);
  console.log(`║  Sinyal penceresi  : ≤${String(Math.round(SIGNAL_WINDOW_H*60)+'dk').padEnd(35)}║`);
  console.log(`║  Trivial oran eşiği: <${String(TRIVIAL_ODDS_THR).padEnd(35)}║`);
  console.log(`║  UNDER odds eşiği  : <${String(UNDER_ODDS_THR+' (ou25.under)').padEnd(35)}║`);
  console.log(`║  UNDER öğrenme thr : >%${String((UNDER_LEARNED_THR*100).toFixed(0)+' pattern beklentisi').padEnd(34)}║`);
  console.log(`║  Kapanış şok eşiği : %${String((CLOSING_SHOCK_THR*100).toFixed(0)+' hareket → bypass').padEnd(34)}║`);
  console.log('║  ─────────────────────────────────────────────────────  ║');
  console.log('║  [V37-1] Market Direction Engine (Bölüm 0.7)           ║');
  console.log('║  [V37-2] OU25_UNDER + OU35_UNDER sinyal üretimi        ║');
  console.log('║  [V37-3] Trivial oran filtresi (ms1/ms2 < 1.22)        ║');
  console.log('║  [V37-4] Kapanış şoku tespiti + snapshot override      ║');
  console.log('║  [V37-5] nohtft stateKey 8-parça (ms1dir eklendi)      ║');
  console.log('║  [V37-6] learnMarketDirectionOutcome entegrasyonu       ║');
  console.log('║  [FIX-P0..P3] v3.6 düzeltmeleri korundu               ║');
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
  logTrapReport();
  saveCache();

  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║  OTURUM TAMAMLANDI                                       ║');
  console.log(`║  Döngü         : ${String(cycleCount).padEnd(40)}║`);
  console.log(`║  İzlenen       : ${String(matchCache.size).padEnd(40)}║`);
  console.log(`║  Pattern       : ${String(Object.keys(memory.patterns).length).padEnd(40)}║`);
  console.log(`║  Öğrenilen     : ${String(memory.totalLearned).padEnd(40)}║`);
  console.log(`║  Tuzak Pattern : ${String(Object.keys(memory.trapPatterns).length).padEnd(40)}║`);
  console.log(`║  Yön Pattern   : ${String(Object.keys(memory.marketDirectionPatterns).length).padEnd(40)}║`);
  console.log(`║  Maç Geçmişi   : ${String(memory.matchHistory.length).padEnd(40)}║`);
  console.log(`║  Sinyal Tipi   : ${String(Object.keys(memory.signalAccuracy).length).padEnd(40)}║`);
  console.log(`║  Bekleyen      : ${String(Object.keys(memory.pendingSignals).length).padEnd(40)}║`);
  console.log('╚══════════════════════════════════════════════════════════╝');
}

process.on('SIGINT', () => {
  console.log('\n[Sistem] 🛑 Kapanma — kaydediliyor...');
  logAccuracyReport(); logTrapReport(); saveCache();
  console.log('[Sistem] ✅ Kaydedildi.'); process.exit(0);
});
process.on('SIGTERM', () => { saveCache(); process.exit(0); });

main().catch(e => { console.error('[FATAL]', e); process.exit(1); });
