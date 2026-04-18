/**
 * SCOREPOP — analyzer.js  (v1)
 * ════════════════════════════════════════════════════════════════════
 * Hafta boyunca biriken odds_snapshots + match_results_cache verilerini
 * birleştirerek reversal sinyallerinin doğruluk analizini yapar.
 *
 * Kullanım:
 *   node analyzer.js [--days 7] [--min-lift 1.35] [--tier ELITE]
 *
 * Çıktı:
 *   - Konsol özet raporu
 *   - signal_performance tablosuna upsert
 * ════════════════════════════════════════════════════════════════════
 */
'use strict';

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('[Analyzer] SUPABASE_URL ve SUPABASE_KEY gerekli');
  process.exit(1);
}
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

// CLI argümanları
const args = process.argv.slice(2);
const getArg = (name, def) => {
  const i = args.indexOf(name);
  return i !== -1 && args[i+1] ? args[i+1] : def;
};
const DAYS      = parseInt(getArg('--days', '7'));
const MIN_LIFT  = parseFloat(getArg('--min-lift', '1.35'));
const TIER_FILTER = getArg('--tier', null); // null = tümü

/* ─────────────────────────────────────────────────────────────────────
 * reversal_signals.py mantığı (basitleştirilmiş JS portu)
 * Snapshot'lardan sinyal üretmek için kullanılır
 * ───────────────────────────────────────────────────────────────────── */
function getMarket(markets, key, subkey) {
  const v = markets?.[key]?.[subkey];
  return v ? parseFloat(v) : null;
}

function getChange(changes, key, subkey) {
  const v = changes?.[key]?.[subkey];
  return v !== undefined ? parseInt(v) : null;
}

function ftGroupSums(changes) {
  const s = (k) => getChange(changes, 'ht_ft', k) || 0;
  return {
    ev_ft:  s('1/1') + s('2/1') + s('X/1'),
    dep_ft: s('1/2') + s('2/2') + s('X/2'),
    bera:   s('1/X') + s('2/X') + s('X/X'),
  };
}

function evaluateSignals(markets, changes) {
  if (!markets || !changes) return [];
  const signals = [];

  const ms1    = getMarket(markets, '1x2',    'home');
  const ms2    = getMarket(markets, '1x2',    'away');
  const iy1    = getMarket(markets, 'ht_1x2', 'home');
  const iy2    = getMarket(markets, 'ht_1x2', 'away');
  const iyms21 = getMarket(markets, 'ht_ft',  '2/1');
  const iyms12 = getMarket(markets, 'ht_ft',  '1/2');
  const iyms11 = getMarket(markets, 'ht_ft',  '1/1');
  const iyms22 = getMarket(markets, 'ht_ft',  '2/2');

  const ch_ms1    = getChange(changes, '1x2',    'home');
  const ch_iy1    = getChange(changes, 'ht_1x2', 'home');
  const ch_iy2    = getChange(changes, 'ht_1x2', 'away');
  const ch_iyms21 = getChange(changes, 'ht_ft',  '2/1');
  const ch_iyms12 = getChange(changes, 'ht_ft',  '1/2');
  const ch_iyms22 = getChange(changes, 'ht_ft',  '2/2');

  const { ev_ft, dep_ft } = ftGroupSums(changes);
  const is_ev_dominance  = ev_ft <= -2 && dep_ft >= 2;
  const is_dep_dominance = dep_ft <= -3;

  const sig = (type, rule, prec, lift, tier) => {
    if (lift < MIN_LIFT) return;
    const t = tier || (lift >= 2.0 ? 'PREMIER' : 'STANDART');
    signals.push({ type, tier: t, rule, prec: `%${prec.toFixed(2)}`, lift: `${lift.toFixed(2)}x`, _lift: lift });
  };

  // 2/1 sinyalleri
  if (ms1 && ms1 <= 1.30 && iy2 && iy2 >= 3.5 && iy1 && iy1 <= 1.70 && iyms21 && iyms21 <= 25) {
    sig('2/1', 'Lens FP', 4.20, 1.47);
    if (ch_iyms21 === -1) {
      if (!ch_ms1 || ch_ms1 <= 0) {
        if (is_ev_dominance) sig('2/1', 'ELITE: Lens FP + IYMS↓ + EV FT TEMİZ', 8.0, 2.80, 'ELITE');
        else sig('2/1', 'ELITE: Lens FP + IYMS↓', 7.0, 2.44, 'ELITE');
      }
    }
  }

  if (iyms21 && iyms21 <= 20 && (!ch_ms1 || ch_ms1 <= 0)) {
    let lift = 1.60;
    if (ch_iyms21 === -1) { lift = is_ev_dominance ? 2.44 : 2.27; }
    if (iy2 && iy2 < 4.0) lift *= 0.70;
    if (dep_ft >= 3) lift *= 0.80;
    sig('2/1', 'IYMS_2/1≤20', 4.58, lift);
  } else if (iyms21 && iyms21 <= 25) {
    sig('2/1', 'IYMS_2/1≤25', 3.82, 1.33);
  }

  if (is_ev_dominance && ev_ft <= -3 && iyms21 && iyms21 <= 22 && iy2 && iy2 >= 4.5 && ch_iyms21 === -1 && (!ch_ms1 || ch_ms1 <= 0)) {
    sig('2/1', 'v9: 2/1 EV_FT+IYMS_DIVERGENCE', 6.5, 2.27, dep_ft <= 2 ? 'PREMIER' : 'STANDART');
  }

  // 1/2 sinyalleri
  const is_pure_12 = ch_iyms12 === -1 && (!ch_iyms22 || ch_iyms22 >= 0) && (!ch_iy1 || ch_iy1 <= 0);
  if (iyms12 && iyms12 <= 25 && is_pure_12) {
    sig('1/2', 'ELITE: IYMS_1/2≤25 + PURE', 7.2, 2.50, 'ELITE');
  } else if (iyms12 && iyms12 <= 25 && ch_iyms12 === -1) {
    sig('1/2', '1/2 ch=-1', 3.8, 1.33);
  }
  if (ms2 && ms2 <= 2.0 && ms1 && ms1 >= 4.0) {
    sig('1/2', 'MS-2≤2.0 + MS-1≥4.0', 4.26, 1.92);
  }
  if (iyms12 && iyms12 <= 20) {
    sig('1/2', 'IYMS_1/2≤20', 4.06, 1.83);
  }

  // 1/1 sinyalleri
  if (is_ev_dominance) {
    const iyms21_safe = !iyms21 || iyms21 > 24;
    const ch_iy1ok    = ch_iy1 !== null && ch_iy1 <= 0;
    if (ch_iy1ok && iyms21_safe && ms1 && ms1 <= 2.0) {
      let lift = dep_ft >= 3 ? 1.60 : 1.92;
      if (!iy2 || iy2 < 5.0) lift *= 0.85;
      sig('1/1', 'v9: 1/1 Ev Hakimiyeti', 5.5, lift, dep_ft >= 3 ? 'STANDART' : 'PREMIER');
    }
  }
  if (is_ev_dominance && ev_ft <= -3 && dep_ft >= 2 && iyms21 && 22 <= iyms21 && iyms21 <= 24 && ms1 && ms1 <= 2.0 && ch_iyms21 === -1) {
    sig('1/1', 'v9: GRİ BÖLGE 1/1?', 4.5, 1.57, 'STANDART');
  }

  // 2/2 sinyalleri
  if (iyms22 && iyms22 <= 5.0 && ms2 && ms2 <= 2.5) {
    let lift = ch_iyms22 === -1 ? 2.10 : 1.80;
    sig('2/2', 'v9: 2/2 DOĞAL', 5.2, lift, lift >= 2.0 ? 'PREMIER' : 'STANDART');
  }
  if (iyms22 && iyms22 <= 10 && ch_iyms22 === -1 && ms2 && ms2 <= 3.5 && ch_iyms12 !== -1) {
    sig('2/2', 'v9: 2/2 DİVERGENCE', 5.0, 1.74, 'STANDART');
  }

  return signals.sort((a, b) => b._lift - a._lift);
}

/* ─────────────────────────────────────────────────────────────────────
 * Maç bazında en iyi (maç öncesi son) snapshot'ı bul
 * ───────────────────────────────────────────────────────────────────── */
async function getPreMatchSnapshot(fixtureId, matchDate) {
  // Maç tarihinden 2 saat önceye kadar olan son snapshot'ı al
  const cutoff = new Date(matchDate);
  cutoff.setHours(cutoff.getHours() - 2);

  const { data, error } = await sb
    .from('odds_snapshots')
    .select('*')
    .eq('fixture_id', fixtureId)
    .lt('snapshot_time', cutoff.toISOString())
    .order('snapshot_time', { ascending: false })
    .limit(1);

  if (error || !data || data.length === 0) {
    // Herhangi bir snapshot al
    const { data: any } = await sb
      .from('odds_snapshots')
      .select('*')
      .eq('fixture_id', fixtureId)
      .order('snapshot_time', { ascending: false })
      .limit(1);
    return any?.[0] || null;
  }
  return data[0];
}

/* ─────────────────────────────────────────────────────────────────────
 * ANA ANALİZ FONKSİYONU
 * ───────────────────────────────────────────────────────────────────── */
async function analyze() {
  const now = new Date();
  const since = new Date(now.getTime() - DAYS * 86400000).toISOString();

  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║  SCOREPOP Sinyal→Sonuç Korelasyon Analizi               ║');
  console.log(`║  Son ${DAYS} gün | Min lift: ${MIN_LIFT}x${TIER_FILTER ? ` | Tier: ${TIER_FILTER}` : ''}`.padEnd(58) + '║');
  console.log('╚══════════════════════════════════════════════════════════╝');

  // ── 1. Biten maçları çek ──────────────────────────────────────────
  const { data: results, error: rErr } = await sb
    .from('match_results_cache')
    .select('*')
    .eq('is_final', true)
    .gte('captured_at', since);

  if (rErr) { console.error('[Analyzer] Sonuç hatası:', rErr.message); process.exit(1); }
  console.log(`\n[Analyzer] ${(results || []).length} biten maç bulundu`);
  if (!results?.length) { console.log('[Analyzer] Analiz edilecek veri yok.'); return; }

  // ── 2. fixture_ids → future_matches eşleştir ──────────────────────
  const fixIds = results.map(r => r.fixture_id);
  const { data: fixtures } = await sb
    .from('future_matches')
    .select('fixture_id, date, data')
    .in('fixture_id', fixIds);

  const fixtureMap = {};
  (fixtures || []).forEach(f => {
    fixtureMap[f.fixture_id] = {
      date: f.date,
      home: f.data?.teams?.home?.name || '',
      away: f.data?.teams?.away?.name || '',
    };
  });

  // ── 3. Her maç için analiz ────────────────────────────────────────
  const performances = [];
  const stats = {
    total: 0, with_signals: 0,
    by_type: {},
    by_tier: { ELITE: { correct: 0, total: 0 }, PREMIER: { correct: 0, total: 0 }, STANDART: { correct: 0, total: 0 } },
    lift_buckets: { '1.35-1.50': { correct: 0, total: 0 }, '1.50-2.00': { correct: 0, total: 0 }, '2.00+': { correct: 0, total: 0 } },
  };

  for (const result of results) {
    stats.total++;
    const fid = result.fixture_id;
    const fix = fixtureMap[fid] || {};

    // Maç öncesi snapshot
    const snap = await getPreMatchSnapshot(fid, fix.date || result.captured_at);
    if (!snap) continue;

    // Sinyal üret (en kümülatif değişimler snapshot'ta zaten var)
    const signals = evaluateSignals(snap.markets, snap.markets_change);

    // Tier filtresi
    const filteredSigs = TIER_FILTER
      ? signals.filter(s => s.tier === TIER_FILTER)
      : signals;

    const eliteCount   = filteredSigs.filter(s => s.tier === 'ELITE').length;
    const premierCount = filteredSigs.filter(s => s.tier === 'PREMIER').length;
    const stdCount     = filteredSigs.filter(s => s.tier === 'STANDART').length;

    if (eliteCount === 0 && premierCount === 0 && stdCount < 2) continue;

    stats.with_signals++;
    const topSig = filteredSigs[0];

    // Gerçek sonuç
    const actualHtFt = result.ht_ft_result;
    const correct    = topSig && actualHtFt && topSig.type === actualHtFt;

    // İstatistik güncelle
    if (!stats.by_type[topSig.type]) {
      stats.by_type[topSig.type] = { correct: 0, total: 0, examples: [] };
    }
    stats.by_type[topSig.type].total++;
    if (correct) stats.by_type[topSig.type].correct++;

    const tierStat = stats.by_tier[topSig.tier];
    if (tierStat) { tierStat.total++; if (correct) tierStat.correct++; }

    const liftBucket =
      topSig._lift >= 2.0   ? '2.00+' :
      topSig._lift >= 1.5   ? '1.50-2.00' :
                               '1.35-1.50';
    const liftStat = stats.lift_buckets[liftBucket];
    if (liftStat) { liftStat.total++; if (correct) liftStat.correct++; }

    // Örnek kaydet (yanlış tahminler için debug)
    if (!correct && stats.by_type[topSig.type].examples.length < 5) {
      stats.by_type[topSig.type].examples.push({
        match: `${fix.home || '?'} vs ${fix.away || '?'}`,
        signal: topSig.type, actual: actualHtFt,
        lift: topSig.lift, rule: topSig.rule,
        ev_ft: snap.ev_ft_sum, dep_ft: snap.dep_ft_sum,
      });
    }

    // signal_performance tablosu için hazırla
    performances.push({
      fixture_id:        fid,
      analyzed_at:       now.toISOString(),
      match_name:        `${fix.home} vs ${fix.away}`,
      match_date:        fix.date ? fix.date.slice(0, 10) : null,
      top_signal_type:   topSig.type,
      top_signal_tier:   topSig.tier,
      top_lift:          parseFloat(topSig._lift.toFixed(2)),
      all_signals:       filteredSigs.map(s => ({ type: s.type, tier: s.tier, lift: s.lift, rule: s.rule })),
      pre_match_ms1:     snap.ms1,
      pre_match_ms2:     snap.ms2,
      pre_match_iy1:     snap.iy1,
      pre_match_iy2:     snap.iy2,
      pre_match_iyms21:  snap.iyms21,
      ev_ft_trend:       snap.ev_ft_sum,
      dep_ft_trend:      snap.dep_ft_sum,
      actual_ht_home:    result.ht_home,
      actual_ht_away:    result.ht_away,
      actual_ft_home:    result.ft_home,
      actual_ft_away:    result.ft_away,
      actual_htft:       actualHtFt,
      signal_correct:    correct,
    });
  }

  // ── 4. signal_performance tablosuna yaz ──────────────────────────
  if (performances.length > 0) {
    const { error } = await sb
      .from('signal_performance')
      .upsert(performances, { onConflict: 'fixture_id' });
    if (error) console.error('[Analyzer] Performance kayıt hatası:', error.message);
    else console.log(`[Analyzer] ✅ ${performances.length} performans kaydı yazıldı`);
  }

  // ── 5. RAPOR ──────────────────────────────────────────────────────
  console.log('\n' + '═'.repeat(72));
  console.log('  ANALİZ RAPORU');
  console.log('═'.repeat(72));
  console.log(`  Taranan maç       : ${stats.total}`);
  console.log(`  Sinyal bulunan    : ${stats.with_signals}`);
  console.log(`  Analiz edilen     : ${performances.length}`);

  // Tier bazında
  console.log('\n  📊 TİER BAZINDA DOĞRULUK:');
  console.log('  ─'.repeat(36));
  for (const [tier, s] of Object.entries(stats.by_tier)) {
    if (s.total === 0) continue;
    const pct = (s.correct / s.total * 100).toFixed(1);
    const bar = '█'.repeat(Math.round(s.correct / s.total * 20));
    const icon = tier === 'ELITE' ? '💎' : tier === 'PREMIER' ? '⭐' : '·';
    console.log(`  ${icon} ${tier.padEnd(8)}: ${s.correct}/${s.total} = ${pct}%  ${bar}`);
  }

  // Tip bazında
  console.log('\n  📊 SİNYAL TİPİ BAZINDA DOĞRULUK:');
  console.log('  ─'.repeat(36));
  const typeOrder = ['2/1','1/2','1/1','2/2'];
  for (const type of typeOrder) {
    const s = stats.by_type[type];
    if (!s || s.total === 0) continue;
    const pct = (s.correct / s.total * 100).toFixed(1);
    const icon = { '2/1':'🟢','1/2':'🔵','1/1':'🟡','2/2':'🟣' }[type];
    console.log(`  ${icon} ${type.padEnd(4)}: ${s.correct}/${s.total} = ${pct}%`);
    if (s.examples.length > 0) {
      console.log('    Yanlış tahmin örnekleri:');
      for (const ex of s.examples) {
        console.log(`      • ${ex.match} | sinyal:${ex.signal} gerçek:${ex.actual} | lift:${ex.lift} | ev_ft=${ex.ev_ft} dep_ft=${ex.dep_ft}`);
        console.log(`        ↳ ${ex.rule}`);
      }
    }
  }

  // Lift bazında
  console.log('\n  📊 LIFT ARALIKLARI:');
  console.log('  ─'.repeat(36));
  for (const [bucket, s] of Object.entries(stats.lift_buckets)) {
    if (s.total === 0) continue;
    const pct = (s.correct / s.total * 100).toFixed(1);
    console.log(`  ${bucket.padEnd(12)}: ${s.correct}/${s.total} = ${pct}%`);
  }

  // Genel
  const totalCorrect = performances.filter(p => p.signal_correct).length;
  const totalAnalyzed = performances.length;
  if (totalAnalyzed > 0) {
    const overallPct = (totalCorrect / totalAnalyzed * 100).toFixed(1);
    console.log('\n' + '═'.repeat(72));
    console.log(`  GENEL DOĞRULUK: ${totalCorrect}/${totalAnalyzed} = ${overallPct}%`);
    console.log('═'.repeat(72));
  }

  console.log('\n[Analyzer] Analiz tamamlandı.');
}

analyze().catch(e => {
  console.error('[Analyzer] Kritik hata:', e);
  process.exit(1);
});
