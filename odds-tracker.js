/**
 * SCOREPOP — odds-tracker.js  (v1)
 * ════════════════════════════════════════════════════════════════════
 * GitHub Actions'ta 4.5 saat boyunca dönen ana tracker loop'u.
 *
 * Her INTERVAL_MS (varsayılan 5 dakika) döngüde:
 *   1. Nesine CDN'den bülteni çek
 *   2. future_matches'taki maçları eşleştir (odds_update.js mantığı)
 *   3. Her maç için odds_snapshots'a yaz (market değişimleri dahil)
 *   4. live_matches'ı oku → HT/FT geçişlerini match_results_cache'e yaz
 *   5. Bellek içi delta hesapla (snapshot'tan snapshot'a değişim)
 *
 * Çevre değişkenleri:
 *   SUPABASE_URL, SUPABASE_KEY
 *   INTERVAL_MS   — döngü aralığı ms (varsayılan: 300000 = 5 dk)
 *   MAX_RUNTIME_MS — toplam çalışma süresi ms (varsayılan: 16200000 = 4.5 saat)
 * ════════════════════════════════════════════════════════════════════
 */
'use strict';

const https  = require('https');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL   = process.env.SUPABASE_URL;
const SUPABASE_KEY   = process.env.SUPABASE_KEY;
const INTERVAL_MS    = parseInt(process.env.INTERVAL_MS    || '300000');   // 5 dk
const MAX_RUNTIME_MS = parseInt(process.env.MAX_RUNTIME_MS || '16200000'); // 4.5 saat

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('[Tracker] SUPABASE_URL ve SUPABASE_KEY gerekli');
  process.exit(1);
}
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

/* ─────────────────────────────────────────────────────────────────────
 * HTTP GET
 * ───────────────────────────────────────────────────────────────────── */
function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'Accept':          'application/json',
        'Accept-Encoding': 'identity',
        'Referer':         'https://www.nesine.com/',
        'Origin':          'https://www.nesine.com',
        'User-Agent':      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
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

/* ─────────────────────────────────────────────────────────────────────
 * Normalize & Alias (odds_update.js'den kopyalandı)
 * ───────────────────────────────────────────────────────────────────── */
function norm(s) {
  return (s || '')
    .toLowerCase()
    .replace(/ğ/g,'g').replace(/ü/g,'u').replace(/ş/g,'s')
    .replace(/ı/g,'i').replace(/ö/g,'o').replace(/ç/g,'c')
    .replace(/[^a-z0-9]/g,' ')
    .replace(/\s+/g,' ').trim();
}

const TEAM_ALIASES = {
  'seattle s':'seattle sounders','st louis':'s louis city',
  's san jose':'deportivo saprissa','cs cartagines':'cartagines',
  'gabala':'kabala','panaitolikos':'paneitolikos','panserraikos':'panseraikos',
  'rz pellets wac':'wolfsberger','tsv egger glas hartberg':'hartberg',
  'fc red bull salzburg':'salzburg','ksv 1919':'kapfenberger sv',
  'sk rapid ii':'r wien amt','sw bregenz':'schwarz weiss b',
  'sk austria klagenfurt':'klagenfurt','skn st polten':'st polten',
  'skn st pölten':'st polten','fc hertha wels':'wsc hertha',
  'b68 toftir':'tofta itrottarfelag b68','ca ferrocarril midland':'f midland',
  'gimnasia y esgrima de men':'gimnasia y','estudiantes rio cuarto':'e rio cuarto',
  'ind medellin':'ind medellin','america de cali':'america cali',
  'napredak':'fk napredak kru','tsc backa to':'tsc backa t',
  'd makhachkala':'dyn makhachkala','rfc liege':'rfc liege',
  'raal la louviere':'raal la louviere','racing genk b':'j krc genk u23',
  'h w welders':'harland wolff w','adelaide united fc k':'adelaide utd k',
  'canberra utd k':'canberra utd k','brisbane roar fc k':'brisbane r k',
  'kyzylzhar':'kyzyl zhar sk','d batumi':'dinamo b',
  'algeciras cf':'algeciras','ibiza':'i eivissa',
  'gubbio':'as gubbio 1910','pineto':'asd pineto calcio',
  'mont tuscia':'monterosi t','ssd casarano calcio':'casarano',
  'palermo':'us palermo','avellino':'as avellino 1912',
  'utdofmanch':'utd of manch','sg sonnenhof grossaspach':'grossaspach',
  'chengdu':'chengdu ron','qingdao y i':'qingdao yth is',
  'bragantino':'rb bragantino','palmeiras':'palmeiras sp',
  'gremio':'gremio p','baltika':'b kaliningrad','velez':'v sarsfield',
  's shenhua':'shanghai s','tianjin jinmen':'tianjin jin',
  'g birliği':'gençlerbirliği','1 fc slovacko':'slovacko',
  'jagiellonia':'j bialystok','ilves':'tampereen i',
  'auvergne':'le puy foot 43','juventud':'ca juventud de las piedras',
  'akademisk bo':'ab gladsaxe','lusitania de lourosa':'lusitania',
  'stade nyonnais':'std nyonnis','fc zurich':'zurih',
  'cordoba cf':'cordoba','deportivo':'dep la coruna',
  'future fc':'modern sport club','new york rb':'ny red bulls',
  'the new saints':'tns','vancouver':'v whitecaps',
  'fc hradec kralove':'h kralove','fc midtjylland':'midtjylland',
  'sønderjyske':'sonderjyske','pacos de ferreira':'p ferreira',
  's cristal':'sporting cristal','palmeiras sp':'palmeiras',
  'not forest':'nottingham forest','cry. palace':'crystal palace',
};

function normWithAlias(s) {
  const n = norm(s);
  return TEAM_ALIASES[n] || n;
}

function tokenSim(a, b) {
  const ta = new Set(norm(a).split(' ').filter(x => x.length > 1));
  const tb = new Set(norm(b).split(' ').filter(x => x.length > 1));
  if (!ta.size || !tb.size) return 0;
  let hit = 0;
  for (const t of ta) {
    if (tb.has(t)) { hit++; continue; }
    for (const u of tb) {
      if (t.startsWith(u) || u.startsWith(t)) { hit += 0.7; break; }
    }
  }
  return hit / Math.max(ta.size, tb.size);
}

function matchScore(homeDB, awayDB, ev) {
  const hs  = tokenSim(normWithAlias(homeDB), norm(ev.HN));
  const as_ = tokenSim(normWithAlias(awayDB), norm(ev.AN));
  return { hs, as_, avg: (hs + as_) / 2 };
}

function findBestMatch(fix, events) {
  const THRESHOLD    = 0.35;
  const MIN_PER_TEAM = 0.20;
  const ONE_SIDE_HIGH= 0.65;
  const CROSS_MIN    = 0.25;

  // Aşama 1: Normal
  let bestNormal = null, bestNormalScore = THRESHOLD - 0.01;
  for (const ev of events) {
    const normal  = matchScore(fix.home_team, fix.away_team, ev);
    const swapped = matchScore(fix.away_team, fix.home_team, ev);
    const s = normal.avg >= swapped.avg ? normal : swapped;
    if (s.hs >= MIN_PER_TEAM && s.as_ >= MIN_PER_TEAM && s.avg > bestNormalScore) {
      bestNormalScore = s.avg;
      bestNormal = ev;
    }
  }
  if (bestNormal) return { ev: bestNormal, score: bestNormalScore, method: 'normal' };

  // Aşama 2: Çapraz
  let bestCross = null, bestCrossScore = -1;
  const homeDB = normWithAlias(fix.home_team);
  const awayDB = normWithAlias(fix.away_team);
  for (const ev of events) {
    const hn = norm(ev.HN);
    const an = norm(ev.AN);
    const combos = [
      { strong: tokenSim(homeDB, hn), cross: tokenSim(awayDB, an) },
      { strong: tokenSim(homeDB, an), cross: tokenSim(awayDB, hn) },
      { strong: tokenSim(awayDB, hn), cross: tokenSim(homeDB, an) },
      { strong: tokenSim(awayDB, an), cross: tokenSim(homeDB, hn) },
    ];
    for (const c of combos) {
      if (c.strong >= ONE_SIDE_HIGH && c.cross >= CROSS_MIN) {
        const confidence = (c.strong + c.cross) / 2;
        if (confidence >= THRESHOLD && confidence > bestCrossScore) {
          bestCrossScore = confidence;
          bestCross = { ev };
        }
      }
    }
  }
  if (bestCross && bestCrossScore >= THRESHOLD)
    return { ev: bestCross.ev, score: bestCrossScore, method: 'cross' };

  return null;
}

/* ─────────────────────────────────────────────────────────────────────
 * Market Parse (odds_update.js'den kopyalandı)
 * ───────────────────────────────────────────────────────────────────── */
function parseMarkets(maArr) {
  const markets = {};
  if (!Array.isArray(maArr)) return markets;
  for (const m of maArr) {
    const mtid = m.MTID;
    const sov  = parseFloat(m.SOV ?? 0);
    const oca  = m.OCA || [];
    const get  = (n) => { const o = oca.find(x => x.N === n); return o ? +o.O : 0; };
    if (mtid === 1   && oca.length === 3) { markets['1x2']        = { home: get(1), draw: get(2), away: get(3) }; }
    if (mtid === 3   && oca.length === 3) { markets['dc']         = { '1x': get(1), '12': get(2), 'x2': get(3) }; }
    if (mtid === 5   && oca.length === 9) {
      markets['ht_ft'] = { '1/1': get(1), '1/X': get(2), '1/2': get(3), 'X/1': get(4), 'X/X': get(5), 'X/2': get(6), '2/1': get(7), '2/X': get(8), '2/2': get(9) };
    }
    if (mtid === 7   && oca.length === 3) { markets['ht_1x2']     = { home: get(1), draw: get(2), away: get(3) }; }
    if (mtid === 9   && oca.length === 3) { markets['2h_1x2']     = { home: get(1), draw: get(2), away: get(3) }; }
    if (mtid === 11  && oca.length === 2) { markets['ou15']       = { under: get(1), over: get(2) }; }
    if (mtid === 12  && oca.length === 2) { markets['ou25']       = { under: get(1), over: get(2) }; }
    if (mtid === 13  && oca.length === 2) { markets['ou35']       = { under: get(1), over: get(2) }; }
    if (mtid === 38  && oca.length === 2) { markets['btts']       = { yes: get(1), no: get(2) }; }
    if (mtid === 48  && oca.length === 3) { markets['more_goals_half'] = { first: get(1), equal: get(2), second: get(3) }; }
    if (mtid === 342 && oca.length === 6) { markets['ms_ou15']    = { 'h_u': get(1), 'x_u': get(2), 'a_u': get(3), 'h_o': get(4), 'x_o': get(5), 'a_o': get(6) }; }
    if (mtid === 343 && oca.length === 6) { markets['ms_ou25']    = { 'h_u': get(1), 'x_u': get(2), 'a_u': get(3), 'h_o': get(4), 'x_o': get(5), 'a_o': get(6) }; }
    if (mtid === 414 && oca.length === 6) { markets['ms_kg']      = { 'h_y': get(1), 'x_y': get(3), 'a_y': get(5), 'h_n': get(2), 'x_n': get(4), 'a_n': get(6) }; }
    if (mtid === 291 && oca.length === 3) { markets['first_goal'] = { home: get(1), none: get(2), away: get(3) }; }
    if (mtid === 268 && oca.length === 3) {
      const sign = sov >= 0 ? `p${String(sov).replace('.','_')}` : `m${String(Math.abs(sov)).replace('.','_')}`;
      markets[`ah_${sign}`] = { home: get(1), draw: get(2), away: get(3), line: sov };
    }
    if (mtid === 155 && oca.length === 2) {
      if      (Math.abs(sov - 4.5) < 0.01) { markets['ou45'] = { under: get(1), over: get(2) }; }
      else if (Math.abs(sov - 5.5) < 0.01) { markets['ou55'] = { under: get(1), over: get(2) }; }
    }
    if (mtid === 49  && oca.length === 2) { markets['odd_even']   = { odd: get(1), even: get(2) }; }
    if (mtid === 43  && oca.length === 4) { markets['goal_range'] = { '0_1': get(1), '2_3': get(2), '4_5': get(3), '6p': get(4) }; }
  }
  return markets;
}

/* ─────────────────────────────────────────────────────────────────────
 * HT/FT Delta Hesabı (iki snapshot arasındaki fark)
 * ───────────────────────────────────────────────────────────────────── */
function calcHtFtDelta(prevMarkets, currMarkets) {
  const changes = {};
  const htft_keys = ['1/1','1/X','1/2','X/1','X/X','X/2','2/1','2/X','2/2'];
  const prev = prevMarkets?.ht_ft || {};
  const curr = currMarkets?.ht_ft || {};
  changes.ht_ft = {};
  for (const k of htft_keys) {
    if (prev[k] && curr[k] && prev[k] !== curr[k]) {
      changes.ht_ft[k] = curr[k] > prev[k] ? 1 : -1;
    } else {
      changes.ht_ft[k] = 0;
    }
  }
  // 1x2
  changes['1x2'] = {};
  for (const k of ['home','draw','away']) {
    const p = prevMarkets?.['1x2']?.[k];
    const c = currMarkets?.['1x2']?.[k];
    if (p && c && p !== c) changes['1x2'][k] = c > p ? 1 : -1;
    else changes['1x2'][k] = 0;
  }
  // ht_1x2
  changes['ht_1x2'] = {};
  for (const k of ['home','draw','away']) {
    const p = prevMarkets?.['ht_1x2']?.[k];
    const c = currMarkets?.['ht_1x2']?.[k];
    if (p && c && p !== c) changes['ht_1x2'][k] = c > p ? 1 : -1;
    else changes['ht_1x2'][k] = 0;
  }
  return changes;
}

/* ─────────────────────────────────────────────────────────────────────
 * EV/DEP FT Grup Toplamları (reversal_signals.py mantığı)
 * ───────────────────────────────────────────────────────────────────── */
function ftGroupSums(changes) {
  const s = (k) => changes?.ht_ft?.[k] || 0;
  const ev_ft  = s('1/1') + s('2/1') + s('X/1');
  const dep_ft = s('1/2') + s('2/2') + s('X/2');
  const bera   = s('1/X') + s('2/X') + s('X/X');
  return { ev_ft, dep_ft, bera };
}

/* ─────────────────────────────────────────────────────────────────────
 * HT/FT Sonuç Hesabı
 * ───────────────────────────────────────────────────────────────────── */
function calcHtFtResult(htHome, htAway, ftHome, ftAway) {
  if (htHome === null || ftHome === null) return null;
  const ht = htHome > htAway ? '1' : htHome < htAway ? '2' : 'X';
  const ft = ftHome > ftAway ? '1' : ftHome < ftAway ? '2' : 'X';
  return `${ht}/${ft}`;
}

/* ─────────────────────────────────────────────────────────────────────
 * Bellek İçi Durum
 * ───────────────────────────────────────────────────────────────────── */
const state = {
  // fixture_id → { markets, changes, ev_ft_cumulative, dep_ft_cumulative }
  snapCache: new Map(),
  // fixture_id → { status, htHome, htAway }
  liveCache: new Map(),
  cycleCount: 0,
  startTime:  Date.now(),
};

/* ─────────────────────────────────────────────────────────────────────
 * Tek Döngü Adımı
 * ───────────────────────────────────────────────────────────────────── */
async function runCycle() {
  state.cycleCount++;
  const now = new Date().toISOString();
  const elapsed = Math.round((Date.now() - state.startTime) / 60000);
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`[Tracker] Döngü #${state.cycleCount} | ${now} | +${elapsed}dk`);
  console.log('═'.repeat(60));

  // ── 1. future_matches çek ─────────────────────────────────────────
  const { data: rawFixtures, error: fErr } = await sb
    .from('future_matches')
    .select('fixture_id, date, data')
    .limit(1200);

  if (fErr) { console.error('[Tracker] Supabase hata:', fErr.message); return; }

  const allFixtures = (rawFixtures || []).map(row => {
    const d = row.data || {};
    return {
      fixture_id: row.fixture_id,
      date:       row.date,
      home_team:  d.teams?.home?.name || '',
      away_team:  d.teams?.away?.name || '',
    };
  }).filter(f => f.home_team && f.away_team);

  // ── 2. Nesine bülten çek ──────────────────────────────────────────
  let nesineData;
  try {
    nesineData = await fetchJSON('https://cdnbulten.nesine.com/api/bulten/getprebultenfull');
  } catch (e) {
    console.error('[Tracker] Nesine hatası:', e.message);
    return;
  }
  const events = (nesineData?.sg?.EA || []).filter(e => e.TYPE === 1);
  console.log(`[Tracker] ${allFixtures.length} maç | ${events.length} Nesine event`);

  // ── 3. Her maç için snapshot al ───────────────────────────────────
  const snapUpserts = [];
  let matched = 0, unmatched = 0;

  for (const fix of allFixtures) {
    const result = findBestMatch(fix, events);
    if (!result) { unmatched++; continue; }

    const { ev: best, method } = result;
    const markets = parseMarkets(best.MA);
    if (Object.keys(markets).length === 0) continue;

    // Önceki snapshot ile delta hesapla
    const prev = state.snapCache.get(fix.fixture_id);
    const changes = prev ? calcHtFtDelta(prev.markets, markets) : {};
    const { ev_ft, dep_ft } = ftGroupSums(changes);

    // Kümülatif trend güncelle (tüm oturum boyunca birikim)
    if (!prev) {
      state.snapCache.set(fix.fixture_id, {
        markets, changes,
        ev_ft_cum: 0, dep_ft_cum: 0,
      });
    } else {
      const prevState = state.snapCache.get(fix.fixture_id);
      state.snapCache.set(fix.fixture_id, {
        markets, changes,
        ev_ft_cum:  (prevState.ev_ft_cum  || 0) + ev_ft,
        dep_ft_cum: (prevState.dep_ft_cum || 0) + dep_ft,
      });
    }

    const cached = state.snapCache.get(fix.fixture_id);

    // Düz market kolonları
    const ms1    = markets['1x2']?.home    || null;
    const ms2    = markets['1x2']?.away    || null;
    const iy1    = markets['ht_1x2']?.home || null;
    const iy2    = markets['ht_1x2']?.away || null;
    const iyms21 = markets['ht_ft']?.['2/1'] || null;
    const iyms12 = markets['ht_ft']?.['1/2'] || null;
    const iyms11 = markets['ht_ft']?.['1/1'] || null;
    const iyms22 = markets['ht_ft']?.['2/2'] || null;

    snapUpserts.push({
      fixture_id:    fix.fixture_id,
      snapshot_time: now,
      markets,
      markets_change: changes,
      nesine_name:   `${best.HN} - ${best.AN}`,
      match_method:  method,
      ms1, ms2, iy1, iy2, iyms21, iyms12, iyms11, iyms22,
      ev_ft_sum:     cached.ev_ft_cum,
      dep_ft_sum:    cached.dep_ft_cum,
    });

    // Önemli değişimleri logla
    if (Math.abs(ev_ft) > 0 || Math.abs(dep_ft) > 0) {
      console.log(
        `  📊 ${fix.home_team} vs ${fix.away_team}` +
        ` | ev_ft=${ev_ft >= 0?'+':''}${ev_ft}` +
        ` dep_ft=${dep_ft >= 0?'+':''}${dep_ft}` +
        ` | cum: ev=${cached.ev_ft_cum >= 0?'+':''}${cached.ev_ft_cum}` +
        ` dep=${cached.dep_ft_cum >= 0?'+':''}${cached.dep_ft_cum}`
      );
    }
    matched++;
  }

  console.log(`[Tracker] Eşleşme: ${matched} ✓ | ${unmatched} ✗`);

  // ── 4. Snapshot'ları kaydet ───────────────────────────────────────
  if (snapUpserts.length > 0) {
    // Toplu insert — çakışma olursa yeni satır ekle (tarihsel veri)
    const BATCH = 50;
    let saved = 0;
    for (let i = 0; i < snapUpserts.length; i += BATCH) {
      const chunk = snapUpserts.slice(i, i + BATCH);
      const { error } = await sb.from('odds_snapshots').insert(chunk);
      if (error) {
        // Duplicate key → normalde beklenebilir (aynı dakika içinde 2 çalışma)
        if (!error.message.includes('duplicate')) {
          console.error('[Tracker] Snapshot kayıt hatası:', error.message);
        }
      } else {
        saved += chunk.length;
      }
    }
    console.log(`[Tracker] ✅ ${saved} snapshot kaydedildi`);
  }

  // ── 5. live_matches → HT/FT cache ────────────────────────────────
  await syncLiveMatches();
}

/* ─────────────────────────────────────────────────────────────────────
 * Live Matches Sync
 * ───────────────────────────────────────────────────────────────────── */
async function syncLiveMatches() {
  let liveRows;
  try {
    const { data, error } = await sb
      .from('live_matches')
      .select('fixture_id, status_short, home_score, away_score')
      .in('status_short', ['1H','HT','2H','FT']);

    if (error) { console.error('[Live] Supabase hata:', error.message); return; }
    liveRows = data || [];
  } catch (e) {
    console.error('[Live] Hata:', e.message); return;
  }

  if (liveRows.length === 0) {
    console.log('[Live] Aktif/biten maç yok');
    return;
  }

  console.log(`[Live] ${liveRows.length} aktif/biten maç bulundu`);
  const cacheInserts = [];
  const now = new Date().toISOString();

  for (const row of liveRows) {
    const fid    = row.fixture_id;
    const status = row.status_short;
    const hScore = row.home_score ?? null;
    const aScore = row.away_score ?? null;
    const prev   = state.liveCache.get(fid) || {};

    // Statü değişimi kontrolü
    if (prev.status === status && status !== 'FT') continue;

    let htHome = prev.htHome ?? null;
    let htAway = prev.htAway ?? null;
    let ftHome = null, ftAway = null;
    let htFtResult = null;

    if (status === 'HT') {
      htHome = hScore;
      htAway = aScore;
      state.liveCache.set(fid, { ...prev, status, htHome, htAway });
      console.log(`  ⏸  HT yakalandı: fixture=${fid} | ${hScore}-${aScore}`);
    } else if (status === 'FT') {
      ftHome = hScore;
      ftAway = aScore;
      htFtResult = calcHtFtResult(htHome, htAway, ftHome, ftAway);
      state.liveCache.set(fid, { ...prev, status, ftHome, ftAway, htFtResult });
      console.log(`  🏁 FT yakalandı: fixture=${fid} | HT:${htHome}-${htAway} FT:${ftHome}-${ftAway} → ${htFtResult}`);
    } else {
      state.liveCache.set(fid, { ...prev, status });
    }

    cacheInserts.push({
      fixture_id:   fid,
      captured_at:  now,
      status_short: status,
      home_score:   hScore,
      away_score:   aScore,
      ht_home:      htHome,
      ht_away:      htAway,
      ft_home:      ftHome,
      ft_away:      ftAway,
      ht_ft_result: htFtResult,
      is_final:     status === 'FT',
    });
  }

  if (cacheInserts.length > 0) {
    const { error } = await sb
      .from('match_results_cache')
      .upsert(cacheInserts, { onConflict: 'fixture_id,status_short' });
    if (error && !error.message.includes('duplicate')) {
      console.error('[Live] Cache kayıt hatası:', error.message);
    } else {
      console.log(`[Live] ✅ ${cacheInserts.length} live durum kaydedildi`);
    }
  }
}

/* ─────────────────────────────────────────────────────────────────────
 * ANA LOOP
 * ───────────────────────────────────────────────────────────────────── */
async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║  SCOREPOP Odds Tracker — Sürekli İzleme Modu            ║');
  console.log(`║  Aralık: ${Math.round(INTERVAL_MS/60000)} dk | Maksimum: ${Math.round(MAX_RUNTIME_MS/3600000)} saat           ║`);
  console.log('╚══════════════════════════════════════════════════════════╝');

  const deadline = state.startTime + MAX_RUNTIME_MS;

  while (Date.now() < deadline) {
    const cycleStart = Date.now();

    try {
      await runCycle();
    } catch (e) {
      console.error('[Tracker] Döngü hatası:', e.message);
    }

    // Kalan süre kontrolü
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;

    const cycleElapsed = Date.now() - cycleStart;
    const wait = Math.max(0, INTERVAL_MS - cycleElapsed);
    const waitMin = Math.round(wait / 60000 * 10) / 10;

    console.log(`\n[Tracker] Sonraki döngü: ${waitMin} dk sonra | Kalan oturum: ${Math.round(remaining/60000)} dk`);

    if (wait > 0 && remaining > wait) {
      await new Promise(r => setTimeout(r, wait));
    }
  }

  // ── Özet ──────────────────────────────────────────────────────────
  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║  OTURUM SONA ERDİ                                        ║');
  console.log(`║  Toplam döngü: ${state.cycleCount.toString().padEnd(40)}║`);
  console.log(`║  İzlenen maç : ${state.snapCache.size.toString().padEnd(40)}║`);
  console.log('╚══════════════════════════════════════════════════════════╝');

  // Kümülatif trend özeti
  const topMovers = [...state.snapCache.entries()]
    .map(([fid, s]) => ({ fid, ev: s.ev_ft_cum, dep: s.dep_ft_cum }))
    .filter(m => Math.abs(m.ev) >= 2 || Math.abs(m.dep) >= 2)
    .sort((a, b) => Math.abs(b.ev) + Math.abs(b.dep) - Math.abs(a.ev) - Math.abs(a.dep))
    .slice(0, 20);

  if (topMovers.length > 0) {
    console.log('\n[Tracker] EN ÇOK HAREKET EDEN MAÇLAR (Oturum Kümülatif):');
    console.log('─'.repeat(60));
    for (const m of topMovers) {
      const evTag  = m.ev  <= -2 ? '⬇EV' : m.ev  >= 2 ? '⬆EV' : '  ';
      const depTag = m.dep >= 2  ? '⬆DEP': m.dep <= -2 ? '⬇DEP': '   ';
      console.log(`  fixture=${m.fid} | ev_ft=${m.ev >= 0?'+':''}${m.ev} ${evTag} | dep_ft=${m.dep >= 0?'+':''}${m.dep} ${depTag}`);
    }
  }
}

main().catch(e => {
  console.error('[Tracker] Kritik hata:', e);
  process.exit(1);
});
