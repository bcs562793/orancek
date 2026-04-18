/**
 * ai_tracker.js — ScorePop AI Tracker v1
 * ═══════════════════════════════════════════════════════════════════
 * Nesine CDN'den canlı oran çeker → piyasa delta'larını hesaplar →
 * sinyal motorunu çalıştırır → Claude AI ile yorumlar → e-posta atar.
 *
 * Cache GitHub artifact olarak yaşar. Sadece Canlı Skor okumak için Supabase'e bağlanır.
 *
 * Ortam değişkenleri (GitHub Secrets):
 * ANTHROPIC_API_KEY   — Claude API anahtarı
 * SMTP_HOST           — Mail sunucusu (örn: smtp.gmail.com)
 * SMTP_PORT           — Port (örn: 587)
 * SMTP_USER           — Gönderen e-posta
 * SMTP_PASS           — App password
 * MAIL_TO             — Alıcı e-posta (virgülle ayırarak birden fazla)
 * INTERVAL_MS         — Döngü aralığı ms (varsayılan: 300000 = 5 dk)
 * MAX_RUNTIME_MS      — Toplam çalışma ms (varsayılan: 17100000 = 4.75 saat)
 * LOOKAHEAD_HOURS     — Kaç saat içindeki maçlar (varsayılan: 8)
 * CACHE_FILE          — Cache dosya yolu (varsayılan: tracker_cache.json)
 * FIRED_FILE          — Gönderilen alarmlar (varsayılan: fired_alerts.json)
 * DRY_RUN             — true → mail göndermez, loglar
 * SUPABASE_URL        — Supabase Proje URL'si (Canlı skorlar için)
 * SUPABASE_KEY        — Supabase API Anahtarı (Canlı skorlar için)
 * ═══════════════════════════════════════════════════════════════════
 */
'use strict';

const https    = require('https');
const fs       = require('fs');
const path     = require('path');
const nodemailer = require('nodemailer');
const { createClient } = require('@supabase/supabase-js');

// ── Config ──────────────────────────────────────────────────────────
const ANTHROPIC_KEY  = process.env.ANTHROPIC_API_KEY || '';
const INTERVAL_MS    = parseInt(process.env.INTERVAL_MS    || '300000');
const MAX_RUNTIME_MS = parseInt(process.env.MAX_RUNTIME_MS || '17100000');
const LOOKAHEAD_H    = parseInt(process.env.LOOKAHEAD_HOURS || '8');
const CACHE_FILE     = process.env.CACHE_FILE  || 'tracker_cache.json';
const FIRED_FILE     = process.env.FIRED_FILE  || 'fired_alerts.json';
const DRY_RUN        = process.env.DRY_RUN === 'true';
const MIN_SIGNALS    = 2;   // E-posta için minimum sinyal sayısı

// Canlı skor okumak için Supabase (Eğer ayarlı değilse sessizce iptal olur)
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const sb = (SUPABASE_URL && SUPABASE_KEY) ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ── State ────────────────────────────────────────────────────────────
// matchCache: fixture_id → { markets, ev_ft_cum, dep_ft_cum, snapshots:[], liveData:{}, ... }
const matchCache = new Map();
let firedAlerts  = {};       // fixture_id → [sinyal tipi, ...]
let cycleCount   = 0;
const startTime  = Date.now();

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 1 — CACHE (Disk ↔ Bellek)
// ════════════════════════════════════════════════════════════════════
function loadCache() {
  if (fs.existsSync(CACHE_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
      for (const [fid, val] of Object.entries(data.matchCache || {})) {
        matchCache.set(fid, val);
      }
      console.log(`[Cache] ${matchCache.size} maç yüklendi`);
    } catch (e) { console.warn('[Cache] Yüklenemedi:', e.message); }
  }

  if (fs.existsSync(FIRED_FILE)) {
    try { firedAlerts = JSON.parse(fs.readFileSync(FIRED_FILE, 'utf8')); }
    catch { firedAlerts = {}; }
  }
}

function saveCache() {
  const obj = { savedAt: new Date().toISOString(), matchCache: {} };
  for (const [fid, val] of matchCache.entries()) {
    obj.matchCache[fid] = val;
  }
  fs.writeFileSync(CACHE_FILE, JSON.stringify(obj, null, 2));
  fs.writeFileSync(FIRED_FILE, JSON.stringify(firedAlerts, null, 2));
}

function alreadyFired(fid, label) {
  return (firedAlerts[fid] || []).includes(label);
}
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
        'User-Agent': 'Mozilla/5.0 (compatible; ScorePop/1.0)',
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
    const id = x.MTID;
    const oca = x.OCA || [];
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
    if (id === 38 && oca.length === 2) m['btts']   = { yes:g(1),   no:g(2) };
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
    ch[k] = {};
    const p = prev[k] || {}, c = curr[k] || {};
    for (const sub of Object.keys({...p,...c})) {
      const pv = p[sub], cv = c[sub];
      if (pv && cv && pv !== cv) ch[k][sub] = +(cv - pv).toFixed(3);
      else ch[k][sub] = 0;
    }
  }
  return ch;
}

// ─ FT grup toplamı ─
function ftGroups(changes) {
  const s = k => changes?.ht_ft?.[k] || 0;
  return {
    ev_ft  : s('1/1') + s('2/1') + s('X/1'),
    dep_ft : s('1/2') + s('2/2') + s('X/2'),
    bera   : s('1/X') + s('2/X') + s('X/X'),
  };
}

// ─ Kümülatif delta (tüm session boyunca biriken sayısal fark) ─
function calcCumDelta(prev, curr) {
  // Sadece yön (+ -) değil, ham fark topla
  const directions = { ev_ft: 0, dep_ft: 0 };
  const htft = curr['ht_ft'] || {};
  const phtft = prev['ht_ft'] || {};

  const evKeys  = ['1/1','2/1','X/1'];
  const depKeys = ['1/2','2/2','X/2'];
  for (const k of evKeys)  { const d = (htft[k]||0) - (phtft[k]||0); if (d < 0) directions.ev_ft  += d; }
  for (const k of depKeys) { const d = (htft[k]||0) - (phtft[k]||0); if (d < 0) directions.dep_ft += d; }
  return directions;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 6 — SİNYAL MOTORU
// ════════════════════════════════════════════════════════════════════
function gm(markets, key, sub) {
  const v = markets?.[key]?.[sub];
  return v ? +v : null;
}
function gc(changes, key, sub) {
  const v = changes?.[key]?.[sub];
  return v !== undefined ? +v : null;
}

function evaluateSignals(markets, changes, cumCache) {
  const signals = [];

  const ms1    = gm(markets,'1x2','home');
  const ms2    = gm(markets,'1x2','away');
  const iy1    = gm(markets,'ht_1x2','home');
  const iy2    = gm(markets,'ht_1x2','away');
  const sy1    = gm(markets,'2h_1x2','home');
  const sy2    = gm(markets,'2h_1x2','away');
  const iyms21 = gm(markets,'ht_ft','2/1');
  const iyms12 = gm(markets,'ht_ft','1/2');
  const iyms11 = gm(markets,'ht_ft','1/1');
  const iyms22 = gm(markets,'ht_ft','2/2');
  const au25u  = gm(markets,'ou25','over');
  const au25a  = gm(markets,'ou25','under');
  const bttsY  = gm(markets,'btts','yes');
  const dcg2h  = gm(markets,'more_goals_half','second');

  const ch_ms1    = gc(changes,'1x2','home');
  const ch_ms2    = gc(changes,'1x2','away');
  const ch_iy1    = gc(changes,'ht_1x2','home');
  const ch_iy2    = gc(changes,'ht_1x2','away');
  const ch_sy1    = gc(changes,'2h_1x2','home');
  const ch_iyms21 = gc(changes,'ht_ft','2/1');
  const ch_iyms22 = gc(changes,'ht_ft','2/2');
  const ch_iyms12 = gc(changes,'ht_ft','1/2');
  const ch_au25   = gc(changes,'ou25','over');

  const hasChange = changes && Object.keys(changes).length > 0;
  const { ev_ft, dep_ft } = ftGroups(changes || {});
  const ev_ft_cum  = cumCache?.ev_ft_cum  || 0;
  const dep_ft_cum = cumCache?.dep_ft_cum || 0;

  const isEvDom  = hasChange && ev_ft  <= -2 && dep_ft >= 2;
  const isDepDom = hasChange && dep_ft <= -3;

  function add(type, tier, rule, prec, lift) {
    if (lift < 1.35) return;
    signals.push({ type, tier, rule, prec, lift });
  }

  // ── 2/1 Lens Fingerprint ─────────────────────────────────────────
  if (ms1 && ms1 <= 1.30 && iy2 && iy2 >= 3.5 &&
      iy1 && iy1 <= 1.70 && sy1 && sy1 <= 1.70 &&
      iyms21 && iyms21 <= 25) {
    if (hasChange && ch_iyms21 < 0 && (ch_ms1 === null || ch_ms1 <= 0)) {
      if (isEvDom) add('2/1','ELITE','Lens FP + IYMS21↓ + EV FT TEMİZ',8.0,2.80);
      else         add('2/1','ELITE','Lens FP + IYMS21↓ (divergence)',7.0,2.44);
    } else {
      add('2/1','PREMIER','Lens FP: ms1≤1.30+iy2≥3.5+iy1≤1.7+2y1≤1.7',4.2,1.47);
    }
  }

  // ── 2/1 IYMS21 eşiği ─────────────────────────────────────────────
  if (iyms21 && iyms21 <= 25 && !(ch_ms1 && ch_ms1 > 0)) {
    let lift = iyms21 <= 20 ? 1.60 : 1.33;
    let tier = 'STANDART', rule = `IYMS21≤${iyms21<=20?20:25}`;
    if (hasChange && ch_iyms21 < 0) {
      lift = isEvDom ? 2.44 : 2.27; tier = 'PREMIER'; rule += ' + IYMS↓';
    }
    if (iy2 && iy2 < 4.0) lift *= 0.70;
    if (dep_ft >= 3)       lift *= 0.80;
    add('2/1', tier, rule, 4.58, lift);
  }

  // ── 2/1 v9: EV_FT≤-3 + IYMS21 ≤ 22 + iy2 ≥ 4.5 ─────────────────
  if (isEvDom && ev_ft <= -3 && iyms21 && iyms21 <= 22 &&
      iy2 && iy2 >= 4.5 && hasChange && ch_iyms21 < 0 &&
      (ch_ms1 === null || ch_ms1 <= 0)) {
    add('2/1','PREMIER','v9: EV_FT+IYMS_DIV — ev FT↓↓ iyms21≤22 iy2≥4.5',6.5,2.27);
  }

  // ── 1/1 Ev Hakimiyeti ─────────────────────────────────────────────
  if (isEvDom && hasChange) {
    const iyms21Safe = (!iyms21 || iyms21 > 24);
    const iy2Strong  = iy2 && iy2 >= 5.0;
    if (ch_iy1 !== null && ch_iy1 <= 0 && iyms21Safe) {
      let lift = 1.92; if (dep_ft >= 3) lift *= 0.85; if (!iy2Strong) lift *= 0.85;
      add('1/1','PREMIER','v9: 1/1 EV HAK — ev FT↓+dep FT↑+IYMS21>24',5.5,lift);
    }
    if (ch_iy1 !== null && ch_iy1 <= 0 && ch_sy1 !== null && ch_sy1 <= 0 &&
        ch_ms1 !== null && ch_ms1 <= 0 && iyms21Safe && ms1 && ms1 <= 1.8) {
      let lift = 2.17; if (dep_ft >= 3) lift *= 0.85;
      add('1/1','PREMIER','v9: 1/1 TAM — IY+2Y+MS ev↓+IYMS21>24',6.2,lift);
    }
  }

  // ── Gemini KURAL_03: 3 Piyasa Aynı Yönde ─────────────────────────
  if (ch_ms1 !== null && ch_ms1 < 0 &&
      ch_iy1 !== null && ch_iy1 < 0 &&
      ch_au25 !== null && ch_au25 < -0.10 &&
      ms1 && ms1 <= 1.4) {
    add('1/1','ELITE','KURAL_03: 3 Piyasa Aynı Yön (ms1↓+iy1↓+au25↓) + ms1≤1.4',6.1,2.65);
  }

  // ── 2/2 Doğal ─────────────────────────────────────────────────────
  if (iyms22 && iyms22 <= 5.0 && ms2 && ms2 <= 2.5) {
    const lift = (ch_iyms22 !== null && ch_iyms22 < 0) ? 2.10 : 1.80;
    add('2/2','PREMIER','v9: 2/2 DOĞAL — iyms22≤5+ms2≤2.5',5.2,lift);
  }

  // ── 2/2 Divergence (Gemini KURAL_02) ─────────────────────────────
  if (iyms22 && iyms22 <= 10 && hasChange && ch_iyms22 < 0 &&
      ms2 && ms2 <= 3.5 && !(ch_iyms12 !== null && ch_iyms12 < 0)) {
    add('2/2','PREMIER','KURAL_02 / v9: 2/2 DIV — iyms22≤10 + ch22↓',5.0,2.56);
  }

  // ── 1/2 ──────────────────────────────────────────────────────────
  if (iyms12 && iyms12 <= 20 && hasChange && ch_iyms12 < 0 &&
      (ch_iyms22 === null || ch_iyms22 >= 0)) {
    add('1/2','ELITE','1/2 PURE — iyms12≤20 + ch12↓ + 2/2 sakin',7.2,2.50);
  }
  if (ms2 && ms2 <= 2.0 && ms1 && ms1 >= 4.0) {
    add('1/2','PREMIER','1/2 Dep Favori — ms2≤2+ms1≥4',4.26,1.92);
  }

  // ── Kümülatif Oturum Sinyali ──────────────────────────────────────
  if (ev_ft_cum <= -6 && dep_ft_cum >= 3) {
    add('2/1','PREMIER',
      `Oturum Kümülatif: ev_ft_cum=${ev_ft_cum} dep_ft_cum=${dep_ft_cum} (session boyunca birikim)`,
      6.0, 2.10);
  }
  if (ev_ft_cum <= -6 && dep_ft_cum < 0) {
    add('1/1','PREMIER',
      `Oturum Kümülatif: ev_ft_cum=${ev_ft_cum} güçlü ev baskısı`,
      5.5, 1.90);
  }

  return signals;
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 7 — CLAUDE AI YORUMLAYICI
// ════════════════════════════════════════════════════════════════════
async function callClaudeAPI(prompt) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1000,
      messages: [{ role: 'user', content: prompt }],
    });

    const req = https.request({
      hostname: 'api.anthropic.com',
      path:     '/v1/messages',
      method:   'POST',
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Length':    Buffer.byteLength(body),
      },
    }, res => {
      let buf = '';
      res.on('data', d => buf += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(buf);
          const text = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
          resolve(text);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function interpretWithAI(matchesWithSignals) {
  if (!ANTHROPIC_KEY) {
    console.warn('[AI] ANTHROPIC_API_KEY yok, AI yorumu atlandı');
    return null;
  }

  // Her maç için özet hazırla
  const summaries = matchesWithSignals.map(m => {
    const top3 = m.signals.slice(0, 3);
    const mkts = m.latestMarkets;
    return `
MAÇI: ${m.name}
  Oranlar: ms1=${mkts['1x2']?.home||'?'} ms2=${mkts['1x2']?.away||'?'} iy1=${mkts['ht_1x2']?.home||'?'} iy2=${mkts['ht_1x2']?.away||'?'}
  IYMS: 2/1=${mkts['ht_ft']?.['2/1']||'?'} 2/2=${mkts['ht_ft']?.['2/2']||'?'} 1/1=${mkts['ht_ft']?.['1/1']||'?'} 1/2=${mkts['ht_ft']?.['1/2']||'?'}
  Alt/Üst 2.5: over=${mkts['ou25']?.over||'?'} under=${mkts['ou25']?.under||'?'}
  Kümülatif: ev_ft_cum=${m.ev_ft_cum} dep_ft_cum=${m.dep_ft_cum}
  Tetiklenen sinyaller:
${top3.map(s => `    - [${s.tier}] ${s.type} | lift=${s.lift}x | ${s.rule}`).join('\n')}`.trim();
  }).join('\n\n---\n\n');

  const prompt = `Sen bir bahis piyasası analistsin. Aşağıdaki maçlar için açılış → kapanış oran değişimleri ve sinyal motorunun çıktıları verilmiştir.

Bağlam — Sinyaller ne anlama gelir:
- ev_ft_cum negatif + dep_ft_cum pozitif = HT/FT piyasasında deplasman FT'ye para girdi, ev FT'ye para çıktı (reversal beklentisi: deplasman ikinci yarı döner)
- IYMS21 ≤ 22 = piyasa "2/1 olabilir" der (deplasman IY önde, sonra ev döner)
- KURAL_03 (3 piyasa aynı yön) = çok güçlü ev sinyali, 1/1 %60.7 hit
- 2/2 DOĞAL/DIV = piyasa her iki yarıda deplasman galibiyeti görüyor
- ELITE sinyal = daha önce test edilmiş yüksek lift, güvenilir

Görevin:
1. Her maç için piyasanın gerçekten ne söylediğini analiz et
2. En olası HT/FT tahminini söyle (1/1, X/1, 2/1, 2/2, 1/2 vb.)
3. Güven düzeyini belirt: YÜKSEK / ORTA / DÜŞÜK
4. Tek cümle gerekçe ver
5. Sonunda tüm maçların özet tablosunu yaz

FORMAT (her maç için):
MAÇI: [Maç Adı]
TAHMİN: [HT/FT]
GÜVEN: [YÜKSEK/ORTA/DÜŞÜK]
GEREKÇE: [1 cümle]

---
${summaries}
---

Türkçe yanıtla. Gereksiz tekrar etme, net ol.`;

  try {
    console.log('[AI] Claude API çağrılıyor…');
    const response = await callClaudeAPI(prompt);
    console.log('[AI] Yanıt alındı');
    return response;
  } catch (e) {
    console.error('[AI] Hata:', e.message);
    return null;
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 8 — E-POSTA
// ════════════════════════════════════════════════════════════════════
function createTransport() {
  return nodemailer.createTransport({
    host:   process.env.SMTP_HOST || 'smtp.gmail.com',
    port:   parseInt(process.env.SMTP_PORT || '587'),
    secure: process.env.SMTP_PORT === '465',
    auth:   { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  });
}

function buildEmailHTML(matchesWithSignals, aiInterpretation, cycleNo) {
  const now = new Date().toLocaleString('tr-TR', { timeZone: 'Europe/Istanbul' });
  const tierColor = { ELITE:'#e74c3c', PREMIER:'#e67e22', STANDART:'#2980b9' };
  const typeEmoji = { '1/1':'🟡','2/2':'🟣','2/1':'🟢','1/2':'🔵','X/1':'⚪','1/X':'⚪' };

  const matchRows = matchesWithSignals.map(m => {
    const top = m.signals[0];
    const allSigs = m.signals.map(s =>
      `<span style="background:${tierColor[s.tier]||'#555'};color:#fff;padding:2px 6px;border-radius:3px;font-size:11px;margin-right:4px;">${s.type} ${s.lift}x</span>`
    ).join('');
    return `
    <tr style="border-bottom:1px solid #eee;">
      <td style="padding:10px 8px;font-weight:bold;">${m.name}</td>
      <td style="padding:10px 8px;text-align:center;font-size:20px;">${typeEmoji[top.type]||'⚪'} ${top.type}</td>
      <td style="padding:10px 8px;">${allSigs}</td>
      <td style="padding:10px 8px;font-size:12px;color:#555;">
        ev_cum: <b>${m.ev_ft_cum >= 0 ? '+' : ''}${m.ev_ft_cum}</b> |
        dep_cum: <b>${m.dep_ft_cum >= 0 ? '+' : ''}${m.dep_ft_cum}</b>
      </td>
    </tr>`;
  }).join('');

  const aiBlock = aiInterpretation
    ? `<div style="background:#f0f8ff;border-left:4px solid #3498db;padding:16px;margin:16px 0;border-radius:4px;">
        <h3 style="margin:0 0 8px;color:#2980b9;">🤖 AI Analizi (Claude)</h3>
        <pre style="white-space:pre-wrap;font-family:Arial,sans-serif;font-size:13px;color:#2c3e50;margin:0;">${aiInterpretation}</pre>
       </div>`
    : '';

  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>ScorePop AI Alarm</title></head>
<body style="font-family:Arial,sans-serif;max-width:900px;margin:0 auto;padding:16px;color:#2c3e50;">
  <div style="background:linear-gradient(135deg,#1a1a2e,#16213e);color:#fff;padding:20px;border-radius:8px;margin-bottom:16px;">
    <h1 style="margin:0;font-size:22px;">⚡ ScorePop AI — Piyasa Hareketi Alarmı</h1>
    <p style="margin:4px 0 0;opacity:.8;">Döngü #${cycleNo} | ${now} (TR)</p>
  </div>

  <p style="color:#7f8c8d;font-size:13px;">
    ${matchesWithSignals.length} maçta güçlü sinyal tespit edildi. 
    Toplam izlenen: <b>${matchCache.size}</b> maç.
  </p>

  <table style="width:100%;border-collapse:collapse;margin-bottom:16px;">
    <thead>
      <tr style="background:#2c3e50;color:#fff;">
        <th style="padding:10px 8px;text-align:left;">Maç</th>
        <th style="padding:10px 8px;">Tahmin</th>
        <th style="padding:10px 8px;text-align:left;">Sinyaller</th>
        <th style="padding:10px 8px;text-align:left;">Kümülatif</th>
      </tr>
    </thead>
    <tbody>${matchRows}</tbody>
  </table>

  ${aiBlock}

  <div style="background:#ffeaa7;padding:12px;border-radius:6px;font-size:12px;">
    ⚠️ Bu analiz istatistiksel örüntülere dayanır. Kesin sonuç garantisi vermez.
    Her zaman kendi değerlendirmenizi yapın.
  </div>
  <p style="font-size:11px;color:#bdc3c7;margin-top:8px;">ScorePop AI Tracker | GitHub Actions</p>
</body>
</html>`;
}

async function sendEmail(subject, html) {
  const to = process.env.MAIL_TO || '';
  if (!to) { console.warn('[Mail] MAIL_TO tanımlı değil'); return false; }

  if (DRY_RUN) {
    console.log(`[DRY_RUN] E-posta gönderilmedi. Konu: ${subject}`);
    console.log('[DRY_RUN] Alıcılar:', to);
    return true;
  }

  try {
    const transport = createTransport();
    const info = await transport.sendMail({
      from:    `"ScorePop AI" <${process.env.SMTP_USER}>`,
      to,
      subject,
      html,
    });
    console.log(`[Mail] ✅ Gönderildi → ${to} (${info.messageId})`);
    return true;
  } catch (e) {
    console.error('[Mail] Hata:', e.message);
    return false;
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 9 — MAÇLARI FUTURE_MATCHES'TAN OKU (JSON dosyasından)
// ════════════════════════════════════════════════════════════════════
function loadFixtures() {
  // future_matches.json varsa oku (Supabase yoksa elle sağlanan fixture listesi)
  const fp = process.env.FIXTURES_FILE || 'future_matches.json';
  if (!fs.existsSync(fp)) {
    console.warn(`[Fixtures] ${fp} bulunamadı. Boş liste ile devam…`);
    return [];
  }
  try {
    const raw = JSON.parse(fs.readFileSync(fp, 'utf8'));
    return (Array.isArray(raw) ? raw : raw.data || []).map(r => ({
      fixture_id: String(r.fixture_id || r.id),
      home_team:  r.home_team || r.data?.teams?.home?.name || '',
      away_team:  r.away_team || r.data?.teams?.away?.name || '',
      kickoff:    r.date || r.kickoff || null,
    })).filter(r => r.home_team && r.away_team);
  } catch (e) {
    console.error('[Fixtures] Okuma hatası:', e.message);
    return [];
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 9.5 — CANLI MAÇ SKORLARI (HT / FT YAKALAYICI)
// ════════════════════════════════════════════════════════════════════
function calcHtFtResult(htHome, htAway, ftHome, ftAway) {
  if (htHome === null || ftHome === null) return null;
  const ht = htHome > htAway ? '1' : htHome < htAway ? '2' : 'X';
  const ft = ftHome > ftAway ? '1' : ftHome < ftAway ? '2' : 'X';
  return `${ht}/${ft}`;
}

async function syncLiveMatches() {
  if (!sb) return; // Supabase ayarlı değilse sessizce atla

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

  if (liveRows.length === 0) return;

  for (const row of liveRows) {
    const fid = String(row.fixture_id);
    const status = row.status_short;
    const hScore = row.home_score ?? null;
    const aScore = row.away_score ?? null;

    // Sadece bizim takip ettiğimiz maçları işle
    if (!matchCache.has(fid)) continue;

    const match = matchCache.get(fid);
    const prevLive = match.liveData || {};

    // Eğer statü değişmediyse (ve maç bitmediyse) işlem yapma
    if (prevLive.status === status && status !== 'FT') continue;

    let htHome = prevLive.htHome ?? null;
    let htAway = prevLive.htAway ?? null;
    let ftHome = prevLive.ftHome ?? null;
    let ftAway = prevLive.ftAway ?? null;
    let htFtResult = prevLive.htFtResult ?? null;

    if (status === 'HT') {
      htHome = hScore;
      htAway = aScore;
      console.log(`  ⏸  HT Yakalandı: ${match.name} | İY: ${hScore}-${aScore}`);
    } else if (status === 'FT' && prevLive.status !== 'FT') {
      ftHome = hScore;
      ftAway = aScore;
      htFtResult = calcHtFtResult(htHome, htAway, ftHome, ftAway);
      console.log(`  🏁 FT Yakalandı: ${match.name} | HT:${htHome}-${htAway} FT:${ftHome}-${ftAway} → [${htFtResult}]`);
    }

    // Güncel durumu doğrudan JSON cache verisine göm
    match.liveData = { status, htHome, htAway, ftHome, ftAway, htFtResult };
    matchCache.set(fid, match);
  }
}

// ════════════════════════════════════════════════════════════════════
// BÖLÜM 10 — ANA DÖNGÜ
// ════════════════════════════════════════════════════════════════════
function hoursToKickoff(ko) {
  if (!ko) return 999;
  try {
    const d = new Date(ko);
    return (d - Date.now()) / 3600000;
  } catch { return 999; }
}

async function runCycle() {
  cycleCount++;
  const elapsed = Math.round((Date.now() - startTime) / 60000);
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`[Tracker] Döngü #${cycleCount} | ${new Date().toISOString()} | +${elapsed}dk`);
  console.log('═'.repeat(60));

  // ── Nesine CDN ───────────────────────────────────────────────────
  let nesineData;
  try {
    nesineData = await fetchJSON('https://cdnbulten.nesine.com/api/bulten/getprebultenfull');
  } catch (e) {
    console.error('[Nesine] Hata:', e.message); return;
  }
  const events = (nesineData?.sg?.EA || []).filter(e => e.TYPE === 1);
  console.log(`[Nesine] ${events.length} event`);

  // ── Maç listesi ──────────────────────────────────────────────────
  const fixtures = loadFixtures();
  if (fixtures.length === 0) {
    console.warn('[Fixtures] Maç bulunamadı.'); return;
  }

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

    const fid  = fix.fixture_id;
    const prev = matchCache.get(fid);

    // Delta hesabı
    const changes = prev?.latestMarkets ? calcDelta(prev.latestMarkets, currMarkets) : {};
    const { ev_ft, dep_ft } = ftGroups(changes);

    // Kümülatif birikim
    const ev_ft_cum  = (prev?.ev_ft_cum  || 0) + (ev_ft  < 0 ? ev_ft  : 0);
    const dep_ft_cum = (prev?.dep_ft_cum || 0) + (dep_ft < 0 ? dep_ft : 0);

    // Snapshot geçmişi (son 10 tutulur)
    const snapshots = prev?.snapshots || [];
    snapshots.push({ time: new Date().toISOString(), markets: currMarkets, ev_ft, dep_ft });
    if (snapshots.length > 10) snapshots.shift();

    // Cache güncelle (Canlı maç verisini de koru)
    const liveData = prev?.liveData || {};
    matchCache.set(fid, {
      name: `${fix.home_team} vs ${fix.away_team}`,
      kickoff: fix.kickoff,
      latestMarkets: currMarkets,
      ev_ft_cum, dep_ft_cum, snapshots, liveData,
    });

    // Sinyal motoru
    const signals = evaluateSignals(currMarkets, changes, { ev_ft_cum, dep_ft_cum });
    if (signals.length === 0) continue;

    // Tier sıralama
    const tierW = { ELITE:3, PREMIER:2, STANDART:1 };
    signals.sort((a,b) => (tierW[b.tier]||0) - (tierW[a.tier]||0) || b.lift - a.lift);

    const hasEliteOrPremier = signals.some(s => s.tier === 'ELITE' || s.tier === 'PREMIER');
    if (!hasEliteOrPremier && signals.length < MIN_SIGNALS) continue;

    // Alarm tekrar kontrolü
    const topLabel = `${signals[0].type}_${signals[0].tier}`;
    if (!alreadyFired(fid, topLabel)) {
      matchesWithSignals.push({
        fid, name: `${fix.home_team} vs ${fix.away_team}`,
        kickoff: fix.kickoff, h2k,
        signals, latestMarkets: currMarkets,
        ev_ft_cum, dep_ft_cum,
      });
    }
  }

  console.log(`[Tracker] Eşleşen: ${matchedCount} | Sinyal bulunan (yeni): ${matchesWithSignals.length}`);

  if (matchesWithSignals.length === 0) {
    console.log('[Tracker] Yeni alarm yok, devam…');
    // ── Canlı Skorları Eşitle ────────────────────────────────────────
    await syncLiveMatches();
    saveCache();
    return;
  }

  // ── AI Yorumu ────────────────────────────────────────────────────
  const aiText = await interpretWithAI(matchesWithSignals);

  // ── E-posta ──────────────────────────────────────────────────────
  const eliteCount = matchesWithSignals.filter(m => m.signals[0].tier === 'ELITE').length;
  const subject = eliteCount > 0
    ? `💎 ScorePop ELITE [${eliteCount}] — ${matchesWithSignals.map(m=>m.signals[0].type).join(', ')}`
    : `⚡ ScorePop AI — ${matchesWithSignals.length} Maç Sinyali`;

  const html = buildEmailHTML(matchesWithSignals, aiText, cycleCount);
  const sent = await sendEmail(subject, html);

  if (sent) {
    for (const m of matchesWithSignals) {
      markFired(m.fid, `${m.signals[0].type}_${m.signals[0].tier}`);
    }
  }

  // ── Canlı Skorları Eşitle ────────────────────────────────────────
  await syncLiveMatches();

  saveCache();
}

// ════════════════════════════════════════════════════════════════════
// MAIN
// ════════════════════════════════════════════════════════════════════
async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║  ScorePop AI Tracker — Piyasa Takip + Claude Analizi    ║');
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

  // Oturum özeti
  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║  OTURUM TAMAMLANDI                                       ║');
  console.log(`║  Döngü: ${String(cycleCount).padEnd(47)}║`);
  console.log(`║  İzlenen: ${String(matchCache.size).padEnd(46)}║`);
  console.log('╚══════════════════════════════════════════════════════════╝');

  // En aktif maçlar
  const movers = [...matchCache.entries()]
    .filter(([,v]) => Math.abs(v.ev_ft_cum) >= 4 || Math.abs(v.dep_ft_cum) >= 4)
    .sort(([,a],[,b]) =>
      (Math.abs(b.ev_ft_cum)+Math.abs(b.dep_ft_cum)) -
      (Math.abs(a.ev_ft_cum)+Math.abs(a.dep_ft_cum)))
    .slice(0, 10);

  if (movers.length) {
    console.log('\n[Top Hareketler]:');
    for (const [fid, v] of movers) {
      console.log(`  ${v.name || fid} | ev_cum=${v.ev_ft_cum} dep_cum=${v.dep_ft_cum}`);
    }
  }
}

main().catch(e => { console.error('[FATAL]', e); process.exit(1); });
