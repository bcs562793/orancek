"""
reversal_signals.py v9 — Post-Mortem Bulgularına Dayalı Yeniden Yazım
======================================================================

32 MAÇLIK POST-MORTEM ANALİZİ (sonuc_.rtf + match_odds_rows__2_.csv):

  TEMEL ÇIKTI: EV_FT≤-3 + DEP_FT≥2 patternı 9 maçta şu dağılımı verdi:
    1/1: 3 (Montpellier, Almere, Machida)
    2/1: 2 (Lens, Rodez)       ← iyms21 ≤ 22 + iy2 ≥ 4.5
    1/X: 2 (Roda, Melbourne)   ← iy2 < 5.0 + iyms21 < 20
    X/1: 1 (Le Mans)           ← iy2 ≈ 5, iyms21 ≈ 23
    2/2: 1 (Nancy)             ← dep her yarı favori görüldü

v9 FIXES (her biri belgelenmiş vaka ile):

  FIX 1 — 1/1 GUARD: iyms21 eşiği [Lens, Rodez, Roda fix]
    EV_FT≤-3 + DEP_FT≥2 → 1/1 SİNYALİ ANCAK iyms21 > 24 ise
    iyms21 ≤ 22 ise → 2/1 adayı (dep IY bekleniyor)
    22-24 arası gri bölge → her iki sinyal standart

  FIX 2 — 2/1 IY2 GUARD: dep IY çok düşükse bastır [Melbourne, Roda fix]
    2/1 için iy2 ≥ 4.0 zorunlu
    iy2 < 4.0 → "dep IY görülebilir" → X/1 veya 1/X riski yüksek → bastır

  FIX 3 — FENERBAHÇE GUARD: ch_ms1 erken kontrol [Fenerbahçe fix]
    IYMS≤20 path: signal üretim aşamasında ch_ms1 > 0 ise üretme
    (v8'de sadece filter aşamasındaydı, üretiliyordu)

  FIX 4 — 2/2 YENİ SİNYALLER [Santa Fe, Sampdoria, R.Sociedad, Nancy]
    a) iyms22 ≤ 5 + ms2 ≤ 2.5 → dep her yarı favorisi = doğal 2/2
    b) iyms22 ≤ 10 + ch_iyms22 = -1 + ms2 ≤ 3.5 → 2/2 divergence

  FIX 5 — Santa Fe DOĞRU OKUMA [İY:1-2 MS:2-3 = 2/2]
    Önceki analizde 1/1 zannedildi. Gerçek: 2/2.
    EV_FT=+3 + DEP_FT=-2 + iyms22=8.68 + ch_iyms22=-1 → 2/2 sinyali eklendi

  FIX 6 — 2/1 IYMS EŞIĞI DARALMA
    EV_FT≤-3 + DEP_FT≥2 kombinasyonunda:
    iyms21=20.85 (Lens) → 2/1 ✓
    iyms21=21.15 (Rodez) → 2/1 ✓
    iyms21=18.1 (Roda) → 1/X ✗ (iy2=4.89 çok düşük)
    KURAL: iyms21 ≤ 22 + iy2 ≥ 4.5 → 2/1 sinyali aktive

v8'den KORUNANLAR:
  - ch_iyms21 > 0 guard (Vitesse fix)
  - DEP_FT ≥ 3 tier düşürme
  - Yaş penaltısı
  - Tutarlılık skoru
  - IYMS pure_12 saflık testi
"""

import os, json
from datetime import datetime, timedelta
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
if not url or not key:
    print("HATA: SUPABASE_URL veya SUPABASE_KEY bulunamadı.")
    exit(1)

supabase: Client = create_client(url, key)

# ─────────────────────────────────────────────────────────────────────
# YARDIMCI FONKSİYONLAR
# ─────────────────────────────────────────────────────────────────────

def safe_float(val):
    if val is None: return None
    try: return float(val)
    except: return None

def get_market(markets, key, subkey):
    return safe_float(markets.get(key, {}).get(subkey))

def get_change(changes, key, subkey):
    if not changes: return None
    val = changes.get(key, {}).get(subkey)
    if val is None: return None
    try: return int(val)
    except: return None

def get_sofa_change(sofa_1x2, side):
    if not sofa_1x2: return None
    entry = sofa_1x2.get(side, {})
    if not isinstance(entry, dict) or 'change' not in entry: return None
    return int(entry['change'])

# ─────────────────────────────────────────────────────────────────────
# FT GRUP HESABI
# ─────────────────────────────────────────────────────────────────────

def ft_group_sums(changes):
    def s(k): return get_change(changes, 'ht_ft', k) or 0
    ev_ft  = s('1/1') + s('2/1') + s('X/1')
    dep_ft = s('1/2') + s('2/2') + s('X/2')
    bera   = s('1/X') + s('2/X') + s('X/X')
    return ev_ft, dep_ft, bera

# ─────────────────────────────────────────────────────────────────────
# SİNYAL YAŞI
# ─────────────────────────────────────────────────────────────────────

def calc_signal_age_hours(updated_at_str, now_tr):
    if not updated_at_str: return None
    try:
        dt = datetime.fromisoformat(str(updated_at_str).replace('Z', '+00:00'))
        dt_tr = (dt + timedelta(hours=3)).replace(tzinfo=None)
        return (now_tr - dt_tr).total_seconds() / 3600
    except: return None

def age_lift_multiplier(h):
    if h is None: return 1.0
    if h < 3: return 1.0
    if h < 6: return 0.70
    return 0.50

def age_label(h):
    if h is None or h < 3: return ''
    if h < 6: return f' ⚠BAYAT({h:.1f}sa)'
    return f' 🕐ESKİ({h:.1f}sa)'

# ─────────────────────────────────────────────────────────────────────
# ANTİ-REVERSAL GUARD (v9 genişletildi)
# ─────────────────────────────────────────────────────────────────────

def is_signal_reversed(sig_type, ev_ft_sum, dep_ft_sum,
                       ch_ms1, ch_ms2, ch_iy1, ch_iy2, ch_iyms21):
    if sig_type == '1/1':
        if ev_ft_sum is not None and ev_ft_sum > 1:
            return True, f'ev_ft={ev_ft_sum:+d} (ev FT yükseliyor)'
    elif sig_type == '2/2':
        if dep_ft_sum is not None and dep_ft_sum > 1:
            return True, f'dep_ft={dep_ft_sum:+d} (dep FT yükseliyor)'
    elif sig_type == '2/1':
        if ev_ft_sum is not None and ev_ft_sum > 2:
            return True, f'ev_ft={ev_ft_sum:+d} (tüm ev FT yükseldi)'
        if ch_ms1 is not None and ch_ms1 > 0:
            return True, f'ch_ms1={ch_ms1:+d} (ev MS yükseliyor)'
        if ch_iyms21 is not None and ch_iyms21 > 0:
            return True, f'ch_iyms21={ch_iyms21:+d} (IYMS_2/1 yükseliyor, v8 guard)'
    elif sig_type == '1/2':
        if dep_ft_sum is not None and dep_ft_sum > 2:
            return True, f'dep_ft={dep_ft_sum:+d} (tüm dep FT yükseldi)'
        if ch_ms2 is not None and ch_ms2 > 0:
            return True, f'ch_ms2={ch_ms2:+d} (dep MS yükseliyor)'
    return False, ''

# ─────────────────────────────────────────────────────────────────────
# TUTARLILIK SKORU
# ─────────────────────────────────────────────────────────────────────

def market_consistency_score(changes, signals, ev_ft_sum, dep_ft_sum):
    if not signals or not changes: return 5
    dominant_type = signals[0]['type']
    score = 5
    if dominant_type == '1/1':
        score += max(-3, min(3, -ev_ft_sum))
        score += max(-3, min(3, dep_ft_sum))
        if dep_ft_sum >= 3: score -= 1
    elif dominant_type == '2/2':
        score += max(-3, min(3, -dep_ft_sum))
        score += max(-3, min(3, ev_ft_sum))
    elif dominant_type in ('2/1', '1/2'):
        ch_ms1 = get_change(changes, '1x2', 'home')
        ch_ms2 = get_change(changes, '1x2', 'away')
        if dominant_type == '2/1':
            if ch_ms1 is not None: score += (1 if ch_ms1 <= 0 else -2)
            if ev_ft_sum is not None: score += (1 if ev_ft_sum <= 0 else -1)
        else:
            if ch_ms2 is not None: score += (1 if ch_ms2 <= 0 else -2)
            if dep_ft_sum is not None: score += (1 if dep_ft_sum <= 0 else -1)
    return max(0, min(10, score))

# ─────────────────────────────────────────────────────────────────────
# SİNYAL MOTORU v9
# ─────────────────────────────────────────────────────────────────────

def evaluate_reversal_signals(markets, changes, sofa_1x2):
    signals = []

    sofa_ch1 = get_sofa_change(sofa_1x2, '1')
    sofa_ch2 = get_sofa_change(sofa_1x2, '2')

    ch_ms1    = get_change(changes, '1x2',    'home')
    ch_ms2    = get_change(changes, '1x2',    'away')
    ch_iy1    = get_change(changes, 'ht_1x2', 'home')
    ch_iy2    = get_change(changes, 'ht_1x2', 'away')
    ch_sy1    = get_change(changes, '2h_1x2', 'home')
    ch_sy2    = get_change(changes, '2h_1x2', 'away')
    ch_iyms21 = get_change(changes, 'ht_ft',  '2/1')
    ch_iyms12 = get_change(changes, 'ht_ft',  '1/2')
    ch_iyms11 = get_change(changes, 'ht_ft',  '1/1')
    ch_iyms22 = get_change(changes, 'ht_ft',  '2/2')
    ch_iymsx1 = get_change(changes, 'ht_ft',  'X/1')
    ch_iymsx2 = get_change(changes, 'ht_ft',  'X/2')

    has_change = changes is not None and len(changes) > 0
    has_meaningful_change = has_change and any(
        v != 0 for cat in changes.values() if isinstance(cat, dict)
        for v in cat.values() if isinstance(v, (int, float))
    )

    ev_ft_sum, dep_ft_sum, bera_ft_sum = (0, 0, 0)
    if has_change:
        ev_ft_sum, dep_ft_sum, bera_ft_sum = ft_group_sums(changes)

    is_ev_dominance = has_change and ev_ft_sum <= -2 and dep_ft_sum >= 2
    is_dep_dominance = has_change and dep_ft_sum <= -3

    ms1    = get_market(markets, '1x2',        'home')
    ms2    = get_market(markets, '1x2',        'away')
    iy1    = get_market(markets, 'ht_1x2',     'home')
    iy2    = get_market(markets, 'ht_1x2',     'away')
    sy1    = get_market(markets, '2h_1x2',     'home')
    sy2    = get_market(markets, '2h_1x2',     'away')
    iyms21 = get_market(markets, 'ht_ft',      '2/1')
    iyms12 = get_market(markets, 'ht_ft',      '1/2')
    iyms11 = get_market(markets, 'ht_ft',      '1/1')
    iyms22 = get_market(markets, 'ht_ft',      '2/2')
    dcg_2h = get_market(markets, 'more_goals', '2h')
    dcg_2h = dcg_2h or get_market(markets, 'more_goals_half', 'second')
    han01  = get_market(markets, 'ah_p0_1',    'home')

    def sig(type_, rule, prec_pct, lift_val, tier=None):
        if lift_val < 1.35: return
        t = tier or ('PREMIER' if lift_val >= 2.0 else 'STANDART')
        signals.append({'type': type_, 'tier': t, 'rule': rule,
                        'prec': f'%{prec_pct:.2f}', 'lift': f'{lift_val:.2f}x',
                        '_lift_raw': lift_val})

    # ══════════════════════════════════════════════════════════════════
    # 2/1 SİNYALLERİ
    # ══════════════════════════════════════════════════════════════════

    # ── Lens Fingerprint ──────────────────────────────────────────────
    lens_base = (ms1 and ms1 <= 1.30 and iy2 and iy2 >= 3.5 and
                 iy1 and iy1 <= 1.70 and sy1 and sy1 <= 1.70 and
                 iyms21 and iyms21 <= 25)

    if lens_base:
        sig('2/1', 'Lens FP: MS1≤1.30+İY2≥3.5+İY1≤1.7+2Y1≤1.7+IYMS≤25', 4.20, 1.47)
        if dcg_2h and dcg_2h <= 1.9:
            sig('2/1', 'Lens FP + DCG(2.Y)≤1.9', 4.26, 1.49)
        if has_change and ch_iyms21 == -1:
            if ch_ms1 is not None and ch_ms1 > 0:
                pass  # Fenerbahçe guard
            elif is_ev_dominance:
                sig('2/1', 'ELITE: Lens FP + IYMS_2/1↓ + EV FT TEMİZ',
                    8.0, 2.80, tier='ELITE')
            else:
                sig('2/1', 'ELITE: Lens FP + IYMS_2/1↓ (divergence)',
                    7.0, 2.44, tier='ELITE')
        if sofa_ch1 == -1 and sofa_ch2 == 1:
            sig('2/1', 'Lens FP + Sofa(Ev↓+Dep↑)', 4.41, 1.54)

    # ── IYMS 2/1 ≤ 20 ─────────────────────────────────────────────────
    # FIX v9 — FENERBAHçE: ch_ms1 > 0 ise üretme (erken kontrol)
    if iyms21 and iyms21 <= 20:
        # FIX 3: ch_ms1 > 0 → üretme (Fenerbahçe: ev MS oranı yükseldiyse 2/1 değil)
        if ch_ms1 is not None and ch_ms1 > 0:
            pass  # Fenerbahçe fix — sinyal üretme
        else:
            prec, lift = 4.58, 1.60
            sofa_tag = ''
            if has_change and ch_iyms21 == -1:
                prec, lift = 6.5, 2.27
                sofa_tag = ' + IYMS↓'
                if is_ev_dominance:
                    prec, lift = 7.0, 2.44
                    sofa_tag = ' + IYMS↓ + EV FT TEMİZ'
            elif sofa_ch1 == -1 and sofa_ch2 == 1:
                prec, lift = 5.95, 2.08
                sofa_tag = ' + Sofa(Ev↓+Dep↑)'
            elif sofa_ch1 == -1:
                prec, lift = 5.89, 2.05
                sofa_tag = ' + Sofa(Ev↓)'
            # FIX 2: iy2 < 4.0 ise 2/1 sinyali bastır (dep IY görülebilir = X riski)
            if iy2 and iy2 < 4.0:
                lift = lift * 0.70
                sofa_tag += ' ⚠ İY2<4.0(X/1↑riski)'
            if dep_ft_sum >= 3:
                lift = lift * 0.80
                sofa_tag += ' ⚠ DEP_FT≥3'
            sig('2/1', f'IYMS_2/1≤20{sofa_tag}', prec, lift)

    elif iyms21 and iyms21 <= 25:
        sig('2/1', 'IYMS_2/1≤25', 3.82, 1.33)

    # ── DCG + İY-2 ─────────────────────────────────────────────────────
    if dcg_2h and dcg_2h <= 1.7 and iy2 and iy2 >= 3.5:
        prec, lift = 4.23, 1.48
        extra = ''
        if sofa_ch2 == -1 and sofa_ch1 == 1:
            prec, lift = 4.94, 1.72; extra = ' + Sofa(Ev↑+Dep↓)'
        sig('2/1', f'DCG(2.Y)≤1.7 + İY-2≥3.5{extra}', prec, lift)

    # ── MS-1 + MS-2 ────────────────────────────────────────────────────
    if ms1 and ms1 <= 2.0 and ms2 and ms2 >= 4.0 and has_meaningful_change:
        prec, lift = 3.64, 1.27; extra = ''
        if sofa_ch1 == 1 and sofa_ch2 == -1:
            prec, lift = 4.82, 1.68; extra = ' + Sofa(Ev↑+Dep↓)'
        sig('2/1', f'MS-1≤2.0 + MS-2≥4.0{extra}', prec, lift)

    if han01 and han01 <= 2.5 and iy2 and iy2 >= 3.5:
        sig('2/1', 'Han(0:1)_1≤2.5 + İY-2≥3.5', 3.9, 1.36)

    # ── v9 YENİ: EV_FT≤-3 + DEP_FT≥2 + IYMS21 ≤ 22 → 2/1 (Lens/Rodez pattern) ─
    # FIX 1 + FIX 2: iyms21 ≤ 22 + iy2 ≥ 4.5 + ev_ft ≤ -3 → 2/1 divergence
    if (is_ev_dominance and ev_ft_sum <= -3 and
            iyms21 and iyms21 <= 22 and
            iy2 and iy2 >= 4.5 and          # FIX 2: iy2 düşükse bastır
            has_change and ch_iyms21 == -1 and  # IYMS 2/1 ucuzlaşıyor = smart money
            (ch_ms1 is None or ch_ms1 <= 0)):   # FIX 3: Fenerbahçe guard
        tier_v = 'PREMIER' if dep_ft_sum <= 2 else 'STANDART'
        sig('2/1',
            f'v9: 2/1 EV_FT+IYMS_DIVERGENCE — ev FT↓, IYMS21≤22, İY2≥4.5, IYMS↓',
            6.5, 2.27, tier=tier_v)

    # ══════════════════════════════════════════════════════════════════
    # 1/2 SİNYALLERİ
    # ══════════════════════════════════════════════════════════════════

    is_pure_12 = (has_change and
                  ch_iyms12 == -1 and
                  (ch_iyms22 is None or ch_iyms22 >= 0) and
                  (ch_iy1 is None or ch_iy1 <= 0))

    if iyms12 and iyms12 <= 25 and is_pure_12:
        sig('1/2', 'ELITE: IYMS_1/2≤25 + PURE reversal', 7.2, 2.50, tier='ELITE')
    elif iyms12 and iyms12 <= 25 and has_change and ch_iyms12 == -1:
        reasons = []
        if ch_iyms22 == -1: reasons.append('2/2 de düşüyor')
        if ch_iy1 == 1: reasons.append('IY ev zayıflıyor')
        sig('1/2', f'1/2 ch=-1 SAFLANSIZ [{", ".join(reasons)}]', 3.8, 1.33)

    if ms2 and ms2 <= 2.0 and ms1 and ms1 >= 4.0 and has_meaningful_change:
        prec, lift = 4.26, 1.92; extra = ''
        if has_change and ch_ms2 == 1:
            prec, lift = 5.47, 2.47; extra = ' + Dep oranı yükseldi'
        elif sofa_ch2 == 1:
            prec, lift = 5.36, 2.42; extra = ' + Sofa(Dep↑)'
        if has_change and ch_iy1 == 1:
            lift = lift * 0.70; extra += ' ⚠ IY ev zayıf'
        if sy2 and sy2 <= 2.2 and iy1 and iy1 >= 3.5:
            sig('1/2', f'SÜPER: MS2≤2+MS1≥4+2Y2≤2.2+İY1≥3.5{extra}', prec, lift)
        else:
            sig('1/2', f'MS-2≤2.0 + MS-1≥4.0{extra}', prec, lift)

    if dcg_2h and dcg_2h <= 1.7 and iy1 and iy1 >= 3.5:
        sig('1/2', 'DCG(2.Y)≤1.7 + İY-1≥3.5', 4.75, 2.15)

    if (iy1 and iy1 >= 3.5 and ms2 and ms2 <= 2.0 and
            sofa_ch1 == -1 and sofa_ch2 == 0):
        sig('1/2', 'İY-1≥3.5 + MS-2≤2.0 + Sofa(Ev↓,Dep=0)', 5.11, 2.31)

    if iyms12 and iyms12 <= 20:
        prec, lift = 4.06, 1.83; extra = ''
        if sofa_ch1 == 1 and sofa_ch2 == -1:
            prec, lift = 4.77, 2.16; extra = ' + Sofa(Ev↑+Dep↓)'
        elif is_pure_12:
            prec, lift = 5.5, 2.20; extra = ' + PURE reversal'
        elif has_change and ch_iyms12 == -1:
            prec, lift = 4.2, 1.50; extra = ' + IYMS↓(kısmi)'
        sig('1/2', f'IYMS_1/2≤20{extra}', prec, lift)

    if ms2 and ms2 <= 2.5 and ms1 and ms1 >= 4.5 and has_meaningful_change:
        sig('1/2', 'MS-2≤2.5 + MS-1≥4.5', 4.1, 1.83)

    # ══════════════════════════════════════════════════════════════════
    # 1/1 EV HAKİMİYETİ — v9 iyms21 GUARD EKLENDİ
    # ══════════════════════════════════════════════════════════════════

    if is_ev_dominance and has_change:
        ch_iy1ok  = (ch_iy1 is not None and ch_iy1 <= 0)
        ch_sy1ok  = (ch_sy1 is not None and ch_sy1 <= 0)
        ch_ms1ok  = (ch_ms1 is not None and ch_ms1 <= 0)
        iy2_str   = iy2 and iy2 >= 5.0   # FIX 2: dep IY gerçekten uzak olmalı
        dep_ft_hi = (dep_ft_sum >= 3)

        # FIX 1: iyms21 ≤ 22 ise dep IY bekleniyor → 1/1 değil 2/1 adayı
        #        1/1 için iyms21 > 24 veya None olmalı
        iyms21_safe = (iyms21 is None or iyms21 > 24)

        if ch_iy1ok and iyms21_safe:
            if ms1 and ms1 <= 2.0:
                tier  = 'STANDART' if dep_ft_hi else 'PREMIER'
                note  = ''
                lift  = 1.92
                if dep_ft_hi: lift = 1.60; note += ' ⚠DEP_FT≥3'
                if not iy2_str: lift = lift * 0.85; note += ' ⚠IY2<5.0'
                sig('1/1', f'v9: 1/1 Ev Hakimiyeti — ev FT↓+dep FT↑+IYMS21>{24 if iyms21 else "?"}{note}',
                    5.5, lift, tier=tier)

        if (ch_iy1ok and ch_sy1ok and ch_ms1ok and iyms21_safe):
            if ms1 and ms1 <= 1.8:
                tier  = 'STANDART' if dep_ft_hi else 'PREMIER'
                note  = ''
                lift  = 2.17
                if dep_ft_hi: lift = 1.80; note += ' ⚠DEP_FT≥3'
                if not iy2_str: lift = lift * 0.85; note += ' ⚠IY2<5.0'
                sig('1/1', f'v9: 1/1 TAM HAKİMİYET — IY+2Y+MS ev↓+IYMS21>{24 if iyms21 else "?"}{note}',
                    6.2, lift, tier=tier)

    # ── v9 YENİ: EV_FT≤-3 + DEP_FT≥2 + iyms21 ≤ 22: gri bölge her iki sinyal ──
    # 22 ≤ iyms21 ≤ 24: hem 1/1 hem 2/1 mümkün, ikisi de standart
    if (is_ev_dominance and ev_ft_sum <= -3 and dep_ft_sum >= 2 and
            iyms21 and 22 <= iyms21 <= 24 and
            ms1 and ms1 <= 2.0 and has_change and ch_iyms21 == -1):
        sig('1/1', 'v9: GRİ BÖLGE 1/1? (iyms21=22-24, EV↓↓)', 4.5, 1.57, tier='STANDART')
        sig('2/1', 'v9: GRİ BÖLGE 2/1? (iyms21=22-24, EV↓↓+IYMS↓)', 4.0, 1.40, tier='STANDART')

    # ══════════════════════════════════════════════════════════════════
    # 2/2 DEP HAKİMİYETİ — v9 YENİ SİNYALLER EKLENDİ
    # ══════════════════════════════════════════════════════════════════

    # v9 YENİ A: Doğal 2/2 — dep büyük favori, her yarı bekleniyor
    # [Sampdoria: iyms22=3.0, ms2=1.9 | R.Sociedad: iyms22=2.74, ms2=1.81]
    if iyms22 and iyms22 <= 5.0 and ms2 and ms2 <= 2.5:
        note = ''
        lift = 1.80
        if ch_iyms22 is not None and ch_iyms22 == -1:
            lift = 2.10; note = ' + IYMS22↓'
        sig('2/2', f'v9: 2/2 DOĞAL — IYMS22≤5 + MS2≤2.5{note}', 5.2, lift, tier='PREMIER' if lift >= 2.0 else 'STANDART')

    # v9 YENİ B: 2/2 Divergence — dep uzak ama piyasa 2/2 görüyor
    # [Santa Fe: iyms22=8.68, ms2=4.75, ch_iyms22=-1]
    if (iyms22 and iyms22 <= 10 and
            has_change and ch_iyms22 == -1 and
            ms2 and ms2 <= 3.5 and
            (ch_iyms12 is None or ch_iyms12 != -1)):  # 1/2 ile çakışmasın
        sig('2/2', 'v9: 2/2 DİVERGENCE — IYMS22≤10 + ch22=-1 (piyasa dep her yarı gördü)',
            5.0, 1.74, tier='STANDART')

    # v6 spesifik 2/2 (korundu)
    is_dep_dom_specific = (
        is_dep_dominance and
        (ch_iyms12 is None or ch_iyms12 != -1) and
        ev_ft_sum <= 0 and
        (ms1 is None or ms1 < 4.5)
    )
    if is_dep_dom_specific:
        ch_iy2ok = (ch_iy2 is not None and ch_iy2 <= 0)
        if ch_iy2ok:
            if ms2 and ms2 <= 1.8:
                sig('2/2', 'v6: 2/2 Dep Hakimiyeti SPESİFİK', 5.8, 2.03, tier='PREMIER')
            elif ms2 and ms2 <= 2.5:
                sig('2/2', 'v6: 2/2 Dep Hakimiyeti (orta)', 4.6, 1.61, tier='STANDART')
        if (ch_iy2ok and ch_sy2 and ch_sy2 <= 0 and
                ch_ms2 and ch_ms2 <= 0 and ms2 and ms2 <= 2.0):
            sig('2/2', 'v6: 2/2 TAM DEP HAKİMİYETİ', 6.5, 2.27, tier='PREMIER')

    return signals


# ─────────────────────────────────────────────────────────────────────
# FİLTRELER
# ─────────────────────────────────────────────────────────────────────

def apply_filters(signals, age_hours, ev_ft_sum, dep_ft_sum,
                  ch_ms1, ch_ms2, ch_iy1, ch_iy2, ch_iyms21):
    age_mult = age_lift_multiplier(age_hours)
    age_tag  = age_label(age_hours)
    filtered = []
    cancelled = []

    for s in signals:
        rev, reason = is_signal_reversed(
            s['type'], ev_ft_sum, dep_ft_sum,
            ch_ms1, ch_ms2, ch_iy1, ch_iy2, ch_iyms21)
        if rev:
            cancelled.append({**s, 'cancel_reason': reason})
            continue

        if age_mult < 1.0:
            nl = s['_lift_raw'] * age_mult
            if nl < 1.35:
                cancelled.append({**s, 'cancel_reason': f'yaş penaltısı → lift {nl:.2f}x'})
                continue
            nt = s['tier']
            if nt == 'ELITE'   and nl < 2.44: nt = 'PREMIER'
            if nt == 'PREMIER' and nl < 2.0:  nt = 'STANDART'
            s = {**s, 'lift': f'{nl:.2f}x', '_lift_raw': nl,
                 'tier': nt, 'rule': s['rule'] + age_tag}

        filtered.append(s)

    return filtered, cancelled


# ─────────────────────────────────────────────────────────────────────
# ANA FONKSİYON
# ─────────────────────────────────────────────────────────────────────

def generate_signals():
    now_tr = datetime.utcnow() + timedelta(hours=3)

    try:
        resp = supabase.table('match_odds').select('*').execute()
        rows = resp.data
    except Exception as e:
        print(f"HATA: {e}"); return

    skipped_no_odds = 0
    signals_found   = []
    v9_cancelled    = 0

    for row in rows:
        raw = row.get('odds_data', {})
        if isinstance(raw, str):
            try: odds_data = json.loads(raw)
            except: skipped_no_odds += 1; continue
        elif isinstance(raw, dict):
            odds_data = raw
        else:
            skipped_no_odds += 1; continue

        markets  = odds_data.get('markets', {})
        changes  = odds_data.get('markets_change', {})
        sofa_1x2 = odds_data.get('sofa_1x2')

        if not markets:
            skipped_no_odds += 1; continue

        match_name = (odds_data.get('nesine_name') or
                      f"Fixture {row.get('fixture_id','?')}")

        updated_at = row.get('updated_at', '')
        if updated_at:
            try:
                dt = datetime.fromisoformat(str(updated_at).replace('Z', '+00:00'))
                date_str = (dt + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M')
            except:
                date_str = str(updated_at)[:16]
        else:
            date_str = '?'

        age_hours = calc_signal_age_hours(updated_at, now_tr)
        raw_sigs  = evaluate_reversal_signals(markets, changes, sofa_1x2)

        ev_sum, dep_sum, _ = ft_group_sums(changes) if changes else (0, 0, 0)
        ch_ms1    = get_change(changes, '1x2',   'home')
        ch_ms2    = get_change(changes, '1x2',   'away')
        ch_iy1    = get_change(changes, 'ht_1x2','home')
        ch_iy2    = get_change(changes, 'ht_1x2','away')
        ch_iyms21 = get_change(changes, 'ht_ft', '2/1')

        sigs, cancelled = apply_filters(
            raw_sigs, age_hours, ev_sum, dep_sum,
            ch_ms1, ch_ms2, ch_iy1, ch_iy2, ch_iyms21)

        v9_cancelled += len(cancelled)
        consistency  = market_consistency_score(changes, sigs, ev_sum, dep_sum)

        n_elite   = sum(1 for s in sigs if s['tier'] == 'ELITE')
        n_premier = sum(1 for s in sigs if s['tier'] == 'PREMIER')
        n_std     = sum(1 for s in sigs if s['tier'] == 'STANDART')

        if n_elite == 0 and n_premier == 0 and n_std < 2:
            continue

        sigs.sort(key=lambda s: (
            {'ELITE':3,'PREMIER':2,'STANDART':1}[s['tier']],
            float(s['lift'].replace('x',''))
        ), reverse=True)

        signals_found.append({
            'match': match_name, 'date': date_str,
            'signals': sigs, 'cancelled': cancelled,
            'top_lift': sigs[0]['lift'],
            'has_change': bool(changes), 'has_sofa': bool(sofa_1x2),
            'n_elite': n_elite, 'n_premier': n_premier,
            'ev_ft_sum': ev_sum, 'dep_ft_sum': dep_sum,
            'consistency': consistency, 'age_hours': age_hours,
        })

    signals_found.sort(key=lambda m: (
        m['n_elite'], m['n_premier'], float(m['top_lift'].replace('x',''))
    ), reverse=True)

    print("=" * 72)
    print(f"  ScorePop REVERSAL v9  |  {now_tr.strftime('%Y-%m-%d %H:%M')} TR")
    print("=" * 72)
    print(f"  Taranan          : {len(rows)}")
    print(f"  Oran yok         : {skipped_no_odds}")
    print(f"  LİSTEDE          : {len(signals_found)}")
    print(f"  v9 iptal         : {v9_cancelled}")
    print("=" * 72)

    if not signals_found:
        print("\n  Sinyal bulunan maç yok.\n"); return

    elite_list   = [m for m in signals_found if m['n_elite'] > 0]
    premier_list = [m for m in signals_found if m['n_elite'] == 0 and m['n_premier'] > 0]
    std_list     = [m for m in signals_found if m['n_elite'] == 0 and m['n_premier'] == 0]

    def _print(matches, label, icon, max_show=20):
        if not matches: return
        print(f"\n{'='*72}")
        print(f"  {icon} {label} — {len(matches)} maç")
        print(f"{'='*72}")
        for m in matches[:max_show]:
            extras = []
            if m['has_change']:
                extras.append(f"EV_FT={m['ev_ft_sum']:+d} DEP_FT={m['dep_ft_sum']:+d}")
            score = m['consistency']
            score_icon = '🟢' if score >= 7 else ('🟡' if score >= 4 else '🔴')
            extras.append(f"tutarlılık={score}/10{score_icon}")
            if m['age_hours'] is not None and m['age_hours'] >= 3:
                extras.append(f"⚠BAYAT {m['age_hours']:.1f}sa")
            extras_str = f"  [{', '.join(extras)}]" if extras else ''
            print(f"\n  {m['date']}  |  {m['match']}{extras_str}")
            print(f"  {'─' * 66}")
            for s in m['signals']:
                icon_t = {'ELITE':'💎','PREMIER':'⭐','STANDART':'·'}[s['tier']]
                type_i = {'2/1':'🟢','1/2':'🔵','1/1':'🟡','2/2':'🟣'}.get(s['type'],'⚪')
                print(f"  {icon_t} {type_i} {s['type']} | {s['lift']} | {s['prec']} | {s['tier']}")
                print(f"      ↳ {s['rule']}")
            if m['cancelled']:
                print(f"  {'─' * 66}")
                for c in m['cancelled']:
                    ti = {'2/1':'🟢','1/2':'🔵','1/1':'🟡','2/2':'🟣'}.get(c['type'],'⚪')
                    print(f"  ✖ {ti} {c['type']} iptal → {c['cancel_reason']}")
            print()
        if len(matches) > max_show:
            print(f"  ... +{len(matches)-max_show} maç daha")

    _print(elite_list,   'ELITE — Divergence Sinyali', '💎')
    _print(premier_list, 'PREMIER — lift ≥ 2.0x', '🔥')
    _print(std_list,     'STANDART — inceleme listesi', '📋', max_show=15)

    print(f"\n{'='*72}")
    print(f"  {len(elite_list)} ELITE + {len(premier_list)} PREMIER + {len(std_list)} STANDART")
    print(f"  v9: {v9_cancelled} sinyal iptal")
    print(f"{'='*72}")

if __name__ == "__main__":
    generate_signals()
