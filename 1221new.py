"""
reversal_signals.py v5 — FT Grup Saflık Filtresi + 1/1 / 2/2 Sinyalleri
=========================================================================

Tablo yapısı: fixture_id | odds_data | updated_at
  - Tarih kolonu YOK → tarih filtresi tamamen kaldırıldı
  - Sorgu: supabase.table('match_odds').select('*').execute()
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
    """
    markets_change dict'inden change okur.
      -1 = oran düştü (piyasa bu sonucu daha olası gördü)
       0 = sabit
      +1 = oran yükseldi (piyasa bu sonuçtan uzaklaştı)
    """
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
    """
    9 IYMS outcome'u 3 FT kazanan grubuna toplar.
    EV FT  : 1/1 + 2/1 + X/1
    DEP FT : 1/2 + 2/2 + X/2
    BERA FT: 1/X + 2/X + X/X
    """
    def s(k): return get_change(changes, 'ht_ft', k) or 0
    ev_ft  = s('1/1') + s('2/1') + s('X/1')
    dep_ft = s('1/2') + s('2/2') + s('X/2')
    bera   = s('1/X') + s('2/X') + s('X/X')
    return ev_ft, dep_ft, bera

# ─────────────────────────────────────────────────────────────────────
# SINYAL MOTORU v5
# ─────────────────────────────────────────────────────────────────────

def evaluate_reversal_signals(markets, changes, sofa_1x2):
    signals = []

    sofa_ch1 = get_sofa_change(sofa_1x2, '1')
    sofa_ch2 = get_sofa_change(sofa_1x2, '2')

    ch_ms1    = get_change(changes, '1x2',        'home')
    ch_ms2    = get_change(changes, '1x2',        'away')
    ch_iy1    = get_change(changes, 'ht_1x2',     'home')
    ch_iy2    = get_change(changes, 'ht_1x2',     'away')
    ch_sy1    = get_change(changes, '2h_1x2',     'home')
    ch_sy2    = get_change(changes, '2h_1x2',     'away')
    ch_iyms21 = get_change(changes, 'ht_ft',      '2/1')
    ch_iyms12 = get_change(changes, 'ht_ft',      '1/2')
    ch_iyms11 = get_change(changes, 'ht_ft',      '1/1')
    ch_iyms22 = get_change(changes, 'ht_ft',      '2/2')
    ch_iymsx1 = get_change(changes, 'ht_ft',      'X/1')
    ch_iymsx2 = get_change(changes, 'ht_ft',      'X/2')
    ch_dcg_2h = get_change(changes, 'more_goals_half', 'second')

    has_change = changes is not None and len(changes) > 0

    ev_ft_sum, dep_ft_sum, bera_ft_sum = (0, 0, 0)
    if has_change:
        ev_ft_sum, dep_ft_sum, bera_ft_sum = ft_group_sums(changes)

    is_clean_ev_reversal = has_change and ev_ft_sum <= -2 and dep_ft_sum >= 1
    is_dep_dominance     = has_change and dep_ft_sum <= -3
    is_ev_dominance      = has_change and ev_ft_sum <= -2 and dep_ft_sum >= 2

    is_pure_12_reversal  = (has_change and
                            ch_iyms12 == -1 and
                            (ch_iyms22 is None or ch_iyms22 >= 0) and
                            (ch_iy1 is None or ch_iy1 <= 0))

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
                        'prec': f'%{prec_pct:.2f}', 'lift': f'{lift_val:.2f}x'})

    # ══════════════════════════════════════════════════════════════════
    # 2/1 SİNYALLERİ
    # ══════════════════════════════════════════════════════════════════

    lens_base = (ms1 and ms1 <= 1.30 and iy2 and iy2 >= 3.5 and
                 iy1 and iy1 <= 1.70 and sy1 and sy1 <= 1.70 and
                 iyms21 and iyms21 <= 25)

    if lens_base:
        sig('2/1', 'Lens FP: MS1≤1.30+İY2≥3.5+İY1≤1.7+2Y1≤1.7+IYMS≤25', 4.20, 1.47)

        if dcg_2h and dcg_2h <= 1.9:
            sig('2/1', 'Lens FP + DCG(2.Y)≤1.9', 4.26, 1.49)

        if has_change and ch_iyms21 == -1:
            if is_clean_ev_reversal and dep_ft_sum >= 1:
                sig('2/1',
                    'ELITE v5: Lens FP + IYMS_2/1↓ + EV FT GRUBU TEMİZ',
                    8.0, 2.80, tier='ELITE')
            else:
                sig('2/1',
                    'ELITE: Lens FP + IYMS_2/1 oranı DÜŞTÜ (divergence)',
                    7.0, 2.44, tier='ELITE')

        if sofa_ch1 == -1 and sofa_ch2 == 1:
            sig('2/1', 'Lens FP + Sofa(Ev↓+Dep↑)', 4.41, 1.54)

        if has_change and ch_ms1 == -1 and ch_iy1 == -1 and ch_sy1 == -1:
            sig('2/1', 'Lens FP + Ev tüm markette favori oldu', 4.6, 1.60)

        if has_change and ch_iyms21 == -1 and sofa_ch1 == -1:
            sig('2/1', 'ELITE: Lens FP + IYMS↓ + Sofa(Ev↓)',
                7.5, 2.61, tier='ELITE')

    if iyms21 and iyms21 <= 20:
        prec, lift = 4.58, 1.60
        sofa_tag = ''
        if has_change and ch_iyms21 == -1:
            prec, lift = 6.5, 2.27
            sofa_tag = ' + IYMS oranı düştü'
            if is_clean_ev_reversal:
                prec, lift = 7.0, 2.44
                sofa_tag = ' + IYMS↓ + EV FT TEMİZ'
        elif sofa_ch1 == -1 and sofa_ch2 == 1:
            prec, lift = 5.95, 2.08
            sofa_tag = ' + Sofa(Ev↓+Dep↑)'
        elif sofa_ch1 == -1:
            prec, lift = 5.89, 2.05
            sofa_tag = ' + Sofa(Ev↓)'
        sig('2/1', f'IYMS_2/1≤20{sofa_tag}', prec, lift)

    elif iyms21 and iyms21 <= 25:
        sig('2/1', 'IYMS_2/1≤25', 3.82, 1.33)

    if dcg_2h and dcg_2h <= 1.7 and iy2 and iy2 >= 3.5:
        prec, lift = 4.23, 1.48
        extra = ''
        if sofa_ch2 == -1 and sofa_ch1 == 1:
            prec, lift = 4.94, 1.72
            extra = ' + Sofa(Ev↑+Dep↓)'
        sig('2/1', f'DCG(2.Y)≤1.7 + İY-2≥3.5{extra}', prec, lift)

    if ms1 and ms1 <= 2.0 and ms2 and ms2 >= 4.0:
        prec, lift = 3.64, 1.27
        extra = ''
        if has_change and ch_ms1 == -1 and sofa_ch1 == 1 and sofa_ch2 == -1:
            prec, lift = 4.82, 1.68
            extra = ' + Sofa(Ev↑+Dep↓)'
        elif sofa_ch1 == 1 and sofa_ch2 == -1:
            prec, lift = 4.82, 1.68
            extra = ' + Sofa(Ev↑+Dep↓)'
        sig('2/1', f'MS-1≤2.0 + MS-2≥4.0{extra}', prec, lift)

    if han01 and han01 <= 2.5 and iy2 and iy2 >= 3.5:
        sig('2/1', 'Han(0:1)_1≤2.5 + İY-2≥3.5', 3.9, 1.36)

    # ══════════════════════════════════════════════════════════════════
    # 1/2 SİNYALLERİ
    # ══════════════════════════════════════════════════════════════════

    if iyms12 and iyms12 <= 25 and is_pure_12_reversal:
        sig('1/2',
            'ELITE v5: IYMS_1/2≤25 + PURE reversal (ch22≥0, IY ev≤0)',
            7.2, 2.50, tier='ELITE')
    elif iyms12 and iyms12 <= 25 and has_change and ch_iyms12 == -1:
        reasons = []
        if ch_iyms22 == -1: reasons.append('2/2 de düşüyor')
        if ch_iy1 == 1:     reasons.append('IY ev zayıflıyor')
        sig('1/2',
            f'1/2 ch=-1 ama SAFLİK TESTI BAŞARISIZ [{", ".join(reasons)}]',
            3.8, 1.33)

    if ms2 and ms2 <= 2.0 and ms1 and ms1 >= 4.0:
        iy1_penalty = has_change and (ch_iy1 == 1)
        extra = ''
        prec, lift = 4.26, 1.92
        if has_change and ch_ms2 == 1:
            prec, lift = 5.47, 2.47
            extra = ' + Dep oranı yükseldi'
        elif sofa_ch2 == 1:
            prec, lift = 5.36, 2.42
            extra = ' + Sofa(Dep↑)'

        if iy1_penalty:
            lift = lift * 0.70
            extra += ' ⚠ IY ev zayıf'

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
        prec, lift = 4.06, 1.83
        extra = ''
        if sofa_ch1 == 1 and sofa_ch2 == -1:
            prec, lift = 4.77, 2.16
            extra = ' + Sofa(Ev↑+Dep↓)'
        elif is_pure_12_reversal:
            prec, lift = 5.5, 2.20
            extra = ' + IYMS↓ PURE reversal'
        elif has_change and ch_iyms12 == -1:
            prec, lift = 4.2, 1.50
            extra = ' + IYMS↓ (saflık kısmı)'
        sig('1/2', f'IYMS_1/2≤20{extra}', prec, lift)

    if ms2 and ms2 <= 2.5 and ms1 and ms1 >= 4.5:
        sig('1/2', 'MS-2≤2.5 + MS-1≥4.5', 4.1, 1.83)

    # ══════════════════════════════════════════════════════════════════
    # YENİ v5: 1/1 EV HAKİMİYETİ SİNYALİ
    # ══════════════════════════════════════════════════════════════════

    if is_ev_dominance and has_change:
        iy1_ok = (ch_iy1 is not None and ch_iy1 <= 0)
        if iy1_ok:
            if ms1 and ms1 <= 2.5:
                sig('1/1',
                    'v5 YENİ: 1/1 Ev Hakimiyeti — ev FT grubu temiz↓ + dep FT↑',
                    5.5, 1.92, tier='PREMIER')
            elif ms1 and ms1 <= 4.0:
                sig('1/1',
                    'v5 YENİ: 1/1 Ev Sürprizi — underdog ev FT grubu↓ + dep FT↑',
                    4.8, 1.67, tier='STANDART')

        if (iy1_ok and
                ch_sy1 is not None and ch_sy1 <= 0 and
                ch_ms1 is not None and ch_ms1 <= 0):
            if ms1 and ms1 <= 3.5:
                sig('1/1',
                    'v5 YENİ: 1/1 TAM HAKİMİYET — IY+2Y+MS+IYMS hepsi ev↓',
                    6.2, 2.17, tier='PREMIER')

    # ══════════════════════════════════════════════════════════════════
    # YENİ v5: 2/2 DEP HAKİMİYETİ SİNYALİ
    # ══════════════════════════════════════════════════════════════════

    if is_dep_dominance and has_change:
        iy2_ok = (ch_iy2 is not None and ch_iy2 <= 0)
        if iy2_ok:
            if ms2 and ms2 <= 1.8:
                sig('2/2',
                    'v5 YENİ: 2/2 Dep Hakimiyeti — tüm dep FT grubu↓ (dep≤1.8)',
                    5.8, 2.03, tier='PREMIER')
            elif ms2 and ms2 <= 2.5:
                sig('2/2',
                    'v5 YENİ: 2/2 Dep Hakimiyeti (orta) — dep FT grubu tam↓',
                    4.6, 1.61, tier='STANDART')

        if (iy2_ok and
                ch_sy2 is not None and ch_sy2 <= 0 and
                ch_ms2 is not None and ch_ms2 <= 0):
            if ms2 and ms2 <= 2.0:
                sig('2/2',
                    'v5 YENİ: 2/2 TAM DEP HAKİMİYETİ — IY+2Y+MS+IYMS hepsi dep↓',
                    6.5, 2.27, tier='PREMIER')

    return signals

# ─────────────────────────────────────────────────────────────────────
# ANA FONKSİYON
# ─────────────────────────────────────────────────────────────────────

def generate_signals():
    now_tr = datetime.utcnow() + timedelta(hours=3)

    # Tablo yapısı: fixture_id | odds_data | updated_at
    # Tarih kolonu yok → basit select(*) kullan
    try:
        resp = supabase.table('match_odds').select('*').execute()
        rows = resp.data
    except Exception as e:
        print(f"HATA: Tablo sorgusu başarısız → {e}")
        return

    skipped_no_odds = 0
    signals_found   = []

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
                      f"Fixture {row.get('fixture_id', '?')}")

        # updated_at'i tarih olarak kullan (sadece gösterim için)
        updated_at = row.get('updated_at', '')
        if updated_at:
            try:
                dt = datetime.fromisoformat(str(updated_at).replace('Z', '+00:00'))
                date_str = (dt + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M')
            except:
                date_str = str(updated_at)[:16]
        else:
            date_str = '?'

        sigs = evaluate_reversal_signals(markets, changes, sofa_1x2)

        n_elite   = sum(1 for s in sigs if s['tier'] == 'ELITE')
        n_premier = sum(1 for s in sigs if s['tier'] == 'PREMIER')
        n_std     = sum(1 for s in sigs if s['tier'] == 'STANDART')

        if n_elite == 0 and n_premier == 0 and n_std < 2:
            continue

        sigs.sort(key=lambda s: (
            {'ELITE':3,'PREMIER':2,'STANDART':1}[s['tier']],
            float(s['lift'].replace('x',''))
        ), reverse=True)

        ev_sum, dep_sum, _ = ft_group_sums(changes) if changes else (0, 0, 0)

        signals_found.append({
            'match': match_name, 'date': date_str,
            'signals': sigs, 'top_lift': sigs[0]['lift'],
            'has_change': bool(changes), 'has_sofa': bool(sofa_1x2),
            'n_elite': n_elite, 'n_premier': n_premier,
            'ev_ft_sum': ev_sum, 'dep_ft_sum': dep_sum,
        })

    signals_found.sort(key=lambda m: (
        m['n_elite'], m['n_premier'], float(m['top_lift'].replace('x',''))
    ), reverse=True)

    print("=" * 72)
    print(f"  ScorePop REVERSAL v5  |  {now_tr.strftime('%Y-%m-%d %H:%M')} TR")
    print("=" * 72)
    print(f"  Taranan       : {len(rows)}")
    print(f"  Oran yok      : {skipped_no_odds}")
    print(f"  LİSTEDE       : {len(signals_found)}")
    print(f"  markets_change: {sum(1 for m in signals_found if m['has_change'])}/{len(signals_found)}")
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
            if m['has_sofa']:
                extras.append('sofa✓')
            extras_str = f"  [{', '.join(extras)}]" if extras else ''
            print(f"\n  {m['date']}  |  {m['match']}{extras_str}")
            print(f"  {'─' * 66}")
            for s in m['signals']:
                icon_t = {'ELITE':'💎','PREMIER':'⭐','STANDART':'·'}[s['tier']]
                type_icon = {'2/1':'🟢','1/2':'🔵','1/1':'🟡','2/2':'🟣'}.get(s['type'],'⚪')
                print(f"  {icon_t} {type_icon} {s['type']} | {s['lift']} | {s['prec']} | {s['tier']}")
                print(f"      ↳ {s['rule']}")
            print()
        if len(matches) > max_show:
            print(f"  ... +{len(matches)-max_show} maç daha")

    _print(elite_list,   'ELITE — Divergence Sinyali', '💎')
    _print(premier_list, 'PREMIER — lift ≥ 2.0x', '🔥')
    _print(std_list,     'STANDART — inceleme listesi', '📋', max_show=15)

    print(f"\n{'='*72}")
    print(f"  {len(elite_list)} ELITE + {len(premier_list)} PREMIER + {len(std_list)} STANDART")
    print(f"{'='*72}")

if __name__ == "__main__":
    generate_signals()
