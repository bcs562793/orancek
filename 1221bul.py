"""
reversal_signals.py v2 — ScorePop Reversal (2/1 & 1/2) Sinyal Motoru
======================================================================
84.749 maç analizinden çıkan 2/1 ve 1/2 geri dönüş sinyalleri
Sofascore opening→closing trend verisiyle birleştirilmiştir.

YENİ BULGULAR (v2 — sofa trend analizi):
  ─ 2/1 en güçlü pattern ─
    • DCG(2.Y)≤1.7 + İY-2≥3.5 + Sofa(Ev↑,Dep↓) → 7.25% / 2.53x lift
    • IYMS_2/1≤20 + Ev↑%5 + Dep↓%5             → 6.83% / 2.38x lift
    • IYMS_2/1≤20 + Sofa(Ev↑,Dep↓)             → 5.95% / 2.08x lift

  ─ 1/2 en güçlü pattern ─
    • MS-2≤2.0 + MS-1≥4.0 + Sofa(Dep↑,Ev↓)     → 5.47% / 2.47x lift
    • MS-2≤2.0 + MS-1≥4.0 + Dep %2+ arttı       → 5.26% / 2.38x lift
    • MS-2≤2.0 + MS-1≥4.0 + Sofa(Dep↑)          → 5.25% / 2.37x lift

HATA DÜZELTMELERİ (v1'e göre):
  1. Handikap key:  'ah_m1'   → 'ah_p0_1'
     Handikaplı Maç Sonucu (0:1): ev=0 < dep=1 → prefix 'p' → key 'ah_p0_1'
  2. 2.Y Gol key:   '2h_ou15' yok → 'more_goals' / subkey '2h'
     Daha Çok Gol Olacak Yarı: {'1h': ..., '2h': ..., 'equal': ...}
  3. Sofa ber. key: küçük 'x' → büyük 'X' (Supabase normalize sonrası)
  4. get_sofa_change() artık None döner (0 değil) — sofa yoksa bypass için

ÇALIŞTIRMA:
  python reversal_signals.py
"""

import os
import json
from datetime import datetime, timedelta
from supabase import create_client, Client

# ─────────────────────────────────────────────────────────────────────
# 1. SUPABASE BAĞLANTISI
# ─────────────────────────────────────────────────────────────────────
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("HATA: SUPABASE_URL veya SUPABASE_KEY bulunamadı.")
    exit(1)

supabase: Client = create_client(url, key)


# ─────────────────────────────────────────────────────────────────────
# 2. YARDIMCI FONKSİYONLAR
# ─────────────────────────────────────────────────────────────────────

def safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def get_market(markets: dict, key: str, subkey: str) -> float | None:
    """
    markets dict'inden belirli bir market ve alt anahtar değerini çeker.

    Supabase'de app.js normalize sonrası key→subkey formatları:
      '1x2'          → Maç Sonucu          | 'home', 'draw', 'away'
      'ht_1x2'       → 1. Yarı Sonucu      | 'home', 'draw', 'away'
      '2h_1x2'       → 2. Yarı Sonucu      | 'home', 'draw', 'away'
      'ht_ft'        → İlk Yarı/Maç Sonucu | '1/1','1/2','2/1','2/2' ...
      'more_goals'   → Daha Çok Gol Yarı   | '1h', '2h', 'equal'
      'ah_p0_1'      → Handikap (0:1) — Ev dezavantajlı (h=0 < a=1 → prefix p)
      'ah_m1_0'      → Handikap (1:0) — Dep dezavantajlı
    """
    return safe_float(markets.get(key, {}).get(subkey))


def get_sofa_change(sofa_1x2: dict | None, side: str) -> int | None:
    """
    sofa_1x2 dict'inden opening→closing change değerini okur.

    Format (app.js _sofaTo1x2 çıktısı — Supabase'de odds_data.sofa_1x2):
      sofa_1x2['1']['change']  → ev sahibi  (string key '1')
      sofa_1x2['X']['change']  → beraberlik (büyük 'X'!)
      sofa_1x2['2']['change']  → deplasman  (string key '2')

    Değerler:
      -1 → kapanış oranı düştü (piyasa o tarafı daha çok favori gördü)
       0 → sabit kaldı
      +1 → kapanış oranı yükseldi (piyasa o taraftan uzaklaştı)

    None döner sofa yoksa — 0 ile karıştırılmasın diye.
    """
    if not sofa_1x2:
        return None
    entry = sofa_1x2.get(side, {})
    if not isinstance(entry, dict) or 'change' not in entry:
        return None
    return int(entry['change'])


def get_sofa_pct(sofa_1x2: dict | None, side: str) -> float | None:
    """
    Opening→closing oran değişimini yüzde olarak döndürür.
    Pozitif = oran yükseldi (piyasa uzaklaştı).
    Negatif = oran düştü (piyasa favori gördü).
    """
    if not sofa_1x2:
        return None
    entry = sofa_1x2.get(side, {})
    if not isinstance(entry, dict):
        return None
    o = safe_float(entry.get('opening_odds') or entry.get('open'))
    c = safe_float(entry.get('closing_odds') or entry.get('close'))
    if o and c and o > 0:
        return (c - o) / o * 100
    return None


def parse_match_datetime(row: dict) -> datetime | None:
    """Maç tarih/saatini birden fazla kaynaktan arar."""
    matches_join = row.get('matches')
    if matches_join and isinstance(matches_join, dict):
        m_date = matches_join.get('match_date', '')
        m_time = matches_join.get('match_time', '00:00') or '00:00'
        try:
            return datetime.strptime(f"{m_date} {m_time[:5]}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass
    if row.get('match_date'):
        m_date = row.get('match_date', '')
        m_time = (row.get('match_time', '00:00') or '00:00')[:5]
        try:
            return datetime.strptime(f"{m_date} {m_time}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass
    odds_data = row.get('_parsed_odds', {})
    if odds_data.get('match_date'):
        m_date = odds_data['match_date']
        m_time = (odds_data.get('match_time', '00:00') or '00:00')[:5]
        try:
            return datetime.strptime(f"{m_date} {m_time}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass
    return None


# ─────────────────────────────────────────────────────────────────────
# 3. REVERSAL SINYAL KURALLARI v2
#    (Sofa opening→closing delta entegreli)
# ─────────────────────────────────────────────────────────────────────

def evaluate_reversal_signals(markets: dict, sofa_1x2: dict | None) -> list[dict]:
    """
    2/1 ve 1/2 reversal sinyallerini değerlendirir.
    Her sinyal: {'type', 'rule', 'prec', 'lift', 'sofa_enhanced'}

    Sofa change yorumu:
      change = -1 → Favori oldu  (oran düştü)
      change = +1 → Uzaklaştı    (oran yükseldi)
      change =  0 → Sabit

    2/1 için güçlü sofa pattern: Ev oranı yükseldi (+1) AND Dep oranı düştü (-1)
    1/2 için güçlü sofa pattern: Dep oranı yükseldi (+1) — piyasa dep'ten çekildi
    """
    signals = []

    # ── Sofa change ve pct değerleri ──────────────────────────────
    ch1   = get_sofa_change(sofa_1x2, '1')    # Ev change  (-1/0/+1)
    ch2   = get_sofa_change(sofa_1x2, '2')    # Dep change (-1/0/+1)
    chx   = get_sofa_change(sofa_1x2, 'X')    # Ber change
    pct1  = get_sofa_pct(sofa_1x2, '1')       # Ev oran değişimi (%)
    pct2  = get_sofa_pct(sofa_1x2, '2')       # Dep oran değişimi (%)
    has_s = sofa_1x2 is not None

    # ── Market değerleri ───────────────────────────────────────────
    ms1      = get_market(markets, '1x2',        'home')   # MS-1
    ms2      = get_market(markets, '1x2',        'away')   # MS-2
    iy1      = get_market(markets, 'ht_1x2',     'home')   # İY-1
    iy2      = get_market(markets, 'ht_1x2',     'away')   # İY-2
    sy1      = get_market(markets, '2h_1x2',     'home')   # 2Y-1
    sy2      = get_market(markets, '2h_1x2',     'away')   # 2Y-2
    iyms21   = get_market(markets, 'ht_ft',      '2/1')    # İlk Y/Maç Son. 2/1
    iyms12   = get_market(markets, 'ht_ft',      '1/2')    # İlk Y/Maç Son. 1/2
    dcg_2h   = get_market(markets, 'more_goals', '2h')     # Daha Çok Gol: 2. Yarı

    # FIX v1: 'ah_m1' → 'ah_p0_1' (Handikap 0:1, ev dezavantajlı)
    handi01_home = get_market(markets, 'ah_p0_1', 'home')

    # ══════════════════════════════════════════════════════════════
    # 🟢  2/1 SİNYALLERİ
    #     (Deplasman İY kazanır → Ev Maç Sonucu kazanır)
    #     Baz oran: %2.87
    # ══════════════════════════════════════════════════════════════

    # ── S1: DCG(2.Y) + İY-2 ≥ 3.5 — sofa ile 2.53x ──────────────
    if dcg_2h is not None and dcg_2h <= 1.70 and iy2 is not None and iy2 >= 3.5:
        sofa_tag = ''
        prec, lift = 4.34, 1.51
        if has_s and ch1 is not None and ch2 is not None:
            if ch1 >= 1 and ch2 <= -1:      # En güçlü: Ev↑ + Dep↓
                sofa_tag = ' + Sofa(Ev↑,Dep↓)'
                prec, lift = 7.25, 2.53
            elif ch1 >= 1:
                sofa_tag = ' + Sofa(Ev↑)'
                prec, lift = 5.89, 2.05
            elif ch2 <= -1:
                sofa_tag = ' + Sofa(Dep↓)'
                prec, lift = 5.68, 1.98
        signals.append({
            'type': '2/1', 'sofa_enhanced': bool(sofa_tag),
            'rule': f'DCG(2.Y)≤1.7 + İY-2≥3.5{sofa_tag}',
            'prec': f'%{prec}', 'lift': f'{lift:.2f}x',
        })

    # ── S2: IYMS 2/1 ≤ 20 + opening/closing delta ────────────────
    if iyms21 is not None:
        if iyms21 <= 20.0:
            sofa_tag = ''
            prec, lift = 4.58, 1.60
            if has_s:
                if pct1 is not None and pct2 is not None and pct1 > 5 and pct2 < -5:
                    sofa_tag = ' + Ev↑%5+Dep↓%5'
                    prec, lift = 6.83, 2.38
                elif ch1 is not None and ch2 is not None and ch1 >= 1 and ch2 <= -1:
                    sofa_tag = ' + Sofa(Ev↑,Dep↓)'
                    prec, lift = 5.95, 2.08
                elif ch1 is not None and ch1 >= 1:
                    sofa_tag = ' + Sofa(Ev↑)'
                    prec, lift = 5.89, 2.05
            signals.append({
                'type': '2/1', 'sofa_enhanced': bool(sofa_tag),
                'rule': f'IYMS_2/1 ≤ 20{sofa_tag}',
                'prec': f'%{prec}', 'lift': f'{lift:.2f}x',
            })
        elif iyms21 <= 25.0:
            signals.append({
                'type': '2/1', 'sofa_enhanced': False,
                'rule': 'IYMS_2/1 ≤ 25',
                'prec': '%3.82', 'lift': '1.33x',
            })

    # ── S3: MS-1 ≤ 2.0 + MS-2 ≥ 4.0 ─────────────────────────────
    if ms1 is not None and ms1 <= 2.0 and ms2 is not None and ms2 >= 4.0:
        sofa_tag = ''
        prec, lift = 3.64, 1.27
        if has_s and ch1 is not None and ch2 is not None:
            if ch1 >= 1 and ch2 <= -1:
                sofa_tag = ' + Sofa(Ev↑,Dep↓)'
                prec, lift = 4.82, 1.68
        # Alt kural: + İY-2≥4.0 + 2Y-1≤2.2
        if iy2 is not None and iy2 >= 4.0 and sy1 is not None and sy1 <= 2.2:
            signals.append({
                'type': '2/1', 'sofa_enhanced': bool(sofa_tag),
                'rule': f'MS-1≤2.0 + İY-2≥4.0 + 2Y-1≤2.2{sofa_tag}',
                'prec': f'%{max(prec, 3.6):.1f}', 'lift': f'{max(lift,1.24):.2f}x',
            })
        else:
            signals.append({
                'type': '2/1', 'sofa_enhanced': bool(sofa_tag),
                'rule': f'MS-1≤2.0 + MS-2≥4.0{sofa_tag}',
                'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
            })

    # ── S4: 2Y-1 ≤ 2.0 + İY-2 ≥ 3.5 ─────────────────────────────
    if sy1 is not None and sy1 <= 2.0 and iy2 is not None and iy2 >= 3.5:
        signals.append({
            'type': '2/1', 'sofa_enhanced': False,
            'rule': '2Y-1≤2.0 + İY-2≥3.5',
            'prec': '%3.7', 'lift': '1.28x',
        })

    # ── S5: SS change-1 ≤ -1 + MS-1 ≤ 2.0 ───────────────────────
    if has_s and ch1 is not None and ch1 <= -1 and ms1 is not None and ms1 <= 2.0:
        signals.append({
            'type': '2/1', 'sofa_enhanced': True,
            'rule': 'SS change-1≤-1 + MS-1≤2.0',
            'prec': '%3.5', 'lift': '1.23x',
        })

    # ── S6: Han(0:1)_1 ≤ 2.5 + İY-2 ≥ 3.5 ───────────────────────
    # FIX v1: key 'ah_p0_1' kullanılıyor
    if handi01_home is not None and handi01_home <= 2.5 and iy2 is not None and iy2 >= 3.5:
        signals.append({
            'type': '2/1', 'sofa_enhanced': False,
            'rule': 'Han(0:1)_1≤2.5 + İY-2≥3.5',
            'prec': '%3.9', 'lift': '1.36x',
        })

    # ══════════════════════════════════════════════════════════════
    # 🔵  1/2 SİNYALLERİ
    #     (Ev İY kazanır → Deplasman Maç Sonucu kazanır)
    #     Baz oran: %2.21
    # ══════════════════════════════════════════════════════════════

    # ── S1: MS-2 ≤ 2.0 + MS-1 ≥ 4.0 + sofa ─────────────────────
    if ms2 is not None and ms2 <= 2.0 and ms1 is not None and ms1 >= 4.0:
        sofa_tag = ''
        prec, lift = 4.26, 1.92

        if has_s and ch2 is not None:
            if ch2 >= 1:
                # En güçlü: Dep oranı yükselmiş (piyasa dep'ten çekildi)
                if pct1 is not None and pct1 < -2:
                    sofa_tag = ' + Sofa(Dep↑,Ev↓)'
                    prec, lift = 5.47, 2.47
                elif pct2 is not None and pct2 > 2:
                    sofa_tag = ' + Dep↑%2+'
                    prec, lift = 5.26, 2.38
                else:
                    sofa_tag = ' + Sofa(Dep↑)'
                    prec, lift = 5.25, 2.37

        # SÜPER KOMB: + 2Y-2≤2.2 + İY-1≥3.5 eklenirse daha güçlü
        if sy2 is not None and sy2 <= 2.2 and iy1 is not None and iy1 >= 3.5:
            signals.append({
                'type': '1/2', 'sofa_enhanced': bool(sofa_tag),
                'rule': f'SÜPER: MS-2≤2.0+MS-1≥4.0+2Y-2≤2.2+İY-1≥3.5{sofa_tag}',
                'prec': f'%{max(prec, 3.9):.1f}', 'lift': f'{max(lift, 1.77):.2f}x',
            })
        else:
            signals.append({
                'type': '1/2', 'sofa_enhanced': bool(sofa_tag),
                'rule': f'MS-2≤2.0 + MS-1≥4.0{sofa_tag}',
                'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
            })

    # ── S2: DCG(2.Y) + İY-1 ≥ 3.5 ───────────────────────────────
    # FIX v1: 'more_goals'/'2h' key kullanılıyor
    if dcg_2h is not None and dcg_2h <= 1.70 and iy1 is not None and iy1 >= 3.5:
        signals.append({
            'type': '1/2', 'sofa_enhanced': False,
            'rule': 'DCG(2.Y)≤1.7 + İY-1≥3.5',
            'prec': '%4.75', 'lift': '2.15x',
        })

    # ── S3: MS-2 ≤ 2.5 + MS-1 ≥ 4.5 ─────────────────────────────
    if ms2 is not None and ms2 <= 2.5 and ms1 is not None and ms1 >= 4.5:
        signals.append({
            'type': '1/2', 'sofa_enhanced': False,
            'rule': 'MS-2≤2.5 + MS-1≥4.5',
            'prec': '%4.1', 'lift': '1.83x',
        })

    # ── S4: 2Y-2 ≤ 2.0 + İY-1 ≥ 3.5 + sofa ─────────────────────
    if sy2 is not None and sy2 <= 2.0 and iy1 is not None and iy1 >= 3.5:
        sofa_tag = ''
        prec, lift = 4.06, 1.84
        if has_s and ch2 is not None and ch2 >= 1:
            sofa_tag = ' + Sofa(Dep↑)'
            prec, lift = 4.66, 2.11
        signals.append({
            'type': '1/2', 'sofa_enhanced': bool(sofa_tag),
            'rule': f'2Y-2≤2.0 + İY-1≥3.5{sofa_tag}',
            'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
        })

    # ── S5: İY-1 ≥ 3.5 + MS-2 ≤ 2.0 + Sofa(Ev↓, Dep=0) ─────────
    if (iy1 is not None and iy1 >= 3.5 and ms2 is not None and ms2 <= 2.0 and
            has_s and ch1 is not None and ch2 is not None and ch1 == -1 and ch2 == 0):
        signals.append({
            'type': '1/2', 'sofa_enhanced': True,
            'rule': 'İY-1≥3.5 + MS-2≤2.0 + Sofa(Ev↓,Dep=0)',
            'prec': '%5.11', 'lift': '2.31x',
        })

    # ── S6: MS-1 ≥ 4.0 + 2Y-2 ≤ 2.5 + İY-1 ≥ 3.5 ───────────────
    if (ms1 is not None and ms1 >= 4.0 and
            sy2 is not None and sy2 <= 2.5 and
            iy1 is not None and iy1 >= 3.5):
        signals.append({
            'type': '1/2', 'sofa_enhanced': False,
            'rule': 'MS-1≥4.0 + 2Y-2≤2.5 + İY-1≥3.5',
            'prec': '%4.1', 'lift': '1.87x',
        })

    # ── S7: IYMS 1/2 ≤ 20 + sofa ─────────────────────────────────
    if iyms12 is not None and iyms12 <= 20.0:
        sofa_tag = ''
        prec, lift = 4.06, 1.83
        if has_s and ch1 is not None and ch2 is not None:
            if ch1 >= 1 and ch2 <= -1:
                sofa_tag = ' + Sofa(Ev↑,Dep↓)'
                prec, lift = 4.77, 2.16
            elif ch1 >= 1:
                sofa_tag = ' + Sofa(Ev↑)'
                prec, lift = 4.49, 2.03
        signals.append({
            'type': '1/2', 'sofa_enhanced': bool(sofa_tag),
            'rule': f'IYMS_1/2 ≤ 20{sofa_tag}',
            'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
        })

    # ── S8: SS change-2 ≤ -1 + MS-2 ≤ 2.0 ───────────────────────
    if has_s and ch2 is not None and ch2 <= -1 and ms2 is not None and ms2 <= 2.0:
        signals.append({
            'type': '1/2', 'sofa_enhanced': True,
            'rule': 'SS change-2≤-1 + MS-2≤2.0',
            'prec': '%3.8', 'lift': '1.71x',
        })

    return signals


# ─────────────────────────────────────────────────────────────────────
# 4. ANA FONKSİYON
# ─────────────────────────────────────────────────────────────────────

def generate_signals():
    try:
        response = (
            supabase.table('match_odds')
            .select('*, matches(match_date, match_time)')
            .execute()
        )
    except Exception:
        response = supabase.table('match_odds').select('*').execute()

    rows   = response.data
    now_tr = datetime.utcnow() + timedelta(hours=3)

    skipped_past    = 0
    skipped_no_odds = 0
    signals_found   = []

    for row in rows:
        raw = row.get('odds_data', {})
        if isinstance(raw, str):
            try:
                odds_data = json.loads(raw)
            except json.JSONDecodeError:
                skipped_no_odds += 1
                continue
        elif isinstance(raw, dict):
            odds_data = raw
        else:
            skipped_no_odds += 1
            continue

        row['_parsed_odds'] = odds_data
        markets  = odds_data.get('markets', {})
        sofa_1x2 = odds_data.get('sofa_1x2')

        if not markets:
            skipped_no_odds += 1
            continue

        match_name = (
            odds_data.get('nesine_name')
            or f"Fixture {row.get('fixture_id', '?')}"
        )

        match_dt = parse_match_datetime(row)
        if match_dt is None:
            match_date_str = "Tarih belirsiz"
        else:
            match_date_str = match_dt.strftime("%Y-%m-%d %H:%M")
            if match_dt <= now_tr:
                skipped_past += 1
                continue

        sigs = evaluate_reversal_signals(markets, sofa_1x2)

        if sigs:
            sigs.sort(key=lambda s: float(s['lift'].replace('x', '')), reverse=True)
            signals_found.append({
                'fixture_id':    row.get('fixture_id'),
                'match':         match_name,
                'date':          match_date_str,
                'signals':       sigs,
                'top_lift':      sigs[0]['lift'],
                'has_sofa':      sofa_1x2 is not None,
                'best_is_sofa':  sigs[0].get('sofa_enhanced', False),
            })

    signals_found.sort(key=lambda m: float(m['top_lift'].replace('x', '')), reverse=True)

    # ── ÇIKTI ──────────────────────────────────────────────────────
    print("=" * 72)
    print(f"  ScorePop REVERSAL v2  |  {now_tr.strftime('%Y-%m-%d %H:%M')} TR")
    print("=" * 72)
    print(f"  Taranan satır    : {len(rows)}")
    print(f"  Geçmiş maç atla  : {skipped_past}")
    print(f"  Oran yok atla    : {skipped_no_odds}")
    print(f"  Sinyal bulunan   : {len(signals_found)}")
    sofa_count = sum(1 for m in signals_found if m['has_sofa'])
    print(f"  Sofa verisi olan : {sofa_count}")
    print("=" * 72)

    if not signals_found:
        print("\n  Reversal sinyali veren maç bulunamadı.\n")
        return

    for m in signals_found:
        top_lift  = float(m['top_lift'].replace('x',''))
        fire_icon = "🔥" if top_lift >= 2.3 else ("⚡" if top_lift >= 2.0 else "  ")
        sofa_note = " [SOFA ✓]" if m['has_sofa'] else ""
        print(f"\n{fire_icon} {m['date']}  |  {m['match']}{sofa_note}")
        print(f"  {'─' * 68}")
        for s in m['signals']:
            lift_val = float(s['lift'].replace('x',''))
            icon = "🟢" if s['type'] == '2/1' else "🔵"
            stars = " ⭐⭐" if lift_val >= 2.3 else (" ⭐" if lift_val >= 2.0 else "")
            sofa_flag = " 〔SOFA〕" if s.get('sofa_enhanced') else ""
            print(f"  {icon} {s['type']} | Lift: {s['lift']}{stars} | Başarı: {s['prec']}{sofa_flag}")
            print(f"     Filtre: {s['rule']}")
        print()

    print("=" * 72)
    print(f"  Toplam {len(signals_found)} maçta potansiyel geri dönüş sinyali.")
    print("=" * 72)


if __name__ == "__main__":
    generate_signals()
