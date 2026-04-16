"""
reversal_signals.py v2 — ScorePop Reversal (2/1 & 1/2) Sinyal Motoru
======================================================================

84.749 maç analizinden çıkan 2/1 ve 1/2 geri dönüş sinyallerini
Sofascore oran hareket trendi (opening→closing change) ile birleştirerek
daha yüksek lift elde eden kurallar uygulanır.

YENİ BULGULAR (v2):
  • 2/1 için en güçlü sofa pattern: ft_ch1=+1 AND ft_ch2=-1
    (Ev oranı yükselmiş + Deplasman oranı düşmüş → lift 2.08x)
  • 1/2 için en güçlü sofa pattern: ft_ch2=+1
    (Deplasman oranı yükselmiş, piyasa uzaklaşmış → lift 2.37x)

HATA DÜZELTMELERİ (v1'e göre):
  1. Handikap key: 'ah_m1' → 'ah_p0_1'   (app.js format: h<a → 'ah_p{h}_{a}')
  2. 2.Y Gol key:  'more_goals' subkey '2h'  (Daha Çok Gol Olacak Yarı: '2.Y')
  3. Sofa key:  'x' yerine 'X' (beraberlik büyük X — Supabase normalize sonrası)

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

    Supabase'e app.js tarafından normalize edilmiş key formatları:
      '1x2'        → Maç Sonucu        | subkeys: 'home', 'draw', 'away'
      'ht_1x2'     → 1. Yarı Sonucu    | subkeys: 'home', 'draw', 'away'
      '2h_1x2'     → 2. Yarı Sonucu    | subkeys: 'home', 'draw', 'away'
      'ht_ft'      → İlk Yarı/Maç Son. | subkeys: '1/1','1/2','2/1' ...
      'ah_p0_1'    → Handikap (0:1) Ev dezavantajlı  | 'home','draw','away'
      'ah_m1_0'    → Handikap (1:0) Dep dezavantajlı | 'home','draw','away'
      'more_goals' → Daha Çok Gol Yarı | subkeys: '1h', '2h', 'equal'
    """
    return safe_float(markets.get(key, {}).get(subkey))


def get_sofa_change(sofa_1x2: dict | None, side: str) -> int | None:
    """
    sofa_1x2 dict'inden change değerini okur.

    Supabase'deki format (app.js _sofaTo1x2 çıktısı):
      sofa_1x2['1']['change']  → ev       (-1=düştü/favori, 0=sabit, +1=yükseldi)
      sofa_1x2['X']['change']  → beraberlik  (büyük X!)
      sofa_1x2['2']['change']  → deplasman

    DİKKAT: Beraberlik key'i büyük 'X' — küçük 'x' değil!
    None döner sofa verisi yoksa (oran yokluğu ile 0'dan fark edilebilsin diye).
    """
    if not sofa_1x2:
        return None
    val = sofa_1x2.get(side, {})
    if not isinstance(val, dict) or 'change' not in val:
        return None
    return int(val['change'])


def parse_match_datetime(row: dict) -> datetime | None:
    """Maç tarih/saatini birden fazla kaynaktan arar."""
    # 1. matches join objesi
    matches_join = row.get('matches')
    if matches_join and isinstance(matches_join, dict):
        m_date = matches_join.get('match_date', '')
        m_time = matches_join.get('match_time', '00:00') or '00:00'
        try:
            return datetime.strptime(f"{m_date} {m_time[:5]}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 2. Direkt kolonlar
    if row.get('match_date'):
        m_date = row.get('match_date', '')
        m_time = (row.get('match_time', '00:00') or '00:00')[:5]
        try:
            return datetime.strptime(f"{m_date} {m_time}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 3. odds_data içinde
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
# 3. REVERSAL SINYAL KURALLARI (v2 — Sofa trend entegreli)
# ─────────────────────────────────────────────────────────────────────

def evaluate_reversal_signals(markets: dict, sofa_1x2: dict | None) -> list[dict]:
    """
    84.749 maç analizinden çıkan 2/1 ve 1/2 reversal kuralları.
    
    Sofa change değerleri:
      -1 → oran kapanışta düştü (o taraf piyasada daha favori oldu)
       0 → sabit
      +1 → oran kapanışta yükseldi (piyasa uzaklaştı)
    
    2/1 En güçlü sofa pattern: ft_ch1=+1 AND ft_ch2=-1
      (Ev oranı yükselmesi + Dep oranı düşmesi → lift 2.08x)
    1/2 En güçlü sofa pattern: ft_ch2=+1
      (Dep oranı yükselmesi → lift 2.37x)
    """
    signals = []

    # ── Sofa change değerleri ──────────────────────────────────────
    ft_ch1 = get_sofa_change(sofa_1x2, '1')   # Ev FT change
    ft_ch2 = get_sofa_change(sofa_1x2, '2')   # Dep FT change
    ft_chx = get_sofa_change(sofa_1x2, 'X')   # Beraberlik FT change
    has_sofa = sofa_1x2 is not None

    # ── Market değerleri ───────────────────────────────────────────
    ms_home  = get_market(markets, '1x2',     'home')   # MS-1
    ms_away  = get_market(markets, '1x2',     'away')   # MS-2
    iy_home  = get_market(markets, 'ht_1x2',  'home')   # İY-1
    iy_away  = get_market(markets, 'ht_1x2',  'away')   # İY-2
    sh_home  = get_market(markets, '2h_1x2',  'home')   # 2Y-1
    sh_away  = get_market(markets, '2h_1x2',  'away')   # 2Y-2
    iyms_21  = get_market(markets, 'ht_ft',   '2/1')    # İlk Yarı/Maç Son. 2/1
    iyms_12  = get_market(markets, 'ht_ft',   '1/2')    # İlk Yarı/Maç Son. 1/2

    # "Daha Çok Gol Olacak Yarı" → 2. yarıda daha çok gol bekleniyor
    # FIX v1: '2h_ou15' değil → 'more_goals' key, '2h' subkey
    more_goals_2h = get_market(markets, 'more_goals', '2h')

    # Handikap (0:1) → Ev dezavantajlı
    # FIX v1: 'ah_m1' değil → 'ah_p0_1'  (h=0 < a=1 → prefix 'p')
    handi_01_home = get_market(markets, 'ah_p0_1', 'home')

    # ══════════════════════════════════════════════════════════════════
    # 🟢 2/1 SİNYALLERİ — Deplasman İY kazanır → Ev MS kazanır
    # ══════════════════════════════════════════════════════════════════

    # ── Sinyal 2/1-S1: IYMS 2/1 oranı düşük + Sofa trend ─────────
    if iyms_21 is not None:
        if iyms_21 <= 20.0:
            base_prec = 4.58
            base_lift = 1.60
            sofa_tag  = ''

            # YENİ: ft_ch1=+1 AND ft_ch2=-1 → lift 2.08x (en güçlü pattern)
            if has_sofa and ft_ch1 is not None and ft_ch2 is not None:
                if ft_ch1 >= 1 and ft_ch2 <= -1:
                    sofa_tag  = ' + Sofa(Ev↑Dep↓)'
                    base_prec = 5.95
                    base_lift = 2.08
                elif ft_ch1 >= 1:
                    sofa_tag  = ' + Sofa(Ev↑)'
                    base_prec = 5.89
                    base_lift = 2.05
                elif ft_ch2 <= -1:
                    sofa_tag  = ' + Sofa(Dep↓)'
                    base_prec = 5.68
                    base_lift = 1.98

            signals.append({
                'type': '2/1',
                'rule': f'IYMS_2/1 ≤ 20{sofa_tag}',
                'prec': f'%{base_prec}',
                'lift': f'{base_lift:.2f}x',
            })

        elif iyms_21 <= 25.0:
            signals.append({
                'type': '2/1',
                'rule': 'IYMS_2/1 ≤ 25',
                'prec': '%3.82',
                'lift': '1.33x',
            })

    # ── Sinyal 2/1-S2: 2.Y Gol + İY-2 ≥ 3.5 ─────────────────────
    if more_goals_2h is not None and more_goals_2h <= 1.70 and iy_away is not None and iy_away >= 3.50:
        signals.append({
            'type': '2/1',
            'rule': '2.Y Gol ≤ 1.7 + İY-2 ≥ 3.5',
            'prec': '%4.3',
            'lift': '1.51x',
        })

    # ── Sinyal 2/1-S3: MS-1 ≤ 2.0 + MS-2 ≥ 4.0 ──────────────────
    if ms_home is not None and ms_home <= 2.0 and ms_away is not None and ms_away >= 4.0:
        base_prec = 3.64
        base_lift = 1.27
        sofa_tag  = ''

        if has_sofa and ft_ch1 is not None and ft_ch2 is not None:
            if ft_ch1 >= 1 and ft_ch2 <= -1:
                sofa_tag  = ' + Sofa(Ev↑Dep↓)'
                base_prec = 4.82
                base_lift = 1.68
            elif ft_ch1 == 0 and ft_ch2 >= 1:
                sofa_tag  = ' + Sofa(Dep↑)'
                base_prec = 4.78
                base_lift = 1.67

        # Alt-kural: İY-2 ≥ 4.0 + 2Y-1 ≤ 2.2 da varsa daha güçlü
        if iy_away is not None and iy_away >= 4.0 and sh_home is not None and sh_home <= 2.2:
            signals.append({
                'type': '2/1',
                'rule': f'MS-1 ≤ 2.0 + İY-2 ≥ 4.0 + 2Y-1 ≤ 2.2{sofa_tag}',
                'prec': f'%{max(base_prec, 3.6):.1f}',
                'lift': f'{max(base_lift, 1.24):.2f}x',
            })
        else:
            signals.append({
                'type': '2/1',
                'rule': f'MS-1 ≤ 2.0 + MS-2 ≥ 4.0{sofa_tag}',
                'prec': f'%{base_prec:.2f}',
                'lift': f'{base_lift:.2f}x',
            })

    # ── Sinyal 2/1-S4: 2Y-1 ≤ 2.0 + İY-2 ≥ 3.5 ──────────────────
    if sh_home is not None and sh_home <= 2.0 and iy_away is not None and iy_away >= 3.5:
        signals.append({
            'type': '2/1',
            'rule': '2Y-1 ≤ 2.0 + İY-2 ≥ 3.5',
            'prec': '%3.7',
            'lift': '1.28x',
        })

    # ── Sinyal 2/1-S5: SS change-1 ≤ -1 + MS-1 ≤ 2.0 ────────────
    if has_sofa and ft_ch1 is not None and ft_ch1 <= -1 and ms_home is not None and ms_home <= 2.0:
        signals.append({
            'type': '2/1',
            'rule': 'SS change-1 ≤ -1 + MS-1 ≤ 2.0',
            'prec': '%3.5',
            'lift': '1.23x',
        })

    # ── Sinyal 2/1-S6: Han(0:1)_1 ≤ 2.5 + İY-2 ≥ 3.5 ────────────
    if handi_01_home is not None and handi_01_home <= 2.5 and iy_away is not None and iy_away >= 3.5:
        signals.append({
            'type': '2/1',
            'rule': 'Han(0:1)_1 ≤ 2.5 + İY-2 ≥ 3.5',
            'prec': '%3.9',
            'lift': '1.36x',
        })

    # ══════════════════════════════════════════════════════════════════
    # 🔵 1/2 SİNYALLERİ — Ev İY kazanır → Deplasman MS kazanır
    # ══════════════════════════════════════════════════════════════════

    # ── Sinyal 1/2-S1: MS-2 ≤ 2.0 + MS-1 ≥ 4.0 + Sofa trend ─────
    if ms_away is not None and ms_away <= 2.0 and ms_home is not None and ms_home >= 4.0:
        base_prec = 4.26
        base_lift = 1.92
        sofa_tag  = ''

        if has_sofa and ft_ch2 is not None:
            if ft_ch2 >= 1:
                # YENİ: Dep oranı yükselmiş (piyasa uzaklaşmış) → lift 2.37x!
                sofa_tag  = ' + Sofa(Dep↑)'
                base_prec = 5.25
                base_lift = 2.37
            elif ft_ch2 <= -1:
                sofa_tag  = ' + Sofa(Dep↓)'
                base_prec = 4.03
                base_lift = 1.82

        # SÜPER KOMB: + 2Y-2 ≤ 2.2 + İY-1 ≥ 3.5
        if (sh_away is not None and sh_away <= 2.2 and
                iy_home is not None and iy_home >= 3.5):
            signals.append({
                'type': '1/2',
                'rule': f'SÜPER: MS-2≤2.0 + MS-1≥4.0 + 2Y-2≤2.2 + İY-1≥3.5{sofa_tag}',
                'prec': f'%{max(base_prec, 3.9):.1f}',
                'lift': f'{max(base_lift, 1.77):.2f}x',
            })
        else:
            signals.append({
                'type': '1/2',
                'rule': f'MS-2 ≤ 2.0 + MS-1 ≥ 4.0{sofa_tag}',
                'prec': f'%{base_prec:.2f}',
                'lift': f'{base_lift:.2f}x',
            })

    # ── Sinyal 1/2-S2: MS-2 ≤ 2.5 + MS-1 ≥ 4.5 ──────────────────
    if ms_away is not None and ms_away <= 2.5 and ms_home is not None and ms_home >= 4.5:
        signals.append({
            'type': '1/2',
            'rule': 'MS-2 ≤ 2.5 + MS-1 ≥ 4.5',
            'prec': '%4.1',
            'lift': '1.83x',
        })

    # ── Sinyal 1/2-S3: 2Y-2 ≤ 2.0 + İY-1 ≥ 3.5 + Sofa ──────────
    if sh_away is not None and sh_away <= 2.0 and iy_home is not None and iy_home >= 3.5:
        base_prec = 4.06
        base_lift = 1.84
        sofa_tag  = ''
        if has_sofa and ft_ch2 is not None and ft_ch2 >= 1:
            sofa_tag  = ' + Sofa(Dep↑)'
            base_prec = 4.66
            base_lift = 2.11
        signals.append({
            'type': '1/2',
            'rule': f'2Y-2 ≤ 2.0 + İY-1 ≥ 3.5{sofa_tag}',
            'prec': f'%{base_prec:.2f}',
            'lift': f'{base_lift:.2f}x',
        })

    # ── Sinyal 1/2-S4: 2.Y Gol + İY-1 ≥ 3.5 ─────────────────────
    if more_goals_2h is not None and more_goals_2h <= 1.70 and iy_home is not None and iy_home >= 3.5:
        signals.append({
            'type': '1/2',
            'rule': '2.Y Gol ≤ 1.7 + İY-1 ≥ 3.5',
            'prec': '%4.8',
            'lift': '2.15x',
        })

    # ── Sinyal 1/2-S5: MS-1 ≥ 4.0 + 2Y-2 ≤ 2.5 + İY-1 ≥ 3.5 ────
    if (ms_home is not None and ms_home >= 4.0 and
            sh_away is not None and sh_away <= 2.5 and
            iy_home is not None and iy_home >= 3.5):
        signals.append({
            'type': '1/2',
            'rule': 'MS-1 ≥ 4.0 + 2Y-2 ≤ 2.5 + İY-1 ≥ 3.5',
            'prec': '%4.1',
            'lift': '1.87x',
        })

    # ── Sinyal 1/2-S6: IYMS 1/2 ≤ 20 + Sofa ─────────────────────
    if iyms_12 is not None and iyms_12 <= 20.0:
        base_prec = 4.06
        base_lift = 1.83
        sofa_tag  = ''
        if has_sofa and ft_ch2 is not None:
            if ft_ch2 <= -1:
                sofa_tag  = ' + Sofa(Dep↓)'
                base_prec = 4.65
                base_lift = 2.10
            elif ft_ch1 is not None and ft_ch1 >= 1:
                sofa_tag  = ' + Sofa(Ev↑)'
                base_prec = 4.49
                base_lift = 2.03
        signals.append({
            'type': '1/2',
            'rule': f'IYMS_1/2 ≤ 20{sofa_tag}',
            'prec': f'%{base_prec:.2f}',
            'lift': f'{base_lift:.2f}x',
        })

    # ── Sinyal 1/2-S7: SS change-2 ≤ -1 + MS-2 ≤ 2.0 ────────────
    if has_sofa and ft_ch2 is not None and ft_ch2 <= -1 and ms_away is not None and ms_away <= 2.0:
        signals.append({
            'type': '1/2',
            'rule': 'SS change-2 ≤ -1 + MS-2 ≤ 2.0',
            'prec': '%3.8',
            'lift': '1.71x',
        })

    return signals


# ─────────────────────────────────────────────────────────────────────
# 4. ANA ÇALIŞTIRMA FONKSİYONU
# ─────────────────────────────────────────────────────────────────────

def generate_signals():
    # match_odds tablosunu çek
    try:
        response = (
            supabase.table('match_odds')
            .select('*, matches(match_date, match_time)')
            .execute()
        )
    except Exception:
        response = supabase.table('match_odds').select('*').execute()

    rows     = response.data
    now_tr   = datetime.utcnow() + timedelta(hours=3)

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
        sofa_1x2 = odds_data.get('sofa_1x2')   # {'1':{change:-1},'X':{change:0},'2':{change:1}}

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
                'fixture_id': row.get('fixture_id'),
                'match':      match_name,
                'date':       match_date_str,
                'signals':    sigs,
                'top_lift':   sigs[0]['lift'],
            })

    signals_found.sort(key=lambda m: float(m['top_lift'].replace('x', '')), reverse=True)

    # ── ÇIKTI ──────────────────────────────────────────────────────
    print("=" * 72)
    print(f"  ScorePop REVERSAL v2  |  {now_tr.strftime('%Y-%m-%d %H:%M')} TR")
    print("=" * 72)
    print(f"  Toplam taranan satır : {len(rows)}")
    print(f"  Geçmiş maç (atlanan) : {skipped_past}")
    print(f"  Oran yok (atlanan)   : {skipped_no_odds}")
    print(f"  Sinyal bulunan maç   : {len(signals_found)}")
    print("=" * 72)

    if not signals_found:
        print("\n  Reversal sinyali veren maç bulunamadı.\n")
        return

    for m in signals_found:
        has_strong = any(float(s['lift'].replace('x','')) >= 2.0 for s in m['signals'])
        prefix = "🔥" if has_strong else "  "
        print(f"\n{prefix} {m['date']}  |  {m['match']}")
        print(f"  {'─' * 68}")
        for s in m['signals']:
            lift_val = float(s['lift'].replace('x',''))
            color_icon = "🟢" if s['type'] == '2/1' else "🔵"
            star = " ⭐" if lift_val >= 2.0 else ""
            print(f"  {color_icon} {s['type']} | Lift: {s['lift']}{star} | Başarı: {s['prec']}")
            print(f"     Filtre: {s['rule']}")
        print()

    print("=" * 72)
    print(f"  Toplam {len(signals_found)} maçta potansiyel geri dönüş sinyali.")
    print("=" * 72)


if __name__ == "__main__":
    generate_signals()
