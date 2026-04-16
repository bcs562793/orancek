"""
reversal_signals.py — ScorePop Reversal (2/1 & 1/2) Sinyal Motoru
================================================================

Supabase'deki match_odds tablosundan henüz oynanmamış maçları okur,
84.749 maç analizinden çıkan 2/1 ve 1/2 geri dönüş (reversal) 
kombinasyon kurallarını uygular ve ekrana basar.

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
    if val is None: return None
    try: return float(val)
    except (TypeError, ValueError): return None

def get_market(markets: dict, key: str, subkey: str) -> float | None:
    return safe_float(markets.get(key, {}).get(subkey))

def get_sofa_change(sofa_1x2: dict, side: str) -> int:
    if not sofa_1x2: return 0
    return int(sofa_1x2.get(side, {}).get('change', 0))

def parse_match_datetime(row: dict) -> datetime | None:
    matches_join = row.get('matches')
    if matches_join and isinstance(matches_join, dict):
        m_date = matches_join.get('match_date', '')
        m_time = matches_join.get('match_time', '00:00') or '00:00'
        dt_str = f"{m_date} {m_time[:5]}"
        try: return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError: pass

    if row.get('match_date'):
        m_date = row.get('match_date', '')
        m_time = (row.get('match_time', '00:00') or '00:00')[:5]
        dt_str = f"{m_date} {m_time}"
        try: return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError: pass

    odds_data = row.get('_parsed_odds', {})
    if odds_data.get('match_date'):
        m_date = odds_data['match_date']
        m_time = (odds_data.get('match_time', '00:00') or '00:00')[:5]
        dt_str = f"{m_date} {m_time}"
        try: return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError: pass

    return None

# ─────────────────────────────────────────────────────────────────────
# 3. REVERSAL SINYAL KURALLARI (2/1 ve 1/2)
# ─────────────────────────────────────────────────────────────────────

def evaluate_reversal_signals(markets: dict, sofa_1x2: dict | None) -> list[dict]:
    signals = []
    
    # Sofa change değerleri
    ch1 = get_sofa_change(sofa_1x2, '1')
    ch2 = get_sofa_change(sofa_1x2, '2')
    has_sofa = sofa_1x2 is not None

    # Market değerleri (Tablodaki isimlere göre revize edilmiştir)
    iy_home = get_market(markets, 'ht_1x2', 'home')   # İY-1
    iy_away = get_market(markets, 'ht_1x2', 'away')   # İY-2
    ms_home = get_market(markets, '1x2', 'home')      # MS-1
    ms_away = get_market(markets, '1x2', 'away')      # MS-2
    sh_home = get_market(markets, '2h_1x2', 'home')   # 2Y-1 (2. Yarı Sonucu 1)
    sh_away = get_market(markets, '2h_1x2', 'away')   # 2Y-2 (2. Yarı Sonucu 2)
    sh_goals = get_market(markets, '2h_ou15', 'over') # 2.Y Gol Üstü (Tabloya uyumlu 2h formatı)
    
    # İlk Yarı / Maç Sonucu marketleri
    iyms_21 = get_market(markets, 'ht_ft', '2/1') or get_market(markets, 'ht_ft', '2_1')
    iyms_12 = get_market(markets, 'ht_ft', '1/2') or get_market(markets, 'ht_ft', '1_2')
    
    # Handikap (Tabloda Handikap 0:1 ev dezavantajı 'ah_m1' olarak geçiyor)
    handi_01_home = get_market(markets, 'ah_m1', 'home') # Han(0:1)_1

    # ══════════════════════════════════════════════════════════════════
    # 🟢 2/1 SİNYALLERİ (Deplasman İY kazanır → Ev MS kazanır)
    # ══════════════════════════════════════════════════════════════════

    if iyms_21 is not None and iyms_21 <= 20.0:
        signals.append({'type': '2/1', 'rule': 'IYMS_2/1 ≤ 20', 'prec': '%4.6', 'lift': '1.60x'})
    elif iyms_21 is not None and iyms_21 <= 25.0:
        signals.append({'type': '2/1', 'rule': 'IYMS_2/1 ≤ 25', 'prec': '%3.8', 'lift': '1.33x'})

    if sh_goals is not None and sh_goals <= 1.70 and iy_away is not None and iy_away >= 3.50:
        signals.append({'type': '2/1', 'rule': '2.Y Gol ≤ 1.7 + İY-2 ≥ 3.5', 'prec': '%4.3', 'lift': '1.51x'})

    if ms_home is not None and ms_home <= 2.0 and ms_away is not None and ms_away >= 4.0:
        if iy_away is not None and iy_away >= 4.0 and sh_home is not None and sh_home <= 2.2:
            signals.append({'type': '2/1', 'rule': 'MS-1 ≤ 2.0 + İY-2 ≥ 4.0 + 2Y-1 ≤ 2.2', 'prec': '%3.6', 'lift': '1.24x'})
        else:
            signals.append({'type': '2/1', 'rule': 'MS-1 ≤ 2.0 + MS-2 ≥ 4.0', 'prec': '%3.6', 'lift': '1.27x'})

    if sh_home is not None and sh_home <= 2.0 and iy_away is not None and iy_away >= 3.5:
        signals.append({'type': '2/1', 'rule': '2Y-1 ≤ 2.0 + İY-2 ≥ 3.5', 'prec': '%3.7', 'lift': '1.28x'})

    if has_sofa and ch1 <= -1 and ms_home is not None and ms_home <= 2.0:
        signals.append({'type': '2/1', 'rule': 'SS change-1 ≤ -1 + MS-1 ≤ 2.0', 'prec': '%3.5', 'lift': '1.23x'})

    if handi_01_home is not None and handi_01_home <= 2.5 and iy_away is not None and iy_away >= 3.5:
        signals.append({'type': '2/1', 'rule': 'Han(0:1)_1 ≤ 2.5 + İY-2 ≥ 3.5', 'prec': '%3.9', 'lift': '1.36x'})


    # ══════════════════════════════════════════════════════════════════
    # 🔵 1/2 SİNYALLERİ (Ev İY kazanır → Deplasman MS kazanır)
    # ══════════════════════════════════════════════════════════════════

    # "PRATİK KOMBİNASYON" - Dashboard'un sonundaki agresif tavsiye
    if (ms_away is not None and ms_away <= 2.0 and 
        ms_home is not None and ms_home >= 4.0 and 
        sh_away is not None and sh_away <= 2.2 and 
        iy_home is not None and iy_home >= 3.5):
        signals.append({'type': '1/2', 'rule': 'SÜPER KOMB: MS-2≤2.0 + MS-1≥4.0 + 2Y-2≤2.2 + İY-1≥3.5', 'prec': '%5.5+', 'lift': '1.77x'})
    else:
        if sh_goals is not None and sh_goals <= 1.70 and iy_home is not None and iy_home >= 3.5:
            signals.append({'type': '1/2', 'rule': '2.Y Gol ≤ 1.7 + İY-1 ≥ 3.5', 'prec': '%4.8', 'lift': '2.15x'})

        if ms_away is not None and ms_away <= 2.0 and ms_home is not None and ms_home >= 4.0:
            signals.append({'type': '1/2', 'rule': 'MS-2 ≤ 2.0 + MS-1 ≥ 4.0', 'prec': '%4.3', 'lift': '1.93x'})
            
        if ms_away is not None and ms_away <= 2.5 and ms_home is not None and ms_home >= 4.5:
            signals.append({'type': '1/2', 'rule': 'MS-2 ≤ 2.5 + MS-1 ≥ 4.5', 'prec': '%4.1', 'lift': '1.83x'})

        if sh_away is not None and sh_away <= 2.0 and iy_home is not None and iy_home >= 3.5:
            signals.append({'type': '1/2', 'rule': '2Y-2 ≤ 2.0 + İY-1 ≥ 3.5', 'prec': '%4.1', 'lift': '1.84x'})

        if ms_home is not None and ms_home >= 4.0 and sh_away is not None and sh_away <= 2.5 and iy_home is not None and iy_home >= 3.5:
            signals.append({'type': '1/2', 'rule': 'MS-1 ≥ 4.0 + 2Y-2 ≤ 2.5 + İY-1 ≥ 3.5', 'prec': '%4.1', 'lift': '1.87x'})

    if iyms_12 is not None and iyms_12 <= 20.0:
        signals.append({'type': '1/2', 'rule': 'IYMS_1/2 ≤ 20', 'prec': '%4.1', 'lift': '1.83x'})

    if has_sofa and ch2 <= -1 and ms_away is not None and ms_away <= 2.0:
        signals.append({'type': '1/2', 'rule': 'SS change-2 ≤ -1 + MS-2 ≤ 2.0', 'prec': '%3.8', 'lift': '1.71x'})

    return signals


# ─────────────────────────────────────────────────────────────────────
# 4. ANA ÇALIŞTIRMA FONKSİYONU
# ─────────────────────────────────────────────────────────────────────

def generate_signals():
    try:
        response = supabase.table('match_odds').select('*, matches(match_date, match_time)').execute()
    except Exception:
        response = supabase.table('match_odds').select('*').execute()

    rows = response.data
    now_tr = datetime.utcnow() + timedelta(hours=3)

    skipped_past = 0
    skipped_no_odds = 0
    signals_found = []

    for row in rows:
        raw = row.get('odds_data', {})
        if isinstance(raw, str):
            try: odds_data = json.loads(raw)
            except json.JSONDecodeError:
                skipped_no_odds += 1
                continue
        elif isinstance(raw, dict):
            odds_data = raw
        else:
            skipped_no_odds += 1
            continue

        row['_parsed_odds'] = odds_data 
        markets = odds_data.get('markets', {})
        sofa_1x2 = odds_data.get('sofa_1x2')

        if not markets:
            skipped_no_odds += 1
            continue

        match_name = odds_data.get('nesine_name') or f"Fixture {row.get('fixture_id', '?')}"

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
            # En güçlü sinyale göre sırala (lift çarpanına göre)
            sigs.sort(key=lambda s: float(s['lift'].replace('x', '')), reverse=True)
            signals_found.append({
                'fixture_id': row.get('fixture_id'),
                'match': match_name,
                'date': match_date_str,
                'signals': sigs,
                'top_lift': sigs[0]['lift'],
            })

    # Maçları en yüksek lift oranına göre büyükten küçüğe sırala
    signals_found.sort(key=lambda m: float(m['top_lift'].replace('x', '')), reverse=True)

    # ── ÇIKTI ──────────────────────────────────────────────────────
    print("=" * 70)
    print(f"  ScorePop REVERSAL Motoru  |  {now_tr.strftime('%Y-%m-%d %H:%M')} TR")
    print("=" * 70)
    print(f"  Toplam taranan satır : {len(rows)}")
    print(f"  Geçmiş maç (atlanan) : {skipped_past}")
    print(f"  Oran yok (atlanan)   : {skipped_no_odds}")
    print(f"  Sinyal bulunan maç   : {len(signals_found)}")
    print("=" * 70)

    if not signals_found:
        print("\n  Henüz reversal sinyali veren maç bulunamadı.\n")
        return

    for m in signals_found:
        print(f"\n  {m['date']}  |  {m['match']}")
        print(f"  {'─' * 66}")
        for s in m['signals']:
            color_icon = "🟢" if s['type'] == '2/1' else "🔵"
            print(f"  {color_icon} {s['type']} SİNYALİ | Lift: {s['lift']} | Başarı: {s['prec']}")
            print(f"     Filtre: {s['rule']}")
        print()

    print("=" * 70)
    print(f"  Toplam {len(signals_found)} maçta potansiyel geri dönüş yakalandı.")
    print("=" * 70)

if __name__ == "__main__":
    generate_signals()
