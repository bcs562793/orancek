"""
reversal_signals.py v3 — ScorePop Reversal (2/1 & 1/2) Sinyal Motoru
======================================================================
84.749 maç analizinden çıkan 2/1 ve 1/2 geri dönüş sinyalleri
Oran değişimi (trend) artık sofa_1x2 değil, odds_data.markets_change
içindeki '1x2' değerlerinden alınmaktadır.

markets_change['1x2'] yapısı:
  {"home": <float>, "draw": <float>, "away": <float>}
  Negatif → oran düştü (piyasa bu tarafı favori gördü)
  Pozitif → oran yükseldi (piyasa bu taraftan uzaklaştı)
  0       → sabit kaldı

Sinyal yorumu (v2 ile aynı mantık, yeni kaynak):
  2/1: Ev oranı yükseldi (home_chg > 0) AND Dep oranı düştü (away_chg < 0)
  1/2: Dep oranı yükseldi (away_chg > 0) — piyasa dep'ten çekildi

DEĞİŞİKLİKLER (v2 → v3):
  • sofa_1x2 / get_sofa_change / get_sofa_pct tamamen kaldırıldı
  • get_market_change(markets_change, side) eklendi
    - side: 'home', 'draw', 'away'
    - Sayısal float döndürür (örn: -0.15, +0.08, 0.0)
  • change_dir(val) yardımcı fonksiyonu:
    - val < -0.01 → -1 (oran düştü)
    - val > +0.01 → +1 (oran yükseldi)
    - else        →  0 (sabit)
  • Sofa pct eşiği (%5, %2) → markets_change mutlak eşiğe çevrildi:
    - Büyük sinyal: |chg| > 0.10  (~%5+ değişim için proxy)
    - Orta  sinyal: |chg| > 0.04  (~%2+ değişim için proxy)
  • Tüm 'has_sofa', 'sofa_1x2' referansları 'has_chg', 'mc_1x2' olarak güncellendi
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
      'ah_p0_1'      → Handikap (0:1) — Ev dezavantajlı
      'ah_m1_0'      → Handikap (1:0) — Dep dezavantajlı
    """
    return safe_float(markets.get(key, {}).get(subkey))


def get_market_change(mc_1x2: dict | None, side: str) -> float | None:
    """
    markets_change['1x2'] dict'inden belirtilen tarafın oran değişimini döndürür.

    side: 'home' | 'draw' | 'away'

    Dönüş değeri:
      Negatif float → oran düştü (piyasa bu tarafı favori gördü)
      Pozitif float → oran yükseldi (piyasa bu taraftan uzaklaştı)
      0.0           → sabit kaldı
      None          → veri yok (bypass için)
    """
    if not mc_1x2 or not isinstance(mc_1x2, dict):
        return None
    return safe_float(mc_1x2.get(side))


def change_dir(val: float | None) -> int | None:
    """
    Sayısal oran değişimini yön integer'ına dönüştürür:
      +1 → yükseldi  (val > +0.01)
       0 → sabit     (|val| <= 0.01)
      -1 → düştü     (val < -0.01)
      None → veri yok
    """
    if val is None:
        return None
    if val > 0.01:
        return 1
    if val < -0.01:
        return -1
    return 0


def parse_match_datetime(row: dict) -> datetime | None:
    """Maç tarih/saatini birden fazla kaynaktan arar. Öncelik future_matches tablosudur."""
    
    # 1. YENİ YÖNTEM: future_matches içindeki date kolonunu kontrol et
    future_match_data = row.get('future_matches')
    if future_match_data and isinstance(future_match_data, dict):
        iso_date = future_match_data.get('date')
        if iso_date:
            try:
                # ISO formatını (YYYY-MM-DDTHH:MM:SS) direkt parse eder
                return datetime.fromisoformat(iso_date.replace('Z', '+00:00'))
            except ValueError:
                pass
                
    # 2. ESKİ YÖNTEM (Geriye dönük uyumluluk / matches join'i kalmışsa)
    matches_join = row.get('matches')
    if matches_join and isinstance(matches_join, dict):
        m_date = matches_join.get('match_date', '')
        m_time = matches_join.get('match_time', '00:00') or '00:00'
        try:
            return datetime.strptime(f"{m_date} {m_time[:5]}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 3. KÖK DİZİN (Eğer row içine doğrudan yazılmışsa)
    if row.get('match_date'):
        m_date = row.get('match_date', '')
        m_time = (row.get('match_time', '00:00') or '00:00')[:5]
        try:
            return datetime.strptime(f"{m_date} {m_time}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 4. ODDS DATA (Eğer sadece odds_data içindeyse)
    odds_data = row.get('_parsed_odds', {})
    if odds_data.get('match_date'):
        m_date = odds_data['match_date']
        m_time = (odds_data.get('match_time', '00:00') or '00:00')[:5]
        try:
            return datetime.strptime(f"{m_date} {m_time}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # Hiçbir yerde bulamazsa
    return None


# ─────────────────────────────────────────────────────────────────────
# 3. REVERSAL SINYAL KURALLARI v3
#    (markets_change['1x2'] tabanlı trend analizi)
# ─────────────────────────────────────────────────────────────────────

def evaluate_reversal_signals(markets: dict, mc_1x2: dict | None) -> list[dict]:
    """
    2/1 ve 1/2 reversal sinyallerini değerlendirir.
    Her sinyal: {'type', 'rule', 'prec', 'lift', 'chg_enhanced'}

    mc_1x2: markets_change['1x2'] — {'home': float, 'draw': float, 'away': float}

    Oran değişimi yorumu:
      chg < 0 → Piyasa bu tarafı favori gördü (oran düştü)
      chg > 0 → Piyasa bu taraftan uzaklaştı  (oran yükseldi)

    2/1 için güçlü pattern: Ev oranı yükseldi (home_chg > 0) AND Dep oranı düştü (away_chg < 0)
    1/2 için güçlü pattern: Dep oranı yükseldi (away_chg > 0) — piyasa dep'ten çekildi
    """
    signals = []

    # ── markets_change değerleri ───────────────────────────────────
    home_chg = get_market_change(mc_1x2, 'home')   # Ev oran değişimi (float)
    away_chg = get_market_change(mc_1x2, 'away')   # Dep oran değişimi (float)
    # draw_chg = get_market_change(mc_1x2, 'draw') # Gerekirse kullanılabilir

    ch1 = change_dir(home_chg)   # -1 / 0 / +1 veya None
    ch2 = change_dir(away_chg)   # -1 / 0 / +1 veya None
    has_chg = mc_1x2 is not None

    # Büyük değişim eşikleri (sofa %5/%2 eşiğine karşılık gelen proxy değerler)
    # Örn: 2.00 → 2.10 = +0.10 değişim ≈ %5 artış
    BIG_RISE  = 0.10   # |chg| > 0.10 → büyük yükseliş (~%5+)
    MID_RISE  = 0.04   # |chg| > 0.04 → orta yükseliş (~%2+)

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
    handi01_home = get_market(markets, 'ah_p0_1', 'home')  # Handikap 0:1 (ev tarafı)

    # ══════════════════════════════════════════════════════════════
    # 🟢  2/1 SİNYALLERİ
    #     (Deplasman İY kazanır → Ev Maç Sonucu kazanır)
    #     Baz oran: %2.87
    # ══════════════════════════════════════════════════════════════

    # ── S1: DCG(2.Y) ≤ 1.7 + İY-2 ≥ 3.5 ─────────────────────────
    if dcg_2h is not None and dcg_2h <= 1.70 and iy2 is not None and iy2 >= 3.5:
        chg_tag = ''
        prec, lift = 4.34, 1.51
        if has_chg and ch1 is not None and ch2 is not None:
            if ch1 >= 1 and ch2 <= -1:      # En güçlü: Ev↑ + Dep↓
                chg_tag = ' + Trend(Ev↑,Dep↓)'
                prec, lift = 7.25, 2.53
            elif ch1 >= 1:
                chg_tag = ' + Trend(Ev↑)'
                prec, lift = 5.89, 2.05
            elif ch2 <= -1:
                chg_tag = ' + Trend(Dep↓)'
                prec, lift = 5.68, 1.98
        signals.append({
            'type': '2/1', 'chg_enhanced': bool(chg_tag),
            'rule': f'DCG(2.Y)≤1.7 + İY-2≥3.5{chg_tag}',
            'prec': f'%{prec}', 'lift': f'{lift:.2f}x',
        })

    # ── S2: IYMS 2/1 ≤ 20 + oran değişimi ────────────────────────
    if iyms21 is not None:
        if iyms21 <= 20.0:
            chg_tag = ''
            prec, lift = 4.58, 1.60
            if has_chg:
                # Büyük değişim: ev BIG_RISE'dan fazla yükseldi, dep BIG_RISE'dan fazla düştü
                if (home_chg is not None and away_chg is not None
                        and home_chg > BIG_RISE and away_chg < -BIG_RISE):
                    chg_tag = ' + Ev↑+Dep↓(büyük)'
                    prec, lift = 6.83, 2.38
                elif ch1 is not None and ch2 is not None and ch1 >= 1 and ch2 <= -1:
                    chg_tag = ' + Trend(Ev↑,Dep↓)'
                    prec, lift = 5.95, 2.08
                elif ch1 is not None and ch1 >= 1:
                    chg_tag = ' + Trend(Ev↑)'
                    prec, lift = 5.89, 2.05
            signals.append({
                'type': '2/1', 'chg_enhanced': bool(chg_tag),
                'rule': f'IYMS_2/1 ≤ 20{chg_tag}',
                'prec': f'%{prec}', 'lift': f'{lift:.2f}x',
            })
        elif iyms21 <= 25.0:
            signals.append({
                'type': '2/1', 'chg_enhanced': False,
                'rule': 'IYMS_2/1 ≤ 25',
                'prec': '%3.82', 'lift': '1.33x',
            })

    # ── S3: MS-1 ≤ 2.0 + MS-2 ≥ 4.0 ─────────────────────────────
    if ms1 is not None and ms1 <= 2.0 and ms2 is not None and ms2 >= 4.0:
        chg_tag = ''
        prec, lift = 3.64, 1.27
        if has_chg and ch1 is not None and ch2 is not None:
            if ch1 >= 1 and ch2 <= -1:
                chg_tag = ' + Trend(Ev↑,Dep↓)'
                prec, lift = 4.82, 1.68
        if iy2 is not None and iy2 >= 4.0 and sy1 is not None and sy1 <= 2.2:
            signals.append({
                'type': '2/1', 'chg_enhanced': bool(chg_tag),
                'rule': f'MS-1≤2.0 + İY-2≥4.0 + 2Y-1≤2.2{chg_tag}',
                'prec': f'%{max(prec, 3.6):.1f}', 'lift': f'{max(lift,1.24):.2f}x',
            })
        else:
            signals.append({
                'type': '2/1', 'chg_enhanced': bool(chg_tag),
                'rule': f'MS-1≤2.0 + MS-2≥4.0{chg_tag}',
                'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
            })

    # ── S4: 2Y-1 ≤ 2.0 + İY-2 ≥ 3.5 ─────────────────────────────
    if sy1 is not None and sy1 <= 2.0 and iy2 is not None and iy2 >= 3.5:
        signals.append({
            'type': '2/1', 'chg_enhanced': False,
            'rule': '2Y-1≤2.0 + İY-2≥3.5',
            'prec': '%3.7', 'lift': '1.28x',
        })

    # ── S5: Ev oranı düştü (ch1 ≤ -1) + MS-1 ≤ 2.0 ──────────────
    # Not: Bu sinyal v2'de "SS change-1 ≤ -1" idi (sofa change yönü).
    # Artık markets_change: home_chg < 0 (yani ev daha da favori oldu)
    if has_chg and ch1 is not None and ch1 <= -1 and ms1 is not None and ms1 <= 2.0:
        signals.append({
            'type': '2/1', 'chg_enhanced': True,
            'rule': 'Trend(Ev↓) + MS-1≤2.0',
            'prec': '%3.5', 'lift': '1.23x',
        })

    # ── S6: Han(0:1)_1 ≤ 2.5 + İY-2 ≥ 3.5 ───────────────────────
    if handi01_home is not None and handi01_home <= 2.5 and iy2 is not None and iy2 >= 3.5:
        signals.append({
            'type': '2/1', 'chg_enhanced': False,
            'rule': 'Han(0:1)_1≤2.5 + İY-2≥3.5',
            'prec': '%3.9', 'lift': '1.36x',
        })

    # ══════════════════════════════════════════════════════════════
    # 🔵  1/2 SİNYALLERİ
    #     (Ev İY kazanır → Deplasman Maç Sonucu kazanır)
    #     Baz oran: %2.21
    # ══════════════════════════════════════════════════════════════

    # ── S1: MS-2 ≤ 2.0 + MS-1 ≥ 4.0 + oran değişimi ─────────────
    if ms2 is not None and ms2 <= 2.0 and ms1 is not None and ms1 >= 4.0:
        chg_tag = ''
        prec, lift = 4.26, 1.92

        if has_chg and ch2 is not None:
            if ch2 >= 1:
                # En güçlü: Dep yükseldi + Ev düştü (home_chg büyük negatif)
                if home_chg is not None and home_chg < -MID_RISE:
                    chg_tag = ' + Trend(Dep↑,Ev↓)'
                    prec, lift = 5.47, 2.47
                elif away_chg is not None and away_chg > MID_RISE:
                    chg_tag = ' + Dep↑(orta+)'
                    prec, lift = 5.26, 2.38
                else:
                    chg_tag = ' + Trend(Dep↑)'
                    prec, lift = 5.25, 2.37

        if sy2 is not None and sy2 <= 2.2 and iy1 is not None and iy1 >= 3.5:
            signals.append({
                'type': '1/2', 'chg_enhanced': bool(chg_tag),
                'rule': f'SÜPER: MS-2≤2.0+MS-1≥4.0+2Y-2≤2.2+İY-1≥3.5{chg_tag}',
                'prec': f'%{max(prec, 3.9):.1f}', 'lift': f'{max(lift, 1.77):.2f}x',
            })
        else:
            signals.append({
                'type': '1/2', 'chg_enhanced': bool(chg_tag),
                'rule': f'MS-2≤2.0 + MS-1≥4.0{chg_tag}',
                'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
            })

    # ── S2: DCG(2.Y) ≤ 1.7 + İY-1 ≥ 3.5 ─────────────────────────
    if dcg_2h is not None and dcg_2h <= 1.70 and iy1 is not None and iy1 >= 3.5:
        signals.append({
            'type': '1/2', 'chg_enhanced': False,
            'rule': 'DCG(2.Y)≤1.7 + İY-1≥3.5',
            'prec': '%4.75', 'lift': '2.15x',
        })

    # ── S3: MS-2 ≤ 2.5 + MS-1 ≥ 4.5 ─────────────────────────────
    if ms2 is not None and ms2 <= 2.5 and ms1 is not None and ms1 >= 4.5:
        signals.append({
            'type': '1/2', 'chg_enhanced': False,
            'rule': 'MS-2≤2.5 + MS-1≥4.5',
            'prec': '%4.1', 'lift': '1.83x',
        })

    # ── S4: 2Y-2 ≤ 2.0 + İY-1 ≥ 3.5 + trend ─────────────────────
    if sy2 is not None and sy2 <= 2.0 and iy1 is not None and iy1 >= 3.5:
        chg_tag = ''
        prec, lift = 4.06, 1.84
        if has_chg and ch2 is not None and ch2 >= 1:
            chg_tag = ' + Trend(Dep↑)'
            prec, lift = 4.66, 2.11
        signals.append({
            'type': '1/2', 'chg_enhanced': bool(chg_tag),
            'rule': f'2Y-2≤2.0 + İY-1≥3.5{chg_tag}',
            'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
        })

    # ── S5: İY-1 ≥ 3.5 + MS-2 ≤ 2.0 + Trend(Ev↓, Dep=sabit) ────
    # v2: Sofa(Ev↓, Dep=0) → v3: home_chg < 0 AND away_chg ≈ 0
    if (iy1 is not None and iy1 >= 3.5 and ms2 is not None and ms2 <= 2.0 and
            has_chg and ch1 is not None and ch2 is not None
            and ch1 == -1 and ch2 == 0):
        signals.append({
            'type': '1/2', 'chg_enhanced': True,
            'rule': 'İY-1≥3.5 + MS-2≤2.0 + Trend(Ev↓,Dep=sabit)',
            'prec': '%5.11', 'lift': '2.31x',
        })

    # ── S6: MS-1 ≥ 4.0 + 2Y-2 ≤ 2.5 + İY-1 ≥ 3.5 ───────────────
    if (ms1 is not None and ms1 >= 4.0 and
            sy2 is not None and sy2 <= 2.5 and
            iy1 is not None and iy1 >= 3.5):
        signals.append({
            'type': '1/2', 'chg_enhanced': False,
            'rule': 'MS-1≥4.0 + 2Y-2≤2.5 + İY-1≥3.5',
            'prec': '%4.1', 'lift': '1.87x',
        })

    # ── S7: IYMS 1/2 ≤ 20 + trend ────────────────────────────────
    if iyms12 is not None and iyms12 <= 20.0:
        chg_tag = ''
        prec, lift = 4.06, 1.83
        if has_chg and ch1 is not None and ch2 is not None:
            if ch1 >= 1 and ch2 <= -1:
                chg_tag = ' + Trend(Ev↑,Dep↓)'
                prec, lift = 4.77, 2.16
            elif ch1 >= 1:
                chg_tag = ' + Trend(Ev↑)'
                prec, lift = 4.49, 2.03
        signals.append({
            'type': '1/2', 'chg_enhanced': bool(chg_tag),
            'rule': f'IYMS_1/2 ≤ 20{chg_tag}',
            'prec': f'%{prec:.2f}', 'lift': f'{lift:.2f}x',
        })

    # ── S8: Dep oranı düştü (ch2 ≤ -1) + MS-2 ≤ 2.0 ─────────────
    # v2: "SS change-2 ≤ -1" → v3: markets_change away_chg < 0
    if has_chg and ch2 is not None and ch2 <= -1 and ms2 is not None and ms2 <= 2.0:
        signals.append({
            'type': '1/2', 'chg_enhanced': True,
            'rule': 'Trend(Dep↓) + MS-2≤2.0',
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
            .select('*, future_matches(date)')
            .execute()
        )
    except Exception:
      print(f"\n!!! SUPABASE SORGUSU PATLADI !!! Hata: {e}\n")
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
        markets     = odds_data.get('markets', {})
        # v3: markets_change['1x2'] → ev/dep/beraberlik oran değişimi
        mc_raw      = odds_data.get('markets_change', {})
        mc_1x2      = mc_raw.get('1x2') if isinstance(mc_raw, dict) else None

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

        sigs = evaluate_reversal_signals(markets, mc_1x2)

        if sigs:
            sigs.sort(key=lambda s: float(s['lift'].replace('x', '')), reverse=True)
            has_chg_data = mc_1x2 is not None
            signals_found.append({
                'fixture_id':    row.get('fixture_id'),
                'match':         match_name,
                'date':          match_date_str,
                'signals':       sigs,
                'top_lift':      sigs[0]['lift'],
                'has_chg':       has_chg_data,
                'best_is_chg':   sigs[0].get('chg_enhanced', False),
            })

    signals_found.sort(key=lambda m: float(m['top_lift'].replace('x', '')), reverse=True)

    # ── ÇIKTI ──────────────────────────────────────────────────────
    print("=" * 72)
    print(f"  ScorePop REVERSAL v3  |  {now_tr.strftime('%Y-%m-%d %H:%M')} TR")
    print("=" * 72)
    print(f"  Taranan satır      : {len(rows)}")
    print(f"  Geçmiş maç atla   : {skipped_past}")
    print(f"  Oran yok atla     : {skipped_no_odds}")
    print(f"  Sinyal bulunan    : {len(signals_found)}")
    chg_count = sum(1 for m in signals_found if m['has_chg'])
    print(f"  Trend verisi olan : {chg_count}")
    print("=" * 72)

    if not signals_found:
        print("\n  Reversal sinyali veren maç bulunamadı.\n")
        return

    for m in signals_found:
        top_lift  = float(m['top_lift'].replace('x',''))
        fire_icon = "🔥" if top_lift >= 2.3 else ("⚡" if top_lift >= 2.0 else "  ")
        chg_note  = " [TREND ✓]" if m['has_chg'] else ""
        print(f"\n{fire_icon} {m['date']}  |  {m['match']}{chg_note}")
        print(f"  {'─' * 68}")
        for s in m['signals']:
            lift_val = float(s['lift'].replace('x',''))
            icon = "🟢" if s['type'] == '2/1' else "🔵"
            stars = " ⭐⭐" if lift_val >= 2.3 else (" ⭐" if lift_val >= 2.0 else "")
            chg_flag = " 〔TREND〕" if s.get('chg_enhanced') else ""
            print(f"  {icon} {s['type']} | Lift: {s['lift']}{stars} | Başarı: {s['prec']}{chg_flag}")
            print(f"     Filtre: {s['rule']}")
        print()

    print("=" * 72)
    print(f"  Toplam {len(signals_found)} maçta potansiyel geri dönüş sinyali.")
    print("=" * 72)


if __name__ == "__main__":
    generate_signals()
