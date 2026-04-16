"""
generate_signals.py — ScorePop Sinyal Motoru
=============================================

Supabase'deki match_odds tablosundan henüz oynanmamış maçları okur,
75.434 maç analizinden çıkan kombinasyon kurallarını uygular ve
sinyalleri loglar.

KURULUM:
  pip install supabase python-dotenv

ORTAM DEĞİŞKENLERİ (GitHub Actions Secrets veya .env):
  SUPABASE_URL
  SUPABASE_KEY

ÇALIŞTIRMA:
  python generate_signals.py
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
    """None veya geçersiz değerleri güvenle float'a çevirir."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def get_market(markets: dict, key: str, subkey: str) -> float | None:
    """
    markets dict'inden belirli bir market ve alt anahtar değerini çeker.
    Örnek: get_market(markets, 'ht_ou15', 'over')
    """
    return safe_float(markets.get(key, {}).get(subkey))


def get_sofa_change(sofa_1x2: dict, side: str) -> int:
    """
    sofa_1x2 dict'inden change değerini okur.

    ÖNEMLI — app.js _sofaTo1x2 çıktısı key formatı:
      sofa_1x2['1']['change']   → ev sahibi  (string key '1')
      sofa_1x2['x']['change']   → beraberlik (string key 'x', küçük harf!)
      sofa_1x2['2']['change']   → deplasman  (string key '2')

    change değerleri:
      -1 → oran düştü (favori oldu)
       0 → sabit
      +1 → oran yükseldi (uzaklaştı)

    Eski kodda trend_1x2 = markets_change.get('1x2', {}) şeklinde
    markets içinde arıyordunuz — bu hatalıydı. Change verisi
    odds_data.sofa_1x2 altında ayrı bir obje.
    """
    if not sofa_1x2:
        return 0
    return int(sofa_1x2.get(side, {}).get('change', 0))


def handi_key(h: int, a: int) -> str:
    """
    Handikap market key'ini üretir — app.js _macToSite ile aynı mantık.
    Ev(h) > Dep(a) → 'ah_p{h}_{a}' (pozitif handikap = ev sahibi dezavantajlı)
    Ev(h) < Dep(a) → 'ah_m{h}_{a}' (negatif handikap = ev sahibi avantajlı)

    Sık kullanılanlar:
      Handikaplı Maç Sonucu (0:1) → ev dezavantajlı → 'ah_p0_1'  ← DOĞRUSU BU
      Handikaplı Maç Sonucu (1:0) → dep dezavantajlı → 'ah_m1_0'

    Eski kodda handi_1 = markets.get('ah_m1', {}).get('home') şeklinde
    kullanıyordunuz — bu format hatalı, alt çizgi sonrası sayı eksikti.
    """
    if h > a:
        return f"ah_m{h}_{a}"
    return f"ah_p{h}_{a}"


def parse_match_datetime(row: dict) -> datetime | None:
    """
    Maç tarih/saatini bulmak için birden fazla kaynağa bakar:
      1. matches join objesi (foreign key varsa)
      2. Direkt match_date / match_time kolonları
      3. updated_at (son çare)
    """
    # 1. matches join
    matches_join = row.get('matches')
    if matches_join and isinstance(matches_join, dict):
        m_date = matches_join.get('match_date', '')
        m_time = matches_join.get('match_time', '00:00') or '00:00'
        dt_str = f"{m_date} {m_time[:5]}"
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 2. Direkt kolonlar
    if row.get('match_date'):
        m_date = row.get('match_date', '')
        m_time = (row.get('match_time', '00:00') or '00:00')[:5]
        dt_str = f"{m_date} {m_time}"
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 3. odds_data içinde match_date
    odds_data = row.get('_parsed_odds', {})
    if odds_data.get('match_date'):
        m_date = odds_data['match_date']
        m_time = (odds_data.get('match_time', '00:00') or '00:00')[:5]
        dt_str = f"{m_date} {m_time}"
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    return None


# ─────────────────────────────────────────────────────────────────────
# 3. SINYAL KURALLARI
#    Her kural: (açıklama, yüzde, delta, tahmin)
# ─────────────────────────────────────────────────────────────────────

def evaluate_signals(markets: dict, sofa_1x2: dict | None) -> list[dict]:
    """
    Bir maçın oranlarına kural setini uygular.
    Döner: [{'rule': int, 'label': str, 'pct': str, 'delta': str, 'tip': str}, ...]
    """
    signals = []

    # Sofa change değerleri (eksikse 0 — nötr)
    ch1 = get_sofa_change(sofa_1x2, '1')    # ev: key '1'
    chx = get_sofa_change(sofa_1x2, 'x')    # beraberlik: key 'x' (küçük!)
    ch2 = get_sofa_change(sofa_1x2, '2')    # dep: key '2'
    has_sofa = sofa_1x2 is not None

    # ── Market değerleri ──────────────────────────────────────────
    iy_ou15_over = get_market(markets, 'ht_ou15', 'over')    # İY 1.5 Üst
    iy_ou05_over = get_market(markets, 'ht_ou05', 'over')    # İY 0.5 Üst
    iy_home      = get_market(markets, 'ht_1x2', 'home')     # İY 1
    iy_away      = get_market(markets, 'ht_1x2', 'away')     # İY 2
    ou25_over    = get_market(markets, 'ou25', 'over')        # 2.5 Üst
    ou35_over    = get_market(markets, 'ou35', 'over')        # 3.5 Üst
    kg_var       = get_market(markets, 'btts', 'yes')         # KG Var
    ms_home      = get_market(markets, '1x2', 'home')        # MS 1
    ms_away      = get_market(markets, '1x2', 'away')        # MS 2

    # Handikap (0:1) — en yaygın kullanılan
    # app.js: "Handikaplı Maç Sonucu (0:1)" → ev=0, dep=1 → h<a → 'ah_p0_1'
    handi_01_home = get_market(markets, 'ah_p0_1', 'home')   # Handi(0:1) Ev
    handi_01_away = get_market(markets, 'ah_p0_1', 'away')   # Handi(0:1) Dep
    # Handikap (1:0) — dep dezavantajlı → 'ah_m1_0'
    handi_10_home = get_market(markets, 'ah_m1_0', 'home')   # Handi(1:0) Ev

    # ── KURAL 1: İY 1.5 Üst tek market sinyali ───────────────────
    # 75K maç analizi: ≤1.40 → %84.6, ≤1.50 → %81.2 (baza %51.6)
    if iy_ou15_over is not None:
        if iy_ou15_over <= 1.40:
            signals.append({
                'rule': 1,
                'label': f'İY 1.5 Üst oranı {iy_ou15_over:.2f} (≤1.40)',
                'pct': '%84.6', 'delta': '+33.1%',
                'tip': '2.5 ÜST',
                'note': 'Tek market — en saf gol sinyali'
            })
        elif iy_ou15_over <= 1.50:
            signals.append({
                'rule': 1,
                'label': f'İY 1.5 Üst oranı {iy_ou15_over:.2f} (≤1.50)',
                'pct': '%81.2', 'delta': '+29.6%',
                'tip': '2.5 ÜST',
                'note': 'Tek market — en saf gol sinyali'
            })

    # ── KURAL 2: İY 1.5 Üst + Sofa 1↓X↑2↑ — 3 market ──────────
    # %90.0, n=60, baza %44.1 → +45.9%
    if (iy_ou15_over is not None and iy_ou15_over <= 1.50
            and ou25_over is not None and ou25_over <= 1.60
            and has_sofa and ch1 == -1 and chx == 1 and ch2 == 1):
        signals.append({
            'rule': 2,
            'label': (f'İY 1.5 Üst {iy_ou15_over:.2f} + '
                      f'2.5 Üst {ou25_over:.2f} + Sofa 1↓X↑2↑'),
            'pct': '%90.0', 'delta': '+45.9%',
            'tip': '1 KAZANIR + 2.5 ÜST',
            'note': 'En güçlü kombinasyon (n=60 — veri büyüyünce daha sağlam)'
        })

    # ── KURAL 3: İY 1.5 Üst + 2.5 Üst + KG — 3 market gol ─────
    # %80.0, n=185, baza %51.6 → +28.4%
    if (iy_ou15_over is not None and iy_ou15_over <= 1.50
            and ou25_over is not None and ou25_over <= 1.60
            and kg_var is not None and kg_var <= 1.65):
        signals.append({
            'rule': 3,
            'label': (f'İY 1.5 Üst {iy_ou15_over:.2f} + '
                      f'2.5 Üst {ou25_over:.2f} + KG Var {kg_var:.2f}'),
            'pct': '%80.0', 'delta': '+28.4%',
            'tip': '2.5 ÜST',
            'note': '3 market gol kombinasyonu'
        })

    # ── KURAL 4: İY 1 düşük + Sofa 1↓X↑2↑ ──────────────────────
    # %80.1 (n=1335), baza %44.1 → +36%
    if (iy_home is not None and iy_home <= 1.60
            and has_sofa and ch1 == -1 and chx == 1 and ch2 == 1):
        signals.append({
            'rule': 4,
            'label': f'İY 1 oranı {iy_home:.2f} (≤1.60) + Sofa 1↓X↑2↑',
            'pct': '%80.1', 'delta': '+36.0%',
            'tip': 'MS 1 KAZANIR',
            'note': 'İY favorisi + sharp money kombinasyonu'
        })

    # ── KURAL 5: İY 2 düşük + Sofa 1↑X↑2↓ ──────────────────────
    # %79.8 (n=357), baza %31.4 → +48.4% (en yüksek delta)
    if (iy_away is not None and iy_away <= 1.60
            and has_sofa and ch1 == 1 and chx == 1 and ch2 == -1):
        signals.append({
            'rule': 5,
            'label': f'İY 2 oranı {iy_away:.2f} (≤1.60) + Sofa 1↑X↑2↓',
            'pct': '%79.8', 'delta': '+48.4%',
            'tip': 'MS 2 KAZANIR',
            'note': 'Deplasman — en yüksek delta kombinasyonu'
        })

    # ── KURAL 6: Handi(0:1) 1 düşük + Sofa 1↓ ───────────────────
    # ≤1.40 + Sofa 1↓ → %83.4, n=452
    # ≤1.50 + Sofa 1↓ → %81.0, n=826
    if handi_01_home is not None and has_sofa and ch1 == -1:
        if handi_01_home <= 1.40:
            signals.append({
                'rule': 6,
                'label': f'Handi(0:1) Ev {handi_01_home:.2f} (≤1.40) + Sofa 1↓',
                'pct': '%83.4', 'delta': '+39.3%',
                'tip': 'MS 1 KAZANIR',
                'note': 'Handikap + sharp money — yüksek güven'
            })
        elif handi_01_home <= 1.50:
            signals.append({
                'rule': 6,
                'label': f'Handi(0:1) Ev {handi_01_home:.2f} (≤1.50) + Sofa 1↓',
                'pct': '%81.0', 'delta': '+36.9%',
                'tip': 'MS 1 KAZANIR',
                'note': 'Handikap + sharp money'
            })
        elif handi_01_home <= 1.60:
            signals.append({
                'rule': 6,
                'label': f'Handi(0:1) Ev {handi_01_home:.2f} (≤1.60) + Sofa 1↓',
                'pct': '%78.1', 'delta': '+34.1%',
                'tip': 'MS 1 KAZANIR',
                'note': 'Handikap + sharp money'
            })

    # Deplasman handi versiyonu
    if handi_01_away is not None and has_sofa and ch1 == 1 and ch2 == -1:
        if handi_01_away <= 1.80:
            signals.append({
                'rule': 6,
                'label': f'Handi(0:1) Dep {handi_01_away:.2f} (≤1.80) + Sofa 2↓',
                'pct': '%66.1', 'delta': '+34.7%',
                'tip': 'MS 2 KAZANIR',
                'note': 'Dep handikap + sharp money'
            })

    # ── KURAL 7: 4 katmanlı uyum (en kilitli kombinasyon) ────────
    # MS1≤1.50 + Handi(0:1)1≤1.80 + İY0.5Ü≤1.30 + Sofa1↓ → %73.3, n=1526
    if (ms_home is not None and ms_home <= 1.50
            and handi_01_home is not None and handi_01_home <= 1.80
            and iy_ou05_over is not None and iy_ou05_over <= 1.30
            and has_sofa and ch1 == -1):
        signals.append({
            'rule': 7,
            'label': (f'MS1 {ms_home:.2f} + Handi(0:1)1 {handi_01_home:.2f} + '
                      f'İY0.5Ü {iy_ou05_over:.2f} + Sofa 1↓'),
            'pct': '%73.3', 'delta': '+29.2%',
            'tip': 'MS 1 KAZANIR — 4 KATMAN',
            'note': '4 farklı kaynaktan aynı yön — en güvenilir yapı'
        })

    # Deplasman 4 katman versiyonu
    if (ms_away is not None and ms_away <= 1.50
            and handi_01_away is not None and handi_01_away <= 1.80
            and has_sofa and ch1 == 1 and chx == 1 and ch2 == -1):
        signals.append({
            'rule': 7,
            'label': (f'MS2 {ms_away:.2f} + Handi(0:1)Dep {handi_01_away:.2f} + '
                      f'Sofa 1↑X↑2↓'),
            'pct': '%66.1', 'delta': '+34.7%',
            'tip': 'MS 2 KAZANIR — 4 KATMAN',
            'note': 'Deplasman 4 katman kombinasyonu'
        })

    # ── KURAL 8: İY 1.5 Üst + Sofa 1↑X↑2↓ → 2 kazanır ─────────
    # %72.7, n=66, +41.3% (az veri ama yüksek sapma)
    if (iy_ou15_over is not None and iy_ou15_over <= 1.50
            and has_sofa and ch1 == 1 and chx == 1 and ch2 == -1):
        signals.append({
            'rule': 8,
            'label': f'İY 1.5 Üst {iy_ou15_over:.2f} + Sofa 1↑X↑2↓',
            'pct': '%72.7', 'delta': '+41.3%',
            'tip': 'MS 2 KAZANIR',
            'note': 'Gol beklentisi yüksek + deplasman sharp (n=66)'
        })

    # ── KURAL 9: İY 2 tek market kırılımı ───────────────────────
    # ≤1.60 → %78.4, ≤1.70 → %75.6, ≤1.80 → %72.4
    if iy_away is not None and not any(s['rule'] == 5 for s in signals):
        if iy_away <= 1.60:
            signals.append({
                'rule': 9,
                'label': f'İY 2 oranı {iy_away:.2f} (≤1.60)',
                'pct': '%78.4', 'delta': '+46.9%',
                'tip': 'MS 2 KAZANIR',
                'note': 'Tek market — en yüksek delta (baza %31.4)'
            })
        elif iy_away <= 1.70:
            signals.append({
                'rule': 9,
                'label': f'İY 2 oranı {iy_away:.2f} (≤1.70)',
                'pct': '%75.6', 'delta': '+44.1%',
                'tip': 'MS 2 KAZANIR',
                'note': 'Tek market'
            })

    # ── KURAL 10: İY 1 tek market kırılımı ──────────────────────
    # ≤1.60 → %79.7, ≤1.70 → %77.1, ≤1.80 → %74.0
    if iy_home is not None and not any(s['rule'] == 4 for s in signals):
        if iy_home <= 1.60:
            signals.append({
                'rule': 10,
                'label': f'İY 1 oranı {iy_home:.2f} (≤1.60)',
                'pct': '%79.7', 'delta': '+35.6%',
                'tip': 'MS 1 KAZANIR',
                'note': 'Tek market'
            })
        elif iy_home <= 1.70:
            signals.append({
                'rule': 10,
                'label': f'İY 1 oranı {iy_home:.2f} (≤1.70)',
                'pct': '%77.1', 'delta': '+33.0%',
                'tip': 'MS 1 KAZANIR',
                'note': 'Tek market'
            })

    return signals


# ─────────────────────────────────────────────────────────────────────
# 4. ANA FONKSİYON
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

    rows = response.data
    now_tr = datetime.utcnow() + timedelta(hours=3)  # Türkiye saati (UTC+3)

    skipped_past   = 0
    skipped_no_odds = 0
    signals_found  = []

    for row in rows:
        # odds_data parse
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

        row['_parsed_odds'] = odds_data  # parse_match_datetime için

        markets  = odds_data.get('markets', {})
        sofa_1x2 = odds_data.get('sofa_1x2')   # None olabilir

        if not markets:
            skipped_no_odds += 1
            continue

        # Maç adı
        match_name = (
            odds_data.get('nesine_name')
            or f"Fixture {row.get('fixture_id', '?')}"
        )

        # Tarih/saat kontrolü — geçmiş maçları atla
        match_dt = parse_match_datetime(row)
        if match_dt is None:
            match_date_str = "Tarih belirsiz"
        else:
            match_date_str = match_dt.strftime("%Y-%m-%d %H:%M")
            if match_dt <= now_tr:
                skipped_past += 1
                continue

        # Sinyalleri hesapla
        sigs = evaluate_signals(markets, sofa_1x2)

        if sigs:
            # Başarı yüzdesine göre sırala (önce yüksek)
            sigs.sort(key=lambda s: float(s['pct'].replace('%', '')), reverse=True)
            signals_found.append({
                'fixture_id': row.get('fixture_id'),
                'match':      match_name,
                'date':       match_date_str,
                'signals':    sigs,
                'top_pct':    sigs[0]['pct'],
            })

    # Maçları top sinyale göre sırala
    signals_found.sort(key=lambda m: float(m['top_pct'].replace('%', '')), reverse=True)

    # ── ÇIKTI ──────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  ScorePop Sinyal Motoru  |  {now_tr.strftime('%Y-%m-%d %H:%M')} TR")
    print("=" * 65)
    print(f"  Toplam satır     : {len(rows)}")
    print(f"  Geçmiş maç (atla): {skipped_past}")
    print(f"  Oran yok (atla)  : {skipped_no_odds}")
    print(f"  Sinyal bulunan   : {len(signals_found)}")
    print("=" * 65)

    if not signals_found:
        print("\n  Sinyal bulunan maç yok.\n")
        return

    for m in signals_found:
        print(f"\n  {m['date']}  |  {m['match']}")
        print(f"  {'─' * 55}")
        for s in m['signals']:
            icon = "🔥" if float(s['pct'].replace('%','')) >= 85 else (
                   "⚡" if float(s['pct'].replace('%','')) >= 78 else "✅")
            print(f"  {icon}  [{s['pct']} | {s['delta']}]  {s['tip']}")
            print(f"      Filtre : {s['label']}")
            print(f"      Not    : {s['note']}")
        print()

    print("=" * 65)
    print(f"  Toplam {len(signals_found)} maçta sinyal bulundu.")
    print("=" * 65)


if __name__ == "__main__":
    generate_signals()
