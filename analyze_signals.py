import os
import json
from supabase import create_client, Client

# Supabase bağlantı ayarları (GitHub Actions Secret'lardan gelecek)
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("HATA: SUPABASE_URL veya SUPABASE_KEY bulunamadı.")
    exit(1)

supabase: Client = create_client(url, key)

def generate_signals():
    # Bülteni çek (Eğer matches tablosuyla foreign key varsa tarihi oradan joinler)
    try:
         response = supabase.table('match_odds').select('*, matches(match_date, match_time)').execute()
    except Exception:
         # Join başarısız olursa sadece match_odds çek
         response = supabase.table('match_odds').select('*').execute()
         
    matches = response.data
    signals_found = []

    for row in matches:
        # odds_data JSON kolonunu parse et
        odds_data_raw = row.get('odds_data', {})
        if isinstance(odds_data_raw, str):
            try:
                odds_data = json.loads(odds_data_raw)
            except json.JSONDecodeError:
                continue
        else:
            odds_data = odds_data_raw
            
        markets = odds_data.get('markets', {})
        markets_change = odds_data.get('markets_change', {})
        
        if not markets:
            continue

        # Maç ismi
        match_name = odds_data.get('nesine_name', f"Maç ID: {row.get('fixture_id')}")
        
        # --- TARİH VE ZAMAN ÇIKARIMI ---
        match_date = "Tarih Belirsiz"
        matches_join = row.get('matches')
        
        if matches_join and isinstance(matches_join, dict):
            # matches tablosundan geliyorsa
            m_date = matches_join.get('match_date', '')
            m_time = matches_join.get('match_time', '')
            match_date = f"{m_date} {m_time}".strip()
        elif row.get('match_date'):
            # Direkt bu tabloda kolon varsa
            match_date = f"{row.get('match_date')} {row.get('match_time', '')}".strip()
        elif row.get('updated_at'):
            # Hiçbiri yoksa en son verinin çekildiği/güncellendiği zamanı baz al
            match_date = row.get('updated_at')[:16].replace('T', ' ')

        match_signals = []

        # 1. Oranları Çekme
        iy_1_5_ust = markets.get('ht_ou15', {}).get('over')
        iy_2 = markets.get('ht_1x2', {}).get('away')
        handi_1 = markets.get('ah_m1', {}).get('home')
        ms_1 = markets.get('1x2', {}).get('home')
        iy_0_5_ust = markets.get('ht_ou05', {}).get('over')

        # 2. Trend (Değişim) Verilerini Çekme (1x2 marketi üzerinden)
        # Değerler: -1 (Oran Düştü / ↓), 1 (Oran Yükseldi / ↑), 0 (Sabit)
        trend_1x2 = markets_change.get('1x2', {})
        trend_1 = trend_1x2.get('home', 0)
        trend_x = trend_1x2.get('draw', 0)
        trend_2 = trend_1x2.get('away', 0)

        # --- STRATEJİ VE KONTROLLER ---

        # Kural 1: Tek market - en saf sinyal
        if iy_1_5_ust and iy_1_5_ust <= 1.50:
            win_rate = "%84.6" if iy_1_5_ust <= 1.40 else "%81.2"
            match_signals.append(f"🔥 Sinyal 1 (Saf Sinyal): İY 1.5 Üst oranı {iy_1_5_ust} -> Tahmin: 2.5 ÜST (Başarı: {win_rate})")

        # Kural 2: Yüksek Delta Kombinasyonu
        # İY 2 oranı <= 1.60 VE (MS2 Oranı Düşüyor (-1), MS1 ve Beraberlik Oranları Yükseliyor (1))
        if iy_2 and iy_2 <= 1.60 and trend_1 == 1 and trend_x == 1 and trend_2 == -1:
            match_signals.append(f"⚡ Sinyal 2 (Yüksek Delta): İY 2 oranı {iy_2} + Piyasada Deplasman Eğilimi Var -> Tahmin: MS 2")

        # Kural 3: Handikap + Trend Üretim Kalitesi
        # Handikap 1 <= 1.40 VE MS1 Oranı Düşüyor (-1)
        if handi_1 and handi_1 <= 1.40 and trend_1 == -1:
            match_signals.append(f"🛡️ Sinyal 3 (Üretim Kalitesi): Handi(0:1) 1 oranı {handi_1} + Piyasa Ev Sahibine Kayıyor -> Tahmin: MS 1")

        # Kural 4: 4 market filtresi (4 katmanlı uyum)
        # Hepsi beklenen eşiklerin altında VE MS1 Oranı Düşüyor (-1)
        if ms_1 and ms_1 <= 1.50 and handi_1 and handi_1 <= 1.80 and iy_0_5_ust and iy_0_5_ust <= 1.30 and trend_1 == -1:
            match_signals.append(f"🎯 Sinyal 4 (4 Katmanlı Uyum): MS1={ms_1}, Handi={handi_1}, İY0.5Ü={iy_0_5_ust} sağlandı ve oran düşüşte -> Tahmin: MS 1 BANKO")

        # Sinyal varsa listeye yaz
        if match_signals:
            signals_found.append({
                "match": match_name, 
                "date": match_date,
                "signals": match_signals
            })

    # Sonuçları Loglara Yazdır
    print(f"Toplam incelenen maç: {len(matches)}")
    print(f"Sinyal bulunan maç sayısı: {len(signals_found)}\n")

    for s in signals_found:
        print(f"Tarih: {s['date']} | Maç: {s['match']}")
        for sig in s['signals']:
            print(f"  {sig}")
        print("-" * 50)

if __name__ == "__main__":
    generate_signals()
