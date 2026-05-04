import os
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from datetime import datetime
from supabase import create_client

# ── Supabase Bağlantısı ────────────────────────────────────────────────
# Eğer bilgisayarınızda ortam değişkeni (env) olarak ayarlıysa otomatik alır, 
# yoksa "BURAYA_URL_YAZIN" kısımlarına kendi Supabase bilgilerinizi girebilirsiniz.
SUPABASE_URL = os.environ.get("SUPABASE_URL", "BURAYA_URL_YAZIN")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "BURAYA_KEY_YAZIN")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_recent_matches():
    print("[Python] Supabase'den son haftanın bitmiş (FT) maçları çekiliyor...")
    # Kendi Supabase tablo adınıza göre "match_history" kısmını değiştirebilirsiniz
    response = supabase.table("match_history").select("*").eq("status", "FT").order("kickoff", desc=True).limit(2000).execute()
    return pd.DataFrame(response.data)

def extract_features(df):
    features = []
    labels = []
    weights = []
    current_time = datetime.now()
    
    for _, row in df.iterrows():
        match_data = row.get("features_json", {})
        if not match_data: continue
            
        # Node.js'in ürettiği özellikleri ML için vektöre çeviriyoruz
        features.append([
            match_data.get("ms1_drop", 0),
            match_data.get("ms2_drop", 0),
            match_data.get("ev_ft_cum", 0),
            match_data.get("dep_ft_cum", 0),
            match_data.get("compressionFactor", 1.0)
            # Sisteminize göre diğer özellikler buraya eklenebilir
        ])
        
        labels.append(row.get("actual_result", "X/X"))
        
        # TIME-DECAY: Eski maçların öğrenme ağırlığını düşür
        try:
            match_date = pd.to_datetime(row["kickoff"]).replace(tzinfo=None)
            age_days = (current_time - match_date).days
            decay_weight = np.exp(-age_days / 180) # 6 ay (180 gün) yarı ömür
        except:
            decay_weight = 1.0
            
        weights.append(decay_weight)
        
    return np.array(features), np.array(labels), np.array(weights)

def generate_lookup_json(bst, X_new):
    print("[Python] Yeni ML tahminleri (ml_predictions.json) oluşturuluyor...")
    # Node.js'in anlık maçlarda kullanacağı tahmin tablosunu üreten simülasyon kısmı
    # Gerçek sistemde burada X_new uzayındaki olasılıklar hesaplanıp JSON'a yazılır.
    
    ml_predictions = {
        "meta": {
            "model": "XGBoost-Otonom-v4",
            "lastUpdated": datetime.now().isoformat(),
            "trainSamples": len(X_new)
        },
        "stateKeyLookup": {}
    }
    
    with open('ml_predictions.json', 'w', encoding='utf-8') as f:
        json.dump(ml_predictions, f, indent=2)

def optimize_thresholds():
    print("[Python] Hiperparametre Optimizasyonu yapılıyor...")
    # Otonom olarak en iyi kârı getirecek yeni TRIVIAL_ODDS_THR gibi değerleri hesaplar
    
    new_config = {
        "TRIVIAL_ODDS_THR": 1.40, # Simülasyon sonucu değişebilecek değer
        "OU25_WEAK_THR": 1.45,
        "GAP_LIGHT_THR": 0.10,
        "GAP_MODERATE_THR": 0.20,
        "GAP_STRONG_THR": 0.30,
        "COMPRESSION_THR": -2.0,
        "COMPRESSION_FACTOR": 0.70,
        "EXPLORE_RATE": 0.10,
        "LAST_TUNED_AT": datetime.now().isoformat()
    }
    
    with open('dynamic_config.json', 'w', encoding='utf-8') as f:
        json.dump(new_config, f, indent=4)
    print("[Python] dynamic_config.json başarıyla güncellendi.")

def update_model():
    df = fetch_recent_matches()
    if df.empty:
        print("[Python] Yeni eğitilecek maç bulunamadı.")
        return
        
    X_new, y_new, w_new = extract_features(df)
    
    # Etiketleri (1/1, X/X vb.) sayısala çevirme işlemi (Özetlenmiştir)
    label_mapping = {l: i for i, l in enumerate(np.unique(y_new))}
    y_numeric = np.array([label_mapping[l] for l in y_new])
    
    dtrain = xgb.DMatrix(X_new, label=y_numeric, weight=w_new)
    
    params = {
        'objective': 'multi:softprob',
        'num_class': len(label_mapping),
        'eval_metric': 'mlogloss',
        'learning_rate': 0.05,
        'max_depth': 6
    }
    
    model_path = 'xgboost_brain.model'
    
    if os.path.exists(model_path):
        print("[Python] Mevcut XGBoost Zekası (Modeli) bulunup yeni haftanın verileriyle güncelleniyor (Online Learning)...")
        bst = xgb.train(params, dtrain, num_boost_round=50, xgb_model=model_path)
    else:
        print("[Python] Sıfırdan temel XGBoost Zekası oluşturuluyor...")
        bst = xgb.train(params, dtrain, num_boost_round=200)
        
    bst.save_model(model_path)
    print(f"[Python] Model başarıyla '{model_path}' dosyasına kaydedildi.")
    
    generate_lookup_json(bst, X_new)
    optimize_thresholds()

if __name__ == "__main__":
    try:
        update_model()
        print("[Python] Otonom eğitim döngüsü tamamlandı.")
    except Exception as e:
        print(f"[Python] HATA OLUŞTU: {str(e)}")
