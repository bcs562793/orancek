"""
test_analiz.py
──────────────
Oran Analizi özelliğini web sitesi olmadan test eder.

Kullanım:
  # Lokal gz dosyasıyla:
  python test_analiz.py --file data/odds_2026-03-30.json.gz

  # GitHub'daki dosyayla (repo public olmalı):
  python test_analiz.py --github KULLANICI/REPO --date 2026-03-30

  # Maç numarası belirterek:
  python test_analiz.py --file data/odds_2026-03-30.json.gz --mac-id 4437085

  # API anahtarı olmadan (mock sonuç gösterir):
  python test_analiz.py --file data/odds_2026-03-30.json.gz --no-api
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from pathlib import Path
from urllib.request import urlopen, Request


# ─── Renkli çıktı ─────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def c(text, color): return f"{color}{text}{RESET}"


# ─── Veri yükleme ──────────────────────────────────────────────────────────────

def load_gz_local(path: str) -> list:
    with gzip.open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


def load_gz_github(repo: str, date: str) -> list:
    url = f"https://raw.githubusercontent.com/{repo}/main/data/odds_{date}.json.gz"
    print(c(f"  ↓ İndiriliyor: {url}", CYAN))
    req = Request(url, headers={"Accept-Encoding": "gzip"})
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
    # Sunucu gz döndürdüyse decompress et
    if raw[:2] == b"\x1f\x8b":
        import io
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            raw = gz.read()
    return json.loads(raw.decode("utf-8"))


# ─── Analiz ───────────────────────────────────────────────────────────────────

def normalize_markets(markets: list) -> dict:
    """markets listesini {market_name: {outcome_name: odds}} dict'e çevirir."""
    result = {}
    for m in markets:
        name = m.get("market_name", "")
        outs = {}
        for o in m.get("outcomes", []):
            if o.get("odds"):
                outs[o["name"]] = float(o["odds"])
        if outs:
            result[name] = outs
    return result


def find_similar_matches(
    all_matches: list,
    target_mac_id: int,
    tolerance: float = 0.20,
    ignore_league: bool = False,
) -> tuple[dict, list]:
    """
    Hedef maçı bul, aynı ligden benzer oranlı maçları listele.
    ignore_league=True ile test modunda lig filtresi kaldırılır.
    Gerçek kullanımda birden fazla günün gz'si all_matches içinde olur.
    """
    target = next((m for m in all_matches if m["mac_id"] == target_mac_id), None)
    if not target:
        return None, []

    target_mkts = normalize_markets(target["markets"])
    target_1x2  = target_mkts.get("Maç Sonucu", {})
    target_home = target_1x2.get("1")
    target_draw = target_1x2.get("X")
    target_away = target_1x2.get("2")

    league  = target.get("league", "")
    similar = []

    for m in all_matches:
        if m["mac_id"] == target_mac_id:
            continue
        if not ignore_league and m.get("league") != league:
            continue

        mkts  = normalize_markets(m["markets"])
        ms_1x2= mkts.get("Maç Sonucu", {})
        h = ms_1x2.get("1")
        d = ms_1x2.get("X")
        a = ms_1x2.get("2")

        if not (h and d and a):
            continue

        # Tolerans kontrolü — en az 2'si benzer aralıkta olsun
        def in_range(val, ref):
            if not val or not ref:
                return False
            return abs(val - ref) / ref <= tolerance

        score = sum([
            in_range(h, target_home),
            in_range(d, target_draw),
            in_range(a, target_away),
        ])

        if score >= 2:
            similar.append({
                "mac_id":    m["mac_id"],
                "home":      m["home_team"],
                "away":      m["away_team"],
                "markets":   mkts,
                "match_date": m.get("match_date", ""),
            })

    return target, similar


def analyze_market(similar: list, market_name: str, outcome_map: dict) -> dict | None:
    """
    similar maç listesinde belirli bir market için istatistik çıkar.
    outcome_map: { "gösterilecek etiket": ["Kazandı ise bu outcome name'ler"] }
    Örn: {"MS 1": ["1"], "X": ["X"], "MS 2": ["2"]}
    """
    counts = {k: 0 for k in outcome_map}
    total  = 0

    for m in similar:
        outs = m["markets"].get(market_name)
        if not outs:
            continue
        total += 1
        # Bu test için gerçek sonuç yok — oran değerlerine göre favori bul
        # (Gerçek kullanımda Supabase'den skor çekiliyor)
        fav_out = min(outs, key=lambda k: outs[k])  # en düşük oran = favori
        for label, names in outcome_map.items():
            if fav_out in names:
                counts[label] += 1

    if total < 3:
        return None

    results = []
    for label, cnt in counts.items():
        pct = cnt / total * 100
        results.append({"label": label, "count": cnt, "pct": pct})

    best   = max(results, key=lambda x: x["pct"])
    signal = "strong" if best["pct"] >= 65 else \
             "moderate" if best["pct"] >= 55 else \
             "weak"     if best["pct"] >= 45 else "none"

    return {
        "market": market_name,
        "total":  total,
        "results": results,
        "best":    best,
        "signal":  signal,
    }


def run_local_analysis(target: dict, similar: list) -> list:
    """API olmadan lokal istatistik çalıştır (tahmin değil, oran eğilimi)."""
    print(c(f"\n  📊 Lokal analiz (API yok) — {len(similar)} benzer maç", CYAN))
    print(c("  ⚠️  NOT: Gerçek sonuçlar yok, oran eğilimi gösteriliyor.\n", YELLOW))

    analyses = []
    checks = [
        ("Maç Sonucu",      {"MS 1": ["1"], "Beraberlik": ["X"], "MS 2": ["2"]}),
        ("Alt/Üst 2,5 Gol", {"Alt 2.5": ["ALT", "alt", "0", "1", "2"], "Üst 2.5": ["ÜST", "üst", "3+"]}),
        ("Karşılıklı Gol",  {"KG Var": ["VAR", "var"], "KG Yok": ["YOK", "yok"]}),
    ]

    for mkt_name, outcome_map in checks:
        r = analyze_market(similar, mkt_name, outcome_map)
        if r:
            analyses.append(r)

    return analyses


# ─── Claude API ───────────────────────────────────────────────────────────────

def call_claude_api(target: dict, similar: list, api_key: str) -> dict:
    """Claude API'ye gönder, JSON sonuç döndür."""

    target_mkts  = normalize_markets(target["markets"])
    target_1x2   = target_mkts.get("Maç Sonucu", {})
    target_ou    = target_mkts.get("Alt/Üst 2,5 Gol", target_mkts.get("Alt/Üst 2.5 Gol", {}))

    enriched = []
    for m in similar:
        enriched.append({
            "home":    m["home"],
            "away":    m["away"],
            "date":    m["match_date"],
            # Gerçek sonuç yok (test dosyası) — boş bırak
            "result":  None,
            "over25":  None,
            "btts":    None,
            "ms_odds": m["markets"].get("Maç Sonucu", {}),
            "ou25":    m["markets"].get("Alt/Üst 2,5 Gol",
                       m["markets"].get("Alt/Üst 2.5 Gol", {})),
        })

    prompt = f"""
Sen profesyonel bir bahis veri analistisin.

Aşağıda analiz edilecek maç ve aynı ligden benzer oranlara sahip {len(enriched)} maç verisi var.
DİKKAT: Bu test verisi olduğu için 'result', 'over25', 'btts' alanları null — sadece mevcut oran eğilimlerini analiz et.

=== ANALİZ EDİLECEK MAÇ ===
Maç: {target["home_team"]} vs {target["away_team"]}
Lig: {target["league"]}
Tarih: {target["match_date"]}  Saat: {target["match_time"]}
1x2 Oranları: {json.dumps(target_1x2, ensure_ascii=False)}
Alt/Üst 2.5: {json.dumps(target_ou, ensure_ascii=False)}

=== AYNI LİGDEN BENZER ORANLI {len(enriched)} MAÇ ===
{json.dumps(enriched[:30], ensure_ascii=False)}

=== GÖREV ===
Bu ligdeki oran profillerini incele. Ev sahibi/deplasman/beraberlik oranlarının dağılımı,
üst/alt eğilimi, lig genel karakteri hakkında yorum yap.
SADECE JSON döndür, markdown/backtick kullanma.

{{
  "summary": "2-3 cümle: bu ligde benzer oran profilinde ne görülüyor",
  "markets": [
    {{
      "name": "Maç Sonucu (MS 1/X/2)",
      "similar_count": 8,
      "analysis": "açıklama",
      "signal": "strong|moderate|weak|none",
      "signal_text": "En güçlü eğilim kısa özeti"
    }}
  ]
}}
"""

    payload = json.dumps({
        "model":      "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "messages":   [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    req = Request(
        "https://api.anthropic.com/v1/messages",
        data    = payload,
        headers = {
            "Content-Type":      "application/json",
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
        },
        method  = "POST",
    )

    with urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    raw   = next((b["text"] for b in data.get("content", []) if b["type"] == "text"), "")
    clean = raw.replace("```json", "").replace("```", "").strip()
    return json.loads(clean)


# ─── Çıktı yazdırma ───────────────────────────────────────────────────────────

SIG_COLOR = {"strong": GREEN, "moderate": YELLOW, "weak": YELLOW, "none": RESET}
SIG_LABEL = {"strong": "🔥 GÜÇLÜ", "moderate": "⚡ ORTA", "weak": "💧 ZAYIF", "none": "➖ NÖTR"}


def print_result(target: dict, result: dict, similar_count: int):
    print()
    print(c("═" * 60, CYAN))
    print(c(f"  🤖 ORAN ANALİZİ SONUCU", BOLD))
    print(c("═" * 60, CYAN))
    print(f"  Maç    : {c(target['home_team'], BOLD)} vs {c(target['away_team'], BOLD)}")
    print(f"  Lig    : {target['league']}")
    print(f"  Tarih  : {target['match_date']}  {target['match_time']}")
    print(f"  Örnek  : {similar_count} benzer maç analiz edildi")
    print()

    if result.get("summary"):
        print(c("  📝 Özet", BOLD))
        print(f"  {result['summary']}")
        print()

    for mkt in result.get("markets", []):
        sig   = mkt.get("signal", "none")
        color = SIG_COLOR[sig]
        label = SIG_LABEL[sig]
        print(c(f"  ▶ {mkt['name']}", BOLD))
        print(f"    Sinyal  : {c(label, color)}")
        if mkt.get("signal_text"):
            print(f"    Eğilim  : {c(mkt['signal_text'], color)}")
        print(f"    Analiz  : {mkt['analysis']}")
        print()

    print(c("  ⚠️  Sorumluluk Reddi: Geçmiş verilere dayalıdır, kesin sonuç garantisi vermez.", YELLOW))
    print(c("═" * 60, CYAN))


def print_local_result(target: dict, analyses: list, similar_count: int):
    print()
    print(c("═" * 60, CYAN))
    print(c("  📊 LOKAL ORAN EĞİLİM RAPORU (API YOK)", BOLD))
    print(c("═" * 60, CYAN))
    print(f"  Maç    : {c(target['home_team'], BOLD)} vs {c(target['away_team'], BOLD)}")
    print(f"  Lig    : {target['league']}")
    print(f"  Tarih  : {target['match_date']}  {target['match_time']}")
    print(f"  Örnek  : {similar_count} benzer maç")
    print()

    if not analyses:
        print(c("  Yeterli veri bulunamadı.", RED))
    else:
        for a in analyses:
            sig   = a["signal"]
            color = SIG_COLOR[sig]
            label = SIG_LABEL[sig]
            best  = a["best"]
            print(c(f"  ▶ {a['market']}", BOLD))
            print(f"    Sinyal  : {c(label, color)}")
            best_txt = f"{best['label']} — %{best['pct']:.0f} ({best['count']}/{a['total']})"
            print(f"    Sonuç   : {c(best_txt, color)}")
            print(f"    Detay   : " + "  |  ".join(
                f"{r['label']} %{r['pct']:.0f} ({r['count']})" for r in a["results"]
            ))
            print()

    print(c("═" * 60, CYAN))


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Oran Analizi Test")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--file",   metavar="PATH",  help="Lokal .json.gz dosyası")
    src.add_argument("--github", metavar="USER/REPO", help="GitHub reposu (public)")

    p.add_argument("--date",    default="", help="Tarih (--github ile zorunlu)")
    p.add_argument("--mac-id",  type=int,   help="Analiz edilecek maç mac_id'si (belirtilmezse listeler)")
    p.add_argument("--no-api",  action="store_true", help="Claude API çağırma, lokal analiz yap")
    p.add_argument("--tolerance", type=float, default=0.20, help="Oran benzerlik toleransı (default: 0.20 = %%20)")
    p.add_argument("--data-dir",  metavar="DIR", help="Geçmiş gz dosyaları klasörü (birden fazla gün)")
    p.add_argument("--ignore-league", action="store_true", help="Test: lig filtresi olmadan tüm maçlarda ara")
    args = p.parse_args()

    # ── Veri yükle ──
    print(c("\n🔄 Veri yükleniyor...", CYAN))
    try:
        if args.file:
            matches = load_gz_local(args.file)
            print(c(f"  ✅ {len(matches)} maç yüklendi ({args.file})", GREEN))
        else:
            if not args.date:
                print(c("  ❌ --github ile --date gerekli", RED)); sys.exit(1)
            matches = load_gz_github(args.github, args.date)
            print(c(f"  ✅ {len(matches)} maç yüklendi (GitHub)", GREEN))
    except Exception as e:
        print(c(f"  ❌ Yükleme hatası: {e}", RED)); sys.exit(1)

    # ── Geçmiş veri yükle (data-dir) ──
    history = []
    if args.data_dir:
        data_path = Path(args.data_dir)
        gz_files  = sorted(data_path.glob("odds_*.json.gz"))
        for gf in gz_files:
            try:
                history += load_gz_local(str(gf))
            except Exception as e:
                print(c(f"  ⚠️ {gf.name}: {e}", YELLOW))
        print(c(f"  📂 Geçmiş: {len(gz_files)} dosyadan {len(history)} ek maç yüklendi", CYAN))

    all_matches = matches + history

    # ── Maç seçimi ──
    if not args.mac_id:
        print(c("\n📋 Mevcut maçlar:", BOLD))
        for i, m in enumerate(matches[:30]):
            mkts = normalize_markets(m["markets"])
            ms   = mkts.get("Maç Sonucu", {})
            oran = f"  1:{ms.get('1','?')}  X:{ms.get('X','?')}  2:{ms.get('2','?')}" if ms else ""
            print(f"  [{i+1:2d}] mac_id={m['mac_id']}  {m['home_team']} vs {m['away_team']}  ({m['league']}){oran}")
        print()
        try:
            idx = int(input(c("  Maç numarası girin (1-30): ", YELLOW))) - 1
            target_id = matches[idx]["mac_id"]
        except (ValueError, IndexError):
            print(c("  Geçersiz seçim.", RED)); sys.exit(1)
    else:
        target_id = args.mac_id

    # ── Benzer maç bul ──
    print(c(f"\n🔍 mac_id={target_id} için benzer maçlar aranıyor (tolerans: %{args.tolerance*100:.0f})...", CYAN))
    if args.ignore_league:
        print(c("  ⚠️  --ignore-league aktif: lig filtresi kapalı", YELLOW))
    target, similar = find_similar_matches(all_matches, target_id, tolerance=args.tolerance,
                                           ignore_league=getattr(args, "ignore_league", False))

    if target is None:
        print(c(f"  ❌ mac_id={target_id} bulunamadı.", RED)); sys.exit(1)

    target_mkts = normalize_markets(target["markets"])
    ms_oran     = target_mkts.get("Maç Sonucu", {})
    print(c(f"  ✅ Hedef: {target['home_team']} vs {target['away_team']}", GREEN))
    print(f"     Lig  : {target['league']}")
    print(f"     1x2  : 1={ms_oran.get('1','?')}  X={ms_oran.get('X','?')}  2={ms_oran.get('2','?')}")
    print(f"     Aynı ligden benzer oranli: {c(str(len(similar)), BOLD)} maç bulundu")

    if not similar:
        print(c("\n⚠️  Benzer maç bulunamadı. Toleransı artırmayı dene: --tolerance 0.35", YELLOW))
        sys.exit(0)

    # ── Analiz ──
    if args.no_api:
        analyses = run_local_analysis(target, similar)
        print_local_result(target, analyses, len(similar))
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            print(c("\n❌ ANTHROPIC_API_KEY ortam değişkeni bulunamadı.", RED))
            print(   "   Ayarlamak için: export ANTHROPIC_API_KEY=sk-ant-...")
            print(   "   Ya da --no-api ile API olmadan çalıştır.")
            sys.exit(1)

        print(c(f"\n🤖 Claude API'ye gönderiliyor ({len(similar)} maç)...", CYAN))
        try:
            result = call_claude_api(target, similar, api_key)
            print_result(target, result, len(similar))
        except Exception as e:
            print(c(f"\n❌ API hatası: {e}", RED))
            print(c("   --no-api ile lokal analiz dene.", YELLOW))
            sys.exit(1)


if __name__ == "__main__":
    main()
