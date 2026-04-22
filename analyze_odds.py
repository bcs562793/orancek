"""
╔══════════════════════════════════════════════════════════════════╗
║           ODDS TRACKER CACHE — DEEP ANALYSIS ENGINE             ║
║   Benzer oran kümeleme | Hareket korelasyonu | AI tahmin skoru  ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import os
import sys
import math
import logging
import argparse
import time
from datetime import datetime
from collections import defaultdict, Counter
from itertools import combinations
from pathlib import Path
from typing import Iterator, Any

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("odds_analysis.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("OddsAnalyzer")

# ─── Config ───────────────────────────────────────────────────────────────────
ODDS_SIMILARITY_THRESHOLD = 0.05      # %5 fark → "benzer oran" sayılır
MOVEMENT_ALERT_THRESHOLD  = 0.10      # %10'dan büyük hareket → anlamlı
MIN_CLUSTER_SIZE          = 3          # küme için minimum maç sayısı
TOP_N_PATTERNS            = 20         # raporlarda gösterilecek en iyi N
CHUNK_REPORT_EVERY        = 10_000    # her N maçta bir ara log


# ══════════════════════════════════════════════════════════════════════════════
# 1. STREAMING JSON PARSER  (1M+ satır için bellek dostu)
# ══════════════════════════════════════════════════════════════════════════════
def stream_matches(filepath: str) -> Iterator[tuple[str, dict]]:
    """
    __tracker_cache.json formatını akışlı okur.
    Desteklenen yapılar:
      A) {"match_id": { ...match_data... }, ...}          ← dict-of-matches
      B) [{ "id": "...", ...match_data... }, ...]          ← list-of-matches
    """
    path = Path(filepath)
    if not path.exists():
        log.error(f"Dosya bulunamadı: {filepath}")
        sys.exit(1)

    size_mb = path.stat().st_size / 1_048_576
    log.info(f"📂 Dosya: {filepath}  ({size_mb:.1f} MB)")

    try:
        import ijson  # pip install ijson — büyük dosyalar için
        yield from _stream_with_ijson(filepath)
    except ImportError:
        log.warning("ijson kurulu değil → standart json.load kullanılıyor (RAM yoğun)")
        yield from _stream_standard(filepath)


def _stream_with_ijson(filepath: str) -> Iterator[tuple[str, dict]]:
    import ijson
    with open(filepath, "rb") as f:
        # Önce üst seviye yapıyı anla
        first_byte = f.read(1)
        f.seek(0)
        if first_byte == b"{":
            # dict-of-matches
            for match_id, data in ijson.kvitems(f, ""):
                if isinstance(data, dict):
                    yield str(match_id), data
        else:
            # list-of-matches
            for item in ijson.items(f, "item"):
                match_id = item.get("id") or item.get("match_id") or item.get("fixture_id", "unknown")
                yield str(match_id), item


def _stream_standard(filepath: str) -> Iterator[tuple[str, dict]]:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, dict):
                yield k, v
    elif isinstance(data, list):
        for item in data:
            mid = item.get("id") or item.get("match_id") or "unknown"
            yield str(mid), item


# ══════════════════════════════════════════════════════════════════════════════
# 2. MATCH PARSER  — ham veriyi normalize eder
# ══════════════════════════════════════════════════════════════════════════════
class Match:
    __slots__ = ("id", "home", "away", "league", "date",
                 "opening", "current", "history", "result")

    def __init__(self, match_id: str, raw: dict):
        self.id      = match_id
        self.home    = raw.get("home") or raw.get("home_team") or raw.get("homeTeam", "?")
        self.away    = raw.get("away") or raw.get("away_team") or raw.get("awayTeam", "?")
        self.league  = raw.get("league") or raw.get("competition") or raw.get("leagueName", "?")
        self.date    = raw.get("date") or raw.get("datetime") or raw.get("kickoff", "")
        self.result  = self._parse_result(raw)
        self.opening, self.current, self.history = self._parse_odds(raw)

    # ── result ────────────────────────────────────────────────────────────────
    @staticmethod
    def _parse_result(raw: dict) -> str | None:
        for key in ("result", "outcome", "ft_result", "full_time_result"):
            val = raw.get(key)
            if val:
                return str(val).upper().strip()
        score = raw.get("score") or raw.get("ft_score") or raw.get("fullTimeScore")
        if score and isinstance(score, dict):
            h = score.get("home", score.get("H", score.get("homeScore")))
            a = score.get("away", score.get("A", score.get("awayScore")))
            if h is not None and a is not None:
                h, a = int(h), int(a)
                return "1" if h > a else ("2" if a > h else "X")
        return None

    # ── odds ──────────────────────────────────────────────────────────────────
    @staticmethod
    def _parse_odds(raw: dict) -> tuple[dict, dict, list[dict]]:
        """
        Farklı format varyantlarından 1/X/2 oranlarını çıkarır.
        history: [{"ts": ..., "1": ..., "X": ..., "2": ...}, ...]
        """
        def normalise(o: Any) -> dict | None:
            if not o:
                return None
            if isinstance(o, dict):
                h = _get_first(o, ["1", "home", "H", "win1"])
                d = _get_first(o, ["X", "draw", "D"])
                a = _get_first(o, ["2", "away", "A", "win2"])
                if h is not None and d is not None and a is not None:
                    return {"1": float(h), "X": float(d), "2": float(a)}
            return None

        # Tarihçe
        history_raw = (raw.get("odds_history") or raw.get("history") or
                       raw.get("oddsHistory") or raw.get("movements") or [])
        history = []
        for entry in history_raw:
            norm = normalise(entry)
            if norm:
                norm["ts"] = entry.get("ts") or entry.get("timestamp") or entry.get("time") or ""
                history.append(norm)

        # Açılış & güncel
        opening = (normalise(raw.get("opening_odds") or raw.get("openingOdds") or
                              raw.get("open") or (history[0] if history else None)))
        current = (normalise(raw.get("current_odds") or raw.get("currentOdds") or
                              raw.get("odds") or raw.get("close") or
                              (history[-1] if history else None)))

        return opening or {}, current or {}, history

    # ── helpers ───────────────────────────────────────────────────────────────
    def is_valid(self) -> bool:
        return bool(self.current.get("1") and self.current.get("X") and self.current.get("2"))

    def movement(self) -> dict:
        """Her sonuç için açılış→kapanış % değişim."""
        if not self.opening or not self.current:
            return {}
        out = {}
        for k in ("1", "X", "2"):
            o, c = self.opening.get(k), self.current.get(k)
            if o and c and o != 0:
                out[k] = round((c - o) / o * 100, 2)
        return out

    def implied_prob(self, odds_dict: dict | None = None) -> dict:
        """Pazar marjını gidererek normalize edilmiş olasılıklar."""
        d = odds_dict or self.current
        if not d:
            return {}
        raw_probs = {k: 1 / d[k] for k in ("1", "X", "2") if d.get(k)}
        total = sum(raw_probs.values())
        if total == 0:
            return {}
        return {k: round(v / total, 4) for k, v in raw_probs.items()}

    def __repr__(self):
        return f"Match({self.id}: {self.home} vs {self.away})"


def _get_first(d: dict, keys: list) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except (ValueError, TypeError):
                pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# 3. ODDS SIMILARITY CLUSTERING
# ══════════════════════════════════════════════════════════════════════════════
def odds_bucket(match: Match, step: float = 0.05) -> tuple:
    """Oranları %5 aralıklara yuvarlar → benzer maçlar aynı bucket'a düşer."""
    c = match.current
    def bucket(v):
        if not v:
            return 0
        return round(round(v / step) * step, 2)
    return (bucket(c.get("1")), bucket(c.get("X")), bucket(c.get("2")))


def build_similarity_clusters(matches: list["Match"]) -> dict[tuple, list["Match"]]:
    clusters = defaultdict(list)
    for m in matches:
        if m.is_valid():
            clusters[odds_bucket(m)].append(m)
    return {k: v for k, v in clusters.items() if len(v) >= MIN_CLUSTER_SIZE}


def cluster_win_rates(cluster: list["Match"]) -> dict:
    """Bir kümedeki geçmiş maçların kazanma oranlarını hesaplar."""
    counts = Counter(m.result for m in cluster if m.result in ("1", "X", "2"))
    total = sum(counts.values())
    if total == 0:
        return {}
    return {k: round(counts[k] / total * 100, 1) for k in ("1", "X", "2")}


# ══════════════════════════════════════════════════════════════════════════════
# 4. MOVEMENT PATTERN ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
class MovementPattern:
    """
    Oran hareketini sınıflandırır:
      steam_1  → ev sahibine güçlü para girişi (oran düştü)
      steam_2  → deplasmana güçlü para girişi
      reverse  → başta giden sonra dönen hareket
      stable   → oran sabit kaldı
    """

    @staticmethod
    def classify(match: Match) -> str:
        mv = match.movement()
        if not mv:
            return "no_data"
        d1, dX, d2 = mv.get("1", 0), mv.get("X", 0), mv.get("2", 0)
        t = MOVEMENT_ALERT_THRESHOLD * 100  # % cinsinden

        if d1 < -t and abs(d1) > abs(d2):
            return "steam_1"
        if d2 < -t and abs(d2) > abs(d1):
            return "steam_2"
        if dX < -t:
            return "steam_X"
        if d1 > t and d2 < -t / 2:
            return "reverse_to_2"
        if d2 > t and d1 < -t / 2:
            return "reverse_to_1"
        if max(abs(d1), abs(dX), abs(d2)) < 2:
            return "stable"
        return "mixed"

    @staticmethod
    def outcome_after_pattern(matches: list["Match"]) -> dict:
        """Her pattern için sonuç dağılımı."""
        patt_outcomes: dict[str, Counter] = defaultdict(Counter)
        for m in matches:
            if m.result:
                p = MovementPattern.classify(m)
                patt_outcomes[p][m.result] += 1

        report = {}
        for patt, counts in patt_outcomes.items():
            total = sum(counts.values())
            report[patt] = {
                "total": total,
                "rates": {k: round(counts[k] / total * 100, 1) for k in counts},
            }
        return report


# ══════════════════════════════════════════════════════════════════════════════
# 5. AI PREDICTION SCORING
# ══════════════════════════════════════════════════════════════════════════════
def compute_prediction_score(match: Match,
                              clusters: dict[tuple, list["Match"]]) -> dict:
    """
    0–100 arası AI tahmin skoru üretir.
    Bileşenler:
      - implied_prob_score   : pazar tarafından ima edilen olasılık
      - cluster_history_score: benzer oran geçmişi
      - movement_score       : akıllı para yönü
    """
    if not match.is_valid():
        return {}

    ip = match.implied_prob()
    mv = match.movement()
    cluster_key = odds_bucket(match)
    cluster = clusters.get(cluster_key, [])

    scores = {}
    for outcome in ("1", "X", "2"):
        base = ip.get(outcome, 0) * 100  # 0–100

        # Cluster düzeltmesi
        hist_rate = 0.0
        if cluster:
            wr = cluster_win_rates(cluster)
            hist_rate = wr.get(outcome, 0)
            cluster_adj = (hist_rate - base) * 0.3  # %30 ağırlık
        else:
            cluster_adj = 0

        # Hareket düzeltmesi
        move_val = mv.get(outcome, 0)
        if move_val < -MOVEMENT_ALERT_THRESHOLD * 100:     # para girişi → oran düştü
            move_adj = abs(move_val) * 0.15
        elif move_val > MOVEMENT_ALERT_THRESHOLD * 100:    # para çıkışı → oran yükseldi
            move_adj = -move_val * 0.10
        else:
            move_adj = 0

        score = max(0.0, min(100.0, base + cluster_adj + move_adj))
        scores[outcome] = {
            "score": round(score, 2),
            "implied_prob": round(ip.get(outcome, 0) * 100, 2),
            "cluster_hist_rate": round(hist_rate, 2),
            "movement_pct": round(move_val, 2),
        }

    best = max(scores, key=lambda k: scores[k]["score"])
    scores["recommendation"] = best
    scores["confidence"] = round(scores[best]["score"] - sorted(
        [scores[k]["score"] for k in ("1", "X", "2") if k != best], reverse=True)[0], 2)
    return scores


# ══════════════════════════════════════════════════════════════════════════════
# 6. MAIN ANALYSIS PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def run_analysis(filepath: str, output_dir: str = "reports"):
    os.makedirs(output_dir, exist_ok=True)
    start_ts = time.time()

    # ── Pass 1: veri toplama ──────────────────────────────────────────────────
    log.info("═" * 60)
    log.info("PASS 1 — Cache okuma & normalize")
    log.info("═" * 60)

    all_matches: list[Match] = []
    invalid_count = 0
    total_read = 0

    for match_id, raw in stream_matches(filepath):
        total_read += 1
        try:
            m = Match(match_id, raw)
            if m.is_valid():
                all_matches.append(m)
            else:
                invalid_count += 1
        except Exception as e:
            invalid_count += 1
            if invalid_count <= 5:
                log.debug(f"Parse hatası [{match_id}]: {e}")

        if total_read % CHUNK_REPORT_EVERY == 0:
            log.info(f"  ↳ {total_read:,} satır okundu | "
                     f"geçerli: {len(all_matches):,} | geçersiz: {invalid_count:,}")

    log.info(f"✅ Toplam okundu  : {total_read:,}")
    log.info(f"✅ Geçerli maç    : {len(all_matches):,}")
    log.info(f"⚠️  Geçersiz/eksik : {invalid_count:,}")

    if not all_matches:
        log.error("Geçerli maç bulunamadı. Cache formatını kontrol edin.")
        sys.exit(1)

    # ── Pass 2: kümeleme ──────────────────────────────────────────────────────
    log.info("\n" + "═" * 60)
    log.info("PASS 2 — Benzer oran kümeleme")
    log.info("═" * 60)

    clusters = build_similarity_clusters(all_matches)
    log.info(f"📦 Toplam küme sayısı  : {len(clusters):,}")
    clustered_matches = sum(len(v) for v in clusters.values())
    log.info(f"📦 Kümedeki maç sayısı : {clustered_matches:,} "
             f"({clustered_matches / len(all_matches) * 100:.1f}%)")

    # En büyük kümeler
    log.info("\n▸ En büyük 10 oran kümesi:")
    for bucket, mlist in sorted(clusters.items(), key=lambda x: -len(x[1]))[:10]:
        wr = cluster_win_rates(mlist)
        log.info(f"  1={bucket[0]:.2f} X={bucket[1]:.2f} 2={bucket[2]:.2f} "
                 f"| {len(mlist):>4} maç "
                 f"| 1:{wr.get('1',0):.0f}% X:{wr.get('X',0):.0f}% 2:{wr.get('2',0):.0f}%")

    # ── Pass 3: hareket analizi ───────────────────────────────────────────────
    log.info("\n" + "═" * 60)
    log.info("PASS 3 — Oran hareketi & pattern analizi")
    log.info("═" * 60)

    pattern_outcomes = MovementPattern.outcome_after_pattern(all_matches)

    log.info("▸ Pattern → Sonuç korelasyonu:")
    log.info(f"  {'Pattern':<18} {'Toplam':>7}  {'1%':>6}  {'X%':>6}  {'2%':>6}")
    log.info("  " + "─" * 50)
    for patt, info in sorted(pattern_outcomes.items(), key=lambda x: -x[1]["total"]):
        r = info["rates"]
        log.info(f"  {patt:<18} {info['total']:>7,}  "
                 f"{r.get('1', 0):>5.1f}%  {r.get('X', 0):>5.1f}%  {r.get('2', 0):>5.1f}%")

    # Steam hareketlerinin doğruluk analizi
    steam_patterns = [p for p in pattern_outcomes if p.startswith("steam")]
    log.info(f"\n▸ Steam hareketleri toplam: {len(steam_patterns)} pattern")
    for sp in steam_patterns:
        d = pattern_outcomes[sp]
        predicted = sp.replace("steam_", "")
        acc = d["rates"].get(predicted, 0)
        log.info(f"  {sp}: tahmin doğruluğu → %{acc:.1f}  (n={d['total']:,})")

    # ── Pass 4: AI skor hesaplama ─────────────────────────────────────────────
    log.info("\n" + "═" * 60)
    log.info("PASS 4 — AI Prediction Score hesaplama")
    log.info("═" * 60)

    scored = []
    for m in all_matches:
        ps = compute_prediction_score(m, clusters)
        if ps:
            scored.append((m, ps))

    # Yüksek güven skorları
    high_confidence = [(m, ps) for m, ps in scored
                       if ps.get("confidence", 0) >= 15]
    log.info(f"🎯 Toplam skorlanan maç  : {len(scored):,}")
    log.info(f"🎯 Yüksek güven (≥15 pt) : {len(high_confidence):,} "
             f"({len(high_confidence)/max(len(scored),1)*100:.1f}%)")

    # Sonuçlu maçlarda doğruluk testi
    correct, tested = 0, 0
    outcome_accuracy = Counter()
    outcome_tested   = Counter()
    for m, ps in scored:
        if m.result and m.result in ("1", "X", "2"):
            tested += 1
            rec = ps.get("recommendation")
            outcome_tested[rec] += 1
            if rec == m.result:
                correct += 1
                outcome_accuracy[rec] += 1

    if tested:
        acc = correct / tested * 100
        log.info(f"\n📊 Geçmiş maçlarda AI doğruluk oranı: %{acc:.2f} (n={tested:,})")
        log.info("  Sonuç bazlı doğruluk:")
        for out in ("1", "X", "2"):
            n = outcome_tested[out]
            c = outcome_accuracy[out]
            log.info(f"    {out}: %{c/max(n,1)*100:.1f} ({c}/{n})")

    # ── Pass 5: En güçlü patternlar ──────────────────────────────────────────
    log.info("\n" + "═" * 60)
    log.info("PASS 5 — Prediction modeli için en güçlü sinyaller")
    log.info("═" * 60)

    # Oran aralığı bazlı kazanma oranı analizi
    odds_range_analysis(all_matches, log)

    # ── Rapor çıktısı ─────────────────────────────────────────────────────────
    report = build_json_report(all_matches, clusters, pattern_outcomes, scored, tested, correct)
    report_path = os.path.join(output_dir, "analysis_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start_ts
    log.info("\n" + "═" * 60)
    log.info(f"✅ Analiz tamamlandı  — {elapsed:.1f} saniye")
    log.info(f"📄 Rapor kaydedildi  : {report_path}")
    log.info("═" * 60)

    return report


# ══════════════════════════════════════════════════════════════════════════════
# 7. YARDIMCI ANALİZ FONKSİYONLARI
# ══════════════════════════════════════════════════════════════════════════════
def odds_range_analysis(matches: list["Match"], logger):
    """
    Oran aralıklarına göre sonuç dağılımı.
    Örn: ev sahibi 1.50–1.70 arasındayken kazanma oranı nedir?
    """
    ranges = [
        (1.00, 1.30, "1.00–1.30"),
        (1.30, 1.60, "1.30–1.60"),
        (1.60, 2.00, "1.60–2.00"),
        (2.00, 2.50, "2.00–2.50"),
        (2.50, 3.50, "2.50–3.50"),
        (3.50, 5.00, "3.50–5.00"),
        (5.00, 999,  "5.00+"),
    ]

    logger.info("\n▸ Ev sahibi oranı aralığı → kazanma oranı:")
    logger.info(f"  {'Aralık':<12} {'Maç':>6}  {'1%':>6}  {'X%':>6}  {'2%':>6}  {'EV(1)':>7}")
    logger.info("  " + "─" * 55)
    for lo, hi, label in ranges:
        bucket_matches = [m for m in matches
                          if m.current.get("1") and lo <= m.current["1"] < hi
                          and m.result in ("1", "X", "2")]
        if not bucket_matches:
            continue
        total = len(bucket_matches)
        counts = Counter(m.result for m in bucket_matches)
        r1 = counts.get("1", 0) / total * 100
        rX = counts.get("X", 0) / total * 100
        r2 = counts.get("2", 0) / total * 100
        # Beklenen değer (EV) ev sahibi için
        avg_odds = sum(m.current["1"] for m in bucket_matches) / total
        ev = (r1 / 100) * avg_odds - 1
        logger.info(f"  {label:<12} {total:>6,}  {r1:>5.1f}%  {rX:>5.1f}%  {r2:>5.1f}%  "
                    f"{'+'if ev>=0 else ''}{ev:>5.3f}")


def build_json_report(matches, clusters, pattern_outcomes, scored, tested, correct) -> dict:
    """Machine-readable JSON raporu — CI artifact olarak saklanır."""
    cluster_summary = []
    for bucket, mlist in sorted(clusters.items(), key=lambda x: -len(x[1]))[:50]:
        wr = cluster_win_rates(mlist)
        cluster_summary.append({
            "odds_1": bucket[0], "odds_X": bucket[1], "odds_2": bucket[2],
            "count": len(mlist),
            "win_rates": wr,
        })

    top_scored = []
    for m, ps in sorted(scored, key=lambda x: -x[1].get("confidence", 0))[:TOP_N_PATTERNS]:
        top_scored.append({
            "match_id": m.id,
            "home": m.home,
            "away": m.away,
            "league": m.league,
            "current_odds": m.current,
            "movement": m.movement(),
            "prediction": ps,
        })

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "total_matches": len(matches),
            "total_clusters": len(clusters),
            "ai_accuracy_pct": round(correct / max(tested, 1) * 100, 2),
            "tested_matches": tested,
        },
        "top_clusters": cluster_summary,
        "movement_patterns": pattern_outcomes,
        "top_predictions": top_scored,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 8. CLI
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Odds Tracker Cache Analyzer")
    parser.add_argument("--cache", default="__tracker_cache.json",
                        help="Cache dosya yolu (varsayılan: __tracker_cache.json)")
    parser.add_argument("--output", default="reports",
                        help="Çıktı dizini (varsayılan: reports/)")
    args = parser.parse_args()

    run_analysis(args.cache, args.output)
