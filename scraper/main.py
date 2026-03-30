"""
scraper/main.py
───────────────
CLI giriş noktası. GitHub Actions workflow bu dosyayı çalıştırır.

Kullanım:
  python -m scraper.main --date 2026-03-28
  python -m scraper.main --date 2026-03-28 --dry-run
  python -m scraper.main --date 2026-03-28 --all
  python -m scraper.main --date 2026-03-28 --no-supabase   # sadece JSON
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from .mackolik import MackolikScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mackolik geçmiş iddaa oranları çekici")
    p.add_argument("--date",    default=str(date.today()), help="YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--all",     dest="all_matches", action="store_true",
                   help="İddaa filtresi kapalı")
    p.add_argument("--delay",   type=float, default=1.5)
    p.add_argument("--output-json", metavar="PATH",
                   help="Çıktıyı .json.gz olarak kaydet")
    p.add_argument("--no-supabase", action="store_true",
                   help="Supabase'e yazma (sadece JSON üret)")
    return p.parse_args()


def save_gz(path: str, matches: list) -> None:
    """JSON'u gzip ile sıkıştırarak kaydet."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # .json.gz uzantısını garantile
    if not str(out).endswith(".gz"):
        out = Path(str(out) + ".gz")

    payload = json.dumps(
        [m.to_dict() for m in matches],
        ensure_ascii=False,
        indent=None,       # sıkıştırılacak, indent gereksiz
        separators=(",", ":"),
    ).encode("utf-8")

    with gzip.open(out, "wb", compresslevel=9) as f:
        f.write(payload)

    original_kb  = len(payload) / 1024
    compressed_b = out.stat().st_size
    logger.info(
        "JSON.GZ kaydedildi: %s  (%.1f KB → %.1f KB, %.0f%% küçüldü)",
        out, original_kb, compressed_b / 1024,
        (1 - compressed_b / len(payload)) * 100,
    )


def main() -> None:
    args       = parse_args()
    started_at = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("Mackolik Scraper başlatıldı")
    logger.info("Tarih      : %s", args.date)
    logger.info("Dry-run    : %s", args.dry_run)
    logger.info("Tüm maçlar : %s", args.all_matches)
    logger.info("Supabase   : %s", not args.no_supabase)
    logger.info("Gecikme    : %.1f s", args.delay)
    logger.info("=" * 60)

    scraper = MackolikScraper(request_delay=args.delay)
    matches, scrape_errors = scraper.scrape_date(
        date=args.date,
        iddaa_only=not args.all_matches,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("DRY RUN tamamlandı.")
        sys.exit(0)

    if not matches:
        logger.warning("Kaydedilecek maç yok.")
        sys.exit(0)

    # ── JSON.GZ çıktı ──────────────────────────────────────────────
    if args.output_json:
        save_gz(args.output_json, matches)

    # ── Supabase ───────────────────────────────────────────────────
    if args.no_supabase:
        logger.info("--no-supabase: Supabase adımı atlandı.")
        logger.info("Tamamlandı: %d maç, %d hata.", len(matches), len(scrape_errors))
        sys.exit(1 if scrape_errors else 0)

    from .db import get_client, upsert_all, log_scrape_run

    logger.info("Supabase'e yazılıyor...")
    try:
        client = get_client()
    except Exception as exc:
        logger.error("Supabase bağlantısı kurulamadı: %s", exc)
        logger.error("SUPABASE_URL ve SUPABASE_SERVICE_KEY secret'larını kontrol et.")
        sys.exit(1)

    stats = upsert_all(client, matches)
    log_scrape_run(
        client=client,
        scrape_date=args.date,
        stats=stats,
        errors=scrape_errors,
        started_at=started_at,
    )

    logger.info("=" * 60)
    logger.info("ÖZET")
    logger.info("  Toplam maç     : %d", stats["total"])
    logger.info("  Kaydedilen maç : %d", stats["match_rows"])
    logger.info("  Kaydedilen oran: %d", stats["odds_rows"])
    logger.info("  Hata           : %d", stats["errors"] + len(scrape_errors))
    logger.info("=" * 60)

    if stats["errors"] > 0 or scrape_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
