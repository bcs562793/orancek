"""
scraper/main.py
───────────────
CLI giriş noktası. GitHub Actions workflow bu dosyayı çalıştırır.

Kullanım:
  python -m scraper.main --date 2026-03-28
  python -m scraper.main --date 2026-03-28 --dry-run
  python -m scraper.main --date 2026-03-28 --all          # iddaa filtresi kapalı
  python -m scraper.main --date 2026-03-28 --delay 2.0
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone

from .mackolik import MackolikScraper
from .db import get_client, upsert_all, log_scrape_run

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mackolik geçmiş iddaa oranları çekici")
    p.add_argument(
        "--date",
        default=str(date.today()),
        help="Tarih (YYYY-MM-DD). Default: bugün.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Supabase'e kaydetmeden sadece listing çek ve say.",
    )
    p.add_argument(
        "--all",
        dest="all_matches",
        action="store_true",
        help="İddaa filtresi olmadan tüm maçları çek.",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="İstekler arası bekleme süresi (saniye). Default: 1.5",
    )
    p.add_argument(
        "--output-json",
        metavar="PATH",
        help="Ek olarak JSON dosyasına da kaydet.",
    )
    return p.parse_args()


# ─── Entrypoint ───────────────────────────────────────────────────────────────

def main() -> None:
    args       = parse_args()
    started_at = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("Mackolik Scraper başlatıldı")
    logger.info("Tarih     : %s", args.date)
    logger.info("Dry-run   : %s", args.dry_run)
    logger.info("Tüm maçlar: %s", args.all_matches)
    logger.info("Gecikme   : %.1f s", args.delay)
    logger.info("=" * 60)

    # ── Scrape ──
    scraper = MackolikScraper(request_delay=args.delay)
    matches, scrape_errors = scraper.scrape_date(
        date=args.date,
        iddaa_only=not args.all_matches,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("DRY RUN tamamlandı. Supabase'e veri yazılmadı.")
        sys.exit(0)

    if not matches:
        logger.warning("Kaydedilecek maç yok.")
        sys.exit(0)

    # ── JSON çıktı (opsiyonel) ──
    if args.output_json:
        try:
            with open(args.output_json, "w", encoding="utf-8") as f:
                json.dump(
                    [m.to_dict() for m in matches],
                    f, ensure_ascii=False, indent=2
                )
            logger.info("JSON kaydedildi: %s", args.output_json)
        except OSError as exc:
            logger.warning("JSON kaydedilemedi: %s", exc)

    # ── Supabase kayıt ──
    logger.info("Supabase'e yazılıyor...")
    client = get_client()
    stats  = upsert_all(client, matches)

    log_scrape_run(
        client=client,
        scrape_date=args.date,
        stats=stats,
        errors=scrape_errors,
        started_at=started_at,
    )

    # ── Özet ──
    logger.info("=" * 60)
    logger.info("ÖZET")
    logger.info("  Toplam maç     : %d", stats["total"])
    logger.info("  Kaydedilen maç : %d", stats["match_rows"])
    logger.info("  Kaydedilen oran: %d", stats["odds_rows"])
    logger.info("  Hata           : %d", stats["errors"] + len(scrape_errors))
    logger.info("=" * 60)

    # Hata varsa CI'da görünür olsun
    if stats["errors"] > 0 or scrape_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
