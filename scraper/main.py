"""
scraper/main.py
───────────────
CLI giriş noktası.

Modlar:
  --mode combined   Sofascore + Mackolik (default)
  --mode sofa       Sadece Sofascore
  --mode mac        Sadece Mackolik

Kullanım:
  python -m scraper.main --date 2026-03-29 --no-supabase
  python -m scraper.main --date 2026-03-29 --mode sofa --no-supabase
  python -m scraper.main --date 2026-03-29 --sofa-all-markets
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mackolik + Sofascore oranları")
    p.add_argument("--date",    default=str(date.today()))
    p.add_argument("--mode",    choices=["combined", "sofa", "mac"], default="combined")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--all",     dest="all_matches", action="store_true",
                   help="Mac: iddaa filtresi kapalı")
    p.add_argument("--sofa-all-markets", action="store_true",
                   help="Sofascore: her maç için /odds/1/all çek (yavaş)")
    p.add_argument("--sofa-delay",  type=float, default=0.5)
    p.add_argument("--mac-delay",   type=float, default=1.5)
    p.add_argument("--match-threshold", type=float, default=0.5,
                   help="Takım adı eşleştirme eşiği (0-1)")
    p.add_argument("--output-json", metavar="PATH")
    p.add_argument("--no-supabase", action="store_true")
    return p.parse_args()


def save_gz(path: str, data: list) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if not str(out).endswith(".gz"):
        out = Path(str(out) + ".gz")

    payload = json.dumps(
        data, ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")

    with gzip.open(out, "wb", compresslevel=9) as f:
        f.write(payload)

    logger.info(
        "JSON.GZ kaydedildi: %s  (%.1f KB → %.1f KB, %.0f%% küçüldü)",
        out, len(payload) / 1024, out.stat().st_size / 1024,
        (1 - out.stat().st_size / len(payload)) * 100,
    )


def main() -> None:
    args       = parse_args()
    started_at = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("Tarih  : %s  |  Mod: %s", args.date, args.mode)
    logger.info("=" * 60)

    results = []
    stats   = {}

    # ── Combined mod ──────────────────────────────────────────────────────────
    if args.mode == "combined":
        from .pipeline import CombinedPipeline

        pipeline = CombinedPipeline(
            sofa_delay=args.sofa_delay,
            mac_delay=args.mac_delay,
            fetch_sofa_all=args.sofa_all_markets,
            match_threshold=args.match_threshold,
        )

        if args.dry_run:
            # Dry run: sadece listele
            from .sofascore import SofascoreScraper
            from .mackolik import fetch_listings, MackolikSession
            sofa_matches = SofascoreScraper(args.sofa_delay).fetch_scheduled_events(args.date)
            mac_listings = fetch_listings(MackolikSession(args.mac_delay), args.date)
            from .matcher import match_events
            pairs, u_sofa, u_mac = match_events(sofa_matches, mac_listings, args.match_threshold)
            logger.info("[DRY RUN] Eşleşen: %d | Sadece Sofa: %d | Sadece Mac: %d",
                        len(pairs), len(u_sofa), len(u_mac))
            sys.exit(0)

        merged, stats = pipeline.run(args.date)
        results = [m.to_dict() for m in merged]

    # ── Sadece Sofascore ──────────────────────────────────────────────────────
    elif args.mode == "sofa":
        from .sofascore import SofascoreScraper

        scraper = SofascoreScraper(request_delay=args.sofa_delay)

        if args.dry_run:
            matches = scraper.fetch_scheduled_events(args.date)
            logger.info("[DRY RUN] Sofascore: %d maç", len(matches))
            sys.exit(0)

        matches = scraper.scrape_date(args.date, fetch_all_markets=args.sofa_all_markets)
        results = [m.to_dict() for m in matches]
        stats   = {"sofa_total": len(matches)}

    # ── Sadece Mackolik ───────────────────────────────────────────────────────
    elif args.mode == "mac":
        from .mackolik import MackolikScraper

        scraper = MackolikScraper(request_delay=args.mac_delay)
        matches, errors = scraper.scrape_date(
            date=args.date,
            iddaa_only=not args.all_matches,
            dry_run=args.dry_run,
        )
        if args.dry_run:
            sys.exit(0)
        results = [m.to_dict() for m in matches]
        stats   = {"mac_total": len(matches), "errors": len(errors)}

    if not results:
        logger.warning("Sonuç yok.")
        sys.exit(0)

    # ── JSON.GZ çıktı ─────────────────────────────────────────────────────────
    if args.output_json:
        save_gz(args.output_json, results)

    # ── Özet ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ÖZET  —  %d maç kaydedildi", len(results))
    for k, v in stats.items():
        if k != "errors":
            logger.info("  %-20s: %s", k, v)
    logger.info("=" * 60)

    if args.no_supabase:
        sys.exit(0)

    # ── Supabase ──────────────────────────────────────────────────────────────
    from .db import get_client, upsert_combined, log_scrape_run
    logger.info("Supabase'e yazılıyor...")
    try:
        client = get_client()
    except Exception as exc:
        logger.error("Supabase bağlanamadı: %s", exc)
        sys.exit(1)

    db_stats = upsert_combined(client, results, args.date)
    log_scrape_run(client, args.date, {**stats, **db_stats}, [], started_at)

    logger.info("Supabase: %d maç, %d market yazıldı.", db_stats.get("matches", 0), db_stats.get("markets", 0))


if __name__ == "__main__":
    main()
