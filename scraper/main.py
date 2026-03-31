"""
scraper/main.py
───────────────
CLI giriş noktası.

Modlar:
  --mode combined   Sofascore + Mackolik (default)
  --mode sofa       Sadece Sofascore
  --mode mac        Sadece Mackolik

Tek gün:
  python -m scraper.main --date 2026-03-29 --no-supabase

Tarih aralığı (tek birleşik .json.gz):
  python -m scraper.main --date-from 2026-03-25 --date-to 2026-03-29 --no-supabase

Tarih aralığı (her gün ayrı .json.gz):
  python -m scraper.main --date-from 2026-03-25 --date-to 2026-03-29 --split --no-supabase

Çıktı yolu (otomatik .gz eklenir yoksa):
  --output-json data/odds_{date}.json.gz       # {date} → YYYY-MM-DD ile değiştirilir
  --output-json data/odds_{from}_{to}.json.gz  # {from}/{to} → başlangıç/bitiş tarihi
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ─── Yardımcılar ──────────────────────────────────────────────────────────────

def date_range(start: str, end: str) -> list[str]:
    """start..end (dahil) arasındaki tarihleri YYYY-MM-DD listesi olarak döndür."""
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end,   "%Y-%m-%d").date()
    if s > e:
        raise ValueError(f"--date-from ({start}) --date-to ({end})'dan büyük olamaz")
    days, cur = [], s
    while cur <= e:
        days.append(str(cur))
        cur += timedelta(days=1)
    return days


def resolve_output_path(template: str, *, date_str: str = "", from_str: str = "", to_str: str = "") -> Path:
    """Template içindeki {date}/{from}/{to} yer tutucularını doldur."""
    path = (template
            .replace("{date}", date_str or from_str)
            .replace("{from}", from_str)
            .replace("{to}",   to_str))
    p = Path(path)
    return p if p.suffix == ".gz" else Path(str(p) + ".gz")


def save_gz(path: Path, data: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(path, "wb", compresslevel=9) as f:
        f.write(payload)
    raw_kb  = len(payload) / 1024
    gz_kb   = path.stat().st_size / 1024
    logger.info(
        "JSON.GZ kaydedildi: %s  (%d maç | %.1f KB ham → %.1f KB sıkıştırılmış | %.0f%% küçüldü)",
        path, len(data), raw_kb, gz_kb,
        (1 - gz_kb / max(raw_kb, 0.001)) * 100,
    )


# ─── Argümanlar ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mackolik + Sofascore oranları",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Tarih — tek gün VEYA aralık
    date_grp = p.add_mutually_exclusive_group()
    date_grp.add_argument(
        "--date", metavar="YYYY-MM-DD",
        help="Tek gün (varsayılan: bugün)",
    )
    date_grp.add_argument(
        "--date-from", metavar="YYYY-MM-DD",
        help="Tarih aralığı başlangıcı",
    )
    p.add_argument(
        "--date-to", metavar="YYYY-MM-DD",
        help="Tarih aralığı bitişi (--date-from ile kullanılır, varsayılan: bugün)",
    )

    # Aralık modunda çıktı şekli
    p.add_argument(
        "--split", action="store_true",
        help=(
            "Aralık modunda her gün için ayrı .json.gz üret.\n"
            "Varsayılan: tüm günleri tek .json.gz'de birleştir."
        ),
    )

    p.add_argument("--mode",    choices=["combined", "sofa", "mac"], default="combined")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--all",     dest="all_matches", action="store_true",
                   help="Mac: iddaa filtresi kapalı")
    p.add_argument("--sofa-all-markets", action="store_true",
                   help="Sofascore: her maç için /odds/1/all çek (yavaş)")
    p.add_argument("--sofa-delay",  type=float, default=0.5)
    p.add_argument("--mac-delay",   type=float, default=1.5)
    p.add_argument(
        "--output-json", metavar="PATH",
        help=(
            "Çıktı dosyası yolu. Yer tutucular:\n"
            "  {date}  → tek gün veya --date-from değeri\n"
            "  {from}  → --date-from\n"
            "  {to}    → --date-to\n"
            "Örnek: data/odds_{from}_{to}.json.gz"
        ),
    )
    p.add_argument("--no-supabase", action="store_true")
    return p.parse_args()


# ─── Tek gün işleme ───────────────────────────────────────────────────────────

def process_date(args: argparse.Namespace, target_date: str) -> list[dict]:
    """Bir günü işleyip dict listesi döndür."""
    if args.mode == "combined":
        from .pipeline import CombinedPipeline
        pipeline = CombinedPipeline(
            sofa_delay=args.sofa_delay,
            mac_delay=args.mac_delay,
            fetch_sofa_all=args.sofa_all_markets,
        )
        if args.dry_run:
            from .mackolik import fetch_listings, MackolikSession
            lst = fetch_listings(MackolikSession(args.mac_delay), target_date)
            logger.info("[DRY RUN] %s  Mackolik: %d maç", target_date, len(lst))
            return []
        merged, _ = pipeline.run(target_date)
        return [m.to_dict() for m in merged]

    elif args.mode == "sofa":
        from .sofascore import SofascoreScraper
        scraper = SofascoreScraper(request_delay=args.sofa_delay)
        if args.dry_run:
            ev = scraper.fetch_scheduled_events(target_date)
            logger.info("[DRY RUN] %s  Sofascore: %d maç", target_date, len(ev))
            return []
        matches = scraper.scrape_date(target_date, fetch_all_markets=args.sofa_all_markets)
        return [m.to_dict() for m in matches]

    elif args.mode == "mac":
        from .mackolik import MackolikScraper
        scraper = MackolikScraper(request_delay=args.mac_delay)
        matches, _ = scraper.scrape_date(
            date=target_date,
            iddaa_only=not args.all_matches,
            dry_run=args.dry_run,
        )
        if args.dry_run:
            return []
        return [m.to_dict() for m in matches]

    return []


# ─── Supabase ─────────────────────────────────────────────────────────────────

def push_to_supabase(results: list[dict], target_date: str, started_at: datetime) -> None:
    from .db import get_client, upsert_combined, log_scrape_run
    logger.info("Supabase'e yazılıyor (%s)...", target_date)
    try:
        client   = get_client()
        db_stats = upsert_combined(client, results, target_date)
        log_scrape_run(client, target_date, db_stats, [], started_at)
        logger.info(
            "Supabase [%s]: %d maç, %d market.",
            target_date, db_stats.get("matches", 0), db_stats.get("markets", 0),
        )
    except Exception as exc:
        logger.error("Supabase hatası [%s]: %s", target_date, exc)


# ─── Ana giriş ────────────────────────────────────────────────────────────────

def main() -> None:
    args       = parse_args()
    started_at = datetime.now(timezone.utc)

    # ── Tarih listesini oluştur ───────────────────────────────────────────────
    if args.date_from:
        date_to = args.date_to or str(date.today())
        try:
            dates = date_range(args.date_from, date_to)
        except ValueError as exc:
            logger.error(str(exc))
            sys.exit(1)
    else:
        single = args.date or str(date.today())
        dates  = [single]

    from_str = dates[0]
    to_str   = dates[-1]
    is_range = len(dates) > 1

    logger.info("=" * 60)
    if is_range:
        logger.info(
            "Tarih aralığı: %s → %s  (%d gün)  |  Mod: %s  |  Çıktı: %s",
            from_str, to_str, len(dates), args.mode,
            "ayrı dosyalar" if args.split else "tek birleşik dosya",
        )
    else:
        logger.info("Tarih: %s  |  Mod: %s", from_str, args.mode)
    logger.info("=" * 60)

    # ── Varsayılan çıktı şablonu ──────────────────────────────────────────────
    output_template = args.output_json
    if not output_template:
        if is_range and not args.split:
            output_template = "data/odds_{from}_{to}.json.gz"
        else:
            output_template = "data/odds_{date}.json.gz"

    # ── İşleme döngüsü ───────────────────────────────────────────────────────
    all_results: list[dict] = []

    for target_date in dates:
        if is_range:
            logger.info("── %s işleniyor ──────────────────────────", target_date)

        day_results = process_date(args, target_date)

        if not day_results:
            logger.warning("%s — sonuç yok, atlandı.", target_date)
            continue

        logger.info("%s: %d maç", target_date, len(day_results))
        all_results.extend(day_results)

        # --split: her gün için ayrı dosya
        if args.split:
            out_path = resolve_output_path(
                output_template, date_str=target_date, from_str=from_str, to_str=to_str
            )
            save_gz(out_path, day_results)

        # Tek gün modu: hemen yaz
        if not is_range and args.output_json:
            out_path = resolve_output_path(
                output_template, date_str=target_date, from_str=from_str, to_str=to_str
            )
            save_gz(out_path, day_results)

        # Supabase: her gün ayrı push
        if not args.no_supabase and not args.dry_run:
            push_to_supabase(day_results, target_date, started_at)

    # ── Birleşik tek dosya (aralık + split değil) ────────────────────────────
    if is_range and not args.split and all_results:
        out_path = resolve_output_path(
            output_template, date_str=from_str, from_str=from_str, to_str=to_str
        )
        save_gz(out_path, all_results)

    # ── Özet ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(
        "TOPLAM  —  %d gün  |  %d maç%s",
        len(dates), len(all_results),
        f"  |  {dates[0]} – {dates[-1]}" if is_range else "",
    )
    logger.info("=" * 60)

    if args.dry_run or not all_results:
        sys.exit(0)


if __name__ == "__main__":
    main()
