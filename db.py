"""
scraper/db.py
─────────────
Supabase upsert operasyonları.

Tablolar:
  matches       — maç meta verisi
  match_odds    — market + outcome detayları (JSONB)
  scrape_log    — çalışma geçmişi
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from supabase import create_client, Client

from .mackolik import MatchOdds

logger = logging.getLogger(__name__)


def get_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]   # service_role key (RLS bypass)
    return create_client(url, key)


# ─── Upsert İşlemleri ─────────────────────────────────────────────────────────

def upsert_match(client: Client, match: MatchOdds) -> Optional[int]:
    """
    matches tablosuna maç upsert eder.
    Çakışma: mac_id üzerinden — varsa güncelle.
    """
    row = {
        "mac_id":     match.mac_id,
        "slug":       match.slug,
        "home_team":  match.home_team,
        "away_team":  match.away_team,
        "league":     match.league,
        "sub_league": match.sub_league,
        "match_date": match.match_date,
        "match_time": match.match_time,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        resp = (
            client.table("matches")
            .upsert(row, on_conflict="mac_id")
            .execute()
        )
        if resp.data:
            return resp.data[0]["id"]
        logger.error("matches upsert sonuç döndürmedi: mac_id=%d", match.mac_id)
        return None
    except Exception as exc:
        logger.error("matches upsert hatası (mac_id=%d): %s", match.mac_id, exc)
        return None


def upsert_odds(client: Client, match: MatchOdds) -> int:
    """
    match_odds tablosuna tüm marketleri upsert eder.
    Çakışma: (mac_id, market_code, bet_id) üzerinden.

    Returns: başarıyla eklenen market sayısı
    """
    rows = []
    now  = datetime.now(timezone.utc).isoformat()

    for market in match.markets:
        rows.append({
            "mac_id":       match.mac_id,
            "market_name":  market.market_name,
            "market_code":  market.market_code,
            "match_ref_id": market.match_ref_id,
            "bet_id":       market.bet_id,
            "outcomes":     [o.to_dict() for o in market.outcomes],
            "scraped_at":   now,
        })

    if not rows:
        return 0

    # Supabase 1000 satır limiti — büyük maçlar için chunk'la
    CHUNK = 500
    inserted = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i + CHUNK]
        try:
            resp = (
                client.table("match_odds")
                .upsert(chunk, on_conflict="mac_id,market_code,bet_id")
                .execute()
            )
            inserted += len(resp.data or [])
        except Exception as exc:
            logger.error(
                "match_odds upsert hatası (mac_id=%d, chunk=%d): %s",
                match.mac_id, i, exc
            )

    return inserted


def upsert_all(client: Client, matches: list[MatchOdds]) -> dict:
    """
    Tüm maçları tek seferde kaydet.

    Returns:
        {
          "total": int,
          "match_rows": int,
          "odds_rows": int,
          "errors": int,
        }
    """
    match_rows = 0
    odds_rows  = 0
    errors     = 0

    for match in matches:
        mid = upsert_match(client, match)
        if mid is None:
            errors += 1
            continue
        match_rows += 1

        cnt = upsert_odds(client, match)
        odds_rows += cnt
        logger.info(
            "  ✓ mac_id=%d kaydedildi → %d market", match.mac_id, cnt
        )

    return {
        "total":      len(matches),
        "match_rows": match_rows,
        "odds_rows":  odds_rows,
        "errors":     errors,
    }


# ─── Scrape Log ───────────────────────────────────────────────────────────────

def log_scrape_run(
    client:      Client,
    scrape_date: str,
    stats:       dict,
    errors:      list[dict],
    started_at:  datetime,
) -> None:
    """scrape_log tablosuna çalışma kaydı ekler."""
    row = {
        "scrape_date":   scrape_date,
        "total_matches": stats.get("total", 0),
        "success_count": stats.get("match_rows", 0),
        "error_count":   stats.get("errors", 0) + len(errors),
        "errors":        errors or None,
        "started_at":    started_at.isoformat(),
        "finished_at":   datetime.now(timezone.utc).isoformat(),
    }
    try:
        client.table("scrape_log").insert(row).execute()
    except Exception as exc:
        logger.error("Scrape log eklenemedi: %s", exc)
