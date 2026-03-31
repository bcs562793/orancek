"""
scraper/pipeline.py
───────────────────
Sofascore + Mackolik birleşik pipeline.

Akış:
  1. Mackolik listing  → o günün iddaa maç listesi
  2. Mackolik detail   → her maç için iddaa oranları (HTML)
  3. Sofascore bulk    → /sport/football/odds/1/{date} (tek istek, 1X2)
  4. Sofascore enrich  → eşleşen her maç için /event/{id}/odds/1/all

Çıktı yapısı (her maç için):
{
  "match_date":    "2026-03-29",
  "match_time":    "20:45",
  "sofa_event_id": 14083488,       # null → sadece Mackolik
  "mac_id":        4437085,        # null → sadece Sofascore bulk (gelecek)
  "home_team":     "FC Andorra",
  "away_team":     "Cultural Leonesa",
  "tournament":    "LaLiga 2",     # Sofascore'dan
  "country":       "Spain",
  "league":        "İspanya...",   # Mackolik Türkçe
  "home_score":    4,
  "away_score":    0,
  "status":        "finished",
  "sofascore_markets": [...],      # açılış + kapanış + winning flag
  "mackolik_markets":  [...]       # iddaa oranları
}
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class MergedMatch:
    match_date:    str
    match_time:    str
    sofa_event_id: Optional[int]
    mac_id:        Optional[int]
    home_team:     str
    away_team:     str
    tournament:    str
    country:       str
    league:        str
    home_score:    Optional[int]
    away_score:    Optional[int]
    ht_home_score: Optional[int]   # ilk yarı ev sahibi skoru
    ht_away_score: Optional[int]   # ilk yarı deplasman skoru
    status:        str
    sofascore_markets: list = field(default_factory=list)
    mackolik_markets:  list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "match_date":    self.match_date,
            "match_time":    self.match_time,
            "sofa_event_id": self.sofa_event_id,
            "mac_id":        self.mac_id,
            "home_team":     self.home_team,
            "away_team":     self.away_team,
            "tournament":    self.tournament,
            "country":       self.country,
            "league":        self.league,
            "home_score":    self.home_score,
            "away_score":    self.away_score,
            "ht_home_score": self.ht_home_score,
            "ht_away_score": self.ht_away_score,
            "status":        self.status,
            "sofascore_markets": [m.to_dict() for m in self.sofascore_markets],
            "mackolik_markets":  [m.to_dict() for m in self.mackolik_markets],
        }


class CombinedPipeline:
    def __init__(
        self,
        sofa_delay:       float = 0.8,
        mac_delay:        float = 1.5,
        max_retries:      int   = 3,
        fetch_sofa_all:   bool  = False,
        iddaa_only:       bool  = True,
    ):
        self.sofa_delay     = sofa_delay
        self.mac_delay      = mac_delay
        self.max_retries    = max_retries
        self.fetch_sofa_all = fetch_sofa_all
        self.iddaa_only     = iddaa_only

    def run(self, date: str) -> tuple[list[MergedMatch], dict]:
        from .mackolik import MackolikScraper, MackolikSession, fetch_listings, fetch_match_detail
        from .sofascore import SofascoreClient

        stats = {
            "mac_total":   0,
            "mac_odds":    0,
            "sofa_matched": 0,
            "errors":      [],
        }

        # ── 1. Mackolik listing ───────────────────────────────────────────────
        logger.info("── Mackolik listing ──")
        mac_session = MackolikSession(request_delay=self.mac_delay, max_retries=self.max_retries)
        listings    = fetch_listings(mac_session, date)

        if self.iddaa_only:
            listings = [l for l in listings if l.has_iddaa]
        stats["mac_total"] = len(listings)
        logger.info("Mackolik: %d maç listelendi", len(listings))

        if not listings:
            return [], stats

        # ── 2. Mackolik detail (iddaa oranları) ──────────────────────────────
        logger.info("── Mackolik oranları çekiliyor ──")
        mac_odds_map: dict[int, object] = {}

        for idx, listing in enumerate(listings, 1):
            logger.info("[%d/%d] mac_id=%d  %s vs %s",
                        idx, len(listings), listing.mac_id,
                        listing.home_team, listing.away_team)
            odds = fetch_match_detail(mac_session, listing, date)
            if odds and odds.markets:
                mac_odds_map[listing.mac_id] = odds
                stats["mac_odds"] += 1
                logger.info("  ✓ %d iddaa marketi", len(odds.markets))
            else:
                logger.info("  – oran yok")

        # ── 3. Sofascore enrich ───────────────────────────────────────────────
        logger.info("── Sofascore enrich ──")
        sofa_client  = SofascoreClient(request_delay=self.sofa_delay, max_retries=self.max_retries)

        # Sadece Mackolik oranı bulunan maçları Sofascore ile zenginleştir
        mac_with_odds = [l for l in listings if l.mac_id in mac_odds_map]

        sofa_map = sofa_client.enrich_matches(
            mac_matches=mac_with_odds,
            date=date,
            fetch_all_markets=self.fetch_sofa_all,
        )
        stats["sofa_matched"] = len(sofa_map)

        # ── 4. Birleştir ─────────────────────────────────────────────────────
        results: list[MergedMatch] = []

        listing_map = {l.mac_id: l for l in listings}

        for listing in listings:
            mac_odds  = mac_odds_map.get(listing.mac_id)
            sofa_info = sofa_map.get(listing.mac_id)

            # Sofascore meta
            sofa_event_id = sofa_info["event_id"] if sofa_info else None
            sofa_markets  = sofa_info["markets"]  if sofa_info else []

            # Meta: Sofascore'dan varsa kullan, yoksa Mackolik'ten
            tournament = ""
            country    = ""
            home_score = None
            away_score = None
            status     = "unknown"

            if sofa_info and sofa_event_id:
                from .sofascore import SofaEventMeta
                # Meta cache'den çek (enrich_matches içinde doldu)
                meta = sofa_client.session.get(f"/event/{sofa_event_id}")
                if meta:
                    try:
                        from .sofascore import _parse_event_meta
                        m = _parse_event_meta(meta.json().get("event", {}))
                        if m:
                            tournament = m.tournament
                            country    = m.country
                            home_score = m.home_score
                            away_score = m.away_score
                            status     = m.status
                    except Exception:
                        pass

            results.append(MergedMatch(
                match_date=date,
                match_time=listing.match_time,
                sofa_event_id=sofa_event_id,
                mac_id=listing.mac_id,
                home_team=listing.home_team,
                away_team=listing.away_team,
                tournament=tournament,
                country=country,
                league=listing.league,
                home_score=home_score,
                away_score=away_score,
                ht_home_score=listing.ht_home_score,
                ht_away_score=listing.ht_away_score,
                status=status,
                sofascore_markets=sofa_markets,
                mackolik_markets=mac_odds.markets if mac_odds else [],
            ))

        logger.info(
            "Pipeline tamamlandı: %d maç  "
            "(%d iddaa oranı | %d Sofascore eşleşmesi)",
            len(results), stats["mac_odds"], stats["sofa_matched"],
        )
        return results, stats
