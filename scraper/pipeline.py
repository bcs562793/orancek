"""
scraper/pipeline.py
───────────────────
Sofascore + Mackolik birleşik pipeline.

Çıktı yapısı (her maç için):
{
  "match_date":    "2026-03-29",
  "match_time":    "18:00",

  # Kimlik
  "sofa_event_id": 14083488,      # null → sadece Mackolik
  "mac_id":        4437085,        # null → sadece Sofascore

  # Takım / Lig (Sofascore öncelikli, yoksa Mackolik)
  "home_team":     "Cultural Leonesa",
  "away_team":     "FC Andorra",
  "tournament":    "LaLiga 2",
  "country":       "Spain",
  "league":        "İspanya LaLiga 2",   # Mackolik Türkçe

  # Skor (maç bittiyse)
  "home_score":    0,
  "away_score":    4,
  "status":        "finished",

  # Oranlar
  "sofascore_markets": [          # Sofascore: açılış + kapanış + kazanan
    {
      "market_id":    1,
      "market_name":  "Full time",
      "market_group": "1X2",
      "market_period": "Full-time",
      "choice_group": null,
      "choices": [
        {"name": "1", "opening_odds": 2.45, "closing_odds": 2.20, "winning": false, "change": -1},
        ...
      ]
    }
  ],
  "mackolik_markets": [           # Mackolik: iddaa oranları
    {
      "market_name":  "Maç Sonucu",
      "market_code":  "43901",
      "outcomes": [
        {"name": "1", "odds": 2.32},
        ...
      ]
    }
  ]
}
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .sofascore import SofascoreScraper, SofaMatch
from .mackolik import MackolikScraper, MatchListing, fetch_match_detail
from .matcher import match_events, MatchPair

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
    league:        str      # Mackolik Türkçe lig adı

    home_score:    Optional[int]
    away_score:    Optional[int]
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
            "status":        self.status,
            "sofascore_markets": [m.to_dict() for m in self.sofascore_markets],
            "mackolik_markets":  [m.to_dict() for m in self.mackolik_markets],
        }


class CombinedPipeline:
    def __init__(
        self,
        sofa_delay:      float = 0.5,
        mac_delay:       float = 1.5,
        max_retries:     int   = 3,
        fetch_sofa_all:  bool  = False,   # /odds/1/all her maç için
        match_threshold: float = 0.5,
    ):
        self.sofa    = SofascoreScraper(request_delay=sofa_delay, max_retries=max_retries)
        self.mac_session = MackolikScraper(request_delay=mac_delay, max_retries=max_retries).session
        self.fetch_sofa_all  = fetch_sofa_all
        self.match_threshold = match_threshold

    def run(self, date: str) -> tuple[list[MergedMatch], dict]:
        """
        Tam pipeline.
        Returns: (merged_matches, stats)
        """
        stats = {
            "sofa_total":    0,
            "mac_total":     0,
            "matched":       0,
            "sofa_only":     0,
            "mac_only":      0,
            "mac_no_odds":   0,
            "errors":        [],
        }

        # ── 1. Sofascore maç listesi + oranlar ────────────────────────────────
        logger.info("── Sofascore başlatılıyor ──")
        sofa_matches = self.sofa.scrape_date(date, fetch_all_markets=self.fetch_sofa_all)
        stats["sofa_total"] = len(sofa_matches)
        logger.info("Sofascore: %d maç", len(sofa_matches))

        # ── 2. Mackolik maç listesi ────────────────────────────────────────────
        logger.info("── Mackolik listesi başlatılıyor ──")
        from .mackolik import fetch_listings, MackolikSession
        mac_session   = MackolikSession(request_delay=1.5)
        mac_listings  = fetch_listings(mac_session, date)
        stats["mac_total"] = len(mac_listings)
        logger.info("Mackolik: %d maç", len(mac_listings))

        # ── 3. Eşleştirme ─────────────────────────────────────────────────────
        logger.info("── Eşleştirme ──")
        pairs, unmatched_sofa, unmatched_mac = match_events(
            sofa_matches, mac_listings, threshold=self.match_threshold
        )
        stats["matched"]   = len(pairs)
        stats["sofa_only"] = len(unmatched_sofa)
        stats["mac_only"]  = len(unmatched_mac)

        # Kolay lookup
        sofa_map = {sm.event_id: sm for sm in sofa_matches}
        mac_map  = {ml.mac_id:   ml for ml in mac_listings}

        results: list[MergedMatch] = []

        # ── 4a. Eşleşen maçlar ────────────────────────────────────────────────
        for pair in pairs:
            sm = sofa_map[pair.sofa_event_id]
            ml = mac_map[pair.mac_id]

            # Mackolik detayını çek
            mac_odds = fetch_match_detail(mac_session, ml, date)
            if mac_odds and not mac_odds.markets:
                stats["mac_no_odds"] += 1

            results.append(MergedMatch(
                match_date=date,
                match_time=sm.match_time or ml.match_time,
                sofa_event_id=sm.event_id,
                mac_id=ml.mac_id,
                home_team=sm.home_team,
                away_team=sm.away_team,
                tournament=sm.tournament,
                country=sm.country,
                league=ml.league,
                home_score=sm.home_score,
                away_score=sm.away_score,
                status=sm.status,
                sofascore_markets=sm.markets,
                mackolik_markets=mac_odds.markets if mac_odds else [],
            ))

        # ── 4b. Sadece Sofascore'da olanlar ───────────────────────────────────
        for sm in unmatched_sofa:
            results.append(MergedMatch(
                match_date=date,
                match_time=sm.match_time,
                sofa_event_id=sm.event_id,
                mac_id=None,
                home_team=sm.home_team,
                away_team=sm.away_team,
                tournament=sm.tournament,
                country=sm.country,
                league="",
                home_score=sm.home_score,
                away_score=sm.away_score,
                status=sm.status,
                sofascore_markets=sm.markets,
                mackolik_markets=[],
            ))

        # ── 4c. Sadece Mackolik'te olanlar ────────────────────────────────────
        for ml in unmatched_mac:
            mac_odds = fetch_match_detail(mac_session, ml, date)
            if mac_odds and not mac_odds.markets:
                stats["mac_no_odds"] += 1
                continue   # oranya yoksa dahil etme

            results.append(MergedMatch(
                match_date=date,
                match_time=ml.match_time,
                sofa_event_id=None,
                mac_id=ml.mac_id,
                home_team=ml.home_team,
                away_team=ml.away_team,
                tournament="",
                country="",
                league=ml.league,
                home_score=None,
                away_score=None,
                status="unknown",
                sofascore_markets=[],
                mackolik_markets=mac_odds.markets if mac_odds else [],
            ))

        logger.info(
            "Pipeline tamamlandı: %d toplam maç "
            "(%d eşleşti | %d sadece Sofa | %d sadece Mac)",
            len(results),
            stats["matched"],
            stats["sofa_only"],
            stats["mac_only"],
        )
        return results, stats
