"""
scraper/pipeline.py
───────────────────
Mackolik (marketler) + Sofascore (oran değişimi + meta) birleşik pipeline.

Akış — toplam istek sayısı:
  1. Mackolik livedata       → listing            (1 istek)
  2. Mackolik arsiv          → 27 market/maç      (N istek)
  3. Sofascore scheduled-events → meta + skor     (1 istek)
  4. Sofascore bulk 1X2      → opening/closing    (1 istek)
  5. Bellek içi eşleştirme  → 0 ek istek
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
    ht_home_score: Optional[int]
    ht_away_score: Optional[int]
    status:        str
    sofascore_markets: list = field(default_factory=list)
    mackolik_markets:  list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "match_date":        self.match_date,
            "match_time":        self.match_time,
            "sofa_event_id":     self.sofa_event_id,
            "mac_id":            self.mac_id,
            "home_team":         self.home_team,
            "away_team":         self.away_team,
            "tournament":        self.tournament,
            "country":           self.country,
            "league":            self.league,
            "home_score":        self.home_score,
            "away_score":        self.away_score,
            "ht_home_score":     self.ht_home_score,
            "ht_away_score":     self.ht_away_score,
            "status":            self.status,
            "sofascore_markets": [m.to_dict() for m in self.sofascore_markets],
            "mackolik_markets":  [m.to_dict() for m in self.mackolik_markets],
        }


class CombinedPipeline:
    def __init__(
        self,
        sofa_delay:     float = 0.5,
        mac_delay:      float = 1.5,
        max_retries:    int   = 3,
        fetch_sofa_all: bool  = False,   # artık kullanılmıyor ama imza uyumu için
        iddaa_only:     bool  = True,
    ):
        self.sofa_delay  = sofa_delay
        self.mac_delay   = mac_delay
        self.max_retries = max_retries
        self.iddaa_only  = iddaa_only

    def run(self, date: str) -> tuple[list[MergedMatch], dict]:
        from .mackolik import MackolikSession, fetch_listings, fetch_match_detail
        from .sofascore import SofascoreClient
        from .matcher  import similarity

        stats = {
            "mac_total":    0,
            "mac_odds":     0,
            "sofa_matched": 0,
            "errors":       [],
        }

        # ── 1. Mackolik listing ───────────────────────────────────────────────
        logger.info("── Mackolik listing ──")
        mac_session = MackolikSession(
            request_delay=self.mac_delay,
            max_retries=self.max_retries,
        )
        listings = fetch_listings(mac_session, date)

        if self.iddaa_only:
            listings = [l for l in listings if l.has_iddaa]

        stats["mac_total"] = len(listings)
        logger.info("Mackolik: %d maç", len(listings))
        if not listings:
            return [], stats

        # ── 2. Mackolik detay (27 market) ────────────────────────────────────
        logger.info("── Mackolik oranları ──")
        mac_odds_map: dict[int, object] = {}

        for idx, listing in enumerate(listings, 1):
            logger.info(
                "[%d/%d] mac_id=%d  %s vs %s",
                idx, len(listings), listing.mac_id,
                listing.home_team, listing.away_team,
            )
            odds = fetch_match_detail(mac_session, listing, date)
            if odds and odds.markets:
                mac_odds_map[listing.mac_id] = odds
                stats["mac_odds"] += 1
                logger.info("  ✓ %d market", len(odds.markets))
            else:
                logger.info("  – oran yok")

        # ── 3. Sofascore — 2 istek ────────────────────────────────────────────
        logger.info("── Sofascore enrich (2 istek) ──")
        sofa = SofascoreClient(
            request_delay=self.sofa_delay,
            max_retries=self.max_retries,
        )

        # scheduled-events: meta + skor + iy skoru
        sofa_events = sofa.fetch_scheduled_events(date)   # {event_id: SofaEventMeta}

        # bulk 1X2: opening / closing / change
        sofa_bulk   = sofa.fetch_bulk_1x2(date)           # {event_id: SofaMarket}

        # ── 4. Bellek içi eşleştirme ─────────────────────────────────────────
        # sofa_events listesini bir kez hazırla
        sofa_list = list(sofa_events.values())   # [SofaEventMeta]

        sofa_map: dict[int, dict] = {}   # mac_id → {event_id, meta, market}

        for listing in listings:
            best_eid:   Optional[int] = None
            best_score: float         = 0.0

            for meta in sofa_list:
                h = similarity(listing.home_team, meta.home_team)
                a = similarity(listing.away_team, meta.away_team)
                score = (h + a) / 2
                if score > best_score and score >= 0.50:
                    best_score = score
                    best_eid   = meta.event_id

            if best_eid:
                meta = sofa_events[best_eid]
                market = sofa_bulk.get(best_eid)
                sofa_map[listing.mac_id] = {
                    "event_id": best_eid,
                    "meta":     meta,
                    "market":   market,   # SofaMarket | None
                }
                logger.debug(
                    "  ✓ [%.2f] %s ↔ %s",
                    best_score, listing.home_team, meta.home_team,
                )

        stats["sofa_matched"] = len(sofa_map)
        logger.info(
            "Eşleşme: %d / %d maç",
            stats["sofa_matched"], len(listings),
        )

        # ── 5. Birleştir ─────────────────────────────────────────────────────
        results: list[MergedMatch] = []

        for listing in listings:
            mac_odds  = mac_odds_map.get(listing.mac_id)
            sofa_info = sofa_map.get(listing.mac_id)

            if sofa_info:
                meta          = sofa_info["meta"]
                sofa_event_id = sofa_info["event_id"]
                sofa_market   = sofa_info["market"]
                sofa_markets  = [sofa_market] if sofa_market else []
                # Skor ve meta Sofascore'dan (daha güvenilir)
                home_score    = meta.home_score    if meta.home_score is not None else listing.home_score
                away_score    = meta.away_score    if meta.away_score is not None else listing.away_score
                ht_home_score = meta.ht_home_score if meta.ht_home_score is not None else listing.ht_home_score
                ht_away_score = meta.ht_away_score if meta.ht_away_score is not None else listing.ht_away_score
                tournament    = meta.tournament
                country       = meta.country
                status        = meta.status
            else:
                sofa_event_id = None
                sofa_markets  = []
                home_score    = listing.home_score
                away_score    = listing.away_score
                ht_home_score = listing.ht_home_score
                ht_away_score = listing.ht_away_score
                tournament    = ""
                country       = listing.country
                status        = listing.status or "unknown"

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
                ht_home_score=ht_home_score,
                ht_away_score=ht_away_score,
                status=status,
                sofascore_markets=sofa_markets,
                mackolik_markets=mac_odds.markets if mac_odds else [],
            ))

        logger.info(
            "Pipeline tamamlandı: %d maç  (%d iddaa | %d Sofascore eşleşmesi)",
            len(results), stats["mac_odds"], stats["sofa_matched"],
        )
        return results, stats
