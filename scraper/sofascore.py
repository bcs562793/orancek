"""
scraper/sofascore.py
────────────────────
Sofascore API client.

Kullanılan endpoint'ler:
  1. /sport/football/scheduled-events/{date}   → o günün tüm maçları
  2. /sport/football/odds/1/{date}             → tüm maçların 1X2 bulk oranları
  3. /event/{id}/odds/1/all                   → tek maç tüm marketler

Oran formatı: kesirli ("6/5") → decimal (2.2) çevrimi burada yapılır.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://www.sofascore.com/api/v1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Referer":         "https://www.sofascore.com/",
}


# ─── Modeller ─────────────────────────────────────────────────────────────────

@dataclass
class SofaOddsChoice:
    name:          str
    opening_odds:  Optional[float]   # initialFractionalValue → decimal
    closing_odds:  Optional[float]   # fractionalValue → decimal
    winning:       Optional[bool]
    change:        int               # -1 düştü / 0 sabit / 1 yükseldi

    def to_dict(self) -> dict:
        return {
            "name":         self.name,
            "opening_odds": self.opening_odds,
            "closing_odds": self.closing_odds,
            "winning":      self.winning,
            "change":       self.change,
        }


@dataclass
class SofaMarket:
    market_id:    int
    market_name:  str
    market_group: str
    market_period: str
    choice_group: Optional[str]      # Alt/Üst çizgisi: "2.5" gibi
    choices:      list[SofaOddsChoice] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "market_id":    self.market_id,
            "market_name":  self.market_name,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "choice_group": self.choice_group,
            "choices":      [c.to_dict() for c in self.choices],
        }


@dataclass
class SofaMatch:
    event_id:      int
    slug:          str
    home_team:     str
    home_team_id:  int
    away_team:     str
    away_team_id:  int
    tournament:    str
    tournament_id: int
    country:       str
    match_date:    str               # YYYY-MM-DD
    match_time:    str               # HH:MM
    start_ts:      int               # unix timestamp
    home_score:    Optional[int]
    away_score:    Optional[int]
    status:        str               # "finished" / "notstarted" / ...
    markets:       list[SofaMarket] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "event_id":      self.event_id,
            "slug":          self.slug,
            "home_team":     self.home_team,
            "home_team_id":  self.home_team_id,
            "away_team":     self.away_team,
            "away_team_id":  self.away_team_id,
            "tournament":    self.tournament,
            "tournament_id": self.tournament_id,
            "country":       self.country,
            "match_date":    self.match_date,
            "match_time":    self.match_time,
            "start_ts":      self.start_ts,
            "home_score":    self.home_score,
            "away_score":    self.away_score,
            "status":        self.status,
            "markets":       [m.to_dict() for m in self.markets],
        }


# ─── Yardımcılar ──────────────────────────────────────────────────────────────

def frac_to_decimal(frac: str) -> Optional[float]:
    """
    Kesirli oran → decimal.
    "6/5" → 2.2   |   "11/5" → 3.2   |   "EVS" → 2.0
    """
    if not frac:
        return None
    frac = frac.strip()
    if frac.upper() in ("EVS", "EVENS"):
        return 2.0
    m = re.match(r"^(-?\d+)\s*/\s*(\d+)$", frac)
    if m:
        num, den = int(m.group(1)), int(m.group(2))
        if den == 0:
            return None
        return round(num / den + 1, 3)
    # Zaten decimal olabilir
    try:
        return round(float(frac), 3)
    except ValueError:
        return None


def _parse_market(raw: dict) -> SofaMarket:
    choices = []
    for c in raw.get("choices", []):
        choices.append(SofaOddsChoice(
            name=c.get("name", ""),
            opening_odds=frac_to_decimal(c.get("initialFractionalValue", "")),
            closing_odds=frac_to_decimal(c.get("fractionalValue", "")),
            winning=c.get("winning"),
            change=c.get("change", 0),
        ))
    return SofaMarket(
        market_id=raw.get("marketId", 0),
        market_name=raw.get("marketName", ""),
        market_group=raw.get("marketGroup", ""),
        market_period=raw.get("marketPeriod", ""),
        choice_group=raw.get("choiceGroup"),
        choices=choices,
    )


def _parse_event(raw: dict, match_date: str) -> Optional[SofaMatch]:
    """Scheduled-events yanıtından tek maç parse eder."""
    try:
        event_id  = raw["id"]
        slug      = raw.get("slug", str(event_id))
        home      = raw["homeTeam"]
        away      = raw["awayTeam"]
        tourney   = raw.get("tournament", {})
        category  = tourney.get("category", {})
        country   = category.get("name", "")

        start_ts  = raw.get("startTimestamp", 0)
        from datetime import datetime, timezone
        dt        = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        match_time = dt.strftime("%H:%M")

        score_home = None
        score_away = None
        if "homeScore" in raw:
            score_home = raw["homeScore"].get("current")
            score_away = raw["awayScore"].get("current")

        status = raw.get("status", {}).get("type", "unknown")

        return SofaMatch(
            event_id=event_id,
            slug=slug,
            home_team=home.get("name", ""),
            home_team_id=home.get("id", 0),
            away_team=away.get("name", ""),
            away_team_id=away.get("id", 0),
            tournament=tourney.get("name", ""),
            tournament_id=tourney.get("uniqueTournament", {}).get("id", 0),
            country=country,
            match_date=match_date,
            match_time=match_time,
            start_ts=start_ts,
            home_score=score_home,
            away_score=score_away,
            status=status,
        )
    except (KeyError, TypeError) as exc:
        logger.debug("Event parse hatası: %s — %s", exc, raw.get("id"))
        return None


# ─── Session ──────────────────────────────────────────────────────────────────

class SofascoreSession:
    def __init__(self, request_delay: float = 0.5, max_retries: int = 3):
        self.delay       = request_delay
        self.max_retries = max_retries
        self._session    = requests.Session()
        self._session.headers.update(HEADERS)

    def get(self, path: str, **kwargs) -> requests.Response:
        url = f"{BASE_URL}{path}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=20, **kwargs)
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp
            except requests.RequestException as exc:
                logger.warning("Deneme %d/%d: %s → %s", attempt, self.max_retries, url, exc)
                if attempt == self.max_retries:
                    raise
                time.sleep(self.delay * attempt * 2)
        raise RuntimeError("Ulaşılamaz")


# ─── Sofascore Scraper ────────────────────────────────────────────────────────

class SofascoreScraper:
    def __init__(self, request_delay: float = 0.5, max_retries: int = 3):
        self.session = SofascoreSession(
            request_delay=request_delay,
            max_retries=max_retries,
        )

    # ── 1. Maç listesi ──────────────────────────────────────────────────────

    def fetch_scheduled_events(self, date: str) -> list[SofaMatch]:
        """
        GET /sport/football/scheduled-events/{date}
        Tüm gün için maç listesini çeker (sayfalı).
        """
        matches: list[SofaMatch] = []
        page = 0

        while True:
            path = f"/sport/football/scheduled-events/{date}"
            params = {"page": page} if page > 0 else {}
            logger.info("Sofascore scheduled-events sayfa %d", page)

            try:
                resp = self.session.get(path, params=params)
                data = resp.json()
            except Exception as exc:
                logger.error("scheduled-events hatası: %s", exc)
                break

            events = data.get("events", [])
            if not events:
                break

            for raw in events:
                m = _parse_event(raw, date)
                if m:
                    matches.append(m)

            # Sonraki sayfa var mı?
            if not data.get("hasNextPage", False):
                break
            page += 1

        logger.info("Sofascore: %d maç listelendi (%s)", len(matches), date)
        return matches

    # ── 2. Bulk 1X2 oranları (tek istek, tüm gün) ──────────────────────────

    def fetch_bulk_odds(self, date: str) -> dict[int, SofaMarket]:
        """
        GET /sport/football/odds/1/{date}
        Tüm gün için 1X2 opening+closing oranlarını tek istekte çeker.
        Returns: {event_id: SofaMarket}
        """
        logger.info("Sofascore bulk odds çekiliyor: %s", date)
        try:
            resp = self.session.get(f"/sport/football/odds/1/{date}")
            data = resp.json()
        except Exception as exc:
            logger.error("Bulk odds hatası: %s", exc)
            return {}

        result: dict[int, SofaMarket] = {}
        for event_id_str, raw in data.get("odds", {}).items():
            try:
                event_id = int(event_id_str)
                result[event_id] = _parse_market(raw)
            except (ValueError, TypeError):
                continue

        logger.info("Sofascore bulk odds: %d maç", len(result))
        return result

    # ── 3. Tek maç tüm marketler ─────────────────────────────────────────────

    def fetch_event_all_odds(self, event_id: int) -> list[SofaMarket]:
        """
        GET /event/{id}/odds/1/all
        Tek maçın tüm market ve oranlarını çeker.
        """
        try:
            resp = self.session.get(f"/event/{event_id}/odds/1/all")
            data = resp.json()
        except Exception as exc:
            logger.error("event %d odds hatası: %s", event_id, exc)
            return []

        markets = []
        for raw in data.get("markets", []):
            markets.append(_parse_market(raw))
        return markets

    # ── 4. Birleşik fetch: maç listesi + oranlar ─────────────────────────────

    def scrape_date(
        self,
        date: str,
        fetch_all_markets: bool = False,
    ) -> list[SofaMatch]:
        """
        Bir tarihin tüm Sofascore verilerini çeker.

        fetch_all_markets=False → sadece bulk 1X2 (1 istek)
        fetch_all_markets=True  → her maç için /odds/1/all (N istek, yavaş)
        """
        # Maç listesi
        matches = self.fetch_scheduled_events(date)
        if not matches:
            return []

        # Bulk 1X2 oranları
        bulk_odds = self.fetch_bulk_odds(date)

        match_map = {m.event_id: m for m in matches}

        # Bulk oranları maçlara ekle
        for event_id, market in bulk_odds.items():
            if event_id in match_map:
                match_map[event_id].markets = [market]

        if fetch_all_markets:
            # Her maç için tam market seti çek
            logger.info(
                "Tüm marketler çekiliyor: %d maç × /odds/1/all", len(matches)
            )
            for idx, match in enumerate(matches, 1):
                logger.debug("[%d/%d] event_id=%d", idx, len(matches), match.event_id)
                all_markets = self.fetch_event_all_odds(match.event_id)
                if all_markets:
                    match.markets = all_markets

        return list(match_map.values())
