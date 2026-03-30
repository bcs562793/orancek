"""
scraper/sofascore.py
────────────────────
Sofascore odds client.

scheduled-events → 403 (bot korumalı, kullanılmıyor)

Kullanılan endpoint'ler:
  1. GET /sport/football/odds/1/{date}  → tüm günün event_id → 1X2 bulk oranları
  2. GET /event/{id}                   → tek maç: takım adı, lig, skor, zaman
  3. GET /event/{id}/odds/1/all        → tek maç tüm marketler

Akış:
  - Mackolik her zaman primary listing kaynağı
  - Her Mackolik maçı için Sofascore event_id bulmak üzere
    /event/{id} ile arama yapılır (küçük veri seti = makul istek sayısı)
  - Eşleşme bulunamazsa Sofascore kısmı boş bırakılır
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://www.sofascore.com/api/v1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept":             "*/*",
    "Accept-Language":    "tr,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding":    "gzip, deflate, br",
    "Cache-Control":      "no-cache",
    "Pragma":             "no-cache",
    "Referer":            "https://www.sofascore.com/tr/football/",
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "x-requested-with":   "d1e946",
}


# ─── Modeller ─────────────────────────────────────────────────────────────────

@dataclass
class SofaChoice:
    name:         str
    opening_odds: Optional[float]   # initialFractionalValue → decimal
    closing_odds: Optional[float]   # fractionalValue → decimal
    winning:      Optional[bool]
    change:       int               # -1 düştü / 0 sabit / +1 yükseldi

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
    market_id:     int
    market_name:   str
    market_group:  str
    market_period: str
    choice_group:  Optional[str]
    choices:       list[SofaChoice] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "market_id":     self.market_id,
            "market_name":   self.market_name,
            "market_group":  self.market_group,
            "market_period": self.market_period,
            "choice_group":  self.choice_group,
            "choices":       [c.to_dict() for c in self.choices],
        }


@dataclass
class SofaEventMeta:
    """Tek /event/{id} çağrısından gelen meta bilgisi."""
    event_id:      int
    home_team:     str
    home_team_id:  int
    away_team:     str
    away_team_id:  int
    tournament:    str
    tournament_id: int
    country:       str
    match_time:    str    # HH:MM UTC
    start_ts:      int
    home_score:    Optional[int]
    away_score:    Optional[int]
    status:        str


# ─── Yardımcılar ──────────────────────────────────────────────────────────────

def frac_to_decimal(frac: str) -> Optional[float]:
    """
    Kesirli oran → decimal.
    "6/5" → 2.2  |  "EVS" → 2.0  |  "1/1" → 2.0
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
        return round(num / den + 1, 4)
    try:
        return round(float(frac), 4)
    except ValueError:
        return None


def _parse_market(raw: dict) -> SofaMarket:
    choices = []
    for c in raw.get("choices", []):
        choices.append(SofaChoice(
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


def _parse_event_meta(raw: dict) -> Optional[SofaEventMeta]:
    try:
        home  = raw["homeTeam"]
        away  = raw["awayTeam"]
        tour  = raw.get("tournament", {})
        cat   = tour.get("category", {})
        uniq  = tour.get("uniqueTournament", {})

        ts  = raw.get("startTimestamp", 0)
        dt  = datetime.fromtimestamp(ts, tz=timezone.utc)

        h_score = raw.get("homeScore", {}).get("current") if "homeScore" in raw else None
        a_score = raw.get("awayScore", {}).get("current") if "awayScore" in raw else None

        return SofaEventMeta(
            event_id=raw["id"],
            home_team=home.get("name", ""),
            home_team_id=home.get("id", 0),
            away_team=away.get("name", ""),
            away_team_id=away.get("id", 0),
            tournament=uniq.get("name") or tour.get("name", ""),
            tournament_id=uniq.get("id", 0),
            country=cat.get("name", ""),
            match_time=dt.strftime("%H:%M"),
            start_ts=ts,
            home_score=h_score,
            away_score=a_score,
            status=raw.get("status", {}).get("type", "unknown"),
        )
    except (KeyError, TypeError) as exc:
        logger.debug("Event meta parse hatası: %s", exc)
        return None


# ─── Session ──────────────────────────────────────────────────────────────────

class SofascoreSession:
    def __init__(self, request_delay: float = 0.8, max_retries: int = 3):
        self.delay       = request_delay
        self.max_retries = max_retries
        self._session    = requests.Session()
        self._session.headers.update(HEADERS)

    def get(self, path: str, **kwargs) -> Optional[requests.Response]:
        url = f"{BASE_URL}{path}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=20, **kwargs)
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 403:
                    logger.warning("Sofascore 403: %s (deneme %d/%d)", path, attempt, self.max_retries)
                    time.sleep(self.delay * attempt * 3)
                else:
                    logger.warning("HTTP %s: %s → %s", exc.response.status_code if exc.response else "?", path, exc)
                if attempt == self.max_retries:
                    return None
            except requests.RequestException as exc:
                logger.warning("İstek hatası %s: %s", path, exc)
                if attempt == self.max_retries:
                    return None
                time.sleep(self.delay * attempt)
        return None


# ─── API Fonksiyonları ────────────────────────────────────────────────────────

class SofascoreClient:
    def __init__(self, request_delay: float = 0.8, max_retries: int = 3):
        self.session = SofascoreSession(request_delay, max_retries)

    def fetch_bulk_1x2(self, date: str) -> dict[int, SofaMarket]:
        """
        GET /sport/football/odds/1/{date}
        Tüm günün event_id → 1X2 (opening + closing) oranlarını tek istekte çeker.
        Returns: {event_id: SofaMarket}
        """
        logger.info("Sofascore bulk 1X2: %s", date)
        resp = self.session.get(f"/sport/football/odds/1/{date}")
        if resp is None:
            return {}

        data = resp.json()
        result: dict[int, SofaMarket] = {}
        for eid_str, raw in data.get("odds", {}).items():
            try:
                result[int(eid_str)] = _parse_market(raw)
            except (ValueError, TypeError):
                continue

        logger.info("Sofascore bulk 1X2: %d maç oranı alındı", len(result))
        return result

    def fetch_event_meta(self, event_id: int) -> Optional[SofaEventMeta]:
        """
        GET /event/{id}
        Tek maçın meta bilgisini (takım adları, lig, skor) çeker.
        """
        resp = self.session.get(f"/event/{event_id}")
        if resp is None:
            return None
        try:
            return _parse_event_meta(resp.json().get("event", {}))
        except Exception as exc:
            logger.debug("event_meta parse hatası %d: %s", event_id, exc)
            return None

    def fetch_all_markets(self, event_id: int) -> list[SofaMarket]:
        """
        GET /event/{id}/odds/1/all
        Tek maçın tüm market ve oranlarını çeker.
        """
        resp = self.session.get(f"/event/{event_id}/odds/1/all")
        if resp is None:
            return []
        try:
            return [_parse_market(m) for m in resp.json().get("markets", [])]
        except Exception as exc:
            logger.debug("all_markets parse hatası %d: %s", event_id, exc)
            return []

    def enrich_matches(
        self,
        mac_matches: list,          # list[MatchOdds] — Mackolik sonuçları
        date: str,
        fetch_all_markets: bool = False,
    ) -> dict[int, dict]:
        """
        Mackolik maç listesi için Sofascore event_id'lerini bul
        ve oranları ekle.

        Eşleştirme stratejisi:
          1. Bulk 1X2 oranlarını çek (tek istek)
          2. Her Mackolik maçı için bulk event_id listesini tara
          3. Bulunan event_id'ler için meta + (opsiyonel) tüm marketleri çek
          4. Takım adı benzerliği ile maç eşleştir

        Returns: {mac_id: {"event_id": int, "markets": [SofaMarket]}}
        """
        from .matcher import similarity

        bulk = self.fetch_bulk_1x2(date)
        if not bulk:
            logger.warning("Sofascore bulk odds boş, Sofascore verisi atlanıyor.")
            return {}

        # Bulk event_id listesi — meta çekmeden önce
        all_event_ids = list(bulk.keys())
        logger.info("Bulk'ta %d event_id var", len(all_event_ids))

        # Mackolik maç zamanlarına göre candidate'leri daralt
        # Her Mackolik maçı için en iyi Sofascore eşleşmesini bul
        result: dict[int, dict] = {}
        meta_cache: dict[int, SofaEventMeta] = {}

        # Sadece gerekli meta'ları çek (Mackolik maç sayısı kadar)
        # Strateji: bulk'taki tüm event_id'ler için meta çekmek yerine
        # sadece Mackolik'te bulunan takım adlarını kullanarak arama yap
        for mac in mac_matches:
            best_eid:   Optional[int]   = None
            best_score: float           = 0.0

            for eid in all_event_ids:
                # Meta cache'de yoksa çek
                if eid not in meta_cache:
                    meta = self.fetch_event_meta(eid)
                    meta_cache[eid] = meta
                else:
                    meta = meta_cache[eid]

                if meta is None:
                    continue

                h = similarity(mac.home_team, meta.home_team)
                a = similarity(mac.away_team, meta.away_team)
                score = (h + a) / 2

                if score > best_score and score >= 0.5:
                    best_score = score
                    best_eid   = eid

            if best_eid:
                logger.info(
                    "  ✓ mac_id=%-8d ↔ sofa_event_id=%-10d [%.2f] %s vs %s",
                    mac.mac_id, best_eid, best_score,
                    mac.home_team, mac.away_team,
                )
                if fetch_all_markets:
                    markets = self.fetch_all_markets(best_eid)
                else:
                    markets = [bulk[best_eid]]

                result[mac.mac_id] = {
                    "event_id": best_eid,
                    "markets":  markets,
                }
            else:
                logger.debug(
                    "  ✗ mac_id=%-8d eşleşmedi: %s vs %s",
                    mac.mac_id, mac.home_team, mac.away_team,
                )

        logger.info(
            "Sofascore enrich tamamlandı: %d / %d maç eşleşti",
            len(result), len(mac_matches),
        )
        return result
