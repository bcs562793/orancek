"""
scraper/sofascore.py
────────────────────
Saf Sofascore client — Mackolik bağımlılığı yok.

Akış (günlük 2 ana istek):
  1. GET /sport/football/scheduled-events/{date}
       → tüm futbol maçları, takım adları, skor, ilk yarı skoru
  2. GET /sport/football/odds/1/{date}
       → bulk opening + closing 1X2 oranları (event_id ile birleştir)
  3. (opsiyonel) GET /event/{id}/odds/1/all  — sofa_all_markets=True ise
       → maç başına tüm marketler

Bot koruması:
  curl_cffi ile Chrome124 TLS parmak izi taklit edilir.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://www.sofascore.com/api/v1"

HEADERS = {
    "Accept":             "*/*",
    "Accept-Language":    "tr,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding":    "gzip, deflate, br",
    "Cache-Control":      "no-cache",
    "Pragma":             "no-cache",
    "Referer":            "https://www.sofascore.com/tr/football/",
    "sec-ch-ua":          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "x-requested-with":   "d1e946",
}


# ─── curl_cffi / requests fallback ────────────────────────────────────────────

def _make_session():
    try:
        from curl_cffi import requests as cffi_requests
        session = cffi_requests.Session(impersonate="chrome124")
        session.headers.update(HEADERS)
        logger.info("curl_cffi Session başlatıldı (Chrome124 TLS parmak izi)")
        return session, True
    except ImportError:
        import requests
        session = requests.Session()
        session.headers.update(HEADERS)
        logger.warning(
            "curl_cffi bulunamadı — standart requests kullanılıyor. "
            "GitHub Actions'da 403 alabilirsin."
        )
        return session, False


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
    event_id:      int
    home_team:     str
    home_team_id:  int
    away_team:     str
    away_team_id:  int
    tournament:    str
    tournament_id: int
    country:       str
    match_date:    str   # YYYY-MM-DD
    match_time:    str   # HH:MM UTC
    start_ts:      int
    home_score:    Optional[int]
    away_score:    Optional[int]
    ht_home_score: Optional[int]
    ht_away_score: Optional[int]
    status:        str


@dataclass
class SofaMatch:
    meta:    SofaEventMeta
    markets: list[SofaMarket] = field(default_factory=list)

    def to_dict(self) -> dict:
        m = self.meta
        return {
            "match_date":        m.match_date,
            "match_time":        m.match_time,
            "sofa_event_id":     m.event_id,
            "mac_id":            None,
            "home_team":         m.home_team,
            "away_team":         m.away_team,
            "tournament":        m.tournament,
            "country":           m.country,
            "league":            m.tournament,
            "home_score":        m.home_score,
            "away_score":        m.away_score,
            "ht_home_score":     m.ht_home_score,
            "ht_away_score":     m.ht_away_score,
            "status":            m.status,
            "sofascore_markets": [mk.to_dict() for mk in self.markets],
            "mackolik_markets":  [],
        }


# ─── Yardımcılar ──────────────────────────────────────────────────────────────

def frac_to_decimal(frac: str) -> Optional[float]:
    if not frac:
        return None
    frac = frac.strip()
    if frac.upper() in ("EVS", "EVENS"):
        return 2.0
    m = re.match(r"^(-?\d+)\s*/\s*(\d+)$", frac)
    if m:
        num, den = int(m.group(1)), int(m.group(2))
        return round(num / den + 1, 4) if den else None
    try:
        return round(float(frac), 4)
    except ValueError:
        return None


def _parse_market(raw: dict) -> SofaMarket:
    choices = [
        SofaChoice(
            name=c.get("name", ""),
            opening_odds=frac_to_decimal(c.get("initialFractionalValue", "")),
            closing_odds=frac_to_decimal(c.get("fractionalValue", "")),
            winning=c.get("winning"),
            change=c.get("change", 0),
        )
        for c in raw.get("choices", [])
    ]
    return SofaMarket(
        market_id=raw.get("marketId", 0),
        market_name=raw.get("marketName", ""),
        market_group=raw.get("marketGroup", ""),
        market_period=raw.get("marketPeriod", ""),
        choice_group=raw.get("choiceGroup"),
        choices=choices,
    )


def _parse_event_meta(raw: dict, date: str) -> Optional[SofaEventMeta]:
    """scheduled-events yanıtından SofaEventMeta üret."""
    try:
        home = raw["homeTeam"]
        away = raw["awayTeam"]
        tour = raw.get("tournament", {})
        cat  = tour.get("category", {})
        uniq = tour.get("uniqueTournament", {})

        ts = raw.get("startTimestamp", 0)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)

        # Tam skor
        h_score = raw.get("homeScore", {}).get("current")
        a_score = raw.get("awayScore", {}).get("current")

        # İlk yarı skoru — period1 alanı
        ht_h = raw.get("homeScore", {}).get("period1")
        ht_a = raw.get("awayScore", {}).get("period1")

        return SofaEventMeta(
            event_id=raw["id"],
            home_team=home.get("name", ""),
            home_team_id=home.get("id", 0),
            away_team=away.get("name", ""),
            away_team_id=away.get("id", 0),
            tournament=uniq.get("name") or tour.get("name", ""),
            tournament_id=uniq.get("id", 0),
            country=cat.get("name", ""),
            match_date=date,
            match_time=dt.strftime("%H:%M"),
            start_ts=ts,
            home_score=h_score,
            away_score=a_score,
            ht_home_score=ht_h,
            ht_away_score=ht_a,
            status=raw.get("status", {}).get("type", "unknown"),
        )
    except (KeyError, TypeError) as exc:
        logger.debug("Event meta parse hatası: %s", exc)
        return None


# ─── HTTP Session ──────────────────────────────────────────────────────────────

class SofascoreSession:
    def __init__(self, request_delay: float = 0.5, max_retries: int = 3):
        self.delay       = request_delay
        self.max_retries = max_retries
        self._session, self._using_cffi = _make_session()

    def get(self, path: str, **kwargs):
        url = f"{BASE_URL}{path}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=20, **kwargs)

                if resp.status_code == 403:
                    logger.warning(
                        "403 [%d/%d] %s — %s",
                        attempt, self.max_retries, path,
                        "curl_cffi aktif ama yine de bloke, IP kara listesi olabilir."
                        if self._using_cffi else
                        "curl_cffi yok, requirements.txt'e ekle.",
                    )
                    time.sleep(self.delay * attempt * 4)
                    if attempt == self.max_retries:
                        return None
                    continue

                if resp.status_code == 429:
                    wait = self.delay * attempt * 6
                    logger.warning("429 rate limit: %s — %.0fs bekleniyor", path, wait)
                    time.sleep(wait)
                    if attempt == self.max_retries:
                        return None
                    continue

                resp.raise_for_status()
                time.sleep(self.delay)
                return resp

            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", "?")
                logger.warning(
                    "İstek hatası [%d/%d] %s (HTTP %s): %s",
                    attempt, self.max_retries, path, status, exc,
                )
                if attempt == self.max_retries:
                    return None
                time.sleep(self.delay * attempt * 2)

        return None


# ─── Ana Client ───────────────────────────────────────────────────────────────

class SofascoreClient:
    def __init__(self, request_delay: float = 0.5, max_retries: int = 3):
        self.session = SofascoreSession(request_delay, max_retries)

    def fetch_scheduled_events(self, date: str) -> dict[int, SofaEventMeta]:
        """
        GET /sport/football/scheduled-events/{date}
        Günün tüm futbol maçları — meta, skor, ilk yarı skoru.
        Returns: {event_id: SofaEventMeta}
        """
        logger.info("Sofascore scheduled-events: %s", date)
        resp = self.session.get(f"/sport/football/scheduled-events/{date}")
        if resp is None:
            return {}

        result: dict[int, SofaEventMeta] = {}
        for raw in resp.json().get("events", []):
            meta = _parse_event_meta(raw, date)
            if meta:
                result[meta.event_id] = meta

        logger.info("scheduled-events: %d maç alındı", len(result))
        return result

    def fetch_bulk_1x2(self, date: str) -> dict[int, SofaMarket]:
        """
        GET /sport/football/odds/1/{date}
        Tüm günün opening + closing 1X2 oranları — tek istekte.
        Returns: {event_id: SofaMarket}
        """
        logger.info("Sofascore bulk 1X2: %s", date)
        resp = self.session.get(f"/sport/football/odds/1/{date}")
        if resp is None:
            return {}

        result: dict[int, SofaMarket] = {}
        for eid_str, raw in resp.json().get("odds", {}).items():
            try:
                result[int(eid_str)] = _parse_market(raw)
            except (ValueError, TypeError):
                continue

        logger.info("bulk 1X2: %d maç oranı alındı", len(result))
        return result

    def fetch_all_markets(self, event_id: int) -> list[SofaMarket]:
        """
        GET /event/{id}/odds/1/all
        Maç başına tüm marketler — alt/üst, KG, handikap vb.
        """
        resp = self.session.get(f"/event/{event_id}/odds/1/all")
        if resp is None:
            return []
        try:
            return [_parse_market(m) for m in resp.json().get("markets", [])]
        except Exception as exc:
            logger.debug("all_markets parse hatası %d: %s", event_id, exc)
            return []


# ─── Yüksek seviye Scraper ────────────────────────────────────────────────────

class SofascoreScraper:
    """
    main.py'nin beklediği arayüz.

    scrape_date() akışı:
      1. scheduled-events  → meta + skor + iy skoru   (1 istek)
      2. bulk 1X2          → opening/closing oranlar   (1 istek)
      3. event_id ile birleştir — maç başına 0 ek istek
      4. fetch_all_markets → tüm marketler (N istek — sofa_all_markets=True ise)
    """

    def __init__(self, request_delay: float = 0.5, max_retries: int = 3):
        self._client = SofascoreClient(request_delay, max_retries)

    def fetch_scheduled_events(self, date: str) -> dict:
        """Dry-run için."""
        return self._client.fetch_scheduled_events(date)

    def scrape_date(self, date: str, fetch_all_markets: bool = False) -> list[SofaMatch]:
        logger.info("── Sofascore scrape: %s ──────────────────────────", date)

        # 1. Meta + skor + ilk yarı skoru (1 istek)
        events = self._client.fetch_scheduled_events(date)
        if not events:
            logger.warning("scheduled-events boş — veri yok.")
            return []

        # 2. Bulk 1X2 oranları (1 istek)
        bulk = self._client.fetch_bulk_1x2(date)

        oranlı   = sum(1 for eid in events if eid in bulk)
        orансız  = len(events) - oranlı
        logger.info(
            "Birleştirme: %d maç — %d oranlı, %d oran yok",
            len(events), oranlı, orансız,
        )

        # 3. Birleştir + (opsiyonel) tüm marketler
        matches: list[SofaMatch] = []
        for event_id, meta in events.items():
            base_market = bulk.get(event_id)

            if fetch_all_markets:
                # Her maç için 1 ek istek — sofa_all_markets=True ise
                markets = self._client.fetch_all_markets(event_id)
                if not markets and base_market:
                    markets = [base_market]
            else:
                markets = [base_market] if base_market else []

            matches.append(SofaMatch(meta=meta, markets=markets))

        logger.info("scrape_date tamamlandı: %d maç", len(matches))
        return matches
