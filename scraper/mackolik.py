"""
scraper/mackolik.py
───────────────────
Maç listesi : https://vd.mackolik.com/livedata?date=DD/MM/YYYY
Maç oranları: https://arsiv.mackolik.com/Mac/{mac_id}/{slug}  (HTML)

vd.mackolik.com yanıt yapısı:
  {
    "e":  [[canlı_id, mac_id, ?, ülke_id, lig_id, lig_adı, lig_kodu,
             ev_id, ev_sahibi, dep_id, deplasman, saat, durum, ev_skor,
             dep_skor, ?, ?, ?, dakika], ...],
    "m":  { mac_id_str: [...] },   # iddaa market bilgisi
    "t":  { ... },
    "eId": ...
  }
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

LIVEDATA_URL = "https://vd.mackolik.com/livedata"
MATCH_URL    = "https://arsiv.mackolik.com/Mac/{mac_id}/{slug}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/html, */*",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Referer":         "https://arsiv.mackolik.com/",
}

ODDS_DIALOG_PATTERN = re.compile(
    r"openOddsDialog\("
    r"'(\d+)',\s*"
    r"'([^']+)',\s*"
    r"\[([^\]]+)\],\s*"
    r"\[([^\]]+)\],\s*"
    r"'(\d+)',\s*"
    r"'[^']+',\s*"
    r"'(\d+)',\s*"
    r"'(\d+)',\s*"
    r"\[([^\]]+)\]"
    r"\)"
)

# Array index sabitleri
IDX_MAC_ID   = 1
IDX_LIG_ADI  = 5
IDX_LIG_KODU = 6
IDX_EV_ADI   = 8
IDX_DEP_ADI  = 10
IDX_SAAT     = 11


# ─── Modeller ─────────────────────────────────────────────────────────────────

@dataclass
class Outcome:
    name: str
    odds: Optional[float]

    def to_dict(self) -> dict:
        return {"name": self.name, "odds": self.odds}


@dataclass
class Market:
    market_name:  str
    market_code:  str
    match_ref_id: str
    bet_id:       str
    outcomes:     list[Outcome] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "market_name":  self.market_name,
            "market_code":  self.market_code,
            "match_ref_id": self.match_ref_id,
            "bet_id":       self.bet_id,
            "outcomes":     [o.to_dict() for o in self.outcomes],
        }


@dataclass
class MatchListing:
    mac_id:     int
    slug:       str
    home_team:  str
    away_team:  str
    league:     str
    sub_league: str
    match_time: str
    has_iddaa:  bool = False


@dataclass
class MatchOdds:
    mac_id:     int
    slug:       str
    home_team:  str
    away_team:  str
    league:     str
    sub_league: str
    match_time: str
    match_date: str
    markets:    list[Market] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "mac_id":     self.mac_id,
            "slug":       self.slug,
            "home_team":  self.home_team,
            "away_team":  self.away_team,
            "league":     self.league,
            "sub_league": self.sub_league,
            "match_time": self.match_time,
            "match_date": self.match_date,
            "markets":    [m.to_dict() for m in self.markets],
        }


# ─── Session ──────────────────────────────────────────────────────────────────

class MackolikSession:
    def __init__(self, request_delay: float = 1.5, max_retries: int = 3):
        self.delay       = request_delay
        self.max_retries = max_retries
        self._session    = requests.Session()
        self._session.headers.update(HEADERS)

    def get(self, url: str, **kwargs) -> requests.Response:
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=20, **kwargs)
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp
            except requests.RequestException as exc:
                logger.warning(
                    "Deneme %d/%d başarısız: %s → %s",
                    attempt, self.max_retries, url, exc,
                )
                if attempt == self.max_retries:
                    raise
                time.sleep(self.delay * attempt)
        raise RuntimeError("Buraya ulaşılmamalı")


# ─── Listing ──────────────────────────────────────────────────────────────────

def fetch_listings(session: MackolikSession, date: str) -> list[MatchListing]:
    dt       = datetime.strptime(date, "%Y-%m-%d")
    api_date = dt.strftime("%d/%m/%Y")
    logger.info("Listing API: %s?date=%s", LIVEDATA_URL, api_date)

    try:
        resp = session.get(LIVEDATA_URL, params={"date": api_date})
    except requests.RequestException as exc:
        logger.error("Listing API çekilemedi: %s", exc)
        return []

    try:
        data = resp.json()
    except ValueError:
        logger.error("API JSON döndürmedi:\n%s", resp.text[:2000])
        return []

    return _parse_livedata(data, date)


def _parse_livedata(data: dict, date: str) -> list[MatchListing]:
    if not isinstance(data, dict):
        logger.error("Beklenmeyen API yanıt tipi: %s", type(data))
        return []

    events = data.get("e", [])
    if not events:
        logger.warning("'e' anahtarında veri yok. Anahtarlar: %s", list(data.keys()))
        return []

    # ── 'm' anahtarından iddaa mac_id seti oluştur ──
    m_data = data.get("m", {})
    iddaa_mac_ids: set[int] = set()

    if isinstance(m_data, dict) and m_data:
        for k in m_data:
            try:
                iddaa_mac_ids.add(int(k))
            except (ValueError, TypeError):
                pass
        logger.info("'m' anahtarından %d iddaa mac_id'si alındı.", len(iddaa_mac_ids))
    elif isinstance(m_data, list) and m_data:
        for item in m_data:
            try:
                iddaa_mac_ids.add(int(item))
            except (ValueError, TypeError):
                pass
        logger.info("'m' listesinden %d iddaa mac_id'si alındı.", len(iddaa_mac_ids))
    else:
        logger.warning(
            "'m' anahtarı boş veya tanımsız — "
            "tüm maçlar dahil edilecek, detay sayfasında oran kontrolü yapılacak."
        )

    # ── Event'leri parse et — duplicate'leri at (mac_id bazında) ──
    seen_mac_ids: set[int] = set()
    listings: list[MatchListing] = []

    for row in events:
        if not isinstance(row, list) or len(row) <= IDX_SAAT:
            continue

        try:
            mac_id = int(row[IDX_MAC_ID])
        except (ValueError, TypeError):
            continue

        # Duplicate kontrolü
        if mac_id in seen_mac_ids:
            continue
        seen_mac_ids.add(mac_id)

        home_team  = str(row[IDX_EV_ADI]   or "").strip()
        away_team  = str(row[IDX_DEP_ADI]  or "").strip()
        league     = str(row[IDX_LIG_ADI]  or "").strip()
        lig_kodu   = str(row[IDX_LIG_KODU] or "").strip()
        match_time = str(row[IDX_SAAT]     or "").strip()[:5]

        if iddaa_mac_ids:
            has_iddaa = mac_id in iddaa_mac_ids
        else:
            has_iddaa = True   # 'm' boşsa hepsini dene

        listings.append(MatchListing(
            mac_id=mac_id,
            slug=str(mac_id),
            home_team=home_team,
            away_team=away_team,
            league=league,
            sub_league=lig_kodu,
            match_time=match_time,
            has_iddaa=has_iddaa,
        ))

    logger.info(
        "%s için %d tekil maç listelendi (%d toplam event'ten).",
        date, len(listings), len(events),
    )
    return listings


# ─── Detay ────────────────────────────────────────────────────────────────────

def fetch_match_detail(
    session:    MackolikSession,
    listing:    MatchListing,
    match_date: str,
) -> Optional[MatchOdds]:
    """
    /Mac/{mac_id}/ ile çeker — sunucu gerçek slug'a redirect eder.
    Redirect URL'inden slug'ı çekip listing'e kaydeder.
    """
    url = f"https://arsiv.mackolik.com/Mac/{listing.mac_id}/"

    try:
        resp = session.get(url)
    except requests.RequestException as exc:
        logger.error("mac_id=%d çekilemedi: %s", listing.mac_id, exc)
        return None

    # Slug'ı final URL'den al
    m = re.search(r"/Mac/\d+/([^/?#]+)", resp.url)
    if m:
        listing.slug = m.group(1)

    return _parse_detail(resp.text, listing, match_date)


def _parse_detail(html: str, listing: MatchListing, match_date: str) -> MatchOdds:
    markets = _parse_odds_from_js(html)

    if not markets:
        logger.debug("JS parse başarısız, div fallback — mac_id=%d", listing.mac_id)
        markets = _parse_odds_from_divs(BeautifulSoup(html, "html.parser"))

    return MatchOdds(
        mac_id=listing.mac_id,
        slug=listing.slug,
        home_team=listing.home_team,
        away_team=listing.away_team,
        league=listing.league,
        sub_league=listing.sub_league,
        match_time=listing.match_time,
        match_date=match_date,
        markets=markets,
    )


def _parse_odds_from_js(html: str) -> list[Market]:
    markets: list[Market] = []
    seen: set[tuple] = set()

    for m in ODDS_DIALOG_PATTERN.finditer(html):
        _, market_name, names_raw, odds_raw, market_code, match_ref_id, bet_id, _ = m.groups()

        key = (market_code, bet_id)
        if key in seen:
            continue
        seen.add(key)

        outcome_names = [s.strip().strip("'\"") for s in names_raw.split(",")]
        odds_strs     = [s.strip().strip("'\"") for s in odds_raw.split(",")]

        outcomes = []
        for name, odd_str in zip(outcome_names, odds_strs):
            try:
                outcomes.append(Outcome(name=name, odds=float(odd_str)))
            except ValueError:
                outcomes.append(Outcome(name=name, odds=None))

        markets.append(Market(
            market_name=market_name,
            market_code=market_code,
            match_ref_id=match_ref_id,
            bet_id=bet_id,
            outcomes=outcomes,
        ))

    return markets


def _parse_odds_from_divs(soup: BeautifulSoup) -> list[Market]:
    markets: list[Market] = []

    for block in soup.select("div.md"):
        title_el = block.select_one("div.detail-title")
        if not title_el:
            continue

        code_span   = title_el.select_one("span")
        market_code = ""
        if code_span:
            parts       = code_span.get_text(strip=True).split()
            market_code = parts[0] if parts else ""
            code_span.extract()

        market_name = title_el.get_text(strip=True)
        outcomes    = []

        for name_el in block.select("div.sgoutcome-name"):
            name   = name_el.get_text(strip=True)
            val_el = name_el.find_next_sibling("div", class_="sgoutcome-value")
            val    = val_el.get_text(strip=True) if val_el else "-"
            try:
                odd = float(val)
            except ValueError:
                odd = None
            outcomes.append(Outcome(name=name, odds=odd))

        if outcomes:
            markets.append(Market(
                market_name=market_name,
                market_code=market_code,
                match_ref_id="",
                bet_id="",
                outcomes=outcomes,
            ))

    return markets


# ─── Scraper ──────────────────────────────────────────────────────────────────

class MackolikScraper:
    def __init__(self, request_delay: float = 1.5, max_retries: int = 3):
        self.session = MackolikSession(
            request_delay=request_delay,
            max_retries=max_retries,
        )

    def scrape_date(
        self,
        date: str,
        iddaa_only: bool = True,
        dry_run: bool = False,
    ) -> tuple[list[MatchOdds], list[dict]]:

        listings = fetch_listings(self.session, date)

        if not listings:
            logger.warning("%s için maç bulunamadı.", date)
            return [], []

        if iddaa_only:
            filtered = [l for l in listings if l.has_iddaa]
            logger.info(
                "İddaa filtresi: %d / %d maç seçildi.",
                len(filtered), len(listings),
            )
        else:
            filtered = listings

        if not filtered:
            logger.warning("%s için iddaa olan maç yok.", date)
            return [], []

        if dry_run:
            logger.info("[DRY RUN] %d maç listelendi:", len(filtered))
            for l in filtered:
                iddaa_flag = "✓ iddaa" if l.has_iddaa else "✗"
                logger.info(
                    "  mac_id=%-8d  %-30s vs %-30s  %-30s  %s",
                    l.mac_id, l.home_team, l.away_team, l.league, iddaa_flag,
                )
            return [], []

        results: list[MatchOdds] = []
        errors:  list[dict]      = []

        for idx, listing in enumerate(filtered, 1):
            logger.info(
                "[%d/%d] mac_id=%d  %s vs %s",
                idx, len(filtered),
                listing.mac_id, listing.home_team, listing.away_team,
            )

            match_obj = fetch_match_detail(self.session, listing, date)

            if match_obj is None:
                errors.append({"mac_id": listing.mac_id, "error": "fetch failed"})
                continue

            if not match_obj.markets:
                logger.warning("mac_id=%d oran yok, atlanıyor.", listing.mac_id)
                continue

            logger.info("  ✓ %d market  slug=%s", len(match_obj.markets), match_obj.slug)
            results.append(match_obj)

        logger.info("Tamamlandı: %d başarılı, %d hatalı.", len(results), len(errors))
        return results, errors
