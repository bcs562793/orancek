"""
scraper/mackolik.py
───────────────────
Maç listesi : https://vd.mackolik.com/livedata?date=DD/MM/YYYY  (JSON API)
Maç oranları: https://arsiv.mackolik.com/Mac/{mac_id}/{slug}     (HTML)
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
                logger.warning("Deneme %d/%d başarısız: %s → %s", attempt, self.max_retries, url, exc)
                if attempt == self.max_retries:
                    raise
                time.sleep(self.delay * attempt)
        raise RuntimeError("Buraya ulaşılmamalı")


def fetch_listings(session: MackolikSession, date: str) -> list[MatchListing]:
    """
    vd.mackolik.com/livedata?date=DD/MM/YYYY → maç listesi

    İlk çalışmada API yanıtının tam yapısı loglanır.
    Alan adları siteye göre değişirse _map_match() güncellenir.
    """
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

    # İlk çalışmada yapıyı görmek için tüm yanıtı logla
    logger.info("API yanıtı (ilk 3000 karakter):\n%s", str(data)[:3000])

    return _parse_livedata(data, date)


def _parse_livedata(data: dict | list, date: str) -> list[MatchListing]:
    listings: list[MatchListing] = []
    competitions = []

    if isinstance(data, list):
        competitions = data
    elif isinstance(data, dict):
        for key in ("competitions", "data", "leagues", "result", "matches", "events"):
            if key in data:
                competitions = data[key]
                logger.info("API ana anahtar: '%s'", key)
                break

        if not competitions:
            logger.warning("Tanınan API anahtarı yok. Mevcut: %s", list(data.keys()))
            return []

    # Düz maç listesi mi yoksa lig hiyerarşisi mi?
    if (
        isinstance(competitions, list)
        and competitions
        and isinstance(competitions[0], dict)
        and any(k in competitions[0] for k in ("home", "homeTeam", "homeName", "host"))
    ):
        logger.info("API: düz maç listesi (lig sarmalı yok)")
        for match in competitions:
            listing = _map_match(match, league="", sub_league="")
            if listing:
                listings.append(listing)
    else:
        # Lig → maç hiyerarşisi
        for comp in competitions:
            if not isinstance(comp, dict):
                continue

            league_name = (
                comp.get("name") or comp.get("competitionName")
                or comp.get("leagueName") or "Bilinmeyen Lig"
            )
            parent_name = (
                comp.get("parentName") or comp.get("countryName")
                or comp.get("country") or ""
            )
            full_league = f"{parent_name} - {league_name}" if parent_name else league_name

            matches = (
                comp.get("matches") or comp.get("events")
                or comp.get("fixtures") or []
            )

            for match in matches:
                listing = _map_match(match, league=full_league, sub_league=league_name)
                if listing:
                    listings.append(listing)

    logger.info("%s için %d maç listelendi.", date, len(listings))
    return listings


def _map_match(match: dict, league: str, sub_league: str) -> Optional[MatchListing]:
    mac_id = match.get("id") or match.get("matchId") or match.get("macId")
    if not mac_id:
        return None

    slug = match.get("slug") or match.get("url") or str(mac_id)
    if "/" in str(slug):
        slug = str(slug).rstrip("/").split("/")[-1]

    home_team = (
        match.get("home") or match.get("homeTeam")
        or match.get("homeName") or match.get("host") or ""
    )
    away_team = (
        match.get("away") or match.get("awayTeam")
        or match.get("awayName") or match.get("guest") or ""
    )

    match_time = (
        match.get("time") or match.get("matchTime")
        or match.get("startTime") or ""
    )
    if isinstance(match_time, str) and len(match_time) > 5:
        match_time = match_time[:5]

    # İddaa alanını kontrol et
    iddaa_keys = ("hasIddaa", "iddaa", "hasBet", "bettingAvailable", "mbs", "isMbs", "hasMbs")
    has_iddaa_key = any(k in match for k in iddaa_keys)

    if has_iddaa_key:
        has_iddaa = bool(
            match.get("hasIddaa") or match.get("iddaa") or match.get("hasBet")
            or match.get("bettingAvailable") or match.get("mbs") or match.get("isMbs")
            or match.get("hasMbs")
        )
    else:
        # Alan yoksa hepsini dahil et, detayda kontrol edilir
        has_iddaa = True

    return MatchListing(
        mac_id=int(mac_id),
        slug=str(slug),
        home_team=str(home_team),
        away_team=str(away_team),
        league=league,
        sub_league=sub_league,
        match_time=str(match_time),
        has_iddaa=has_iddaa,
    )


def parse_match_detail(html: str, listing: MatchListing, match_date: str) -> MatchOdds:
    markets = _parse_odds_from_js(html)

    if not markets:
        logger.debug("JS parse başarısız, div fallback — mac_id=%d", listing.mac_id)
        markets = _parse_odds_from_divs(BeautifulSoup(html, "html.parser"))

    home_team, away_team = _extract_team_names(html, listing)

    return MatchOdds(
        mac_id=listing.mac_id,
        slug=listing.slug,
        home_team=home_team,
        away_team=away_team,
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
        outcomes = []

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


def _extract_team_names(html: str, listing: MatchListing) -> tuple[str, str]:
    if listing.home_team and listing.away_team:
        return listing.home_team, listing.away_team

    soup = BeautifulSoup(html, "html.parser")

    for home_sel, away_sel in [
        ("div.home-team", "div.away-team"),
        ("span.home",     "span.away"),
        ("td.home",       "td.away"),
    ]:
        h = soup.select_one(home_sel)
        a = soup.select_one(away_sel)
        if h and a:
            return h.get_text(strip=True), a.get_text(strip=True)

    parts = listing.slug.split("-")
    mid   = len(parts) // 2
    return " ".join(parts[:mid]).title(), " ".join(parts[mid:]).title()


class MackolikScraper:
    def __init__(self, request_delay: float = 1.5, max_retries: int = 3):
        self.session = MackolikSession(request_delay=request_delay, max_retries=max_retries)

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
            logger.info("İddaa filtresi: %d / %d maç.", len(filtered), len(listings))
        else:
            filtered = listings

        if not filtered:
            logger.warning("%s için iddaa olan maç yok.", date)
            return [], []

        if dry_run:
            logger.info("[DRY RUN] %d maç bulundu:", len(filtered))
            for l in filtered:
                logger.info(
                    "  mac_id=%-8d  %-35s vs %-35s  %s",
                    l.mac_id, l.home_team, l.away_team, l.league
                )
            return [], []

        results: list[MatchOdds] = []
        errors:  list[dict]      = []

        for idx, listing in enumerate(filtered, 1):
            match_url = MATCH_URL.format(mac_id=listing.mac_id, slug=listing.slug)
            logger.info(
                "[%d/%d] mac_id=%d  %s vs %s",
                idx, len(filtered), listing.mac_id,
                listing.home_team or listing.slug, listing.away_team,
            )

            try:
                resp      = self.session.get(match_url)
                match_obj = parse_match_detail(resp.text, listing, date)

                if not match_obj.markets:
                    logger.warning("mac_id=%d oran yok, atlanıyor.", listing.mac_id)
                    continue

                logger.info("  ✓ %d market", len(match_obj.markets))
                results.append(match_obj)

            except requests.RequestException as exc:
                logger.error("mac_id=%d hata: %s", listing.mac_id, exc)
                errors.append({"mac_id": listing.mac_id, "url": match_url, "error": str(exc)})

        logger.info("Tamamlandı: %d başarılı, %d hatalı.", len(results), len(errors))
        return results, errors
