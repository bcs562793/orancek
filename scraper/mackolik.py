"""
scraper/mackolik.py
───────────────────
arsiv.mackolik.com — Tarih bazlı maç listesi + İddaa oranları çekici.

Akış:
  1. https://arsiv.mackolik.com/Maclar/{tarih}  →  o günün tüm maçları
  2. Her lig / alt-lig altındaki maçları filtrele (iddaa ikonuna göre)
  3. Her maç için https://arsiv.mackolik.com/Mac/{mac_id}/{slug} → oranlar
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# ─── Sabitler ─────────────────────────────────────────────────────────────────

BASE_URL      = "https://arsiv.mackolik.com"
LISTING_URL   = BASE_URL + "/Maclar/{date}"        # date = YYYY-MM-DD
MATCH_URL     = BASE_URL + "/Mac/{mac_id}/{slug}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Referer":         BASE_URL + "/",
}

IDDAA_IMG_PATTERN = re.compile(r"iddaa|mbs", re.IGNORECASE)
MAC_LINK_PATTERN  = re.compile(r"/Mac/(\d+)/([^\"'\s?#]+)", re.IGNORECASE)

ODDS_DIALOG_PATTERN = re.compile(
    r"openOddsDialog\("
    r"'(\d+)',\s*"           # bookmaker_id   (1)
    r"'([^']+)',\s*"         # market_name    (2)
    r"\[([^\]]+)\],\s*"      # outcome_names  (3)
    r"\[([^\]]+)\],\s*"      # odds_values    (4)
    r"'(\d+)',\s*"           # market_code    (5)
    r"'[^']+',\s*"           # fn_name (skip)
    r"'(\d+)',\s*"           # match_ref_id   (6)
    r"'(\d+)',\s*"           # bet_id         (7)
    r"\[([^\]]+)\]"          # option_ids     (8)
    r"\)"
)

# ─── Veri Modelleri ───────────────────────────────────────────────────────────

@dataclass
class Outcome:
    name: str
    odds: Optional[float]   # '-' → None

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
    """Listing sayfasından elde edilen maç özeti."""
    mac_id:      int
    slug:        str
    home_team:   str
    away_team:   str
    league:      str
    sub_league:  str
    match_time:  str          # "18:00" gibi
    has_iddaa:   bool = False


@dataclass
class MatchOdds:
    """Maç detay sayfasından elde edilen tam oran seti."""
    mac_id:      int
    slug:        str
    home_team:   str
    away_team:   str
    league:      str
    sub_league:  str
    match_time:  str
    match_date:  str          # YYYY-MM-DD
    markets:     list[Market] = field(default_factory=list)

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


# ─── HTTP Yardımcısı ──────────────────────────────────────────────────────────

class MackolikSession:
    def __init__(self, request_delay: float = 1.2, max_retries: int = 3):
        self.delay       = request_delay
        self.max_retries = max_retries
        self._session    = requests.Session()
        self._session.headers.update(HEADERS)

    def get(self, url: str) -> requests.Response:
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=20)
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp
            except requests.RequestException as exc:
                logger.warning("Deneme %d/%d başarısız: %s → %s", attempt, self.max_retries, url, exc)
                if attempt == self.max_retries:
                    raise
                time.sleep(self.delay * attempt)
        raise RuntimeError("Buraya ulaşılmamalı")


# ─── Listing Sayfası Parser ───────────────────────────────────────────────────

def parse_listing_page(html: str, date: str) -> list[MatchListing]:
    """
    /Maclar/{date} sayfasını parse eder.

    Sayfa yapısı (gözlemlenen):
      <div class="league-title"> Lig Adı </div>
      <div class="sub-league-title"> Alt Lig </div>
      <div class="match-row" ...>
        <a href="/Mac/{id}/{slug}"> ... </a>
        [iddaa ikonu varsa] <img src="...mbs2.png">
      </div>

    Seçiciler site güncellemelerinde değişebilir — loglara bak.
    """
    soup   = BeautifulSoup(html, "html.parser")
    matches: list[MatchListing] = []

    current_league     = "Bilinmeyen Lig"
    current_sub_league = ""

    # Tüm anlamlı elementleri sırayla tara
    container = (
        soup.find("div", id="matches-list")
        or soup.find("div", class_=re.compile(r"matches|maclar|content", re.I))
        or soup.body
    )

    if container is None:
        logger.error("Listing sayfasında içerik konteyneri bulunamadı.")
        return []

    for el in container.descendants:
        if not isinstance(el, Tag):
            continue

        cls = " ".join(el.get("class", []))

        # ── Lig başlığı ──
        if re.search(r"league.?title|lig.?baslik|competition.?name", cls, re.I):
            current_league     = el.get_text(strip=True)
            current_sub_league = ""
            continue

        # ── Alt-lig başlığı ──
        if re.search(r"sub.?league|alt.?lig|group.?name", cls, re.I):
            current_sub_league = el.get_text(strip=True)
            continue

        # ── Maç satırı ──
        if re.search(r"match.?row|mac.?satir|fixture|event", cls, re.I):
            listing = _parse_match_row(el, current_league, current_sub_league)
            if listing:
                matches.append(listing)
            continue

    # Fallback: sayfa yapısı tanınmadıysa tüm /Mac/ linklerini topla
    if not matches:
        logger.warning("Standart yapı tanınamadı, fallback link taraması yapılıyor.")
        matches = _fallback_link_scan(soup, current_league)

    logger.info("%s tarihi için %d maç listelendi.", date, len(matches))
    return matches


def _parse_match_row(row: Tag, league: str, sub_league: str) -> Optional[MatchListing]:
    """Tek maç satırını parse eder."""
    link = row.find("a", href=MAC_LINK_PATTERN)
    if not link:
        return None

    m = MAC_LINK_PATTERN.search(link["href"])
    if not m:
        return None

    mac_id = int(m.group(1))
    slug   = m.group(2)

    # Takım adları — farklı class'larla gelebilir
    home_el = (
        row.find(class_=re.compile(r"home|ev.?sahibi|local", re.I))
        or row.find("span", {"data-side": "home"})
    )
    away_el = (
        row.find(class_=re.compile(r"away|deplasman|visitor", re.I))
        or row.find("span", {"data-side": "away"})
    )

    # Takım adı bulunamazsa slug'dan türet
    if not home_el or not away_el:
        parts      = slug.replace("-", " ").split("vs") if "vs" in slug.lower() else slug.split("-")
        home_team  = parts[0].strip().title() if len(parts) > 0 else slug
        away_team  = parts[-1].strip().title() if len(parts) > 1 else ""
    else:
        home_team = home_el.get_text(strip=True)
        away_team = away_el.get_text(strip=True)

    # Saat
    time_el   = row.find(class_=re.compile(r"time|saat|hour", re.I))
    match_time = time_el.get_text(strip=True) if time_el else ""

    # İddaa ikonu kontrolü
    has_iddaa = bool(
        row.find("img", src=IDDAA_IMG_PATTERN)
        or row.find(class_=re.compile(r"iddaa|mbs", re.I))
        or row.find("a", href=re.compile(r"iddaa", re.I))
    )

    return MatchListing(
        mac_id=mac_id,
        slug=slug,
        home_team=home_team,
        away_team=away_team,
        league=league,
        sub_league=sub_league,
        match_time=match_time,
        has_iddaa=has_iddaa,
    )


def _fallback_link_scan(soup: BeautifulSoup, default_league: str) -> list[MatchListing]:
    """
    Sayfa yapısı tanınmadığında tüm /Mac/ linklerini topla.
    Bu durumda iddaa filtresi uygulanamaz — tüm maçlar alınır,
    detay sayfasında oran yoksa boş market listesiyle devam edilir.
    """
    seen    = set()
    results = []

    for a in soup.find_all("a", href=MAC_LINK_PATTERN):
        m = MAC_LINK_PATTERN.search(a["href"])
        if not m:
            continue
        mac_id = int(m.group(1))
        if mac_id in seen:
            continue
        seen.add(mac_id)

        # Yakın lig başlığını bulmaya çalış
        league = default_league
        for parent in a.parents:
            header = parent.find_previous(
                lambda t: isinstance(t, Tag)
                and t.name in ("h2", "h3", "h4")
                and t.get_text(strip=True)
            )
            if header:
                league = header.get_text(strip=True)
                break

        results.append(MatchListing(
            mac_id=mac_id,
            slug=m.group(2),
            home_team="",
            away_team="",
            league=league,
            sub_league="",
            match_time="",
            has_iddaa=True,   # fallback'te hepsini dene
        ))

    return results


# ─── Maç Detay Sayfası Parser ─────────────────────────────────────────────────

def parse_match_detail(html: str, listing: MatchListing, match_date: str) -> MatchOdds:
    """
    /Mac/{mac_id}/{slug} sayfasını parse eder.
    Oranlar openOddsDialog() JS çağrılarından çekilir.
    """
    markets = _parse_odds_from_js(html)

    if not markets:
        logger.debug("JS parse başarısız, div fallback deneniyor — mac_id=%d", listing.mac_id)
        markets = _parse_odds_from_divs(BeautifulSoup(html, "html.parser"))

    # Takım adları detail sayfasında daha güvenilir olabilir
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
    """openOddsDialog() argümanlarından market verisi çeker."""
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
    """Fallback: div.md bloklarını parse eder."""
    markets: list[Market] = []

    for block in soup.select("div.md"):
        title_el = block.select_one("div.detail-title")
        if not title_el:
            continue

        # Market adı + kodu
        code_span   = title_el.select_one("span")
        market_code = ""
        if code_span:
            text_parts  = code_span.get_text(strip=True).split()
            market_code = text_parts[0] if text_parts else ""
            code_span.extract()

        market_name = title_el.get_text(strip=True)

        outcomes = []
        name_els = block.select("div.sgoutcome-name")
        for name_el in name_els:
            name     = name_el.get_text(strip=True)
            val_el   = name_el.find_next_sibling("div", class_="sgoutcome-value")
            val_text = val_el.get_text(strip=True) if val_el else "-"
            try:
                odd = float(val_text)
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
    """Detay sayfasından takım adlarını daha güvenilir şekilde çeker."""
    if listing.home_team and listing.away_team:
        return listing.home_team, listing.away_team

    soup = BeautifulSoup(html, "html.parser")

    # Yaygın selector kombinasyonları dene
    selectors = [
        ("div.home-team", "div.away-team"),
        ("span.home",     "span.away"),
        ("td.home",       "td.away"),
    ]
    for home_sel, away_sel in selectors:
        h = soup.select_one(home_sel)
        a = soup.select_one(away_sel)
        if h and a:
            return h.get_text(strip=True), a.get_text(strip=True)

    # Son çare: slug'dan türet
    parts = listing.slug.split("-")
    mid   = len(parts) // 2
    return " ".join(parts[:mid]).title(), " ".join(parts[mid:]).title()


# ─── Ana Scraper Sınıfı ───────────────────────────────────────────────────────

class MackolikScraper:
    def __init__(self, request_delay: float = 1.5, max_retries: int = 3):
        self.session = MackolikSession(request_delay=request_delay, max_retries=max_retries)

    def scrape_date(
        self,
        date: str,                    # YYYY-MM-DD
        iddaa_only: bool = True,
        dry_run: bool = False,
    ) -> tuple[list[MatchOdds], list[dict]]:
        """
        Belirli bir tarihteki tüm iddaa oranlarını çeker.

        Returns:
            (başarılı_maçlar, hatalar)
        """
        # 1) Listing sayfasını çek
        listing_url = LISTING_URL.format(date=date)
        logger.info("Listing sayfası çekiliyor: %s", listing_url)

        try:
            resp = self.session.get(listing_url)
        except requests.RequestException as exc:
            logger.error("Listing sayfası çekilemedi: %s", exc)
            return [], [{"url": listing_url, "error": str(exc)}]

        listings = parse_listing_page(resp.text, date)

        # 2) İddaa filtresi
        if iddaa_only:
            filtered = [l for l in listings if l.has_iddaa]
            logger.info(
                "İddaa filtresi: %d / %d maç seçildi.",
                len(filtered), len(listings)
            )
        else:
            filtered = listings

        if not filtered:
            logger.warning("%s için iddaa olan maç bulunamadı.", date)
            return [], []

        if dry_run:
            logger.info("[DRY RUN] %d maç bulundu, detaylar çekilmeyecek.", len(filtered))
            return [], []

        # 3) Her maçın detayını çek
        results: list[MatchOdds] = []
        errors:  list[dict]      = []

        for idx, listing in enumerate(filtered, 1):
            match_url = MATCH_URL.format(mac_id=listing.mac_id, slug=listing.slug)
            logger.info(
                "[%d/%d] %s vs %s — %s",
                idx, len(filtered),
                listing.home_team or listing.mac_id,
                listing.away_team or listing.slug,
                match_url,
            )

            try:
                resp      = self.session.get(match_url)
                match_obj = parse_match_detail(resp.text, listing, date)

                if not match_obj.markets:
                    logger.warning("mac_id=%d için oran bulunamadı, atlanıyor.", listing.mac_id)
                    continue

                logger.info(
                    "  ✓ %d market bulundu.", len(match_obj.markets)
                )
                results.append(match_obj)

            except requests.RequestException as exc:
                logger.error("mac_id=%d çekilemedi: %s", listing.mac_id, exc)
                errors.append({
                    "mac_id": listing.mac_id,
                    "url":    match_url,
                    "error":  str(exc),
                })

        logger.info(
            "Tamamlandı: %d başarılı, %d hatalı.",
            len(results), len(errors)
        )
        return results, errors
