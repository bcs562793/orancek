"""
scraper/mackolik.py
───────────────────
Maç listesi + özet oranlar : https://vd.mackolik.com/livedata?date=DD/MM/YYYY
  → "e" anahtarı: canlı event'ler (duplicate mac_id içerebilir)
  → "m" anahtarı: o günün TÜM maçları + 1X2 + KG/Alt-Üst oranları

Detay oranlar (27 market): https://arsiv.mackolik.com/Mac/{mac_id}/

m array index haritası:
  [0]  mac_id
  [1]  ev_takım_id
  [2]  ev_adı
  [3]  dep_id
  [4]  dep_adı
  [5]  durum_kod  (4=MS, 9=Ert, 0=oynanmadı/planlandı)
  [6]  durum_yazı ("MS", "Ert.", "")
  [7]  skor       ("3-1")
  [8-11] ?
  [12] ev_skor
  [13] dep_skor
  [14] iddaa_mac_id  (arsiv maç ref ID)
  [15] {flag_nesnesi}
  [16] saat       ("15:00")
  [17] ?
  [18] oran_1     (1X2 ev sahibi)
  [19] oran_X     (beraberlik)
  [20] oran_2     (deplasman)
  [21] oran_kg_var / alt-üst  (teyit gerekiyor)
  [22] oran_kg_yok / alt-üst
  [23] ?
  [24-28] "0.0" (oran değişim?)
  [29] ev_final_skor
  [30] dep_final_skor
  [31] iy_ev_skor
  [32] iy_dep_skor
  [33] null
  [34] ?
  [35] tarih ("29/03/2026")
  [36] [ülke_id, ülke_adı, lig_id, lig_adı, sezon_id, sezon, ?, ülke_flag, ?, lig_kodu, ?, spor_id]
  [37] iddaa_flag (1=var, 0=yok)
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
MATCH_URL    = "https://arsiv.mackolik.com/Mac/{mac_id}/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
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

# m array index sabitleri
IDX_M_MAC_ID       = 0
IDX_M_EV_ID        = 1
IDX_M_EV_ADI       = 2
IDX_M_DEP_ID       = 3
IDX_M_DEP_ADI      = 4
IDX_M_DURUM_KOD    = 5
IDX_M_DURUM        = 6
IDX_M_SKOR         = 7
IDX_M_EV_SKOR      = 12
IDX_M_DEP_SKOR     = 13
IDX_M_IDDAA_REF    = 14   # arsiv.mackolik.com detay ref ID
IDX_M_SAAT         = 16
IDX_M_ORAN_1       = 18
IDX_M_ORAN_X       = 19
IDX_M_ORAN_2       = 20
IDX_M_ORAN_4       = 21   # KG Var? / Alt-Üst?
IDX_M_ORAN_5       = 22   # KG Yok? / Alt-Üst?
IDX_M_FINAL_EV     = 29   # tam skor ev sahibi
IDX_M_FINAL_DEP    = 30   # tam skor deplasman
IDX_M_IY_EV        = 31   # ilk yarı ev sahibi
IDX_M_IY_DEP       = 32   # ilk yarı deplasman
IDX_M_TARIH        = 35
IDX_M_LIG_META     = 36   # [ülke_id, ülke_adı, lig_id, lig_adı, ...]
IDX_M_IDDAA_FLAG   = 37   # 1=iddaa var


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
    mac_id:        int
    slug:          str
    home_team:     str
    away_team:     str
    league:        str
    league_code:   str
    country:       str
    match_time:    str
    match_date:    str
    has_iddaa:     bool
    iddaa_ref_id:  int          # arsiv.mackolik.com ref
    status:        str          # "MS", "Ert.", "", ...
    home_score:    Optional[int]
    away_score:    Optional[int]
    ht_home_score: Optional[int]   # ilk yarı ev sahibi skoru
    ht_away_score: Optional[int]   # ilk yarı deplasman skoru
    # Özet oranlar (m anahtarından)
    odds_1:        Optional[float]
    odds_x:        Optional[float]
    odds_2:        Optional[float]
    odds_extra_1:  Optional[float]   # KG/Alt-Üst ?
    odds_extra_2:  Optional[float]


@dataclass
class MatchOdds:
    mac_id:     int
    slug:       str
    home_team:  str
    away_team:  str
    league:     str
    league_code: str
    country:    str
    match_time: str
    match_date: str
    status:     str
    home_score: Optional[int]
    away_score: Optional[int]
    ht_home_score: Optional[int]   # ilk yarı ev sahibi skoru
    ht_away_score: Optional[int]   # ilk yarı deplasman skoru
    # Özet (livedata'dan)
    odds_1:     Optional[float]
    odds_x:     Optional[float]
    odds_2:     Optional[float]
    # Detay marketler (arsiv'den)
    markets:    list[Market] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "mac_id":      self.mac_id,
            "slug":        self.slug,
            "home_team":   self.home_team,
            "away_team":   self.away_team,
            "league":      self.league,
            "league_code": self.league_code,
            "country":     self.country,
            "match_time":  self.match_time,
            "match_date":  self.match_date,
            "status":      self.status,
            "home_score":  self.home_score,
            "away_score":  self.away_score,
            "ht_home_score": self.ht_home_score,
            "ht_away_score": self.ht_away_score,
            "odds_1":      self.odds_1,
            "odds_x":      self.odds_x,
            "odds_2":      self.odds_2,
            "markets":     [m.to_dict() for m in self.markets],
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
                logger.warning("Deneme %d/%d: %s → %s", attempt, self.max_retries, url, exc)
                if attempt == self.max_retries:
                    raise
                time.sleep(self.delay * attempt)
        raise RuntimeError("Ulaşılamaz")


# ─── Listing: vd.mackolik.com/livedata → m anahtarı ──────────────────────────

def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def fetch_listings(session: MackolikSession, date: str) -> list[MatchListing]:
    """
    vd.mackolik.com/livedata?date=DD/MM/YYYY → m anahtarındaki maç listesi.

    m anahtarı o günün TÜM maçlarını 1X2 oranlarıyla birlikte içeriyor.
    """
    dt       = datetime.strptime(date, "%Y-%m-%d")
    api_date = dt.strftime("%d/%m/%Y")
    logger.info("Mackolik livedata: %s?date=%s", LIVEDATA_URL, api_date)

    try:
        resp = session.get(LIVEDATA_URL, params={"date": api_date})
    except requests.RequestException as exc:
        logger.error("livedata çekilemedi: %s", exc)
        return []

    try:
        data = resp.json()
    except ValueError:
        logger.error("JSON parse hatası:\n%s", resp.text[:500])
        return []

    m_data = data.get("m", [])
    if not m_data:
        logger.warning("'m' anahtarı boş. Mevcut anahtarlar: %s", list(data.keys()))
        return []

    listings: list[MatchListing] = []
    seen: set[int] = set()

    for row in m_data:
        if not isinstance(row, list) or len(row) < 38:
            continue

        try:
            mac_id = int(row[IDX_M_MAC_ID])
        except (TypeError, ValueError):
            continue

        if mac_id in seen:
            continue
        seen.add(mac_id)

        lig_meta = row[IDX_M_LIG_META] if len(row) > IDX_M_LIG_META else []

        # Sadece futbol (spor_id=1) — basketbol, tenis vb. atla
        spor_id = lig_meta[11] if isinstance(lig_meta, list) and len(lig_meta) > 11 else None
        if spor_id is not None and spor_id != 1:
            continue

        listings.append(MatchListing(
            mac_id=mac_id,
            slug=str(mac_id),           # slug redirect ile alınır
            home_team=str(row[IDX_M_EV_ADI]  or "").strip(),
            away_team=str(row[IDX_M_DEP_ADI] or "").strip(),
            league=str(lig_meta[3] if len(lig_meta) > 3 else ""),
            league_code=str(lig_meta[9] if len(lig_meta) > 9 else ""),
            country=str(lig_meta[1] if len(lig_meta) > 1 else ""),
            match_time=str(row[IDX_M_SAAT] or "")[:5],
            match_date=date,
            has_iddaa=bool(row[IDX_M_IDDAA_FLAG]),
            iddaa_ref_id=_safe_int(row[IDX_M_IDDAA_REF]) or 0,
            status=str(row[IDX_M_DURUM] or ""),
            home_score=_safe_int(row[IDX_M_EV_SKOR]),
            away_score=_safe_int(row[IDX_M_DEP_SKOR]),
            ht_home_score=_safe_int(row[IDX_M_IY_EV])  if len(row) > IDX_M_IY_EV  else None,
            ht_away_score=_safe_int(row[IDX_M_IY_DEP]) if len(row) > IDX_M_IY_DEP else None,
            odds_1=_safe_float(row[IDX_M_ORAN_1]),
            odds_x=_safe_float(row[IDX_M_ORAN_X]),
            odds_2=_safe_float(row[IDX_M_ORAN_2]),
            odds_extra_1=_safe_float(row[IDX_M_ORAN_4]),
            odds_extra_2=_safe_float(row[IDX_M_ORAN_5]),
        ))

    iddaa_count = sum(1 for l in listings if l.has_iddaa)
    total_raw   = len([r for r in m_data if isinstance(r, list) and len(r) >= 38])
    logger.info(
        "%s: %d futbol maçı listelendi (%d iddaa) — toplam %d maçtan futbol filtresi uygulandı",
        date, len(listings), iddaa_count, total_raw
    )
    return listings


# ─── Detay: arsiv.mackolik.com/Mac/{mac_id}/ ─────────────────────────────────

def fetch_match_detail(
    session:    MackolikSession,
    listing:    MatchListing,
    match_date: str,
) -> Optional[MatchOdds]:
    """
    /Mac/{mac_id}/ → redirect → slug → 27 iddaa marketi.
    """
    url = MATCH_URL.format(mac_id=listing.mac_id)
    try:
        resp = session.get(url)
    except requests.RequestException as exc:
        logger.error("mac_id=%d çekilemedi: %s", listing.mac_id, exc)
        return None

    # Slug'ı final URL'den al
    m = re.search(r"/Mac/\d+/([^/?#]+)", resp.url)
    slug = m.group(1) if m else str(listing.mac_id)

    markets = _parse_odds_from_js(resp.text)
    if not markets:
        markets = _parse_odds_from_divs(BeautifulSoup(resp.text, "html.parser"))

    return MatchOdds(
        mac_id=listing.mac_id,
        slug=slug,
        home_team=listing.home_team,
        away_team=listing.away_team,
        league=listing.league,
        league_code=listing.league_code,
        country=listing.country,
        match_time=listing.match_time,
        match_date=match_date,
        status=listing.status,
        home_score=listing.home_score,
        away_score=listing.away_score,
        ht_home_score=listing.ht_home_score,
        ht_away_score=listing.ht_away_score,
        odds_1=listing.odds_1,
        odds_x=listing.odds_x,
        odds_2=listing.odds_2,
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


# ─── Ana Scraper ──────────────────────────────────────────────────────────────

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
        fetch_detail: bool = True,     # False → sadece listing + özet oranlar
    ) -> tuple[list[MatchOdds], list[dict]]:

        listings = fetch_listings(self.session, date)
        if not listings:
            logger.warning("%s için maç bulunamadı.", date)
            return [], []

        if iddaa_only:
            filtered = [l for l in listings if l.has_iddaa]
            logger.info("İddaa filtresi: %d / %d", len(filtered), len(listings))
        else:
            filtered = listings

        if not filtered:
            return [], []

        if dry_run:
            logger.info("[DRY RUN] %d maç:", len(filtered))
            for l in filtered:
                logger.info(
                    "  mac_id=%-8d  %-30s vs %-30s  1=%-5s X=%-5s 2=%-5s  %s",
                    l.mac_id, l.home_team, l.away_team,
                    l.odds_1 or "-", l.odds_x or "-", l.odds_2 or "-",
                    l.league,
                )
            return [], []

        if not fetch_detail:
            # Sadece listing bilgisi ile MatchOdds döndür (detay çekmez)
            results = [
                MatchOdds(
                    mac_id=l.mac_id, slug=str(l.mac_id),
                    home_team=l.home_team, away_team=l.away_team,
                    league=l.league, league_code=l.league_code,
                    country=l.country, match_time=l.match_time,
                    match_date=date, status=l.status,
                    home_score=l.home_score, away_score=l.away_score,
                    ht_home_score=l.ht_home_score, ht_away_score=l.ht_away_score,
                    odds_1=l.odds_1, odds_x=l.odds_x, odds_2=l.odds_2,
                    markets=[],
                )
                for l in filtered
            ]
            return results, []

        results: list[MatchOdds] = []
        errors:  list[dict]      = []

        for idx, listing in enumerate(filtered, 1):
            logger.info("[%d/%d] mac_id=%d  %s vs %s  [1=%s X=%s 2=%s]",
                        idx, len(filtered), listing.mac_id,
                        listing.home_team, listing.away_team,
                        listing.odds_1 or "-", listing.odds_x or "-", listing.odds_2 or "-")

            match_obj = fetch_match_detail(self.session, listing, date)

            if match_obj is None:
                errors.append({"mac_id": listing.mac_id, "error": "fetch failed"})
                continue

            if not match_obj.markets:
                logger.warning("  mac_id=%d oran yok", listing.mac_id)

            logger.info("  ✓ %d market  slug=%s", len(match_obj.markets), match_obj.slug)
            results.append(match_obj)

        logger.info("Tamamlandı: %d başarılı, %d hatalı.", len(results), len(errors))
        return results, errors
