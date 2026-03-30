"""
scraper/matcher.py
──────────────────
Sofascore (İngilizce/orijinal takım adı) ile Mackolik (Türkçe)
arasındaki maçları eşleştirir.

Strateji:
  1. Takım adlarını normalize et (küçük harf, noktalama kaldır, Türkçe→ASCII)
  2. Token intersection skoru hesapla
  3. Skor ≥ eşik → eşleşme kabul edilir
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Normalize ────────────────────────────────────────────────────────────────

# Yaygın kısaltma/eşdeğer çiftler (Mackolik Türkçesi → normalize form)
_ALIAS: dict[str, str] = {
    "fc": "",
    "fk": "",
    "sk": "",
    "bk": "",
    "if": "",
    "afc": "",
    "cf": "",
    "sc": "",
    "ac": "",
    "as": "",
    "ss": "",
    "rc": "",
    "cd": "",
    "rcd": "",
    "ud": "",
    "sd": "",
    "athletic": "athletic",
    "atletico": "atletico",
    "atlético": "atletico",
    "united": "united",
    "city": "city",
    "rovers": "rovers",
    "wanderers": "wanderers",
    "sporting": "sporting",
    "dynamo": "dynamo",
    "dinamo": "dynamo",
    "olimpija": "olimpija",
}

_TR_MAP = str.maketrans(
    "çğıöşüÇĞİÖŞÜ",
    "cgioszCGIOSU",  # basit ASCII dönüşüm
)


def normalize(name: str) -> set[str]:
    """
    Takım adını token setine dönüştürür.
    "Hamburger SV (K)" → {"hamburger", "sv", "k"}
    "Bayer 04 Leverkusen" → {"bayer", "04", "leverkusen"}
    """
    # Türkçe karakterler
    name = name.translate(_TR_MAP)
    # Unicode normalize
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Küçük harf
    name = name.lower()
    # Parantez içini de al
    name = re.sub(r"[^\w\s]", " ", name)
    # Token'lara ayır
    tokens = set(name.split())
    # Alias temizle (FC, SK gibi ön ekleri kaldır)
    tokens = {_ALIAS.get(t, t) for t in tokens}
    tokens.discard("")
    # Çok kısa token'ları filtrele (tek harf hariç sayılar)
    tokens = {t for t in tokens if len(t) > 1 or t.isdigit()}
    return tokens


def similarity(a: str, b: str) -> float:
    """
    İki takım adı arasındaki token intersection oranı.
    0.0 = hiç benzer değil, 1.0 = tam eşleşme
    """
    ta, tb = normalize(a), normalize(b)
    if not ta or not tb:
        return 0.0
    intersection = ta & tb
    union = ta | tb
    # Jaccard benzeri, ama union yerine min kullan (kısa isimlere karşı robust)
    return len(intersection) / min(len(ta), len(tb))


# ─── Eşleştirme ───────────────────────────────────────────────────────────────

@dataclass
class MatchPair:
    sofa_event_id: int
    mac_id:        int
    home_score:    float   # ev sahibi adı benzerliği
    away_score:    float   # deplasman adı benzerliği
    combined:      float   # ortalama

    @property
    def is_confident(self) -> bool:
        return self.combined >= 0.5 and self.home_score >= 0.4 and self.away_score >= 0.4


def match_events(
    sofa_matches: list,    # list[SofaMatch]
    mac_listings: list,    # list[MatchListing]
    threshold: float = 0.5,
) -> tuple[list[MatchPair], list, list]:
    """
    İki kaynak arasındaki maçları eşleştirir.

    Returns:
        (pairs, unmatched_sofa, unmatched_mac)
    """
    pairs:           list[MatchPair] = []
    matched_sofa:    set[int] = set()
    matched_mac:     set[int] = set()

    for sm in sofa_matches:
        best_pair: Optional[MatchPair] = None

        for ml in mac_listings:
            h = similarity(sm.home_team, ml.home_team)
            a = similarity(sm.away_team, ml.away_team)
            combined = (h + a) / 2

            if combined < threshold:
                continue

            if best_pair is None or combined > best_pair.combined:
                best_pair = MatchPair(
                    sofa_event_id=sm.event_id,
                    mac_id=ml.mac_id,
                    home_score=h,
                    away_score=a,
                    combined=combined,
                )

        if best_pair and best_pair.is_confident:
            pairs.append(best_pair)
            matched_sofa.add(sm.event_id)
            matched_mac.add(best_pair.mac_id)
            logger.debug(
                "Eşleşti  [%.2f] sofa=%d mac=%d  '%s' vs '%s'",
                best_pair.combined,
                sm.event_id, best_pair.mac_id,
                sm.home_team, sm.away_team,
            )
        else:
            logger.debug(
                "Eşleşmedi sofa=%d '%s vs %s'",
                sm.event_id, sm.home_team, sm.away_team,
            )

    unmatched_sofa = [sm for sm in sofa_matches if sm.event_id not in matched_sofa]
    unmatched_mac  = [ml for ml in mac_listings  if ml.mac_id  not in matched_mac]

    logger.info(
        "Eşleştirme: %d çift | %d Sofa eşleşmedi | %d Mac eşleşmedi",
        len(pairs), len(unmatched_sofa), len(unmatched_mac),
    )
    return pairs, unmatched_sofa, unmatched_mac
