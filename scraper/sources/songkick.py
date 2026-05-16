"""
scraper/sources/songkick.py  — v1
Scraper para Songkick.com — directorio global de conciertos.

Songkick tiene excelente cobertura de México. No requiere API key
para el scraping HTML, y además expone JSON-LD y __NEXT_DATA__ en
casi todas sus páginas.

Estrategias:
  1. Metro areas de México (Songkick tiene IDs para cada ciudad)
  2. Búsqueda HTML por ciudad con paginación
  3. JSON-LD en cada página de resultados

Estimado: 300–800 eventos únicos de México.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SONGKICK_BASE = "https://www.songkick.com"

# Metro areas de Songkick para México
# https://www.songkick.com/metro-areas/{id}-{slug}
MEXICO_METRO_AREAS = [
    {"id": 28527,  "slug": "mexico-guadalajara",     "name": "Guadalajara"},
    {"id": 26529,  "slug": "mexico-mexico-city",     "name": "Ciudad de México"},
    {"id": 28536,  "slug": "mexico-monterrey",       "name": "Monterrey"},
    {"id": 28553,  "slug": "mexico-cancun",          "name": "Cancún"},
    {"id": 28558,  "slug": "mexico-puebla",          "name": "Puebla"},
    {"id": 28563,  "slug": "mexico-queretaro",       "name": "Querétaro"},
    {"id": 28570,  "slug": "mexico-merida",          "name": "Mérida"},
    {"id": 28574,  "slug": "mexico-tijuana",         "name": "Tijuana"},
    {"id": 28577,  "slug": "mexico-san-luis-potosi", "name": "San Luis Potosí"},
    {"id": 28580,  "slug": "mexico-leon",            "name": "León"},
    {"id": 28582,  "slug": "mexico-aguascalientes",  "name": "Aguascalientes"},
    {"id": 28590,  "slug": "mexico-veracruz",        "name": "Veracruz"},
    {"id": 28596,  "slug": "mexico-morelia",         "name": "Morelia"},
    {"id": 28598,  "slug": "mexico-chihuahua",       "name": "Chihuahua"},
    {"id": 28601,  "slug": "mexico-culiacan",        "name": "Culiacán"},
    {"id": 28607,  "slug": "mexico-puerto-vallarta", "name": "Puerto Vallarta"},
    {"id": 28611,  "slug": "mexico-oaxaca",          "name": "Oaxaca"},
    {"id": 28617,  "slug": "mexico-torreon",         "name": "Torreón"},
    {"id": 28622,  "slug": "mexico-acapulco",        "name": "Acapulco"},
    {"id": 28629,  "slug": "mexico-mazatlan",        "name": "Mazatlán"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MAX_PAGES_PER_METRO = 10


class SongkickScraper:
    """
    Scraper asíncrono para Songkick.com.
    Prioriza JSON-LD y __NEXT_DATA__; cae en CSS selectors si no hay.
    """

    def __init__(self, delay: float = 0.8):
        self.delay = delay

    async def fetch_events(self) -> list[dict[str, Any]]:
        all_events: list[dict] = []

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=httpx.Timeout(25.0),
            follow_redirects=True,
        ) as client:
            tasks = [
                self._fetch_metro(client, metro)
                for metro in MEXICO_METRO_AREAS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for metro, result in zip(MEXICO_METRO_AREAS, results):
            if isinstance(result, Exception):
                logger.warning("Songkick '%s' error: %s", metro["name"], result)
            else:
                logger.info("Songkick '%s': %d eventos", metro["name"], len(result))
                all_events.extend(result)

        # Deduplicar por URL
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("Songkick total: %d eventos únicos", len(unique))
        return unique

    async def _fetch_metro(
        self, client: httpx.AsyncClient, metro: dict
    ) -> list[dict]:
        events: list[dict] = []
        base_url = (
            f"{SONGKICK_BASE}/metro-areas/{metro['id']}-{metro['slug']}"
            f"/calendar"
        )

        for page in range(1, MAX_PAGES_PER_METRO + 1):
            url = base_url if page == 1 else f"{base_url}?page={page}"

            try:
                resp = await client.get(url)
                if resp.status_code == 404:
                    break
                if resp.status_code != 200:
                    logger.debug("Songkick %s pág %d → %s", metro["name"], page, resp.status_code)
                    break
                resp.raise_for_status()
            except Exception as exc:
                logger.debug("Songkick %s error pág %d: %s", metro["name"], page, exc)
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Estrategia 1: JSON-LD
            ld_events = self._parse_json_ld(soup, metro)
            if ld_events:
                events.extend(ld_events)
                # JSON-LD a veces tiene todos los eventos sin paginar
                if len(ld_events) >= 20:
                    break
                await asyncio.sleep(self.delay)
                continue

            # Estrategia 2: __NEXT_DATA__
            nd_events = self._parse_next_data(soup, metro)
            if nd_events:
                events.extend(nd_events)
                await asyncio.sleep(self.delay)
                continue

            # Estrategia 3: HTML cards
            html_events = self._parse_html_cards(soup, metro)
            if not html_events:
                break  # Sin resultados, fin de paginación
            events.extend(html_events)
            await asyncio.sleep(self.delay)

        return events

    # ── Parsers ───────────────────────────────────────────────────────

    def _parse_json_ld(self, soup: BeautifulSoup, metro: dict) -> list[dict]:
        events = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = re.sub(r"[\x00-\x1f\x7f]", " ", script.string or "")
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in (
                        "Event", "MusicEvent", "SportsEvent",
                        "TheaterEvent", "SocialEvent",
                    ):
                        evt = self._map_json_ld(item, metro)
                        if evt:
                            events.append(evt)
                    elif item.get("@type") == "ItemList":
                        for elem in item.get("itemListElement", []):
                            inner = elem.get("item") or elem
                            if isinstance(inner, dict) and "Event" in str(inner.get("@type", "")):
                                evt = self._map_json_ld(inner, metro)
                                if evt:
                                    events.append(evt)
            except Exception:
                pass
        return events

    def _parse_next_data(self, soup: BeautifulSoup, metro: dict) -> list[dict]:
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag:
            return []
        try:
            data = json.loads(tag.string or "")
            props = data.get("props", {}).get("pageProps", {})
            raw_list = (
                props.get("concerts")
                or props.get("events")
                or props.get("upcomingEvents")
                or props.get("calendarEntries")
                or []
            )
            return [e for e in (self._map_next_item(item, metro) for item in raw_list) if e]
        except Exception:
            return []

    def _parse_html_cards(self, soup: BeautifulSoup, metro: dict) -> list[dict]:
        events = []
        selectors = [
            "li.event-listings-element",
            "li.concert",
            "article.event",
            "li[class*='event']",
            ".event-listings li",
            "ul.event-listings > li",
        ]
        cards = []
        for sel in selectors:
            cards = soup.select(sel)
            if cards:
                break

        if not cards:
            # Intentar con articles genéricos
            cards = soup.find_all("article")[:50]

        for card in cards:
            try:
                title_el = (
                    card.find("strong", class_=re.compile(r"title|name|event", re.I))
                    or card.find(["h3", "h2", "h4"])
                    or card.find("strong")
                )
                title = title_el.get_text(strip=True) if title_el else ""
                if not title:
                    continue

                link_el = card.find("a", href=True)
                url = ""
                if link_el:
                    href = link_el["href"]
                    url = href if href.startswith("http") else urljoin(SONGKICK_BASE, href)

                date_el = card.find("time") or card.find(attrs={"datetime": True})
                date_str = date_el.get("datetime", "") if date_el else ""

                loc_el = card.find(class_=re.compile(r"venue|location|lugar", re.I))
                location = loc_el.get_text(strip=True) if loc_el else metro["name"]

                img_el = card.find("img")
                image_url = None
                if img_el:
                    src = img_el.get("src") or img_el.get("data-src", "")
                    if src and src.startswith("http"):
                        image_url = src

                events.append({
                    "source_id":   "songkick",
                    "external_id": url,
                    "title":       title,
                    "description": "",
                    "category":    "entretenimiento",
                    "tags":        ["concierto", "musica"],
                    "image_url":   image_url,
                    "date_start":  date_str or None,
                    "date_end":    None,
                    "price":       None,
                    "currency":    "MXN",
                    "url":         url,
                    "location":    location,
                    "latitude":    None,
                    "longitude":   None,
                    "estado":      "México",
                    "ciudad":      metro["name"],
                })
            except Exception as exc:
                logger.debug("Songkick HTML card error: %s", exc)

        return events

    # ── Mappers ───────────────────────────────────────────────────────

    def _map_json_ld(self, item: dict, metro: dict) -> Optional[dict]:
        title = (item.get("name") or "").strip()
        if not title:
            return None

        loc = item.get("location") or {}
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        location_name = ""
        lat = lon = None
        if isinstance(loc, dict):
            location_name = loc.get("name", "")
            addr = loc.get("address") or {}
            if isinstance(addr, dict):
                parts = [
                    addr.get("streetAddress", ""),
                    addr.get("addressLocality", ""),
                    addr.get("addressRegion", ""),
                ]
                location_name = location_name or ", ".join(p for p in parts if p)
            geo = loc.get("geo") or {}
            if isinstance(geo, dict):
                lat = self._to_float(geo.get("latitude"))
                lon = self._to_float(geo.get("longitude"))

        image = item.get("image")
        if isinstance(image, list):
            image = image[0] if image else None
        if isinstance(image, dict):
            image = image.get("url")

        offers = item.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        price = self._to_float(offers.get("price")) if isinstance(offers, dict) else None

        return {
            "source_id":   "songkick",
            "external_id": item.get("url") or item.get("@id") or "",
            "title":       title,
            "description": item.get("description", ""),
            "category":    "entretenimiento",
            "tags":        ["concierto", "musica"],
            "image_url":   image,
            "date_start":  item.get("startDate"),
            "date_end":    item.get("endDate"),
            "price":       price,
            "currency":    "MXN",
            "url":         item.get("url") or "",
            "location":    location_name or metro["name"],
            "latitude":    lat,
            "longitude":   lon,
            "estado":      "México",
            "ciudad":      metro["name"],
        }

    def _map_next_item(self, item: dict, metro: dict) -> Optional[dict]:
        title = (
            item.get("displayName")
            or item.get("name")
            or item.get("title")
            or ""
        ).strip()
        if not title:
            return None

        venue = item.get("venue") or {}
        location = (
            venue.get("displayName")
            or venue.get("name")
            or metro["name"]
        )

        start = item.get("start") or {}
        date_str = (
            start.get("datetime")
            or start.get("date")
            or item.get("startDate")
            or item.get("date")
            or ""
        )

        return {
            "source_id":   "songkick",
            "external_id": str(item.get("id") or item.get("uri") or ""),
            "title":       title,
            "description": item.get("description", ""),
            "category":    "entretenimiento",
            "tags":        ["concierto", "musica"],
            "image_url":   item.get("imageUrl") or item.get("image"),
            "date_start":  date_str or None,
            "date_end":    None,
            "price":       None,
            "currency":    "MXN",
            "url":         item.get("uri") or item.get("url") or "",
            "location":    location,
            "latitude":    self._to_float((venue.get("lat"))),
            "longitude":   self._to_float((venue.get("lng"))),
            "estado":      "México",
            "ciudad":      metro["name"],
        }

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None