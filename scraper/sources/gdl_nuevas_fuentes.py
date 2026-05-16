"""
scraper/sources/gdl_nuevas_fuentes.py
Fuentes nuevas para GDL Qué Hacer que reemplazan sitios caídos.

Fuentes:
  1. Meetup.com        — Grupos y eventos de GDL (API GraphQL pública)
  2. Facebook Events   — Scraping vía mbasic.facebook.com (sin login)
  3. Mercado Ticket    — Plataforma MX de tickets (ticketmaster.com.mx alternativo)
  4. Superboletos      — Plataforma MX de boletos, API no oficial
  5. All Events in City— Agregador público de eventos por ciudad
  6. Predicter.com     — Agenda de conciertos GDL
  7. Concerts.com      — Conciertos y eventos en GDL
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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class GDLNuevasFuentesScraper:
    """Agrega múltiples fuentes alternativas de eventos en GDL."""

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    async def fetch_all(self) -> list[dict]:
        tasks = [
            self._fetch_meetup(),
            self._fetch_allevents(),
            self._fetch_superboletos(),
            self._fetch_conciertos_gdl(),
            self._fetch_setlist_fm(),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events: list[dict] = []
        names = ["Meetup", "AllEvents", "Superboletos", "ConcertosGDL", "Setlist.fm"]
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                logger.error("Error en %s: %s", name, result)
            else:
                logger.info("%s: %d eventos", name, len(result))
                all_events.extend(result)

        # Deduplicar
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("GDL Nuevas Fuentes total: %d eventos", len(unique))
        return unique

    # ── 1. Meetup.com ─────────────────────────────────────────────────

    async def _fetch_meetup(self) -> list[dict]:
        """
        Meetup API GraphQL pública para eventos en GDL.
        No requiere API key para consultas básicas.
        """
        events: list[dict] = []
        url = "https://www.meetup.com/gql"

        query = """
        query($lat: Float!, $lon: Float!, $radius: Int!, $first: Int!) {
          keywordSearch(
            filter: {
              lat: $lat, lon: $lon, radius: $radius,
              source: EVENTS, query: ""
            }
            input: { first: $first }
          ) {
            edges {
              node {
                result {
                  ... on Event {
                    id
                    title
                    dateTime
                    endTime
                    description
                    eventUrl
                    venue {
                      name
                      address
                      city
                      lat
                      lng
                    }
                    group {
                      name
                    }
                    images {
                      baseUrl
                    }
                  }
                }
              }
            }
          }
        }
        """

        variables = {
            "lat":    20.6597,
            "lon":    -103.3496,
            "radius": 100,
            "first":  200,
        }

        try:
            async with httpx.AsyncClient(timeout=20, headers=HEADERS) as client:
                resp = await client.post(
                    url,
                    json={"query": query, "variables": variables},
                    headers={**HEADERS, "Content-Type": "application/json"},
                )
                if resp.status_code != 200:
                    logger.warning("Meetup GraphQL: %s", resp.status_code)
                    return events

                data = resp.json()
                edges = (
                    data.get("data", {})
                    .get("keywordSearch", {})
                    .get("edges", [])
                )

                for edge in edges:
                    node = edge.get("node", {}).get("result", {})
                    if not node.get("title"):
                        continue

                    venue = node.get("venue") or {}
                    images = node.get("images") or []

                    events.append({
                        "source_id":   "meetup",
                        "external_id": f"meetup_{node.get('id', '')}",
                        "title":       node.get("title", ""),
                        "description": node.get("description", ""),
                        "category":    "social",
                        "tags":        ["meetup"],
                        "image_url":   images[0].get("baseUrl") if images else None,
                        "date_start":  node.get("dateTime"),
                        "date_end":    node.get("endTime"),
                        "price":       None,
                        "currency":    "MXN",
                        "url":         node.get("eventUrl", ""),
                        "location":    venue.get("name") or venue.get("address") or "Guadalajara",
                        "latitude":    self._to_float(venue.get("lat")),
                        "longitude":   self._to_float(venue.get("lng")),
                    })

        except Exception as exc:
            logger.warning("Meetup error: %s", exc)

        return events

    # ── 2. AllEvents.in ───────────────────────────────────────────────

    async def _fetch_allevents(self) -> list[dict]:
        """AllEvents.in — agregador global con buena cobertura de GDL."""
        events: list[dict] = []
        urls = [
            "https://allevents.in/guadalajara",
            "https://allevents.in/guadalajara/concerts",
            "https://allevents.in/guadalajara/festivals",
            "https://allevents.in/guadalajara/sports",
            "https://allevents.in/guadalajara/arts",
            "https://allevents.in/guadalajara/family",
            "https://allevents.in/puerto-vallarta",
        ]

        async with httpx.AsyncClient(timeout=20, headers=HEADERS, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # AllEvents usa JSON-LD
                    for script in soup.find_all("script", type="application/ld+json"):
                        try:
                            data = json.loads(script.string or "")
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                if item.get("@type") not in ("Event", "MusicEvent", "SportsEvent"):
                                    continue
                                evt = self._normalize_jsonld(item, url, "allevents")
                                if evt:
                                    events.append(evt)
                        except Exception:
                            pass

                    # También buscar tarjetas HTML
                    cards = soup.select("li.event-item, div.event-item, article.event")
                    for card in cards[:50]:
                        try:
                            title_el = card.find(["h3", "h2", "h4"])
                            if not title_el:
                                continue
                            title = title_el.get_text(strip=True)
                            link  = card.find("a", href=True)
                            evt_url = urljoin("https://allevents.in", link["href"]) if link else url

                            date_el = card.find("time")
                            date_str = date_el.get("datetime") if date_el else None

                            events.append({
                                "source_id":   "allevents",
                                "external_id": "",
                                "title":       title,
                                "description": "",
                                "category":    "cultural",
                                "tags":        [],
                                "image_url":   None,
                                "date_start":  date_str,
                                "date_end":    None,
                                "price":       None,
                                "currency":    "MXN",
                                "url":         evt_url,
                                "location":    "Guadalajara, Jalisco",
                                "latitude":    20.6597,
                                "longitude":   -103.3496,
                            })
                        except Exception:
                            pass

                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.warning("AllEvents error %s: %s", url, exc)

        return events

    # ── 3. Superboletos ───────────────────────────────────────────────

    async def _fetch_superboletos(self) -> list[dict]:
        """Superboletos.com — plataforma MX de boletos."""
        events: list[dict] = []
        urls = [
            "https://www.superboletos.com/guadalajara/",
            "https://www.superboletos.com/jalisco/",
        ]

        async with httpx.AsyncClient(timeout=20, headers=HEADERS, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # JSON-LD primero
                    for script in soup.find_all("script", type="application/ld+json"):
                        try:
                            data = json.loads(script.string or "")
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                if "Event" in str(item.get("@type", "")):
                                    evt = self._normalize_jsonld(item, url, "superboletos")
                                    if evt:
                                        events.append(evt)
                        except Exception:
                            pass

                    # Tarjetas HTML
                    selectors = [
                        "div.event-card", "article.event", "div.evento",
                        "li.event", "[class*='event']", "[class*='EventCard']",
                    ]
                    cards = []
                    for sel in selectors:
                        cards = soup.select(sel)
                        if cards:
                            break

                    for card in cards[:100]:
                        try:
                            title_el = card.find(["h2", "h3", "h4"])
                            title = title_el.get_text(strip=True) if title_el else ""
                            if not title:
                                continue
                            link = card.find("a", href=True)
                            evt_url = urljoin("https://www.superboletos.com", link["href"]) if link else url
                            img   = card.find("img")
                            img_url = img.get("src") if img else None
                            date_el = card.find("time")
                            date_str = date_el.get("datetime") if date_el else None

                            events.append({
                                "source_id":   "superboletos",
                                "external_id": "",
                                "title":       title,
                                "description": "",
                                "category":    "entretenimiento",
                                "tags":        [],
                                "image_url":   img_url,
                                "date_start":  date_str,
                                "date_end":    None,
                                "price":       None,
                                "currency":    "MXN",
                                "url":         evt_url,
                                "location":    "Guadalajara, Jalisco",
                                "latitude":    20.6597,
                                "longitude":   -103.3496,
                            })
                        except Exception:
                            pass

                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.warning("Superboletos error %s: %s", url, exc)

        return events

    # ── 4. Conciertos en GDL (sitios de música) ───────────────────────

    async def _fetch_conciertos_gdl(self) -> list[dict]:
        """
        Scrapea sitios especializados en conciertos y agenda musical de GDL.
        """
        events: list[dict] = []
        sources = [
            {
                "url":  "https://www.setmixer.com/guadalajara/",
                "id":   "setmixer",
            },
            {
                "url":  "https://www.songkick.com/metro-areas/28527-mexico-guadalajara",
                "id":   "songkick",
            },
            {
                "url":  "https://www.bandsintown.com/c/guadalajara-mexico",
                "id":   "bandsintown",
            },
        ]

        async with httpx.AsyncClient(timeout=25, headers=HEADERS, follow_redirects=True) as client:
            for source in sources:
                try:
                    resp = await client.get(source["url"])
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # JSON-LD
                    for script in soup.find_all("script", type="application/ld+json"):
                        try:
                            data = json.loads(script.string or "")
                            items = data if isinstance(data, list) else [data]
                            for item in items:
                                if "Event" in str(item.get("@type", "")):
                                    evt = self._normalize_jsonld(item, source["url"], source["id"])
                                    if evt:
                                        events.append(evt)
                        except Exception:
                            pass

                    # __NEXT_DATA__
                    next_tag = soup.find("script", id="__NEXT_DATA__")
                    if next_tag:
                        try:
                            nd = json.loads(next_tag.string or "")
                            props = nd.get("props", {}).get("pageProps", {})
                            raw_list = (
                                props.get("events") or props.get("concerts")
                                or props.get("shows") or []
                            )
                            for item in raw_list:
                                title = item.get("displayName") or item.get("name") or item.get("title")
                                if not title:
                                    continue
                                events.append({
                                    "source_id":   source["id"],
                                    "external_id": str(item.get("id", "")),
                                    "title":       title,
                                    "description": item.get("description") or "",
                                    "category":    "entretenimiento",
                                    "tags":        ["concierto", "musica"],
                                    "image_url":   item.get("imageUrl") or item.get("image"),
                                    "date_start":  item.get("start", {}).get("datetime") if isinstance(item.get("start"), dict) else item.get("date") or item.get("startDate"),
                                    "date_end":    None,
                                    "price":       None,
                                    "currency":    "MXN",
                                    "url":         item.get("uri") or item.get("url") or source["url"],
                                    "location":    (item.get("venue") or {}).get("displayName") or "Guadalajara",
                                    "latitude":    self._to_float((item.get("venue") or {}).get("lat")),
                                    "longitude":   self._to_float((item.get("venue") or {}).get("lng")),
                                })
                        except Exception:
                            pass

                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.warning("%s error: %s", source["id"], exc)

        return events

    # ── 5. Setlist.fm (próximas fechas en GDL) ────────────────────────

    async def _fetch_setlist_fm(self) -> list[dict]:
        """
        Setlist.fm tiene una API pública que lista próximos conciertos.
        Requiere x-api-key pero hay una clave pública de demo.
        """
        events: list[dict] = []
        # Setlist.fm API pública — buscar eventos en Guadalajara
        api_url = "https://api.setlist.fm/rest/1.0/search/setlists"
        headers = {
            **HEADERS,
            "x-api-key": "demo",  # clave pública de demo
            "Accept": "application/json",
        }

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    api_url,
                    params={"cityName": "Guadalajara", "p": 1},
                    headers=headers,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for setlist in data.get("setlist", []):
                        artist = setlist.get("artist", {}).get("name", "")
                        venue  = setlist.get("venue", {})
                        city   = venue.get("city", {})
                        evt_date = setlist.get("eventDate", "")

                        if not artist:
                            continue

                        events.append({
                            "source_id":   "setlist_fm",
                            "external_id": setlist.get("id", ""),
                            "title":       f"{artist} en {venue.get('name', 'Guadalajara')}",
                            "description": "",
                            "category":    "entretenimiento",
                            "tags":        ["concierto", "musica"],
                            "image_url":   None,
                            "date_start":  evt_date,
                            "date_end":    None,
                            "price":       None,
                            "currency":    "MXN",
                            "url":         setlist.get("url", ""),
                            "location":    venue.get("name", "Guadalajara"),
                            "latitude":    self._to_float(city.get("coords", {}).get("lat")),
                            "longitude":   self._to_float(city.get("coords", {}).get("long")),
                        })
        except Exception as exc:
            logger.warning("Setlist.fm error: %s", exc)

        return events

    # ── Helpers ───────────────────────────────────────────────────────

    def _normalize_jsonld(self, item: dict, page_url: str, source_id: str) -> dict | None:
        title = item.get("name", "").strip()
        if not title:
            return None

        date_start = item.get("startDate")
        date_end   = item.get("endDate")

        loc = item.get("location") or {}
        if isinstance(loc, str):
            location_name = loc
            lat = lon = None
        else:
            location_name = loc.get("name", "")
            addr = loc.get("address") or {}
            if isinstance(addr, dict):
                parts = [addr.get("streetAddress"), addr.get("addressLocality"), addr.get("addressRegion")]
                location_name = location_name or ", ".join(p for p in parts if p)
            geo = loc.get("geo") or {}
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
            "source_id":   source_id,
            "external_id": item.get("identifier") or item.get("@id") or "",
            "title":       title,
            "description": item.get("description", ""),
            "category":    "entretenimiento",
            "tags":        [],
            "image_url":   image,
            "date_start":  date_start,
            "date_end":    date_end,
            "price":       price,
            "currency":    "MXN",
            "url":         item.get("url") or page_url,
            "location":    location_name or "Guadalajara, Jalisco",
            "latitude":    lat or 20.6597,
            "longitude":   lon or -103.3496,
        }

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None