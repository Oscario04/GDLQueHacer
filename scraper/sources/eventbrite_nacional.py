"""
scraper/sources/eventbrite_nacional.py  — v2
Eventbrite con cobertura nacional COMPLETA.
Estimado: 1,500–3,000 eventos únicos.

CAMBIOS v2:
  - 25 geopoints (antes 17) con radio 250 km
  - 50 queries de texto (antes 24)
  - MAX_PAGES por geopoint subido a 20 (antes 10)
  - MAX_PAGES por query subido a 10 (antes 5)
  - Búsquedas adicionales por categoría (music, food-drink, arts, etc.)
  - Semáforo a 6 (antes 4)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from scraper.sources.eventbrite import EventbriteScraper

logger = logging.getLogger(__name__)

EVENTBRITE_API = "https://www.eventbriteapi.com/v3"

# ── 25 geopoints con radio 250 km ────────────────────────────────────
MEXICO_GEO_POINTS = [
    (19.4326,  -99.1332,  250, "CDMX"),
    (20.6597,  -103.3496, 250, "Guadalajara"),
    (25.6866,  -100.3161, 250, "Monterrey"),
    (21.1619,  -86.8515,  200, "Cancún"),
    (20.5888,  -100.3899, 200, "Querétaro"),
    (19.0414,  -98.2063,  200, "Puebla"),
    (32.5149,  -117.0382, 150, "Tijuana"),
    (20.9674,  -89.6237,  250, "Mérida"),
    (24.8091,  -107.3940, 200, "Culiacán"),
    (29.0729,  -110.9559, 200, "Hermosillo"),
    (28.6353,  -106.0889, 200, "Chihuahua"),
    (31.7333,  -106.4833, 150, "Ciudad Juárez"),
    (22.1565,  -100.9855, 200, "San Luis Potosí"),
    (21.8853,  -102.2916, 150, "Aguascalientes"),
    (19.7069,  -101.1950, 200, "Morelia"),
    (19.1738,  -96.1342,  200, "Veracruz"),
    (17.0669,  -96.7203,  200, "Oaxaca"),
    (16.7569,  -93.1292,  200, "Tuxtla Gutiérrez"),
    (16.8531,  -99.8237,  150, "Acapulco"),
    (23.2494,  -106.4111, 150, "Mazatlán"),
    (20.6597,  -105.2253, 100, "Puerto Vallarta"),
    (25.4232,  -100.9734, 200, "Saltillo"),
    (25.5428,  -103.4068, 200, "Torreón"),
    (18.9242,  -99.2216,  150, "Cuernavaca"),
    (21.0190,  -101.2574, 150, "León"),
]

# ── 50 queries de texto (ciudad + categoría) ─────────────────────────
MEXICO_CITY_QUERIES = [
    # Ciudades principales
    "Ciudad de México", "CDMX", "Guadalajara", "Jalisco",
    "Monterrey", "Nuevo León", "Cancún", "Querétaro",
    "Puebla", "Tijuana", "Mérida", "San Luis Potosí",
    "Aguascalientes", "Morelia", "Chihuahua", "Culiacán",
    "Hermosillo", "Veracruz", "Oaxaca", "Mazatlán",
    "Puerto Vallarta", "Torreón", "León", "Saltillo",
    "Acapulco", "Los Cabos", "Vallarta", "Guanajuato",
    "Toluca", "Xalapa", "Cuernavaca", "Tepic",
    "Villahermosa", "Tuxtla", "Tampico", "Durango",
    # Categorías + ciudad
    "concierto México", "festival México", "teatro México",
    "expo México", "música en vivo México",
    "conferencia México", "networking México", "startup México",
    "gastronomía México", "arte México",
    "concierto Guadalajara", "festival Guadalajara",
    "concierto CDMX", "festival CDMX",
    "concierto Monterrey", "festival Monterrey",
]

# ── Categorías de Eventbrite para búsquedas adicionales ─────────────
# ID: nombre
EVENTBRITE_CATEGORIES = {
    "103": "Music",
    "110": "Food & Drink",
    "105": "Performing & Visual Arts",
    "104": "Film, Media & Entertainment",
    "108": "Sports & Fitness",
    "102": "Science & Technology",
    "101": "Business & Professional",
    "107": "Health & Wellness",
    "109": "Travel & Outdoor",
    "111": "Social Activities",
    "113": "Community & Culture",
    "115": "Family & Education",
}

PAGE_SIZE       = 50
MAX_PAGES_GEO   = 20   # 20 × 50 = 1,000 por geopoint
MAX_PAGES_QUERY = 10   # 10 × 50 = 500 por query
MAX_PAGES_CAT   = 5    # 5 × 50 = 250 por categoría+geo


class EventbriteNacionalScraper:
    """
    Scraper nacional de Eventbrite.
    Estrategias:
      1. 25 geopoints con radio variable
      2. 50 queries de texto por ciudad/categoría
      3. 12 categorías × 4 geopoints clave
    """

    def __init__(self, token: str, months_ahead: int = 12) -> None:
        if not token:
            raise ValueError("EVENTBRITE_TOKEN no configurado")
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.months_ahead = months_ahead
        self._base = EventbriteScraper(token=token)
        self._sem = asyncio.Semaphore(6)

    async def fetch_events(self) -> list[dict]:
        now      = datetime.now(timezone.utc)
        end_date = now + timedelta(days=30 * self.months_ahead)
        start_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str   = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        seen_ids: set[str] = set()
        all_events: list[dict] = []

        async with httpx.AsyncClient(timeout=30, headers=self.headers) as client:

            # ── 1. Geopoints nacionales ──────────────────────────────
            geo_tasks = [
                self._search_geopoint(client, lat, lon, radius, start_str, end_str)
                for lat, lon, radius, _ in MEXICO_GEO_POINTS
            ]
            geo_results = await asyncio.gather(*geo_tasks, return_exceptions=True)
            for (lat, lon, radius, label), result in zip(MEXICO_GEO_POINTS, geo_results):
                if isinstance(result, Exception):
                    logger.warning("EB geo %s error: %s", label, result)
                    continue
                new = self._merge(result, seen_ids, all_events)
                if new:
                    logger.info("EB geo '%s': +%d nuevos", label, new)
            logger.info("EB geopoints subtotal: %d", len(all_events))

            await asyncio.sleep(2)

            # ── 2. Queries por ciudad/categoría ─────────────────────
            for query in MEXICO_CITY_QUERIES:
                try:
                    q_events = await self._search_by_query(client, query, start_str, end_str)
                    new = self._merge(q_events, seen_ids, all_events)
                    if new:
                        logger.info("EB query='%s': +%d nuevos", query, new)
                except Exception as exc:
                    logger.warning("EB query '%s' error: %s", query, exc)
                await asyncio.sleep(0.4)

            logger.info("EB queries subtotal: %d", len(all_events))

            # ── 3. Categorías × geopoints clave ─────────────────────
            key_geos = [
                (19.4326,  -99.1332,  200, "CDMX"),
                (20.6597,  -103.3496, 200, "GDL"),
                (25.6866,  -100.3161, 200, "MTY"),
                (21.1619,  -86.8515,  150, "CUN"),
            ]
            for cat_id, cat_name in EVENTBRITE_CATEGORIES.items():
                for lat, lon, radius, label in key_geos:
                    try:
                        cat_events = await self._search_by_category(
                            client, cat_id, lat, lon, radius, start_str, end_str
                        )
                        new = self._merge(cat_events, seen_ids, all_events)
                        if new:
                            logger.info("EB cat '%s'/%s: +%d nuevos", cat_name, label, new)
                    except Exception as exc:
                        logger.warning("EB cat '%s'/%s error: %s", cat_name, label, exc)
                    await asyncio.sleep(0.3)

        logger.info("Eventbrite Nacional total: %d eventos únicos", len(all_events))
        return all_events

    async def _search_geopoint(
        self,
        client: httpx.AsyncClient,
        lat: float,
        lon: float,
        radius: int,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        raw: list[dict] = []
        page = 1

        async with self._sem:
            while page <= MAX_PAGES_GEO:
                params = {
                    "location.latitude":       lat,
                    "location.longitude":      lon,
                    "location.within":         f"{radius}km",
                    "start_date.range_start":  start_str,
                    "start_date.range_end":    end_str,
                    "expand":                  "venue,category,ticket_availability",
                    "sort_by":                 "date",
                    "page":                    page,
                    "page_size":               PAGE_SIZE,
                }
                try:
                    resp = await client.get(
                        f"{EVENTBRITE_API}/events/search/", params=params
                    )
                    if resp.status_code == 429:
                        logger.warning("EB rate limit, esperando 60s")
                        await asyncio.sleep(60)
                        continue
                    if resp.status_code in (401, 403, 404):
                        return raw
                    resp.raise_for_status()
                    data = resp.json()
                except httpx.HTTPError as exc:
                    logger.error("EB geo HTTP error pág %d: %s", page, exc)
                    break

                for event in data.get("events", []):
                    venue   = event.get("venue") or {}
                    addr    = venue.get("address") or {}
                    country = addr.get("country", "").upper()
                    if country and country != "MX":
                        continue
                    mapped = self._base._map_event(event)
                    if mapped:
                        raw.append(mapped)

                if not data.get("pagination", {}).get("has_more_items"):
                    break
                page += 1
                await asyncio.sleep(0.3)

        return raw

    async def _search_by_query(
        self,
        client: httpx.AsyncClient,
        query: str,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        raw: list[dict] = []
        page = 1

        async with self._sem:
            while page <= MAX_PAGES_QUERY:
                params = {
                    "q":                       query,
                    "start_date.range_start":  start_str,
                    "start_date.range_end":    end_str,
                    "expand":                  "venue,category,ticket_availability",
                    "page":                    page,
                    "page_size":               PAGE_SIZE,
                }
                try:
                    resp = await client.get(
                        f"{EVENTBRITE_API}/events/search/", params=params
                    )
                    if resp.status_code in (401, 403, 404):
                        return raw
                    if resp.status_code == 429:
                        await asyncio.sleep(60)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as exc:
                    logger.debug("EB query '%s' error: %s", query, exc)
                    break

                for event in data.get("events", []):
                    venue   = event.get("venue") or {}
                    addr    = venue.get("address") or {}
                    country = addr.get("country", "").upper()
                    if country and country != "MX":
                        continue
                    mapped = self._base._map_event(event)
                    if mapped:
                        raw.append(mapped)

                if not data.get("pagination", {}).get("has_more_items"):
                    break
                page += 1
                await asyncio.sleep(0.3)

        return raw

    async def _search_by_category(
        self,
        client: httpx.AsyncClient,
        category_id: str,
        lat: float,
        lon: float,
        radius: int,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        raw: list[dict] = []
        page = 1

        async with self._sem:
            while page <= MAX_PAGES_CAT:
                params = {
                    "categories":              category_id,
                    "location.latitude":       lat,
                    "location.longitude":      lon,
                    "location.within":         f"{radius}km",
                    "start_date.range_start":  start_str,
                    "start_date.range_end":    end_str,
                    "expand":                  "venue,category,ticket_availability",
                    "page":                    page,
                    "page_size":               PAGE_SIZE,
                }
                try:
                    resp = await client.get(
                        f"{EVENTBRITE_API}/events/search/", params=params
                    )
                    if resp.status_code in (401, 403, 404):
                        return raw
                    if resp.status_code == 429:
                        await asyncio.sleep(60)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as exc:
                    logger.debug("EB cat error: %s", exc)
                    break

                for event in data.get("events", []):
                    venue   = event.get("venue") or {}
                    addr    = venue.get("address") or {}
                    country = addr.get("country", "").upper()
                    if country and country != "MX":
                        continue
                    mapped = self._base._map_event(event)
                    if mapped:
                        raw.append(mapped)

                if not data.get("pagination", {}).get("has_more_items"):
                    break
                page += 1
                await asyncio.sleep(0.3)

        return raw

    @staticmethod
    def _merge(new_events: list[dict], seen_ids: set[str], all_events: list[dict]) -> int:
        added = 0
        for evt in new_events:
            eid = evt.get("external_id", "")
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                all_events.append(evt)
                added += 1
        return added