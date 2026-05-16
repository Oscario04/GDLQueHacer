"""
scraper/sources/ticketmaster_jalisco.py
Ticketmaster expandido para todo el estado de Jalisco.

MEJORAS v2:
  - MAX_PAGES_GEO subido a 40 (límite real de la API = 2,000 eventos)
  - MAX_PAGES_CITY subido a 10 por ciudad
  - Búsqueda adicional por clasificación (Music, Arts, Sports, Family)
  - Radio extra de 100km para el área metropolitana
  - Ventana de tiempo ampliada a 12 meses (antes 6)
  - Retry automático en rate-limit con back-off exponencial
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from scraper.sources.ticketmaster import TicketmasterScraper

logger = logging.getLogger(__name__)

TICKETMASTER_API = "https://app.ticketmaster.com/discovery/v2"

GDL_LAT = 20.6597
GDL_LON = -103.3496

# Dos radios: metro GDL (50 km) + todo Jalisco (200 km)
GEO_SEARCHES = [
    {"radius": 50,  "label": "GDL metro"},
    {"radius": 200, "label": "Jalisco estado"},
]

JALISCO_EXTRA_CITIES = [
    "Puerto Vallarta",
    "Lagos de Moreno",
    "Tepatitlan",
    "Chapala",
    "Tequila",
    "Ciudad Guzman",
    "Ocotlan",
    "Zapopan",
    "Tlaquepaque",
    "Tonala",
    "Tlajomulco",
    "Ameca",
    "Autlan",
]

# Segmentos/clasificaciones de Ticketmaster para forzar más resultados
# (cada clasificación puede tener eventos distintos en la respuesta)
SEGMENT_IDS = {
    "Music":  "KZFzniwnSyZfZ7v7nJ",
    "Arts":   "KZFzniwnSyZfZ7v7na",
    "Sports": "KZFzniwnSyZfZ7v7nE",
    "Family": "KZFzniwnSyZfZ7v7n1",
    "Film":   "KZFzniwnSyZfZ7v7nn",
}

PAGE_SIZE    = 50
MAX_PAGES_GEO  = 40   # 40 × 50 = 2,000 por radio
MAX_PAGES_CITY = 10   # 10 × 50 = 500 por ciudad
MAX_PAGES_SEG  = 10   # 10 × 50 = 500 por segmento+geo


class TicketmasterJaliscoScraper:
    """
    Scraper expandido de Ticketmaster para todo Jalisco.
    Combina búsqueda geográfica (multi-radio), multi-ciudad y
    multi-segmento para maximizar cobertura.
    """

    def __init__(self, api_key: str, months_ahead: int = 12) -> None:
        if not api_key:
            raise ValueError("TICKETMASTER_API_KEY no configurada en .env")
        self.api_key = api_key
        self.months_ahead = months_ahead
        self._base = TicketmasterScraper(api_key=api_key)

    async def fetch_events(self) -> list[dict]:
        now = datetime.now(timezone.utc)
        end_date = now + timedelta(days=30 * self.months_ahead)
        start_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str   = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        seen_ids: set[str] = set()
        all_events: list[dict] = []

        async with httpx.AsyncClient(timeout=30) as client:

            # ── 1. GeoPoint multi-radio ──────────────────────────────
            for geo in GEO_SEARCHES:
                geo_events = await self._search_geopoint(
                    client, start_str, end_str, geo["radius"]
                )
                new = self._merge(geo_events, seen_ids, all_events)
                logger.info("TM geopoint %s: +%d nuevos", geo["label"], new)
                await asyncio.sleep(0.5)

            # ── 2. GeoPoint × segmento ───────────────────────────────
            for seg_name, seg_id in SEGMENT_IDS.items():
                seg_events = await self._search_geopoint(
                    client, start_str, end_str,
                    radius=200, segment_id=seg_id
                )
                new = self._merge(seg_events, seen_ids, all_events)
                if new:
                    logger.info("TM segmento '%s': +%d nuevos", seg_name, new)
                await asyncio.sleep(0.3)

            # ── 3. Ciudades extra ────────────────────────────────────
            for city in JALISCO_EXTRA_CITIES:
                city_events = await self._search_city(client, city, start_str, end_str)
                new = self._merge(city_events, seen_ids, all_events)
                if new:
                    logger.info("TM ciudad '%s': +%d nuevos", city, new)
                await asyncio.sleep(0.3)

        logger.info("Ticketmaster Jalisco total: %d eventos únicos", len(all_events))
        return all_events

    # ── helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _merge(
        new_events: list[dict],
        seen_ids: set[str],
        all_events: list[dict],
    ) -> int:
        added = 0
        for evt in new_events:
            eid = evt.get("external_id", "")
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                all_events.append(evt)
                added += 1
        return added

    async def _search_geopoint(
        self,
        client: httpx.AsyncClient,
        start_str: str,
        end_str: str,
        radius: int = 200,
        segment_id: str | None = None,
    ) -> list[dict]:
        events: list[dict] = []
        for page in range(MAX_PAGES_GEO if not segment_id else MAX_PAGES_SEG):
            params: dict = {
                "apikey":        self.api_key,
                "latlong":       f"{GDL_LAT},{GDL_LON}",
                "radius":        str(radius),
                "unit":          "km",
                "countryCode":   "MX",
                "startDateTime": start_str,
                "endDateTime":   end_str,
                "size":          PAGE_SIZE,
                "page":          page,
                "sort":          "date,asc",
                "locale":        "es-mx,en-us",
            }
            if segment_id:
                params["segmentId"] = segment_id

            page_events, total_pages = await self._fetch_page(client, params, page)
            events.extend(page_events)

            if not page_events or page + 1 >= total_pages:
                break
            await asyncio.sleep(0.25)

        return events

    async def _search_city(
        self,
        client: httpx.AsyncClient,
        city: str,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        events: list[dict] = []
        for page in range(MAX_PAGES_CITY):
            params = {
                "apikey":        self.api_key,
                "city":          city,
                "countryCode":   "MX",
                "startDateTime": start_str,
                "endDateTime":   end_str,
                "size":          PAGE_SIZE,
                "page":          page,
                "sort":          "date,asc",
                "locale":        "es-mx,en-us",
            }
            page_events, total_pages = await self._fetch_page(client, params, page)
            events.extend(page_events)

            if not page_events or page + 1 >= total_pages:
                break
            await asyncio.sleep(0.25)

        return events

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        params: dict,
        page: int,
        retries: int = 3,
    ) -> tuple[list[dict], int]:
        for attempt in range(retries):
            try:
                resp = await client.get(
                    f"{TICKETMASTER_API}/events.json",
                    params=params,
                )

                if resp.status_code == 429:
                    wait = 10 * (2 ** attempt)   # back-off: 10s, 20s, 40s
                    logger.warning("TM rate limit — esperando %ds (intento %d)", wait, attempt + 1)
                    await asyncio.sleep(wait)
                    continue

                if resp.status_code == 401:
                    logger.error("TICKETMASTER_API_KEY inválida.")
                    return [], 0

                resp.raise_for_status()
                data = resp.json()

            except httpx.HTTPError as exc:
                logger.error("TM HTTP error página %d: %s", page, exc)
                return [], 0
            except Exception as exc:
                logger.error("TM error inesperado: %s", exc)
                return [], 0

            raw_events  = data.get("_embedded", {}).get("events", [])
            total_pages = data.get("page", {}).get("totalPages", 1)

            events: list[dict] = []
            for raw in raw_events:
                mapped = self._base._map_event(raw)
                if mapped:
                    events.append(mapped)

            return events, total_pages

        return [], 0