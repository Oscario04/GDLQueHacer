"""
scraper/sources/ticketmaster_jalisco.py
Ticketmaster expandido para todo el estado de Jalisco.

Extiende la lógica de ticketmaster.py con:
  1. GeoPoint: búsqueda por lat/long + radio 200km (cubre todo Jalisco)
  2. Multi-ciudad: ciudades extra del estado (Vallarta, Lagos, Tepa, etc.)
  3. Paginación completa (hasta 500 resultados por geopoint)

Sigue exactamente el mismo esquema de salida que TicketmasterScraper.
Requiere: TICKETMASTER_API_KEY en .env
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

# Reusar helpers del scraper original para consistencia total
from scraper.sources.ticketmaster import TicketmasterScraper

logger = logging.getLogger(__name__)

TICKETMASTER_API = "https://app.ticketmaster.com/discovery/v2"

# Centro geográfico de Jalisco (Guadalajara) y radio para cubrir el estado
GDL_LAT = 20.6597
GDL_LON = -103.3496
JALISCO_RADIUS_KM = 200  # Cubre GDL, Vallarta, Lagos de Moreno, Tepa, etc.

# Ciudades adicionales que pueden quedar fuera del radio o tener resultados propios
JALISCO_EXTRA_CITIES = [
    "Puerto Vallarta",
    "Lagos de Moreno",
    "Tepatitlan",
    "Chapala",
    "Tequila",
    "Ciudad Guzman",
    "Ocotlan",
]

PAGE_SIZE = 50
MAX_PAGES_GEO = 10   # 10 × 50 = 500 eventos por geopoint
MAX_PAGES_CITY = 2   # 2 × 50 = 100 eventos por ciudad extra


class TicketmasterJaliscoScraper:
    """
    Scraper expandido de Ticketmaster para todo el estado de Jalisco.
    Combina búsqueda geográfica + multi-ciudad para maximizar cobertura.
    Usa internamente _map_event de TicketmasterScraper para que el esquema
    de salida sea 100% idéntico al scraper original.
    """

    def __init__(self, api_key: str, months_ahead: int = 6) -> None:
        if not api_key:
            raise ValueError("TICKETMASTER_API_KEY no configurada en .env")
        self.api_key = api_key
        self.months_ahead = months_ahead
        # Instancia del scraper original — solo para reutilizar _map_event
        self._base = TicketmasterScraper(api_key=api_key)

    async def fetch_events(self) -> list[dict]:
        """Retorna eventos únicos de todo Jalisco en el esquema estándar."""
        now = datetime.now(timezone.utc)
        end_date = now + timedelta(days=30 * self.months_ahead)
        start_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        seen_ids: set[str] = set()
        all_events: list[dict] = []

        async with httpx.AsyncClient(timeout=30) as client:

            # ── 1. GeoPoint — búsqueda amplia para todo Jalisco ───────
            geo_events = await self._search_geopoint(client, start_str, end_str)
            for evt in geo_events:
                eid = evt.get("external_id", "")
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    all_events.append(evt)
            logger.info("TM Jalisco geopoint: %d eventos", len(geo_events))

            await asyncio.sleep(0.5)

            # ── 2. Ciudades extra — captura eventos fuera del radio ────
            for city in JALISCO_EXTRA_CITIES:
                city_events = await self._search_city(client, city, start_str, end_str)
                new_count = 0
                for evt in city_events:
                    eid = evt.get("external_id", "")
                    if eid not in seen_ids:
                        seen_ids.add(eid)
                        all_events.append(evt)
                        new_count += 1
                if new_count:
                    logger.info("TM ciudad '%s': +%d nuevos", city, new_count)
                await asyncio.sleep(0.3)

        logger.info(
            "Ticketmaster Jalisco total: %d eventos únicos",
            len(all_events),
        )
        return all_events

    # ── Métodos de búsqueda ───────────────────────────────────────────

    async def _search_geopoint(
        self,
        client: httpx.AsyncClient,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        events: list[dict] = []
        for page in range(MAX_PAGES_GEO):
            params = {
                "apikey": self.api_key,
                "latlong": f"{GDL_LAT},{GDL_LON}",
                "radius": str(JALISCO_RADIUS_KM),
                "unit": "km",
                "countryCode": "MX",
                "startDateTime": start_str,
                "endDateTime": end_str,
                "size": PAGE_SIZE,
                "page": page,
                "sort": "date,asc",
                "locale": "es-mx,en-us",
            }
            page_events, total_pages = await self._fetch_page(client, params, page)
            events.extend(page_events)

            if not page_events or page + 1 >= total_pages:
                break
            await asyncio.sleep(0.2)

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
                "apikey": self.api_key,
                "city": city,
                "countryCode": "MX",
                "startDateTime": start_str,
                "endDateTime": end_str,
                "size": PAGE_SIZE,
                "page": page,
                "sort": "date,asc",
                "locale": "es-mx,en-us",
            }
            page_events, total_pages = await self._fetch_page(client, params, page)
            events.extend(page_events)

            if not page_events or page + 1 >= total_pages:
                break
            await asyncio.sleep(0.2)

        return events

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        params: dict,
        page: int,
    ) -> tuple[list[dict], int]:
        """
        Descarga una página y retorna (eventos_normalizados, total_pages).
        Usa _map_event del scraper original para que el esquema sea idéntico.
        """
        try:
            resp = await client.get(
                f"{TICKETMASTER_API}/events.json",
                params=params,
            )

            if resp.status_code == 429:
                logger.warning("TM Jalisco rate limit — esperando 10s")
                await asyncio.sleep(10)
                return [], 1

            if resp.status_code == 401:
                logger.error("TICKETMASTER_API_KEY inválida.")
                return [], 0

            resp.raise_for_status()
            data = resp.json()

        except httpx.HTTPError as exc:
            logger.error("TM Jalisco HTTP error página %d: %s", page, exc)
            return [], 0
        except Exception as exc:
            logger.error("TM Jalisco error inesperado: %s", exc)
            return [], 0

        raw_events = data.get("_embedded", {}).get("events", [])
        page_info = data.get("page", {})
        total_pages = page_info.get("totalPages", 1)

        logger.info("TM Jalisco página %d: %d eventos", page, len(raw_events))

        # _map_event del scraper original — mismo esquema, mismo parseo de fechas
        events: list[dict] = []
        for raw in raw_events:
            mapped = self._base._map_event(raw)
            if mapped:
                events.append(mapped)

        return events, total_pages