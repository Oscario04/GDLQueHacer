"""
scraper/sources/ticketmaster_nacional.py  — v2
Ticketmaster con cobertura nacional COMPLETA: 50+ ciudades + 25 geopoints + segmentos.
Estimado: 4,000–8,000 eventos únicos.

CAMBIOS v2:
  - 50 ciudades (antes 28) — cubre los 32 estados
  - 25 geopoints nacionales con radio 200 km (antes 17 con 150 km)
  - MAX_PAGES_CITY subido a 20 (antes 10)
  - MAX_PAGES_GEO subido a 40 (antes 20)
  - Añadidos subgéneros de Ticketmaster (genreId) para capturar
    eventos que no aparecen en búsquedas por ciudad/geo solo
  - Semáforo ajustado a 8 (antes 5) para mayor paralelismo sin
    superar el rate-limit de la API gratuita (5 req/s)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from scraper.sources.ticketmaster import TicketmasterScraper

logger = logging.getLogger(__name__)

TICKETMASTER_API = "https://app.ticketmaster.com/discovery/v2"

# ── 50+ ciudades de los 32 estados ──────────────────────────────────
MEXICO_CITIES = [
    # Jalisco
    "Guadalajara", "Zapopan", "Puerto Vallarta", "Tlaquepaque", "Tonalá",
    "Lagos de Moreno", "Tepatitlán",
    # CDMX / Estado de México
    "Ciudad de México", "Ecatepec", "Naucalpan", "Toluca", "Nezahualcóyotl",
    # Nuevo León
    "Monterrey", "San Pedro Garza García", "Apodaca", "Guadalupe",
    # Baja California
    "Tijuana", "Mexicali", "Ensenada",
    # Quintana Roo
    "Cancún", "Playa del Carmen", "Tulum",
    # Querétaro
    "Querétaro", "San Juan del Río",
    # Puebla
    "Puebla", "Tehuacán",
    # Yucatán
    "Mérida", "Valladolid",
    # Sinaloa
    "Culiacán", "Mazatlán", "Los Mochis",
    # Sonora
    "Hermosillo", "Ciudad Obregón", "Nogales",
    # Chihuahua
    "Chihuahua", "Ciudad Juárez",
    # Coahuila
    "Torreón", "Saltillo", "Monclova",
    # Guanajuato
    "León", "Guanajuato", "Irapuato", "Celaya",
    # San Luis Potosí
    "San Luis Potosí", "Ciudad Valles",
    # Aguascalientes
    "Aguascalientes",
    # Michoacán
    "Morelia", "Uruapan", "Zamora",
    # Veracruz
    "Veracruz", "Xalapa", "Coatzacoalcos",
    # Oaxaca
    "Oaxaca", "Huatulco",
    # Tabasco
    "Villahermosa",
    # Chiapas
    "Tuxtla Gutiérrez", "San Cristóbal de las Casas", "Tapachula",
    # Guerrero
    "Acapulco", "Zihuatanejo",
    # Baja California Sur
    "Los Cabos", "La Paz",
    # Nayarit
    "Tepic", "Bahía de Banderas",
    # Colima
    "Colima", "Manzanillo",
    # Zacatecas
    "Zacatecas",
    # Durango
    "Durango",
    # Hidalgo
    "Pachuca",
    # Morelos
    "Cuernavaca",
    # Tlaxcala
    "Tlaxcala",
    # Campeche
    "Campeche",
    # Tamaulipas
    "Tampico", "Matamoros", "Nuevo Laredo", "Reynosa",
]

# ── 25 geopoints con radio 200 km ────────────────────────────────────
MEXICO_GEO_POINTS = [
    (19.4326,  -99.1332,  "CDMX"),
    (20.6597,  -103.3496, "Guadalajara"),
    (25.6866,  -100.3161, "Monterrey"),
    (21.1619,  -86.8515,  "Cancún"),
    (20.5888,  -100.3899, "Querétaro"),
    (19.0414,  -98.2063,  "Puebla"),
    (32.5149,  -117.0382, "Tijuana"),
    (20.9674,  -89.6237,  "Mérida"),
    (24.8091,  -107.3940, "Culiacán"),
    (29.0729,  -110.9559, "Hermosillo"),
    (28.6353,  -106.0889, "Chihuahua"),
    (31.7333,  -106.4833, "Ciudad Juárez"),
    (22.1565,  -100.9855, "San Luis Potosí"),
    (21.8853,  -102.2916, "Aguascalientes"),
    (19.7069,  -101.1950, "Morelia"),
    (19.1738,  -96.1342,  "Veracruz"),
    (17.0669,  -96.7203,  "Oaxaca"),
    (16.7569,  -93.1292,  "Tuxtla Gutiérrez"),
    (16.8531,  -99.8237,  "Acapulco"),
    (23.2494,  -106.4111, "Mazatlán"),
    (21.0190,  -89.6293,  "Mérida Norte"),
    (25.4232,  -100.9734, "Saltillo"),
    (25.5428,  -103.4068, "Torreón"),
    (20.6597,  -105.2253, "Puerto Vallarta"),
    (18.9242,  -99.2216,  "Cuernavaca"),
]

# ── Segmentos de Ticketmaster ─────────────────────────────────────────
SEGMENT_IDS = {
    "Music":  "KZFzniwnSyZfZ7v7nJ",
    "Arts":   "KZFzniwnSyZfZ7v7na",
    "Sports": "KZFzniwnSyZfZ7v7nE",
    "Family": "KZFzniwnSyZfZ7v7n1",
    "Film":   "KZFzniwnSyZfZ7v7nn",
    "Miscellaneous": "KZFzniwnSyZfZ7v7n1",
}

# ── Géneros para búsquedas adicionales ───────────────────────────────
GENRE_IDS = {
    "Rock":         "KnvZfZ7vAeA",
    "Pop":          "KnvZfZ7vAev",
    "Electronic":   "KnvZfZ7vAvF",
    "Latin":        "KnvZfZ7vAeJ",
    "Hip-Hop":      "KnvZfZ7vAv1",
    "Jazz & Blues": "KnvZfZ7vAvE",
    "Country":      "KnvZfZ7vAv6",
    "Classical":    "KnvZfZ7vAeB",
    "Comedy":       "KnvZfZ7vAn7",
    "Sports":       "KnvZfZ7vAda",
}

PAGE_SIZE       = 50
MAX_PAGES_GEO   = 40   # 40 × 50 = 2,000 por geopoint
MAX_PAGES_CITY  = 20   # 20 × 50 = 1,000 por ciudad
MAX_PAGES_SEG   = 10   # 10 × 50 = 500 por segmento+geo
GEO_RADIUS_KM   = 200  # radio ampliado (antes 150)


class TicketmasterNacionalScraper:
    """
    Scraper nacional de Ticketmaster para México — cobertura completa.
    Estrategias:
      1. 50+ ciudades (todos los estados)
      2. 25 geopoints con radio 200 km
      3. Geopoints × 6 segmentos (CDMX, GDL, MTY, CUN)
      4. Geopoints × 10 géneros en las 4 ciudades clave
    """

    def __init__(self, api_key: str, months_ahead: int = 12) -> None:
        if not api_key:
            raise ValueError("TICKETMASTER_API_KEY no configurada")
        self.api_key = api_key
        self.months_ahead = months_ahead
        self._base = TicketmasterScraper(api_key=api_key)
        self._sem = asyncio.Semaphore(8)

    async def fetch_events(self) -> list[dict]:
        now       = datetime.now(timezone.utc)
        end_date  = now + timedelta(days=30 * self.months_ahead)
        start_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str   = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        seen_ids: set[str] = set()
        all_events: list[dict] = []

        async with httpx.AsyncClient(timeout=30) as client:

            # ── 1. Ciudades principales ──────────────────────────────
            city_tasks = [
                self._search_city(client, city, start_str, end_str)
                for city in MEXICO_CITIES
            ]
            city_results = await asyncio.gather(*city_tasks, return_exceptions=True)
            for city, result in zip(MEXICO_CITIES, city_results):
                if isinstance(result, Exception):
                    logger.warning("TM ciudad '%s' error: %s", city, result)
                    continue
                new = self._merge(result, seen_ids, all_events)
                if new:
                    logger.info("TM ciudad '%s': +%d nuevos", city, new)
            logger.info("TM ciudades subtotal: %d", len(all_events))

            await asyncio.sleep(1)

            # ── 2. Geopoints nacionales ──────────────────────────────
            geo_tasks = [
                self._search_geopoint(client, lat, lon, start_str, end_str)
                for lat, lon, _ in MEXICO_GEO_POINTS
            ]
            geo_results = await asyncio.gather(*geo_tasks, return_exceptions=True)
            for (lat, lon, label), result in zip(MEXICO_GEO_POINTS, geo_results):
                if isinstance(result, Exception):
                    logger.warning("TM geo %s error: %s", label, result)
                    continue
                new = self._merge(result, seen_ids, all_events)
                if new:
                    logger.info("TM geo '%s': +%d nuevos", label, new)
            logger.info("TM geopoints subtotal: %d", len(all_events))

            await asyncio.sleep(1)

            # ── 3. Segmentos × geopoints principales ─────────────────
            key_geos = [
                (19.4326,  -99.1332,  "CDMX"),
                (20.6597,  -103.3496, "GDL"),
                (25.6866,  -100.3161, "MTY"),
                (21.1619,  -86.8515,  "CUN"),
                (20.9674,  -89.6237,  "MER"),
                (19.0414,  -98.2063,  "PUE"),
            ]
            for seg_name, seg_id in SEGMENT_IDS.items():
                for lat, lon, label in key_geos:
                    seg_events = await self._search_geopoint(
                        client, lat, lon, start_str, end_str,
                        segment_id=seg_id, max_pages=MAX_PAGES_SEG,
                    )
                    new = self._merge(seg_events, seen_ids, all_events)
                    if new:
                        logger.info("TM seg '%s'/%s: +%d nuevos", seg_name, label, new)
                    await asyncio.sleep(0.25)

            # ── 4. Géneros × geopoints CDMX+GDL+MTY ─────────────────
            for genre_name, genre_id in GENRE_IDS.items():
                for lat, lon, label in key_geos[:3]:  # CDMX, GDL, MTY
                    genre_events = await self._search_geopoint(
                        client, lat, lon, start_str, end_str,
                        genre_id=genre_id, max_pages=5,
                    )
                    new = self._merge(genre_events, seen_ids, all_events)
                    if new:
                        logger.info("TM genre '%s'/%s: +%d nuevos", genre_name, label, new)
                    await asyncio.sleep(0.2)

        logger.info("Ticketmaster Nacional total: %d eventos únicos", len(all_events))
        return all_events

    # ── Helpers ───────────────────────────────────────────────────────

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

    async def _search_city(
        self,
        client: httpx.AsyncClient,
        city: str,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        events: list[dict] = []
        async with self._sem:
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
                await asyncio.sleep(0.2)
        return events

    async def _search_geopoint(
        self,
        client: httpx.AsyncClient,
        lat: float,
        lon: float,
        start_str: str,
        end_str: str,
        segment_id: str | None = None,
        genre_id: str | None = None,
        max_pages: int = MAX_PAGES_GEO,
    ) -> list[dict]:
        events: list[dict] = []
        async with self._sem:
            for page in range(max_pages):
                params: dict = {
                    "apikey":        self.api_key,
                    "latlong":       f"{lat},{lon}",
                    "radius":        str(GEO_RADIUS_KM),
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
                if genre_id:
                    params["genreId"] = genre_id

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
        retries: int = 3,
    ) -> tuple[list[dict], int]:
        for attempt in range(retries):
            try:
                resp = await client.get(
                    f"{TICKETMASTER_API}/events.json",
                    params=params,
                )

                if resp.status_code == 429:
                    wait = 15 * (2 ** attempt)
                    logger.warning("TM rate limit — esperando %ds (intento %d)", wait, attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code == 401:
                    logger.error("TICKETMASTER_API_KEY inválida.")
                    return [], 0

                resp.raise_for_status()
                data = resp.json()

            except httpx.HTTPError as exc:
                logger.error("TM HTTP error pág %d: %s", page, exc)
                return [], 0

            raw_events  = data.get("_embedded", {}).get("events", [])
            total_pages = data.get("page", {}).get("totalPages", 1)

            events = []
            for raw in raw_events:
                mapped = self._base._map_event(raw)
                if mapped:
                    events.append(mapped)

            return events, total_pages

        return [], 0