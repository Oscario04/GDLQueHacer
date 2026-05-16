"""
scraper/sources/ticketmaster_latam.py
Ticketmaster para toda América Latina — agrega 2,000+ eventos adicionales.

Países cubiertos: MX, US (eventos frontera), BR, AR, CL, CO, PE
Esto da más datos para entrenar el modelo ML de recomendaciones.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TICKETMASTER_API = "https://app.ticketmaster.com/discovery/v2"

# Geopoints adicionales: ciudades frontera USA con mucho tráfico MX
# + LATAM para enriquecer el dataset del modelo ML
LATAM_GEO_POINTS = [
    # México extra (ciudades pequeñas no cubiertas)
    (21.5042, -104.8954, "MX", "Tepic"),
    (19.2452, -103.7241, "MX", "Colima"),
    (20.1319, -101.1869, "MX", "Zamora"),
    (20.3557, -99.9862,  "MX", "Pachuca"),
    (18.9242, -103.8767, "MX", "Manzanillo"),
    (27.4863, -109.9308, "MX", "Ciudad Obregon"),
    (26.9239, -101.4390, "MX", "Monclova"),
    (20.5248, -103.0109, "MX", "Chapala"),
    (17.9995, -92.9475,  "MX", "Villahermosa2"),
    (16.7500, -93.1167,  "MX", "Tuxtla2"),
    # USA frontera (eventos que atraen mexicanos)
    (32.7157, -117.1611, "US", "San Diego"),
    (31.7619, -106.4850, "US", "El Paso"),
    (26.1224, -80.1373,  "US", "Miami"),          # eventos latinos
    (29.7604, -95.3698,  "US", "Houston"),         # gran comunidad MX
    (33.4484, -112.0740, "US", "Phoenix"),
    (34.0522, -118.2437, "US", "Los Angeles"),     # latinos y MX
    (29.4241, -98.4936,  "US", "San Antonio"),
    (30.2672, -97.7431,  "US", "Austin"),
    (32.7767, -96.7970,  "US", "Dallas"),
    (25.7617, -80.1918,  "US", "Miami2"),
    # Brasil
    (23.5505, -46.6333,  "BR", "Sao Paulo"),
    (22.9068, -43.1729,  "BR", "Rio de Janeiro"),
    (19.9167, -43.9345,  "BR", "Belo Horizonte"),
    # Argentina
    (-34.6037, -58.3816, "AR", "Buenos Aires"),
    (-31.4201, -64.1888, "AR", "Cordoba"),
    # Chile
    (-33.4569, -70.6483, "CL", "Santiago"),
    (-23.5505, -46.6333, "CL", "Valparaiso"),
    # Colombia
    (4.6097,  -74.0817,  "CO", "Bogota"),
    (6.2442,  -75.5812,  "CO", "Medellin"),
    (3.8801,  -77.0307,  "CO", "Cali"),
    # Perú
    (-12.0464, -77.0428, "PE", "Lima"),
    # España (muchos artistas MX van)
    (40.4168, -3.7038,   "ES", "Madrid"),
    (41.3851, 2.1734,    "ES", "Barcelona"),
]

# Todos los segmentos y géneros de Ticketmaster
ALL_SEGMENTS = {
    "Music":       "KZFzniwnSyZfZ7v7nJ",
    "Arts":        "KZFzniwnSyZfZ7v7na",
    "Sports":      "KZFzniwnSyZfZ7v7nE",
    "Family":      "KZFzniwnSyZfZ7v7n1",
    "Film":        "KZFzniwnSyZfZ7v7nn",
}

# Géneros extra para MX
EXTRA_GENRES = {
    "Rock":           "KnvZfZ7vAeA",
    "Pop":            "KnvZfZ7vAev",
    "Electronic":     "KnvZfZ7vAvF",
    "Latin":          "KnvZfZ7vAeJ",
    "HipHop":         "KnvZfZ7vAv1",
    "JazzBlues":      "KnvZfZ7vAvE",
    "Country":        "KnvZfZ7vAv6",
    "Classical":      "KnvZfZ7vAeB",
    "RnB":            "KnvZfZ7vAee",
    "Reggae":         "KnvZfZ7vAe6",
    "Folk":           "KnvZfZ7vAeI",
    "WorldMusic":     "KnvZfZ7vAva",
    "Comedy":         "KnvZfZ7vAn7",
    "Dance":          "KnvZfZ7vAe1",
    "Alternative":    "KnvZfZ7vAv1",
    "Banda":          "KnvZfZ7vAeJ",
}

PAGE_SIZE   = 50
MAX_PAGES   = 20
RADIUS_KM   = 200


class TicketmasterLatamScraper:
    """
    Ticketmaster para LATAM + USA frontera.
    Agrega ~2,000 eventos para enriquecer el dataset ML.
    """

    def __init__(self, api_key: str, months_ahead: int = 12):
        if not api_key:
            raise ValueError("TICKETMASTER_API_KEY no configurada")
        self.api_key = api_key
        self.months_ahead = months_ahead
        self._sem = asyncio.Semaphore(6)

    async def fetch_events(self) -> list[dict]:
        now       = datetime.now(timezone.utc)
        end_date  = now + timedelta(days=30 * self.months_ahead)
        start_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str   = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        seen_ids: set[str] = set()
        all_events: list[dict] = []

        async with httpx.AsyncClient(timeout=30) as client:

            # ── 1. Geopoints LATAM ───────────────────────────────────
            geo_tasks = [
                self._search_geo(client, lat, lon, country, start_str, end_str)
                for lat, lon, country, _ in LATAM_GEO_POINTS
            ]
            geo_results = await asyncio.gather(*geo_tasks, return_exceptions=True)
            for (lat, lon, country, label), result in zip(LATAM_GEO_POINTS, geo_results):
                if isinstance(result, Exception):
                    logger.warning("TM LATAM %s/%s error: %s", country, label, result)
                    continue
                new = self._merge(result, seen_ids, all_events)
                if new:
                    logger.info("TM LATAM %s/%s: +%d", country, label, new)
            logger.info("TM LATAM geopoints: %d total", len(all_events))

            await asyncio.sleep(1)

            # ── 2. Géneros extra × geopoints MX principales ──────────
            mx_geos = [(lat, lon) for lat, lon, c, _ in LATAM_GEO_POINTS if c == "MX"][:5]
            for genre_name, genre_id in EXTRA_GENRES.items():
                for lat, lon in mx_geos:
                    try:
                        ge = await self._search_geo(
                            client, lat, lon, "MX", start_str, end_str,
                            genre_id=genre_id, max_pages=5
                        )
                        new = self._merge(ge, seen_ids, all_events)
                        if new:
                            logger.info("TM genre '%s': +%d", genre_name, new)
                    except Exception as exc:
                        logger.warning("TM genre %s error: %s", genre_name, exc)
                    await asyncio.sleep(0.2)

        logger.info("Ticketmaster LATAM total: %d eventos únicos", len(all_events))
        return all_events

    async def _search_geo(
        self,
        client: httpx.AsyncClient,
        lat: float,
        lon: float,
        country: str,
        start_str: str,
        end_str: str,
        segment_id: str | None = None,
        genre_id: str | None = None,
        max_pages: int = MAX_PAGES,
    ) -> list[dict]:
        events: list[dict] = []
        async with self._sem:
            for page in range(max_pages):
                params: dict = {
                    "apikey":        self.api_key,
                    "latlong":       f"{lat},{lon}",
                    "radius":        str(RADIUS_KM),
                    "unit":          "km",
                    "countryCode":   country,
                    "startDateTime": start_str,
                    "endDateTime":   end_str,
                    "size":          PAGE_SIZE,
                    "page":          page,
                    "sort":          "date,asc",
                    "locale":        "es,en-us",
                }
                if segment_id:
                    params["segmentId"] = segment_id
                if genre_id:
                    params["genreId"] = genre_id

                try:
                    resp = await client.get(
                        f"{TICKETMASTER_API}/events.json", params=params
                    )
                    if resp.status_code == 429:
                        await asyncio.sleep(15)
                        continue
                    if resp.status_code in (401, 404):
                        break
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as exc:
                    logger.debug("TM LATAM geo error pág %d: %s", page, exc)
                    break

                raw_events  = data.get("_embedded", {}).get("events", [])
                total_pages = data.get("page", {}).get("totalPages", 1)

                for raw in raw_events:
                    mapped = self._map_event(raw, country)
                    if mapped:
                        events.append(mapped)

                if not raw_events or page + 1 >= total_pages:
                    break
                await asyncio.sleep(0.2)

        return events

    def _map_event(self, event: dict, country: str) -> Optional[dict]:
        try:
            title = event.get("name", "").strip()
            if not title:
                return None

            dates = event.get("dates") or {}
            start = dates.get("start") or {}
            date_str = start.get("dateTime") or start.get("localDate")
            if not date_str:
                return None

            embedded = event.get("_embedded") or {}
            venues   = embedded.get("venues") or [{}]
            venue    = venues[0] if venues else {}
            location = venue.get("name", "")
            city     = (venue.get("city") or {}).get("name", "")
            state    = (venue.get("state") or {}).get("name", "")
            country_name = (venue.get("country") or {}).get("name", "")
            full_loc = ", ".join(filter(None, [location, city, state, country_name]))

            geo = venue.get("location") or {}
            lat = self._to_float(geo.get("latitude"))
            lon = self._to_float(geo.get("longitude"))

            classifications = event.get("classifications") or [{}]
            segment = (classifications[0].get("segment") or {}).get("name", "").lower()
            genre   = (classifications[0].get("genre") or {}).get("name", "").lower()

            prices = event.get("priceRanges") or []
            price  = self._to_float(prices[0].get("min")) if prices else None

            images = sorted(
                event.get("images") or [],
                key=lambda x: x.get("width", 0) * x.get("height", 0),
                reverse=True,
            )
            image_url = images[0].get("url") if images else None

            category = self._map_cat(segment, genre)

            return {
                "source_id":   "ticketmaster_latam",
                "external_id": event.get("id", ""),
                "title":       title,
                "description": event.get("info") or event.get("pleaseNote") or "",
                "category":    category,
                "tags":        [genre, country.lower()] if genre else [country.lower()],
                "image_url":   image_url,
                "date_start":  date_str,
                "date_end":    None,
                "price":       price,
                "currency":    "MXN" if country == "MX" else "USD",
                "url":         event.get("url", ""),
                "location":    full_loc or "México",
                "latitude":    lat,
                "longitude":   lon,
                "estado":      state,
                "ciudad":      city,
            }
        except Exception as exc:
            logger.debug("TM LATAM map error: %s", exc)
            return None

    @staticmethod
    def _map_cat(segment: str, genre: str) -> str:
        combined = f"{segment} {genre}".lower()
        if any(w in combined for w in ["music", "concert", "jazz", "rock", "pop"]):
            return "entretenimiento"
        if any(w in combined for w in ["sport", "fútbol", "basketball"]):
            return "deportivo"
        if any(w in combined for w in ["art", "theatre", "film", "cine", "dance"]):
            return "cultural"
        if any(w in combined for w in ["comedy", "family"]):
            return "entretenimiento"
        return "entretenimiento"

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

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None