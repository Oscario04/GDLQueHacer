"""
scraper/sources/ticketmaster.py
Scraper para la API de Ticketmaster — eventos en Guadalajara, México.

API gratuita: https://developer.ticketmaster.com
Requiere en .env:
    TICKETMASTER_API_KEY=tu_key_aqui

Endpoint usado:
    GET https://app.ticketmaster.com/discovery/v2/events.json
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TICKETMASTER_API = "https://app.ticketmaster.com/discovery/v2"


class TicketmasterScraper:
    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("TICKETMASTER_API_KEY no configurada en .env")
        self.api_key = api_key

    async def fetch_events(
        self,
        city: str = "Guadalajara",
        country_code: str = "MX",
        max_pages: int = 5,
    ) -> list[dict]:
        """Retorna eventos de Guadalajara mapeados al esquema base."""
        raw_events: list[dict] = []
        page = 0  # Ticketmaster usa paginación base-0

        # Rango de fechas: hoy hasta 90 días adelante
        now = datetime.now(timezone.utc)
        date_end = now + timedelta(days=90)

        async with httpx.AsyncClient(timeout=30) as client:
            while page < max_pages:
                params = {
                    "apikey": self.api_key,
                    "city": city,
                    "countryCode": country_code,
                    "startDateTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "endDateTime": date_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "size": 50,
                    "page": page,
                    "sort": "date,asc",
                    "locale": "es-mx,en-us",
                }

                try:
                    resp = await client.get(
                        f"{TICKETMASTER_API}/events.json",
                        params=params,
                    )

                    if resp.status_code == 429:
                        logger.warning("Rate limit Ticketmaster. Esperando 30s...")
                        await asyncio.sleep(30)
                        continue

                    if resp.status_code == 401:
                        logger.error("TICKETMASTER_API_KEY inválida.")
                        break

                    resp.raise_for_status()
                    data = resp.json()

                except httpx.HTTPError as exc:
                    logger.error("Error HTTP Ticketmaster página %d: %s", page, exc)
                    break

                embedded = data.get("_embedded", {})
                events = embedded.get("events", [])
                logger.info("Ticketmaster página %d: %d eventos", page, len(events))

                if not events:
                    break

                for event in events:
                    mapped = self._map_event(event)
                    if mapped:
                        raw_events.append(mapped)

                # Verificar si hay más páginas
                page_info = data.get("page", {})
                total_pages = page_info.get("totalPages", 1)
                if page + 1 >= total_pages:
                    break
                page += 1

        logger.info("Ticketmaster total: %d eventos válidos", len(raw_events))
        return raw_events

    def _map_event(self, event: dict) -> Optional[dict]:
        try:
            title = event.get("name", "").strip()
            if not title:
                return None

            # Fechas
            dates = event.get("dates") or {}
            start = dates.get("start") or {}
            date_start = self._parse_dt(
                start.get("dateTime") or start.get("localDate")
            )
            if not date_start:
                return None

            # Venue
            embedded = event.get("_embedded") or {}
            venues = embedded.get("venues") or [{}]
            venue = venues[0] if venues else {}

            location_name = venue.get("name", "Guadalajara")
            city_name = (venue.get("city") or {}).get("name", "Guadalajara")
            state_name = (venue.get("state") or {}).get("name", "Jalisco")
            address_line = (venue.get("address") or {}).get("line1", "")

            full_address = ", ".join(
                filter(None, [location_name, address_line, city_name, state_name])
            )

            geo = venue.get("location") or {}
            lat = self._to_float(geo.get("latitude"))
            lon = self._to_float(geo.get("longitude"))

            # Categoría
            classifications = event.get("classifications") or [{}]
            segment = (classifications[0].get("segment") or {}).get("name", "").lower()
            genre = (classifications[0].get("genre") or {}).get("name", "").lower()
            category = self._map_category(segment, genre)

            # Precio
            price_ranges = event.get("priceRanges") or []
            price = None
            if price_ranges:
                price = self._to_float(price_ranges[0].get("min"))

            # Imagen (la más grande disponible)
            images = event.get("images") or []
            image_url = None
            if images:
                # Ordenar por resolución y tomar la más grande
                sorted_images = sorted(
                    images,
                    key=lambda x: (x.get("width", 0) * x.get("height", 0)),
                    reverse=True,
                )
                image_url = sorted_images[0].get("url")

            # URL del evento
            url = event.get("url", "")

            # Descripción (Ticketmaster rara vez tiene descripción larga)
            info = event.get("info") or event.get("pleaseNote") or ""

            return {
                "source_id": "ticketmaster",
                "external_id": event.get("id", ""),
                "title": title,
                "description": info,
                "category": category,
                "tags": [genre] if genre and genre != "undefined" else [],
                "image_url": image_url,
                "date_start": date_start,
                "date_end": None,
                "price": price,
                "currency": "MXN",
                "url": url,
                "location": full_address,
                "latitude": lat,
                "longitude": lon,
            }

        except Exception as exc:
            logger.warning(
                "Error mapeando evento Ticketmaster %s: %s",
                event.get("id"), exc
            )
            return None

    @staticmethod
    def _map_category(segment: str, genre: str) -> str:
        """Mapea segmento/género de Ticketmaster al esquema de GDL Qué Hacer."""
        combined = f"{segment} {genre}".lower()

        if any(w in combined for w in ["music", "música", "concert", "jazz", "rock", "pop", "banda"]):
            return "entretenimiento"
        if any(w in combined for w in ["sport", "deporte", "fútbol", "futbol", "basketball", "béisbol"]):
            return "deportivo"
        if any(w in combined for w in ["food", "comida", "gastro", "wine", "beer"]):
            return "gastronomico"
        if any(w in combined for w in ["art", "arte", "theatre", "theater", "dance", "ballet", "film", "cine"]):
            return "cultural"
        if any(w in combined for w in ["comedy", "comedia", "family", "familia"]):
            return "entretenimiento"
        return "entretenimiento"  # Default para Ticketmaster es entretenimiento

    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            # Manejar formato ISO con y sin timezone
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            try:
                # Solo fecha sin hora
                return datetime.strptime(value[:10], "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                return None

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None