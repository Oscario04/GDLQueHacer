"""
scraper/sources/eventbrite.py
Scraper para la API pública de Eventbrite.
Filtra eventos en Guadalajara, Jalisco, México.

Requiere en .env:
    EVENTBRITE_TOKEN=tu_token_aqui
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

EVENTBRITE_API = "https://www.eventbriteapi.com/v3"


class EventbriteScraper:
    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("EVENTBRITE_TOKEN no configurado en .env")
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}

    async def fetch_events(
        self,
        city: str = "Guadalajara",
        country: str = "MX",
        max_pages: int = 5,
    ) -> list[dict]:
        """Devuelve lista de eventos mapeados al esquema base de GDL Qué Hacer."""
        raw_events: list[dict] = []
        page = 1

        async with httpx.AsyncClient(timeout=30, headers=self.headers) as client:
            while page <= max_pages:
                params = {
                    "location.address": f"{city}, {country}",
                    "location.within": "50km",
                    "expand": "venue,category,ticket_availability",
                    "sort_by": "date",
                    "start_date.range_start": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "page": page,
                    "page_size": 50,
                }

                try:
                    resp = await client.get(
                        f"{EVENTBRITE_API}/events/search/",
                        params=params,
                    )

                    if resp.status_code == 429:
                        logger.warning("Rate limit Eventbrite. Esperando 60s...")
                        await asyncio.sleep(60)
                        continue

                    if resp.status_code == 401:
                        logger.error("EVENTBRITE_TOKEN inválido o expirado.")
                        break

                    resp.raise_for_status()
                    data = resp.json()

                except httpx.HTTPError as exc:
                    logger.error("Error HTTP Eventbrite página %d: %s", page, exc)
                    break

                events = data.get("events", [])
                logger.info("Eventbrite página %d: %d eventos", page, len(events))

                for event in events:
                    mapped = self._map_event(event)
                    if mapped:
                        raw_events.append(mapped)

                pagination = data.get("pagination", {})
                if not pagination.get("has_more_items", False):
                    break
                page += 1

        logger.info("Eventbrite total: %d eventos válidos", len(raw_events))
        return raw_events

    def _map_event(self, event: dict) -> Optional[dict]:
        try:
            start_raw = (event.get("start") or {}).get("utc")
            end_raw = (event.get("end") or {}).get("utc")

            start_date = self._parse_dt(start_raw)
            end_date = self._parse_dt(end_raw)

            # Descartar eventos pasados
            if start_date and start_date < datetime.now(timezone.utc):
                return None

            # Descartar sin título
            title = (event.get("name") or {}).get("text", "").strip()
            if not title:
                return None

            venue = event.get("venue") or {}
            address_obj = venue.get("address") or {}
            address = ", ".join(
                filter(None, [
                    address_obj.get("address_1"),
                    address_obj.get("city"),
                    address_obj.get("region"),
                ])
            ) or "Guadalajara, Jalisco"

            lat = self._to_float(venue.get("latitude"))
            lon = self._to_float(venue.get("longitude"))

            # Categoría
            category_obj = event.get("category") or {}
            raw_category = category_obj.get("short_name", "").lower()
            category = self._map_category(raw_category)

            # Precio
            is_free = event.get("is_free", False)
            ticket_avail = event.get("ticket_availability") or {}
            min_price_obj = ticket_avail.get("minimum_ticket_price") or {}
            price_value = min_price_obj.get("value", 0)
            price = 0.0 if is_free else self._to_float(price_value)

            # Imagen
            logo = event.get("logo") or {}
            image_url = (
                (logo.get("original") or {}).get("url")
                or logo.get("url")
            )

            # Descripción
            description = (event.get("description") or {}).get("text", "")
            if not description:
                description = (event.get("summary") or "")

            return {
                "source_id": "eventbrite",
                "external_id": event.get("id", ""),
                "title": title,
                "description": description,
                "category": category,
                "tags": [],
                "image_url": image_url,
                "date_start": start_date,
                "date_end": end_date,
                "price": price,
                "currency": "MXN",
                "url": event.get("url", ""),
                "location": venue.get("name") or address,
                "latitude": lat,
                "longitude": lon,
            }

        except Exception as exc:
            logger.warning(
                "Error mapeando evento Eventbrite %s: %s",
                event.get("id"), exc
            )
            return None

    @staticmethod
    def _map_category(raw: str) -> str:
        """Mapea categorías de Eventbrite al esquema de GDL Qué Hacer."""
        mapping = {
            "music": "entretenimiento",
            "sports": "deportivo",
            "food": "gastronomico",
            "arts": "cultural",
            "film": "cultural",
            "performing arts": "cultural",
            "visual arts": "cultural",
            "health": "deportivo",
            "science": "cultural",
            "technology": "cultural",
            "business": "cultural",
            "community": "cultural",
            "fashion": "entretenimiento",
            "home": "otro",
            "auto": "otro",
            "hobbies": "entretenimiento",
            "other": "otro",
        }
        for key, val in mapping.items():
            if key in raw:
                return val
        return "otro"

    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None