"""
eventbrite.py
Scraper para la API pública de Eventbrite.
Filtra eventos en Guadalajara, Jalisco, México.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import requests


EVENTBRITE_API = "https://www.eventbriteapi.com/v3"
GDL_LOCATION_ID = "ES"          # Eventbrite usa "ES" para Jalisco; ajusta según respuesta real


class EventbriteScraper:
    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("EVENTBRITE_TOKEN no configurado.")
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    # -----------------------------------------------------------------------
    # Fetch principal
    # -----------------------------------------------------------------------

    def fetch_events(
        self,
        city: str = "Guadalajara",
        country: str = "MX",
        max_pages: int = 5,
    ) -> list[dict]:
        """Devuelve lista de eventos crudos (raw) ya mapeados al esquema base."""
        raw_events: list[dict] = []
        page = 1

        while page <= max_pages:
            params = {
                "location.address": f"{city}, {country}",
                "location.within": "10km",
                "expand": "venue,category",
                "page": page,
            }
            try:
                resp = self.session.get(
                    f"{EVENTBRITE_API}/events/search/",
                    params=params,
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.HTTPError as exc:
                print(f"[eventbrite] HTTP error página {page}: {exc}")
                break
            except Exception as exc:
                print(f"[eventbrite] Error inesperado: {exc}")
                break

            events = data.get("events", [])
            for event in events:
                mapped = self._map_event(event)
                if mapped:
                    raw_events.append(mapped)

            pagination = data.get("pagination", {})
            if not pagination.get("has_more_items", False):
                break
            page += 1

        return raw_events

    # -----------------------------------------------------------------------
    # Mapeo de un evento Eventbrite → esquema base
    # -----------------------------------------------------------------------

    def _map_event(self, event: dict) -> Optional[dict]:
        try:
            start_raw = (event.get("start") or {}).get("utc")
            end_raw = (event.get("end") or {}).get("utc")

            start_date = self._parse_dt(start_raw)
            end_date = self._parse_dt(end_raw)

            # Descartar eventos pasados
            if start_date and start_date < datetime.now(timezone.utc):
                return None

            venue = event.get("venue") or {}
            address_obj = venue.get("address") or {}
            address = ", ".join(
                filter(None, [
                    address_obj.get("address_1"),
                    address_obj.get("city"),
                    address_obj.get("region"),
                ])
            )
            lat = self._to_float(venue.get("latitude"))
            lon = self._to_float(venue.get("longitude"))

            category_obj = event.get("category") or {}
            category = category_obj.get("short_name", "")

            # Precio
            is_free = event.get("is_free", False)
            ticket_availability = event.get("ticket_availability") or {}
            min_price = ticket_availability.get("minimum_ticket_price", {})
            price = 0.0 if is_free else self._to_float(
                (min_price.get("value") or 0) / 100
            )

            logo = event.get("logo") or {}
            image_url = logo.get("original", {}).get("url") or logo.get("url")

            return {
                "source_id": event.get("id", ""),
                "title": event.get("name", {}).get("text", ""),
                "description": event.get("description", {}).get("text", ""),
                "category": category,
                "tags": [],
                "image_url": image_url,
                "start_date": start_date,
                "end_date": end_date,
                "price": price,
                "currency": "MXN",
                "url": event.get("url", ""),
                "location": {
                    "address": address,
                    "lat": lat,
                    "lon": lon,
                },
            }
        except Exception as exc:
            print(f"[eventbrite] Error mapeando evento {event.get('id')}: {exc}")
            return None

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None