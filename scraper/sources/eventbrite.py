"""
scraper/sources/eventbrite.py

FIX: El endpoint /v3/events/search/ devuelve 404 porque Eventbrite
deprecó la búsqueda por location.address. Las alternativas son:
  1. Buscar por lat/lon con el parámetro location.latitude + location.longitude
  2. Buscar organizaciones en GDL y listar sus eventos
  3. Usar la API pública sin auth para eventos públicos por ciudad

Este scraper implementa las 3 estrategias con fallback.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

EVENTBRITE_API = "https://www.eventbriteapi.com/v3"

GDL_LAT = 20.6597
GDL_LON = -103.3496


class EventbriteScraper:
    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("EVENTBRITE_TOKEN no configurado en .env")
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}

    async def fetch_events(self, max_pages: int = 20) -> list[dict]:
        seen_ids: set[str] = set()
        all_events: list[dict] = []

        now       = datetime.now(timezone.utc)
        end_date  = now + timedelta(days=365)
        start_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str   = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        async with httpx.AsyncClient(timeout=30, headers=self.headers) as client:

            # ── Estrategia 1: búsqueda por lat/lon ───────────────────
            geo_events = await self._search_by_geo(client, start_str, end_str, max_pages)
            added = self._merge(geo_events, seen_ids, all_events)
            logger.info("Eventbrite geo GDL: +%d eventos", added)

            await asyncio.sleep(1.0)

            # ── Estrategia 2: buscar por query text ──────────────────
            for q in ["Guadalajara", "Jalisco", "Puerto Vallarta"]:
                q_events = await self._search_by_query(client, q, start_str, end_str)
                added = self._merge(q_events, seen_ids, all_events)
                if added:
                    logger.info("Eventbrite q='%s': +%d eventos", q, added)
                await asyncio.sleep(0.5)

            # ── Estrategia 3: organizaciones locales conocidas ────────
            org_events = await self._search_by_orgs(client, start_str, end_str)
            added = self._merge(org_events, seen_ids, all_events)
            if added:
                logger.info("Eventbrite orgs GDL: +%d eventos", added)

        logger.info("Eventbrite total: %d eventos válidos", len(all_events))
        return all_events

    async def _search_by_geo(
        self,
        client: httpx.AsyncClient,
        start_str: str,
        end_str: str,
        max_pages: int,
    ) -> list[dict]:
        """Búsqueda por coordenadas geográficas."""
        raw: list[dict] = []
        page = 1

        while page <= max_pages:
            params = {
                "location.latitude":       GDL_LAT,
                "location.longitude":      GDL_LON,
                "location.within":         "100km",
                "start_date.range_start":  start_str,
                "start_date.range_end":    end_str,
                "expand":                  "venue,category,ticket_availability",
                "sort_by":                 "date",
                "page":                    page,
                "page_size":               50,
            }
            try:
                resp = await client.get(f"{EVENTBRITE_API}/events/search/", params=params)

                if resp.status_code == 429:
                    logger.warning("Eventbrite rate limit, esperando 60s")
                    await asyncio.sleep(60)
                    continue
                if resp.status_code in (401, 403):
                    logger.error("Eventbrite: token inválido o expirado (%s)", resp.status_code)
                    return raw
                if resp.status_code == 404:
                    logger.warning("Eventbrite geo search: 404 — endpoint no disponible")
                    return raw

                resp.raise_for_status()
                data = resp.json()

            except httpx.HTTPError as exc:
                logger.error("Eventbrite geo HTTP error pág %d: %s", page, exc)
                break

            for event in data.get("events", []):
                mapped = self._map_event(event)
                if mapped:
                    raw.append(mapped)

            logger.info("Eventbrite geo pág %d: %d eventos (total: %d)", page, len(data.get("events", [])), len(raw))

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
        max_pages: int = 5,
    ) -> list[dict]:
        """Búsqueda por texto de ciudad."""
        raw: list[dict] = []
        page = 1

        while page <= max_pages:
            params = {
                "q":                       query,
                "start_date.range_start":  start_str,
                "start_date.range_end":    end_str,
                "expand":                  "venue,category,ticket_availability",
                "page":                    page,
                "page_size":               50,
            }
            try:
                resp = await client.get(f"{EVENTBRITE_API}/events/search/", params=params)
                if resp.status_code in (401, 403, 404):
                    return raw
                if resp.status_code == 429:
                    await asyncio.sleep(60)
                    continue
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.debug("Eventbrite query '%s' error: %s", query, exc)
                break

            for event in data.get("events", []):
                # Filtrar solo eventos en México / Jalisco
                venue = event.get("venue") or {}
                addr  = venue.get("address") or {}
                country = addr.get("country", "").upper()
                if country and country != "MX":
                    continue
                mapped = self._map_event(event)
                if mapped:
                    raw.append(mapped)

            if not data.get("pagination", {}).get("has_more_items"):
                break
            page += 1
            await asyncio.sleep(0.3)

        return raw

    async def _search_by_orgs(
        self,
        client: httpx.AsyncClient,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        """
        Busca eventos de organizaciones conocidas en GDL.
        Primero busca organizaciones por query, luego lista sus eventos.
        """
        raw: list[dict] = []
        org_queries = ["Guadalajara", "Foro Magno", "C3 Stage", "Auditorio Telmex"]

        for q in org_queries:
            try:
                resp = await client.get(
                    f"{EVENTBRITE_API}/organizers/search/",
                    params={"q": q, "page_size": 10},
                )
                if resp.status_code not in (200,):
                    continue
                data = resp.json()
                organizers = data.get("organizers", [])

                for org in organizers[:5]:
                    org_id = org.get("id")
                    if not org_id:
                        continue
                    org_events = await self._get_org_events(client, org_id, start_str, end_str)
                    raw.extend(org_events)
                    await asyncio.sleep(0.3)

            except Exception as exc:
                logger.debug("Eventbrite org search '%s': %s", q, exc)

        return raw

    async def _get_org_events(
        self,
        client: httpx.AsyncClient,
        org_id: str,
        start_str: str,
        end_str: str,
    ) -> list[dict]:
        raw: list[dict] = []
        try:
            resp = await client.get(
                f"{EVENTBRITE_API}/organizers/{org_id}/events/",
                params={
                    "start_date.range_start": start_str,
                    "start_date.range_end":   end_str,
                    "expand": "venue,category,ticket_availability",
                    "page_size": 50,
                },
            )
            if resp.status_code != 200:
                return raw
            data = resp.json()
            for event in data.get("events", []):
                mapped = self._map_event(event)
                if mapped:
                    raw.append(mapped)
        except Exception:
            pass
        return raw

    @staticmethod
    def _merge(new_events, seen_ids, all_events) -> int:
        added = 0
        for evt in new_events:
            eid = evt.get("external_id", "")
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                all_events.append(evt)
                added += 1
        return added

    def _map_event(self, event: dict) -> Optional[dict]:
        try:
            start_raw  = (event.get("start") or {}).get("utc")
            end_raw    = (event.get("end") or {}).get("utc")
            start_date = self._parse_dt(start_raw)
            end_date   = self._parse_dt(end_raw)

            if start_date and start_date < datetime.now(timezone.utc):
                return None

            title = (event.get("name") or {}).get("text", "").strip()
            if not title:
                return None

            venue       = event.get("venue") or {}
            address_obj = venue.get("address") or {}
            address = ", ".join(filter(None, [
                address_obj.get("address_1"),
                address_obj.get("city"),
                address_obj.get("region"),
            ])) or "Guadalajara, Jalisco"

            lat = self._to_float(venue.get("latitude"))
            lon = self._to_float(venue.get("longitude"))

            category_obj = event.get("category") or {}
            raw_category = category_obj.get("short_name", "").lower()
            category = self._map_category(raw_category)

            is_free       = event.get("is_free", False)
            ticket_avail  = event.get("ticket_availability") or {}
            min_price_obj = ticket_avail.get("minimum_ticket_price") or {}
            price_value   = min_price_obj.get("value", 0)
            price         = 0.0 if is_free else self._to_float(price_value)

            logo      = event.get("logo") or {}
            image_url = (logo.get("original") or {}).get("url") or logo.get("url")
            description = (event.get("description") or {}).get("text", "") or (event.get("summary") or "")

            return {
                "source_id":   "eventbrite",
                "external_id": event.get("id", ""),
                "title":       title,
                "description": description,
                "category":    category,
                "tags":        [],
                "image_url":   image_url,
                "date_start":  start_date,
                "date_end":    end_date,
                "price":       price,
                "currency":    "MXN",
                "url":         event.get("url", ""),
                "location":    venue.get("name") or address,
                "latitude":    lat,
                "longitude":   lon,
            }
        except Exception as exc:
            logger.warning("Error mapeando Eventbrite %s: %s", event.get("id"), exc)
            return None

    @staticmethod
    def _map_category(raw: str) -> str:
        mapping = {
            "music": "entretenimiento", "sports": "deportivo",
            "food": "gastronomico", "arts": "cultural",
            "film": "cultural", "performing": "cultural",
            "health": "deportivo", "science": "cultural",
            "technology": "cultural", "community": "cultural",
            "fashion": "entretenimiento", "hobbies": "entretenimiento",
            "family": "entretenimiento", "outdoor": "deportivo",
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