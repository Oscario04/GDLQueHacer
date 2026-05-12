"""
scraper/sources/gdl_local.py
Scraper de sitios locales de Guadalajara usando JSON-LD y heurísticas HTML.

Sitios incluidos:
  - cultura.guadalajara.gob.mx  (Agenda cultural del municipio)
  - ocio.mx                     (Agenda de ocio)
  - sientegdl.com               (Eventos locales)

No requiere API key. Usa BeautifulSoup + httpx.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Sitios a scrapear con su configuración
SOURCES = [
    {
        "id": "cultura_gdl",
        "url": "https://cultura.guadalajara.gob.mx/agenda",
        "name": "Agenda Cultural GDL",
    },
    {
        "id": "ocio_mx",
        "url": "https://www.ocio.mx/guadalajara/",
        "name": "Ocio.mx Guadalajara",
    },
    {
        "id": "siente_gdl",
        "url": "https://sientegdl.com/eventos/",
        "name": "Siente GDL",
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class GDLLocalScraper:
    """Scraper multi-fuente para sitios locales de Guadalajara."""

    async def fetch_all(self) -> list[dict]:
        """Corre todos los scrapers locales en paralelo."""
        tasks = [self._scrape_source(source) for source in SOURCES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events = []
        for source, result in zip(SOURCES, results):
            if isinstance(result, Exception):
                logger.error("Error en %s: %s", source["id"], result)
            else:
                logger.info("%s: %d eventos", source["id"], len(result))
                all_events.extend(result)

        logger.info("GDL Local total: %d eventos", len(all_events))
        return all_events

    async def _scrape_source(self, source: dict) -> list[dict]:
        """Scrapea un sitio individual."""
        async with httpx.AsyncClient(
            timeout=20,
            headers=HEADERS,
            follow_redirects=True,
        ) as client:
            try:
                resp = await client.get(source["url"])
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("No se pudo acceder a %s: %s", source["url"], exc)
                return []

        soup = BeautifulSoup(resp.text, "html.parser")
        events = []

        # Estrategia 1: JSON-LD (schema.org/Event) — la más confiable
        jsonld_events = self._extract_jsonld_events(soup, source["id"])
        events.extend(jsonld_events)

        # Estrategia 2: Open Graph + heurísticas HTML si no hay JSON-LD
        if not jsonld_events:
            html_events = self._extract_html_events(soup, source)
            events.extend(html_events)

        return events

    def _extract_jsonld_events(self, soup: BeautifulSoup, source_id: str) -> list[dict]:
        """Extrae eventos de bloques JSON-LD embebidos en el HTML."""
        events = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string or ""
                # Limpiar caracteres de control que rompen el parser
                raw = re.sub(r"[\x00-\x1f\x7f]", " ", raw)
                data = json.loads(raw)

                # Puede ser un objeto único o una lista
                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, list):
                    items = data
                else:
                    continue

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    # Manejar @graph
                    if item.get("@type") == "WebPage" and "@graph" in item:
                        items.extend(item["@graph"])
                        continue
                    if item.get("@type") in ("Event", "SocialEvent", "MusicEvent",
                                              "TheaterEvent", "SportsEvent",
                                              "FoodEvent", "ExhibitionEvent"):
                        mapped = self._map_jsonld_event(item, source_id)
                        if mapped:
                            events.append(mapped)

            except (json.JSONDecodeError, Exception) as exc:
                logger.debug("JSON-LD parse error en %s: %s", source_id, exc)

        return events

    def _extract_html_events(self, soup: BeautifulSoup, source: dict) -> list[dict]:
        """
        Heurística HTML: busca tarjetas de eventos con patrones comunes.
        Funciona para sitios que no tienen JSON-LD pero sí estructura semántica.
        """
        events = []
        source_id = source["id"]
        source_url = source["url"]

        # Buscar artículos/tarjetas con patrones de eventos
        selectors = [
            "article.event", "article.evento", ".event-card", ".evento-card",
            ".card-event", "li.event", "div.event-item", "div.evento",
            "[class*='event']", "[class*='evento']",
        ]

        candidates = []
        for selector in selectors:
            found = soup.select(selector)
            if found:
                candidates = found[:20]  # Máximo 20 por sitio
                break

        # Fallback: buscar todos los artículos
        if not candidates:
            candidates = soup.find_all("article")[:15]

        for card in candidates:
            try:
                # Título
                title_el = (
                    card.find("h1") or card.find("h2") or
                    card.find("h3") or card.find(class_=re.compile(r"title|titulo|name"))
                )
                title = title_el.get_text(strip=True) if title_el else ""
                if not title or len(title) < 5:
                    continue

                # Descripción
                desc_el = card.find("p") or card.find(class_=re.compile(r"desc|description"))
                description = desc_el.get_text(strip=True) if desc_el else ""

                # Fecha (buscar texto con patrón de fecha)
                date_start = self._find_date_in_element(card)

                # Imagen
                img = card.find("img")
                image_url = None
                if img:
                    image_url = img.get("src") or img.get("data-src")
                    if image_url and image_url.startswith("/"):
                        # Convertir URL relativa a absoluta
                        base = "/".join(source_url.split("/")[:3])
                        image_url = base + image_url

                # URL del evento
                link = card.find("a")
                event_url = ""
                if link:
                    href = link.get("href", "")
                    if href.startswith("http"):
                        event_url = href
                    elif href.startswith("/"):
                        base = "/".join(source_url.split("/")[:3])
                        event_url = base + href

                events.append({
                    "source_id": source_id,
                    "external_id": "",
                    "title": title,
                    "description": description,
                    "category": "cultural",  # Default, ML lo reclasificará
                    "tags": [],
                    "image_url": image_url,
                    "date_start": date_start,
                    "date_end": None,
                    "price": None,
                    "currency": "MXN",
                    "url": event_url or source_url,
                    "location": "Guadalajara, Jalisco",
                    "latitude": 20.6597,
                    "longitude": -103.3496,
                })

            except Exception as exc:
                logger.debug("Error parseando card HTML: %s", exc)

        return events

    def _map_jsonld_event(self, data: dict, source_id: str) -> Optional[dict]:
        """Mapea un evento JSON-LD (schema.org) al esquema base."""
        try:
            title = data.get("name", "").strip()
            if not title:
                return None

            description = data.get("description", "")

            # Fechas
            date_start = self._parse_dt(data.get("startDate"))
            date_end = self._parse_dt(data.get("endDate"))

            # Filtrar eventos pasados
            if date_start and date_start < datetime.now(timezone.utc):
                # Intentar si es naive datetime (sin timezone)
                if date_start.tzinfo is None:
                    date_start = date_start.replace(tzinfo=timezone.utc)
                else:
                    return None

            # Localización
            location_data = data.get("location") or {}
            if isinstance(location_data, str):
                location_name = location_data
                lat = lon = None
            else:
                location_name = location_data.get("name", "")
                address = location_data.get("address") or {}
                if isinstance(address, str):
                    location_name = location_name or address
                else:
                    street = address.get("streetAddress", "")
                    city = address.get("addressLocality", "Guadalajara")
                    location_name = location_name or f"{street}, {city}".strip(", ")

                geo = location_data.get("geo") or {}
                lat = self._to_float(geo.get("latitude"))
                lon = self._to_float(geo.get("longitude"))

            # Precio
            offers = data.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            price = self._to_float(offers.get("price"))

            # Imagen
            image = data.get("image")
            if isinstance(image, list):
                image = image[0] if image else None
            if isinstance(image, dict):
                image = image.get("url")

            # Categoría por tipo de evento JSON-LD
            event_type = data.get("@type", "")
            category = self._map_jsonld_type(event_type)

            return {
                "source_id": source_id,
                "external_id": data.get("identifier", ""),
                "title": title,
                "description": description,
                "category": category,
                "tags": [],
                "image_url": image,
                "date_start": date_start,
                "date_end": date_end,
                "price": price if price is not None else 0.0,
                "currency": "MXN",
                "url": data.get("url", ""),
                "location": location_name or "Guadalajara, Jalisco",
                "latitude": lat or 20.6597,
                "longitude": lon or -103.3496,
            }

        except Exception as exc:
            logger.debug("Error mapeando JSON-LD: %s", exc)
            return None

    @staticmethod
    def _map_jsonld_type(event_type: str) -> str:
        mapping = {
            "MusicEvent": "entretenimiento",
            "TheaterEvent": "cultural",
            "SportsEvent": "deportivo",
            "FoodEvent": "gastronomico",
            "ExhibitionEvent": "cultural",
            "SocialEvent": "entretenimiento",
            "Event": "cultural",
        }
        return mapping.get(event_type, "cultural")

    @staticmethod
    def _find_date_in_element(element) -> Optional[datetime]:
        """Busca fechas en texto de un elemento HTML."""
        # Buscar atributo datetime en <time>
        time_el = element.find("time")
        if time_el:
            dt_attr = time_el.get("datetime")
            if dt_attr:
                try:
                    return datetime.fromisoformat(
                        dt_attr.replace("Z", "+00:00")
                    )
                except ValueError:
                    pass

        # Buscar patrones de fecha en texto
        text = element.get_text(" ", strip=True)
        patterns = [
            r"(\d{1,2}/\d{1,2}/\d{4})",
            r"(\d{4}-\d{2}-\d{2})",
            r"(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                for fmt in ["%d/%m/%Y", "%Y-%m-%d"]:
                    try:
                        return datetime.strptime(date_str, fmt).replace(
                            tzinfo=timezone.utc
                        )
                    except ValueError:
                        continue
        return None

    @staticmethod
    def _parse_dt(value) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            s = str(value).strip()
            # Intentar ISO 8601
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            # Intentar formatos alternativos
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
                try:
                    return datetime.strptime(str(value)[:10], fmt).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    continue
        return None

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None