"""
scraper/sources/gdl_local.py
Scraper de sitios locales de Guadalajara usando JSON-LD y heurísticas HTML.

MEJORAS v2:
  - 12 sitios (antes 3): agrega visitguadalajara, jalisco.gob.mx,
    zonaocio, taquilla.com, teleticket, GDL cultural agenda, ITESO,
    UdeG eventos, Cinépolis GDL agenda, El Informador agenda, Milenio GDL
  - Paginación automática en sitios que la soporten (hasta 10 páginas)
  - Sin límite artificial de 20 cards — ahora 100 por página
  - Runs en paralelo con asyncio.gather (antes era secuencial por sitio)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCES = [
    # ── Gobierno / Cultura ──────────────────────────────────────────
    {
        "id":    "cultura_gdl",
        "url":   "https://cultura.guadalajara.gob.mx/agenda",
        "name":  "Agenda Cultural GDL",
        "pages": 5,
        "page_param": "page",
    },
    {
        "id":    "jalisco_cultura",
        "url":   "https://cultura.jalisco.gob.mx/agenda-cultural",
        "name":  "Cultura Jalisco",
        "pages": 5,
        "page_param": "page",
    },
    {
        "id":    "visit_gdl",
        "url":   "https://www.visitguadalajara.com/eventos",
        "name":  "Visit Guadalajara",
        "pages": 3,
        "page_param": "p",
    },
    # ── Medios / Agenda ─────────────────────────────────────────────
    {
        "id":    "informador_agenda",
        "url":   "https://www.informador.mx/agenda",
        "name":  "El Informador — Agenda",
        "pages": 5,
        "page_param": "pagina",
    },
    {
        "id":    "milenio_gdl",
        "url":   "https://www.milenio.com/estados/jalisco/eventos",
        "name":  "Milenio Jalisco",
        "pages": 3,
        "page_param": "page",
    },
    {
        "id":    "ocio_mx",
        "url":   "https://www.ocio.mx/guadalajara/",
        "name":  "Ocio.mx Guadalajara",
        "pages": 5,
        "page_param": "page",
    },
    {
        "id":    "siente_gdl",
        "url":   "https://sientegdl.com/eventos/",
        "name":  "Siente GDL",
        "pages": 5,
        "page_param": "page",
    },
    # ── Universidades / Recintos ─────────────────────────────────────
    {
        "id":    "udeg_eventos",
        "url":   "https://www.udg.mx/es/agenda",
        "name":  "UdeG Agenda",
        "pages": 3,
        "page_param": "page",
    },
    {
        "id":    "iteso_eventos",
        "url":   "https://www.iteso.mx/web/general/agenda-iteso",
        "name":  "ITESO Agenda",
        "pages": 2,
        "page_param": "page",
    },
    # ── Entretenimiento / Tickets ────────────────────────────────────
    {
        "id":    "taquilla_gdl",
        "url":   "https://www.taquilla.com/entradas/events/guadalajara",
        "name":  "Taquilla.com GDL",
        "pages": 5,
        "page_param": "page",
    },
    {
        "id":    "zonaocio_gdl",
        "url":   "https://www.zonaocio.com/guadalajara/",
        "name":  "ZonaOcio GDL",
        "pages": 3,
        "page_param": "p",
    },
    {
        "id":    "gdlcultural",
        "url":   "https://www.gdlcultural.com/agenda",
        "name":  "GDL Cultural",
        "pages": 5,
        "page_param": "page",
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
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

        # Deduplicar por URL
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("GDL Local total: %d eventos", len(unique))
        return unique

    async def _scrape_source(self, source: dict) -> list[dict]:
        """Scrapea un sitio con paginación."""
        all_events: list[dict] = []
        max_pages  = source.get("pages", 1)
        page_param = source.get("page_param", "page")

        async with httpx.AsyncClient(
            timeout=20,
            headers=HEADERS,
            follow_redirects=True,
        ) as client:
            for page_num in range(1, max_pages + 1):
                if page_num == 1:
                    url = source["url"]
                else:
                    sep = "&" if "?" in source["url"] else "?"
                    url = f"{source['url']}{sep}{page_param}={page_num}"

                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                except httpx.HTTPError as exc:
                    logger.warning("No se pudo acceder a %s (pág %d): %s", source["url"], page_num, exc)
                    break

                soup   = BeautifulSoup(resp.text, "html.parser")
                events = []

                # Estrategia 1: JSON-LD
                jsonld_events = self._extract_jsonld_events(soup, source["id"])
                events.extend(jsonld_events)

                # Estrategia 2: __NEXT_DATA__ (React/Next.js)
                if not jsonld_events:
                    next_events = self._extract_next_data(soup, source["id"])
                    events.extend(next_events)

                # Estrategia 3: HTML heurístico
                if not events:
                    html_events = self._extract_html_events(soup, source)
                    events.extend(html_events)

                if not events:
                    break   # Sin resultados en esta página — parar paginación

                all_events.extend(events)
                await asyncio.sleep(0.8)

        return all_events

    # ── Parsers ───────────────────────────────────────────────────────

    def _extract_next_data(self, soup: BeautifulSoup, source_id: str) -> list[dict]:
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag:
            return []
        try:
            data   = json.loads(tag.string or "")
            props  = data.get("props", {}).get("pageProps", {})
            events_raw = (
                props.get("events")
                or props.get("agenda")
                or props.get("items")
                or []
            )
            result = []
            for item in events_raw:
                mapped = self._map_next_event(item, source_id)
                if mapped:
                    result.append(mapped)
            return result
        except Exception:
            return []

    def _map_next_event(self, item: dict, source_id: str) -> dict | None:
        title = (item.get("name") or item.get("title") or item.get("nombre") or "").strip()
        if not title:
            return None
        return {
            "source_id":   source_id,
            "external_id": str(item.get("id") or ""),
            "title":       title,
            "description": item.get("description") or item.get("descripcion") or "",
            "category":    "cultural",
            "tags":        [],
            "image_url":   item.get("image") or item.get("imagen") or item.get("thumbnail"),
            "date_start":  item.get("startDate") or item.get("date") or item.get("fecha"),
            "date_end":    item.get("endDate"),
            "price":       item.get("price") or item.get("precio"),
            "currency":    "MXN",
            "url":         item.get("url") or item.get("link") or "",
            "location":    item.get("venue") or item.get("lugar") or "Guadalajara, Jalisco",
            "latitude":    self._to_float(item.get("lat") or item.get("latitude")),
            "longitude":   self._to_float(item.get("lng") or item.get("longitude")),
        }

    def _extract_jsonld_events(self, soup: BeautifulSoup, source_id: str) -> list[dict]:
        events = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string or ""
                raw = re.sub(r"[\x00-\x1f\x7f]", " ", raw)
                data = json.loads(raw)

                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, list):
                    items = data
                else:
                    continue

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if item.get("@type") == "WebPage" and "@graph" in item:
                        items.extend(item["@graph"])
                        continue
                    if item.get("@type") in (
                        "Event", "SocialEvent", "MusicEvent",
                        "TheaterEvent", "SportsEvent",
                        "FoodEvent", "ExhibitionEvent",
                    ):
                        mapped = self._map_jsonld_event(item, source_id)
                        if mapped:
                            events.append(mapped)
            except Exception as exc:
                logger.debug("JSON-LD parse error en %s: %s", source_id, exc)
        return events

    def _extract_html_events(self, soup: BeautifulSoup, source: dict) -> list[dict]:
        events    = []
        source_id  = source["id"]
        source_url = source["url"]

        selectors = [
            "article.event", "article.evento", ".event-card", ".evento-card",
            ".card-event", "li.event", "div.event-item", "div.evento",
            "[class*='event']", "[class*='evento']",
        ]

        candidates = []
        for selector in selectors:
            found = soup.select(selector)
            if found:
                candidates = found[:100]   # hasta 100 por página (antes 20)
                break

        if not candidates:
            candidates = soup.find_all("article")[:50]

        for card in candidates:
            try:
                title_el = (
                    card.find("h1") or card.find("h2") or
                    card.find("h3") or card.find(class_=re.compile(r"title|titulo|name"))
                )
                title = title_el.get_text(strip=True) if title_el else ""
                if not title or len(title) < 5:
                    continue

                desc_el     = card.find("p") or card.find(class_=re.compile(r"desc|description"))
                description = desc_el.get_text(strip=True) if desc_el else ""

                date_start = self._find_date_in_element(card)

                img       = card.find("img")
                image_url = None
                if img:
                    image_url = img.get("src") or img.get("data-src")
                    if image_url and image_url.startswith("/"):
                        base      = "/".join(source_url.split("/")[:3])
                        image_url = base + image_url

                link      = card.find("a")
                event_url = ""
                if link:
                    href = link.get("href", "")
                    if href.startswith("http"):
                        event_url = href
                    elif href.startswith("/"):
                        base      = "/".join(source_url.split("/")[:3])
                        event_url = base + href

                events.append({
                    "source_id":   source_id,
                    "external_id": "",
                    "title":       title,
                    "description": description,
                    "category":    "cultural",
                    "tags":        [],
                    "image_url":   image_url,
                    "date_start":  date_start,
                    "date_end":    None,
                    "price":       None,
                    "currency":    "MXN",
                    "url":         event_url or source_url,
                    "location":    "Guadalajara, Jalisco",
                    "latitude":    20.6597,
                    "longitude":   -103.3496,
                })
            except Exception as exc:
                logger.debug("Error parseando card HTML: %s", exc)

        return events

    def _map_jsonld_event(self, data: dict, source_id: str) -> Optional[dict]:
        try:
            title = data.get("name", "").strip()
            if not title:
                return None

            description = data.get("description", "")
            date_start  = self._parse_dt(data.get("startDate"))
            date_end    = self._parse_dt(data.get("endDate"))

            if date_start and date_start < datetime.now(timezone.utc):
                if date_start.tzinfo is None:
                    date_start = date_start.replace(tzinfo=timezone.utc)
                else:
                    return None

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
                    city   = address.get("addressLocality", "Guadalajara")
                    location_name = location_name or f"{street}, {city}".strip(", ")
                geo = location_data.get("geo") or {}
                lat = self._to_float(geo.get("latitude"))
                lon = self._to_float(geo.get("longitude"))

            offers = data.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            price = self._to_float(offers.get("price"))

            image = data.get("image")
            if isinstance(image, list):
                image = image[0] if image else None
            if isinstance(image, dict):
                image = image.get("url")

            event_type = data.get("@type", "")
            category   = self._map_jsonld_type(event_type)

            return {
                "source_id":   source_id,
                "external_id": data.get("identifier", ""),
                "title":       title,
                "description": description,
                "category":    category,
                "tags":        [],
                "image_url":   image,
                "date_start":  date_start,
                "date_end":    date_end,
                "price":       price if price is not None else 0.0,
                "currency":    "MXN",
                "url":         data.get("url", ""),
                "location":    location_name or "Guadalajara, Jalisco",
                "latitude":    lat or 20.6597,
                "longitude":   lon or -103.3496,
            }
        except Exception as exc:
            logger.debug("Error mapeando JSON-LD: %s", exc)
            return None

    @staticmethod
    def _map_jsonld_type(event_type: str) -> str:
        mapping = {
            "MusicEvent":     "entretenimiento",
            "TheaterEvent":   "cultural",
            "SportsEvent":    "deportivo",
            "FoodEvent":      "gastronomico",
            "ExhibitionEvent":"cultural",
            "SocialEvent":    "entretenimiento",
            "Event":          "cultural",
        }
        return mapping.get(event_type, "cultural")

    @staticmethod
    def _find_date_in_element(element) -> Optional[datetime]:
        time_el = element.find("time")
        if time_el:
            dt_attr = time_el.get("datetime")
            if dt_attr:
                try:
                    return datetime.fromisoformat(dt_attr.replace("Z", "+00:00"))
                except ValueError:
                    pass
        text     = element.get_text(" ", strip=True)
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
                        return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
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
            s  = str(value).strip()
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
                try:
                    return datetime.strptime(str(value)[:10], fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return None

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None