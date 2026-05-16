"""
scraper/sources/boletia.py
Scraper para Boletia.com — plataforma mexicana de tickets.

MEJORAS v2:
  - Intenta la API interna de Boletia (/api/events) antes del HTML
  - 20+ URLs de búsqueda (por categoría, municipio, keyword)
  - max_pages subido a 20 (antes 5)
  - Eliminado el filtro agresivo de keywords de Jalisco en _parse_html_cards
    (confiamos en la URL de búsqueda para la relevancia geográfica)
  - Selectores CSS actualizados con más variantes
  - Paginación por cursor/offset cuando la API lo soporta
"""
import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlencode

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BOLETIA_BASE = "https://boletia.com"

# ── URLs de búsqueda HTML ────────────────────────────────────────────
BOLETIA_SEARCH_URLS = [
    # Por estado / ciudad
    "https://boletia.com/buscar?estado=jalisco",
    "https://boletia.com/buscar?q=guadalajara",
    "https://boletia.com/buscar?q=zapopan",
    "https://boletia.com/buscar?q=jalisco",
    "https://boletia.com/buscar?q=puerto+vallarta",
    "https://boletia.com/buscar?q=tlaquepaque",
    "https://boletia.com/buscar?q=tonala",
    "https://boletia.com/buscar?q=tlajomulco",
    "https://boletia.com/buscar?q=gdl",
    # Por categoría + jalisco
    "https://boletia.com/buscar?q=concierto+jalisco",
    "https://boletia.com/buscar?q=festival+jalisco",
    "https://boletia.com/buscar?q=teatro+guadalajara",
    "https://boletia.com/buscar?q=stand+up+guadalajara",
    "https://boletia.com/buscar?q=expo+guadalajara",
    "https://boletia.com/buscar?q=deportes+guadalajara",
    "https://boletia.com/buscar?q=familia+guadalajara",
    "https://boletia.com/buscar?q=arte+guadalajara",
    "https://boletia.com/buscar?q=musica+guadalajara",
    "https://boletia.com/buscar?q=baile+guadalajara",
    "https://boletia.com/buscar?q=feria+jalisco",
]

# ── Endpoint de API interna de Boletia (no oficial, puede cambiar) ───
BOLETIA_API_URL = "https://boletia.com/api/v1/events"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
}

API_HEADERS = {
    **HEADERS,
    "Accept": "application/json",
}

JALISCO_KEYWORDS = [
    "guadalajara", "zapopan", "tlaquepaque", "tonalá", "tonala",
    "tlajomulco", "jalisco", "puerto vallarta", "vallarta", "gdl",
    "el salto", "lagos de moreno", "chapala", "tequila",
]


class BoletiaScraper:
    """
    Scraper asíncrono para Boletia.com.
    Estrategia: API interna → JSON-LD → CSS selectors.
    """

    def __init__(self, max_pages: int = 20, delay: float = 1.0):
        self.max_pages = max_pages
        self.delay = delay

    async def fetch_events(self) -> list[dict[str, Any]]:
        all_events: list[dict] = []
        timeout = httpx.Timeout(25.0)

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=timeout,
            follow_redirects=True,
        ) as client:

            # ── Estrategia 0: API interna ────────────────────────────
            api_events = await self._fetch_api(client)
            all_events.extend(api_events)
            logger.info("Boletia API interna: %d eventos", len(api_events))

            await asyncio.sleep(self.delay)

            # ── Estrategia 1+2: Scraping HTML por URL de búsqueda ────
            for base_url in BOLETIA_SEARCH_URLS:
                for page in range(1, self.max_pages + 1):
                    url = f"{base_url}&page={page}" if page > 1 else base_url
                    events = await self._fetch_page(client, url)

                    if not events:
                        break

                    all_events.extend(events)
                    await asyncio.sleep(self.delay)

        # Deduplicar por URL (o título si no hay URL)
        unique: list[dict] = []
        seen: set[str] = set()
        for evt in all_events:
            key = evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("Boletia total: %d eventos válidos", len(unique))
        return unique

    # ── API interna ───────────────────────────────────────────────────

    async def _fetch_api(self, client: httpx.AsyncClient) -> list[dict]:
        """
        Intenta consumir la API interna/JSON de Boletia.
        Si no responde con JSON útil, retorna lista vacía sin error.
        """
        events: list[dict] = []
        search_terms = ["guadalajara", "jalisco", "zapopan", "vallarta"]

        for term in search_terms:
            try:
                resp = await client.get(
                    BOLETIA_API_URL,
                    params={"q": term, "per_page": 100},
                    headers=API_HEADERS,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()

                # La API puede retornar {"events": [...]} o directamente [...]
                raw_list = (
                    data if isinstance(data, list)
                    else data.get("events") or data.get("data") or []
                )

                for item in raw_list:
                    evt = self._normalize_api_event(item)
                    if evt:
                        events.append(evt)

                await asyncio.sleep(self.delay * 0.5)

            except Exception as e:
                logger.debug("Boletia API term '%s' error: %s", term, e)
                # API no disponible — continuar con scraping HTML
                break

        return events

    def _normalize_api_event(self, item: dict) -> dict | None:
        title = (item.get("name") or item.get("title") or "").strip()
        if not title:
            return None

        location = item.get("venue_name") or item.get("location") or ""
        city     = item.get("city") or item.get("ciudad") or ""
        state    = item.get("state") or item.get("estado") or ""
        full_loc = f"{location}, {city}, {state}".strip(", ")

        # Filtrar solo Jalisco
        if not any(kw in full_loc.lower() for kw in JALISCO_KEYWORDS):
            if not any(kw in title.lower() for kw in JALISCO_KEYWORDS):
                return None

        return {
            "title":       title,
            "description": item.get("description") or item.get("short_description") or "",
            "date_start":  item.get("starts_at") or item.get("start_date") or item.get("date"),
            "date_end":    item.get("ends_at") or item.get("end_date"),
            "location":    full_loc or "Guadalajara, Jalisco",
            "latitude":    self._to_float(item.get("latitude") or item.get("lat")),
            "longitude":   self._to_float(item.get("longitude") or item.get("lng")),
            "image_url":   item.get("image_url") or item.get("cover_image"),
            "url":         item.get("url") or item.get("event_url") or "",
            "price":       item.get("min_price") or item.get("price"),
            "tags":        [],
            "source_id":   "boletia",
        }

    # ── Scraping HTML ─────────────────────────────────────────────────

    async def _fetch_page(
        self, client: httpx.AsyncClient, url: str
    ) -> list[dict[str, Any]]:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.warning("Boletia HTTP %s: %s", e.response.status_code, url)
            return []
        except Exception as e:
            logger.warning("Boletia error %s: %s", url, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Estrategia 1: JSON-LD
        events = self._parse_json_ld(soup, resp.url)
        if events:
            return events

        # Estrategia 2: JSON embebido en <script> (Next.js / React)
        events = self._parse_next_data(soup)
        if events:
            return events

        # Estrategia 3: CSS selectors
        return self._parse_html_cards(soup, resp.url)

    def _parse_next_data(self, soup: BeautifulSoup) -> list[dict]:
        """
        Algunos sitios modernos inyectan todos los datos en
        <script id="__NEXT_DATA__"> como JSON. Lo intentamos parsear.
        """
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag:
            return []
        try:
            data = json.loads(tag.string or "")
            # Navegar la estructura hasta encontrar una lista de eventos
            props = data.get("props", {}).get("pageProps", {})
            events_raw = (
                props.get("events")
                or props.get("initialData", {}).get("events")
                or []
            )
            result = []
            for item in events_raw:
                evt = self._normalize_api_event(item)
                if evt:
                    result.append(evt)
            return result
        except Exception:
            return []

    def _parse_json_ld(self, soup: BeautifulSoup, base_url) -> list[dict]:
        events = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in (
                    "Event", "MusicEvent", "TheaterEvent",
                    "SportsEvent", "FoodEvent", "SocialEvent",
                ):
                    continue
                evt = self._normalize_json_ld(item, str(base_url))
                if evt:
                    events.append(evt)
        return events

    def _normalize_json_ld(self, item: dict, page_url: str) -> dict | None:
        title = item.get("name", "").strip()
        if not title:
            return None

        date_start = item.get("startDate") or item.get("startdate")
        date_end   = item.get("endDate") or item.get("enddate")

        location_data = item.get("location", {})
        location_name = ""
        lat = lon = None
        if isinstance(location_data, dict):
            location_name = location_data.get("name", "")
            address = location_data.get("address", {})
            if isinstance(address, dict):
                parts = [
                    address.get("streetAddress", ""),
                    address.get("addressLocality", ""),
                    address.get("addressRegion", ""),
                ]
                location_name = location_name or ", ".join(p for p in parts if p)
            geo = location_data.get("geo", {})
            if isinstance(geo, dict):
                try:
                    lat = float(geo.get("latitude", 0)) or None
                    lon = float(geo.get("longitude", 0)) or None
                except (ValueError, TypeError):
                    pass

        image = item.get("image")
        if isinstance(image, list):
            image = image[0] if image else None
        if isinstance(image, dict):
            image = image.get("url")

        price = None
        offers = item.get("offers", {})
        if isinstance(offers, list) and offers:
            offers = offers[0]
        if isinstance(offers, dict):
            price_min = offers.get("price") or offers.get("lowPrice")
            price_max = offers.get("highPrice")
            currency  = offers.get("priceCurrency", "MXN")
            if price_min is not None:
                try:
                    price_min = float(price_min)
                    price = (
                        f"{currency} {price_min:.0f}"
                        if price_max is None
                        else f"{currency} {price_min:.0f}–{float(price_max):.0f}"
                    )
                except (ValueError, TypeError):
                    pass

        return {
            "title":       title,
            "description": item.get("description", ""),
            "date_start":  date_start,
            "date_end":    date_end,
            "location":    location_name,
            "latitude":    lat,
            "longitude":   lon,
            "image_url":   image,
            "url":         item.get("url") or page_url,
            "price":       price,
            "tags":        [],
            "source_id":   "boletia",
        }

    def _parse_html_cards(self, soup: BeautifulSoup, base_url) -> list[dict]:
        events = []

        # Selectores ampliados
        card_selectors = [
            "article[data-event-id]",
            "div.event-card",
            "div.event-item",
            "div[class*='EventCard']",
            "div[class*='event-card']",
            "div[class*='event-item']",
            "div[class*='EventItem']",
            "li[class*='event']",
            "a[href*='/e/']",
            "a[href*='/eventos/']",
            "a[href*='/event/']",
            ".grid > div",   # grids genéricos de React
        ]

        cards = []
        for selector in card_selectors:
            cards = soup.select(selector)
            if len(cards) >= 3:   # mínimo 3 para confiar en el selector
                break

        if not cards:
            logger.debug("Boletia: sin tarjetas en %s", base_url)
            return []

        for card in cards[:100]:   # límite por página
            try:
                evt = self._extract_card(card, str(base_url))
                if evt:
                    events.append(evt)
            except Exception as e:
                logger.debug("Error parseando tarjeta Boletia: %s", e)

        return events

    def _extract_card(self, card: Any, page_url: str) -> dict | None:
        # Título
        title_el = (
            card.find(["h2", "h3", "h4"], class_=re.compile(r"title|name|heading", re.I))
            or card.find(["h2", "h3", "h4"])
        )
        title = title_el.get_text(strip=True) if title_el else ""

        if not title:
            link_el = card.find("a")
            title = link_el.get_text(strip=True) if link_el else ""
        if not title:
            return None

        # URL
        link_el = card.find("a", href=True)
        url = ""
        if link_el:
            href = link_el["href"]
            url = href if href.startswith("http") else urljoin(BOLETIA_BASE, href)

        # Fecha
        date_el = card.find(["time", "span", "div"], attrs={"datetime": True})
        date_start = date_el["datetime"] if date_el else None
        if not date_start:
            date_text_el = card.find(class_=re.compile(r"date|fecha|when", re.I))
            date_start = date_text_el.get_text(strip=True) if date_text_el else None

        # Ubicación
        loc_el = card.find(class_=re.compile(r"location|venue|lugar|recinto|city|ciudad", re.I))
        location = loc_el.get_text(strip=True) if loc_el else ""

        # Imagen
        img_el = card.find("img", src=True)
        image_url = None
        if img_el:
            src = img_el.get("src") or img_el.get("data-src", "")
            image_url = src if src.startswith("http") else urljoin(BOLETIA_BASE, src)

        # Precio
        price_el = card.find(class_=re.compile(r"price|precio|costo", re.I))
        price = price_el.get_text(strip=True) if price_el else None

        # Filtrar por Jalisco SOLO si hay texto de ubicación explícita de otra ciudad
        full_text = (location + " " + title).lower()
        # Sólo rechazar si hay keyword de otra ciudad MX conocida Y no hay keyword Jalisco
        other_cities = ["ciudad de mexico", "monterrey", "cancun", "tijuana", "puebla", "cdmx"]
        is_other_city = any(c in full_text for c in other_cities)
        is_jalisco    = any(k in full_text for k in JALISCO_KEYWORDS)

        if is_other_city and not is_jalisco:
            return None

        return {
            "title":       title,
            "description": "",
            "date_start":  date_start,
            "date_end":    None,
            "location":    location,
            "latitude":    None,
            "longitude":   None,
            "image_url":   image_url,
            "url":         url,
            "price":       price,
            "tags":        [],
            "source_id":   "boletia",
        }

    @staticmethod
    def _to_float(value) -> float | None:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return Nonea