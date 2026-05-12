"""
scraper/sources/boletia.py
Scraper para Boletia.com — plataforma mexicana de tickets.
Estrategia: JSON-LD (schema.org) → CSS selectors fallback.
Sin API key. Respeta delays entre requests.

Ciudades cubiertas: Guadalajara, Zapopan, Tlaquepaque, Tonalá,
                    Tlajomulco, Puerto Vallarta + búsqueda estatal Jalisco.
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

# URLs de búsqueda en Boletia para Jalisco
BOLETIA_SEARCH_URLS = [
    "https://boletia.com/buscar?estado=jalisco",
    "https://boletia.com/buscar?q=guadalajara",
    "https://boletia.com/buscar?q=zapopan",
    "https://boletia.com/buscar?q=jalisco",
    "https://boletia.com/buscar?q=puerto+vallarta",
]

BOLETIA_BASE = "https://boletia.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class BoletiaScraper:
    """
    Scraper asíncrono para Boletia.com.
    Extrae eventos de Guadalajara / Jalisco usando HTML scraping.
    """

    def __init__(self, max_pages: int = 5, delay: float = 1.5):
        """
        Args:
            max_pages: Máximo de páginas a paginar por URL base.
            delay: Segundos de espera entre requests (evita ban).
        """
        self.max_pages = max_pages
        self.delay = delay
        self._seen_urls: set[str] = set()

    async def fetch_events(self) -> list[dict[str, Any]]:
        """Punto de entrada principal. Retorna lista de eventos normalizados."""
        all_events: list[dict] = []
        timeout = httpx.Timeout(20.0)

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            for base_url in BOLETIA_SEARCH_URLS:
                for page in range(1, self.max_pages + 1):
                    url = f"{base_url}&page={page}" if page > 1 else base_url
                    events = await self._fetch_page(client, url)

                    if not events:
                        break  # Sin más resultados en esta URL

                    all_events.extend(events)
                    await asyncio.sleep(self.delay)

        # Deduplicar por URL
        unique: list[dict] = []
        seen: set[str] = set()
        for evt in all_events:
            key = evt.get("url", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("Boletia total: %d eventos válidos", len(unique))
        return unique

    async def _fetch_page(
        self, client: httpx.AsyncClient, url: str
    ) -> list[dict[str, Any]]:
        """Descarga una página y extrae eventos."""
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.warning("Boletia HTTP error %s: %s", e.response.status_code, url)
            return []
        except Exception as e:
            logger.warning("Boletia error accediendo %s: %s", url, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # ── Estrategia 1: JSON-LD (schema.org Event) ─────────────────
        events = self._parse_json_ld(soup, resp.url)
        if events:
            return events

        # ── Estrategia 2: CSS selectors de tarjetas de evento ─────────
        events = self._parse_html_cards(soup, resp.url)
        return events

    def _parse_json_ld(
        self, soup: BeautifulSoup, base_url
    ) -> list[dict[str, Any]]:
        """Extrae eventos de bloques JSON-LD con @type=Event."""
        events = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            # Puede ser un objeto o una lista
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("Event", "MusicEvent", "TheaterEvent",
                                              "SportsEvent", "FoodEvent"):
                    continue
                evt = self._normalize_json_ld(item, str(base_url))
                if evt:
                    events.append(evt)

        return events

    def _normalize_json_ld(
        self, item: dict, page_url: str
    ) -> dict[str, Any] | None:
        """Convierte un JSON-LD Event en formato estándar del pipeline."""
        title = item.get("name", "").strip()
        if not title:
            return None

        # Fechas
        date_start = item.get("startDate") or item.get("startdate")
        date_end = item.get("endDate") or item.get("enddate")

        # Ubicación
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

        # Imagen
        image = item.get("image")
        if isinstance(image, list):
            image = image[0] if image else None
        if isinstance(image, dict):
            image = image.get("url")

        # Precio
        price = None
        offers = item.get("offers", {})
        if isinstance(offers, list) and offers:
            offers = offers[0]
        if isinstance(offers, dict):
            price_min = offers.get("price") or offers.get("lowPrice")
            price_max = offers.get("highPrice")
            currency = offers.get("priceCurrency", "MXN")
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

        url = item.get("url") or page_url

        return {
            "title": title,
            "description": item.get("description", ""),
            "date_start": date_start,
            "date_end": date_end,
            "location": location_name,
            "latitude": lat,
            "longitude": lon,
            "image_url": image,
            "url": url,
            "price": price,
            "tags": [],
            "source_id": "boletia",
        }

    def _parse_html_cards(
        self, soup: BeautifulSoup, base_url
    ) -> list[dict[str, Any]]:
        """
        Fallback: extrae tarjetas de evento por CSS selectors.
        Boletia usa clases como 'event-card', 'event-item', etc.
        """
        events = []

        # Selectores conocidos de Boletia (pueden cambiar con el tiempo)
        card_selectors = [
            "article[data-event-id]",
            "div.event-card",
            "div.event-item",
            "div[class*='EventCard']",
            "div[class*='event-card']",
            "a[href*='/eventos/']",
        ]

        cards = []
        for selector in card_selectors:
            cards = soup.select(selector)
            if cards:
                break

        if not cards:
            logger.debug("Boletia: sin tarjetas en %s", base_url)
            return []

        for card in cards:
            try:
                evt = self._extract_card(card, str(base_url))
                if evt:
                    events.append(evt)
            except Exception as e:
                logger.debug("Error parseando tarjeta Boletia: %s", e)

        return events

    def _extract_card(
        self, card: Any, page_url: str
    ) -> dict[str, Any] | None:
        """Extrae datos de una tarjeta individual."""
        # Título
        title_el = (
            card.find(["h2", "h3", "h4"], class_=re.compile(r"title|name|heading", re.I))
            or card.find(["h2", "h3", "h4"])
        )
        title = title_el.get_text(strip=True) if title_el else ""

        if not title:
            # Usar el texto del enlace principal
            link_el = card.find("a", href=re.compile(r"/eventos/"))
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
            date_text_el = card.find(
                class_=re.compile(r"date|fecha|when", re.I)
            )
            date_start = date_text_el.get_text(strip=True) if date_text_el else None

        # Ubicación
        loc_el = card.find(class_=re.compile(r"location|venue|lugar|recinto", re.I))
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

        # Filtrar eventos que no sean de Jalisco
        full_text = card.get_text(" ", strip=True).lower()
        jalisco_keywords = [
            "guadalajara", "zapopan", "tlaquepaque", "tonalá", "tonala",
            "tlajomulco", "jalisco", "puerto vallarta", "vallarta", "gdl",
        ]
        if not any(kw in full_text for kw in jalisco_keywords):
            if not any(kw in location.lower() for kw in jalisco_keywords):
                return None  # Ignorar eventos de otras ciudades

        return {
            "title": title,
            "description": "",
            "date_start": date_start,
            "date_end": None,
            "location": location,
            "latitude": None,
            "longitude": None,
            "image_url": image_url,
            "url": url,
            "price": price,
            "tags": [],
            "source_id": "boletia",
        }