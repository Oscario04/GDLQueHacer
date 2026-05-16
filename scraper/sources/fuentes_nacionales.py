"""
scraper/sources/fuentes_nacionales.py  — v1
Fuentes gubernamentales y culturales nacionales de México.

Fuentes:
  1. Agenda Cultural CDMX         — cdmx.gob.mx/cultura (API JSON pública)
  2. INBA                         — Instituto Nacional de Bellas Artes
  3. Secretaría de Cultura Federal— cultura.gob.mx agenda
  4. UNAM Agenda Cultural         — agenda.unam.mx
  5. Cineteca Nacional            — cineteca.mx
  6. Centro Cultural Banamex      — eventos de arte
  7. Museos INAH                  — sistema de museos
  8. Predicter.com                — agregador de conciertos México
  9. Conciertos.com.mx            — directorio nacional de conciertos
 10. Ocesa / LiveNation México    — promotora principal de México

Estimado: 500–1,500 eventos únicos.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

JSON_HEADERS = {**HEADERS, "Accept": "application/json"}


class FuentesNacionalesScraper:
    """Agrega fuentes culturales y de entretenimiento nacionales de México."""

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    async def fetch_all(self) -> list[dict]:
        tasks = [
            self._fetch_cdmx_cultura(),
            self._fetch_inba(),
            self._fetch_cultura_federal(),
            self._fetch_unam_agenda(),
            self._fetch_cineteca(),
            self._fetch_predicter(),
            self._fetch_conciertos_mx(),
            self._fetch_ticketrapido(),
            self._fetch_eticket(),
            self._fetch_passline(),
        ]

        names = [
            "CDMX Cultura", "INBA", "Cultura Federal", "UNAM Agenda",
            "Cineteca", "Predicter", "Conciertos.mx", "TicketRapido",
            "eTicket", "Passline",
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events: list[dict] = []
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                logger.error("Error en %s: %s", name, result)
            else:
                logger.info("%s: %d eventos", name, len(result))
                all_events.extend(result)

        # Deduplicar
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("Fuentes Nacionales total: %d eventos únicos", len(unique))
        return unique

    # ── 1. CDMX Cultura API ───────────────────────────────────────────

    async def _fetch_cdmx_cultura(self) -> list[dict]:
        """
        La Secretaría de Cultura CDMX expone una API JSON pública en
        datos.cdmx.gob.mx con la agenda cultural de la ciudad.
        """
        events: list[dict] = []
        # Endpoint principal de la agenda
        urls = [
            "https://agenda.cdmx.gob.mx/api/events?per_page=200&page=1",
            "https://agenda.cdmx.gob.mx/api/events?per_page=200&page=2",
            "https://agenda.cdmx.gob.mx/api/events?per_page=200&page=3",
            "https://datos.cdmx.gob.mx/api/3/action/datastore_search?resource_id=agenda-cultural&limit=500",
            # Respaldo HTML
            "https://cultura.cdmx.gob.mx/agenda",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue

                    content_type = resp.headers.get("content-type", "")
                    if "json" in content_type:
                        data = resp.json()
                        raw_list = (
                            data if isinstance(data, list)
                            else data.get("events")
                            or data.get("result", {}).get("records")
                            or data.get("data")
                            or []
                        )
                        for item in raw_list:
                            evt = self._map_cdmx_event(item)
                            if evt:
                                events.append(evt)
                    else:
                        # HTML fallback
                        soup = BeautifulSoup(resp.text, "html.parser")
                        events.extend(self._extract_json_ld(soup, url, "cdmx_cultura"))
                        events.extend(self._extract_html_cards(soup, url, "cdmx_cultura", "CDMX"))

                    await asyncio.sleep(self.delay * 0.5)
                except Exception as exc:
                    logger.debug("CDMX Cultura error %s: %s", url, exc)

        return events

    def _map_cdmx_event(self, item: dict) -> Optional[dict]:
        title = (
            item.get("title") or item.get("nombre") or item.get("name") or ""
        ).strip()
        if not title or len(title) < 3:
            return None

        return {
            "source_id":   "cdmx_cultura",
            "external_id": str(item.get("id") or item.get("_id") or ""),
            "title":       title,
            "description": item.get("description") or item.get("descripcion") or "",
            "category":    "cultural",
            "tags":        ["cultura", "cdmx"],
            "image_url":   item.get("image_url") or item.get("imagen") or item.get("foto"),
            "date_start":  item.get("start_date") or item.get("fecha_inicio") or item.get("date"),
            "date_end":    item.get("end_date") or item.get("fecha_fin"),
            "price":       item.get("price") or item.get("precio") or 0.0,
            "currency":    "MXN",
            "url":         item.get("url") or item.get("link") or "",
            "location":    item.get("venue") or item.get("lugar") or "Ciudad de México",
            "latitude":    self._to_float(item.get("lat") or item.get("latitude")),
            "longitude":   self._to_float(item.get("lon") or item.get("longitude")),
            "estado":      "Ciudad de México",
            "ciudad":      item.get("borough") or item.get("alcaldía") or "CDMX",
        }

    # ── 2. INBA ────────────────────────────────────────────────────────

    async def _fetch_inba(self) -> list[dict]:
        """Instituto Nacional de Bellas Artes y Literatura — agenda."""
        events: list[dict] = []
        urls = [
            "https://inba.gob.mx/agenda",
            "https://inba.gob.mx/prensa/agenda",
            "https://www.bellasartes.gob.mx/agenda",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                for page in range(1, 6):
                    page_url = f"{url}?page={page}" if page > 1 else url
                    try:
                        resp = await client.get(page_url)
                        if resp.status_code != 200:
                            break
                        soup = BeautifulSoup(resp.text, "html.parser")
                        ld = self._extract_json_ld(soup, page_url, "inba")
                        cards = self._extract_html_cards(soup, page_url, "inba", "México")
                        batch = ld or cards
                        if not batch:
                            break
                        events.extend(batch)
                        await asyncio.sleep(self.delay)
                    except Exception as exc:
                        logger.debug("INBA error %s: %s", page_url, exc)
                        break

        return events

    # ── 3. Cultura Federal ────────────────────────────────────────────

    async def _fetch_cultura_federal(self) -> list[dict]:
        """Secretaría de Cultura del Gobierno Federal."""
        events: list[dict] = []
        urls = [
            "https://www.gob.mx/cultura/agenda",
            "https://www.cultura.gob.mx/agenda",
            "https://www.gob.mx/cultura/agenda?page=2",
            "https://www.gob.mx/cultura/agenda?page=3",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    ld = self._extract_json_ld(soup, url, "cultura_federal")
                    cards = self._extract_html_cards(soup, url, "cultura_federal", "México")
                    events.extend(ld or cards)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Cultura Federal error %s: %s", url, exc)

        return events

    # ── 4. UNAM Agenda ────────────────────────────────────────────────

    async def _fetch_unam_agenda(self) -> list[dict]:
        """UNAM Agenda Cultural — universidad más grande de México."""
        events: list[dict] = []
        urls = [
            "https://agenda.unam.mx/",
            "https://www.unam.mx/agenda-cultural",
            "https://agenda.unam.mx/actividades",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                for page in range(1, 8):
                    sep = "&" if "?" in url else "?"
                    page_url = f"{url}{sep}page={page}" if page > 1 else url
                    try:
                        resp = await client.get(page_url)
                        if resp.status_code != 200:
                            break
                        soup = BeautifulSoup(resp.text, "html.parser")
                        ld    = self._extract_json_ld(soup, page_url, "unam_agenda")
                        cards = self._extract_html_cards(soup, page_url, "unam_agenda", "Ciudad de México")
                        batch = ld or cards
                        if not batch:
                            break
                        events.extend(batch)
                        await asyncio.sleep(self.delay)
                    except Exception as exc:
                        logger.debug("UNAM Agenda error %s: %s", page_url, exc)
                        break

        return events

    # ── 5. Cineteca Nacional ──────────────────────────────────────────

    async def _fetch_cineteca(self) -> list[dict]:
        """Cineteca Nacional — programación de cine."""
        events: list[dict] = []
        urls = [
            "https://www.cinetecanacional.net/cartelera/",
            "https://www.cinetecanacional.net/agenda/",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    ld    = self._extract_json_ld(soup, url, "cineteca")
                    cards = self._extract_html_cards(soup, url, "cineteca", "Ciudad de México")
                    events.extend(ld or cards)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Cineteca error %s: %s", url, exc)

        return events

    # ── 6. Predicter.com ──────────────────────────────────────────────

    async def _fetch_predicter(self) -> list[dict]:
        """Predicter.com — agregador de conciertos para México."""
        events: list[dict] = []
        urls = [
            "https://www.predicter.com/conciertosgdl/guadalajara/",
            "https://www.predicter.com/conciertosmx/ciudad-de-mexico/",
            "https://www.predicter.com/conciertosmty/monterrey/",
            "https://www.predicter.com/concerts/mexico/guadalajara/",
            "https://www.predicter.com/concerts/mexico/mexico-city/",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    # Predicter usa JSON-LD extensamente
                    ld    = self._extract_json_ld(soup, url, "predicter")
                    cards = self._extract_html_cards(soup, url, "predicter", "México")
                    events.extend(ld or cards)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Predicter error %s: %s", url, exc)

        return events

    # ── 7. Conciertos.com.mx ──────────────────────────────────────────

    async def _fetch_conciertos_mx(self) -> list[dict]:
        """Directorio nacional de conciertos en México."""
        events: list[dict] = []
        base_urls = [
            "https://www.conciertos.com.mx/guadalajara/",
            "https://www.conciertos.com.mx/ciudad-de-mexico/",
            "https://www.conciertos.com.mx/monterrey/",
            "https://www.conciertos.com.mx/cancun/",
            "https://www.conciertos.com.mx/puebla/",
            "https://www.conciertos.com.mx/queretaro/",
            "https://www.conciertos.com.mx/tijuana/",
            "https://www.conciertos.com.mx/merida/",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in base_urls:
                for page in range(1, 6):
                    page_url = f"{url}?page={page}" if page > 1 else url
                    try:
                        resp = await client.get(page_url)
                        if resp.status_code != 200:
                            break
                        soup = BeautifulSoup(resp.text, "html.parser")
                        ld    = self._extract_json_ld(soup, page_url, "conciertos_mx")
                        cards = self._extract_html_cards(soup, page_url, "conciertos_mx", "México")
                        batch = ld or cards
                        if not batch:
                            break
                        events.extend(batch)
                        await asyncio.sleep(self.delay * 0.7)
                    except Exception as exc:
                        logger.debug("Conciertos.mx error %s: %s", page_url, exc)
                        break

        return events

    # ── 8. TicketRapido ───────────────────────────────────────────────

    async def _fetch_ticketrapido(self) -> list[dict]:
        """TicketRapido — plataforma mexicana de boletos."""
        events: list[dict] = []
        urls = [
            "https://www.ticketrapido.mx/eventos/guadalajara",
            "https://www.ticketrapido.mx/eventos/cdmx",
            "https://www.ticketrapido.mx/eventos/monterrey",
            "https://www.ticketrapido.mx/eventos",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    ld    = self._extract_json_ld(soup, url, "ticketrapido")
                    cards = self._extract_html_cards(soup, url, "ticketrapido", "México")
                    events.extend(ld or cards)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("TicketRapido error %s: %s", url, exc)

        return events

    # ── 9. eTicket ───────────────────────────────────────────────────

    async def _fetch_eticket(self) -> list[dict]:
        """eticket.mx — plataforma de boletos mexicana."""
        events: list[dict] = []
        urls = [
            "https://eticket.mx/guadalajara",
            "https://eticket.mx/ciudad-de-mexico",
            "https://eticket.mx/monterrey",
            "https://eticket.mx/conciertos",
            "https://eticket.mx/teatro",
            "https://eticket.mx/deportes",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    ld    = self._extract_json_ld(soup, url, "eticket")
                    cards = self._extract_html_cards(soup, url, "eticket", "México")
                    events.extend(ld or cards)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("eTicket error %s: %s", url, exc)

        return events

    # ── 10. Passline ──────────────────────────────────────────────────

    async def _fetch_passline(self) -> list[dict]:
        """
        Passline — plataforma de registro y venta de boletos.
        Tiene API JSON pública de eventos.
        """
        events: list[dict] = []

        # API pública de Passline
        api_urls = [
            "https://api.passline.com/v1/events?country=MX&limit=200&page=1",
            "https://api.passline.com/v1/events?country=MX&limit=200&page=2",
            "https://api.passline.com/v1/events?country=MX&limit=200&page=3",
        ]
        # Fallback HTML
        html_urls = [
            "https://www.passline.com/eventos/guadalajara",
            "https://www.passline.com/eventos/ciudad-de-mexico",
            "https://www.passline.com/eventos/monterrey",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS, timeout=20, follow_redirects=True
        ) as client:
            # Intentar API JSON
            for url in api_urls:
                try:
                    resp = await client.get(url, headers={**HEADERS, "Accept": "application/json"})
                    if resp.status_code == 200 and "json" in resp.headers.get("content-type", ""):
                        data = resp.json()
                        raw_list = data if isinstance(data, list) else data.get("events") or data.get("data") or []
                        for item in raw_list:
                            evt = self._map_generic_api_event(item, "passline", "México")
                            if evt:
                                events.append(evt)
                    await asyncio.sleep(self.delay * 0.5)
                except Exception as exc:
                    logger.debug("Passline API error %s: %s", url, exc)

            # Fallback HTML
            if not events:
                for url in html_urls:
                    try:
                        resp = await client.get(url)
                        if resp.status_code != 200:
                            continue
                        soup = BeautifulSoup(resp.text, "html.parser")
                        ld    = self._extract_json_ld(soup, url, "passline")
                        cards = self._extract_html_cards(soup, url, "passline", "México")
                        events.extend(ld or cards)
                        await asyncio.sleep(self.delay)
                    except Exception as exc:
                        logger.debug("Passline HTML error %s: %s", url, exc)

        return events

    # ── Helpers genéricos ─────────────────────────────────────────────

    def _extract_json_ld(
        self, soup: BeautifulSoup, page_url: str, source_id: str
    ) -> list[dict]:
        events = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = re.sub(r"[\x00-\x1f\x7f]", " ", script.string or "")
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    # Manejar @graph
                    if "@graph" in item:
                        items = items + item["@graph"]
                        continue
                    if item.get("@type") in (
                        "Event", "MusicEvent", "TheaterEvent",
                        "SportsEvent", "FoodEvent", "SocialEvent",
                        "ExhibitionEvent", "Festival",
                    ):
                        evt = self._normalize_json_ld(item, page_url, source_id)
                        if evt:
                            events.append(evt)
            except Exception:
                pass
        return events

    def _extract_html_cards(
        self,
        soup: BeautifulSoup,
        page_url: str,
        source_id: str,
        ciudad_default: str,
    ) -> list[dict]:
        events = []
        selectors = [
            "article.event", "div.event-card", "div.event-item",
            "li.event", "[class*='EventCard']", "[class*='event-card']",
            "[class*='event-item']", ".card", "article",
        ]
        cards = []
        for sel in selectors:
            found = soup.select(sel)
            if len(found) >= 2:
                cards = found[:100]
                break

        for card in cards:
            try:
                title_el = (
                    card.find(["h1", "h2", "h3", "h4"])
                    or card.find(class_=re.compile(r"title|titulo|name|nombre", re.I))
                )
                title = title_el.get_text(strip=True) if title_el else ""
                if not title or len(title) < 5:
                    continue

                link_el = card.find("a", href=True)
                url = ""
                if link_el:
                    href = link_el["href"]
                    base = "/".join(page_url.split("/")[:3])
                    url = href if href.startswith("http") else base + href

                date_el = card.find("time") or card.find(attrs={"datetime": True})
                date_str = date_el.get("datetime", "") if date_el else ""

                img_el = card.find("img")
                image_url = None
                if img_el:
                    src = img_el.get("src") or img_el.get("data-src", "")
                    if src and not src.startswith("data:"):
                        base = "/".join(page_url.split("/")[:3])
                        image_url = src if src.startswith("http") else base + src

                loc_el = card.find(class_=re.compile(r"location|venue|lugar|sede", re.I))
                location = loc_el.get_text(strip=True) if loc_el else ciudad_default

                events.append({
                    "source_id":   source_id,
                    "external_id": url or title,
                    "title":       title,
                    "description": "",
                    "category":    "cultural",
                    "tags":        [source_id],
                    "image_url":   image_url,
                    "date_start":  date_str or None,
                    "date_end":    None,
                    "price":       None,
                    "currency":    "MXN",
                    "url":         url or page_url,
                    "location":    location,
                    "latitude":    None,
                    "longitude":   None,
                    "estado":      "México",
                    "ciudad":      ciudad_default,
                })
            except Exception as exc:
                logger.debug("%s HTML card error: %s", source_id, exc)

        return events

    def _normalize_json_ld(
        self, item: dict, page_url: str, source_id: str
    ) -> Optional[dict]:
        title = (item.get("name") or "").strip()
        if not title:
            return None

        loc = item.get("location") or {}
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        location_name = ""
        lat = lon = None
        if isinstance(loc, dict):
            location_name = loc.get("name", "")
            addr = loc.get("address") or {}
            if isinstance(addr, dict):
                parts = [
                    addr.get("streetAddress", ""),
                    addr.get("addressLocality", ""),
                    addr.get("addressRegion", ""),
                ]
                location_name = location_name or ", ".join(p for p in parts if p)
            geo = loc.get("geo") or {}
            if isinstance(geo, dict):
                lat = self._to_float(geo.get("latitude"))
                lon = self._to_float(geo.get("longitude"))

        image = item.get("image")
        if isinstance(image, list):
            image = image[0] if image else None
        if isinstance(image, dict):
            image = image.get("url")

        offers = item.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        price = self._to_float(offers.get("price")) if isinstance(offers, dict) else None

        return {
            "source_id":   source_id,
            "external_id": item.get("url") or item.get("@id") or "",
            "title":       title,
            "description": item.get("description", ""),
            "category":    "cultural",
            "tags":        [source_id],
            "image_url":   image,
            "date_start":  item.get("startDate"),
            "date_end":    item.get("endDate"),
            "price":       price if price is not None else 0.0,
            "currency":    "MXN",
            "url":         item.get("url") or page_url,
            "location":    location_name or "México",
            "latitude":    lat,
            "longitude":   lon,
            "estado":      "México",
            "ciudad":      "",
        }

    def _map_generic_api_event(
        self, item: dict, source_id: str, ciudad_default: str
    ) -> Optional[dict]:
        title = (
            item.get("name") or item.get("title") or item.get("nombre") or ""
        ).strip()
        if not title or len(title) < 3:
            return None

        return {
            "source_id":   source_id,
            "external_id": str(item.get("id") or item.get("_id") or ""),
            "title":       title,
            "description": item.get("description") or item.get("descripcion") or "",
            "category":    "cultural",
            "tags":        [source_id],
            "image_url":   item.get("image_url") or item.get("imagen") or item.get("cover"),
            "date_start":  item.get("start_date") or item.get("fecha_inicio") or item.get("date"),
            "date_end":    item.get("end_date") or item.get("fecha_fin"),
            "price":       item.get("price") or item.get("precio"),
            "currency":    "MXN",
            "url":         item.get("url") or item.get("link") or "",
            "location":    item.get("venue") or item.get("lugar") or ciudad_default,
            "latitude":    self._to_float(item.get("lat") or item.get("latitude")),
            "longitude":   self._to_float(item.get("lon") or item.get("longitude")),
            "estado":      "México",
            "ciudad":      ciudad_default,
        }

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None