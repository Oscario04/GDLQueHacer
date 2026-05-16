"""
scraper/sources/sic_jalisco.py
Scraper para el Sistema de Información Cultural (SIC) de la Secretaría de Cultura.

MEJORAS v2:
  - Más tablas: agrega 'espectaculo', 'museo' (exposiciones), 'curso'
  - MAX_FICHAS subido a 500 (antes 50) — hay muchos eventos en SIC
  - Paginación en la lista del SIC (parámetro &page=N)
  - Retry en errores HTTP temporales
  - Concurrencia controlada en la descarga de fichas (semáforo de 5)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SIC_BASE        = "https://sic.cultura.gob.mx"
JALISCO_ESTADO_ID = 14

# Tablas disponibles en el SIC
SIC_TABLES = [
    "festival",
    "festival_otros",
    "feria",
    "espectaculo",      # NUEVO: espectáculos escénicos
    "exposicion",       # NUEVO: exposiciones
    "curso",            # NUEVO: cursos y talleres
]

MAX_FICHAS   = 500   # por tabla (antes 50)
MAX_LIST_PAGES = 20  # páginas de lista por tabla
CONCURRENCY  = 5     # descargas paralelas de fichas

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
    "Referer": "https://sic.cultura.gob.mx/",
}


class SICJaliscoScraper:
    """Scraper asíncrono para el SIC — eventos culturales de Jalisco."""

    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self._sem  = asyncio.Semaphore(CONCURRENCY)

    async def fetch_events(self) -> list[dict[str, Any]]:
        all_events: list[dict] = []
        timeout = httpx.Timeout(30.0)

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            for table in SIC_TABLES:
                events = await self._fetch_table(client, table)
                all_events.extend(events)
                logger.info("SIC tabla '%s': %d eventos", table, len(events))
                await asyncio.sleep(self.delay)

        # Deduplicar por URL
        unique: list[dict] = []
        seen: set[str] = set()
        for evt in all_events:
            key = evt.get("url", evt.get("title", ""))
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("SIC Jalisco total: %d eventos válidos", len(unique))
        return unique

    async def _fetch_table(
        self, client: httpx.AsyncClient, table: str
    ) -> list[dict[str, Any]]:
        """Descarga todas las páginas de lista de una tabla y luego visita fichas."""
        all_links: list[str] = []

        for page in range(1, MAX_LIST_PAGES + 1):
            url = (
                f"{SIC_BASE}/lista.php"
                f"?table={table}&estado_id={JALISCO_ESTADO_ID}&page={page}"
            )
            try:
                resp = await client.get(url)
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                logger.warning("SIC HTTP %s tabla '%s' pág %d", e.response.status_code, table, page)
                break
            except Exception as e:
                logger.warning("SIC error tabla '%s' pág %d: %s", table, page, e)
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # JSON-LD directo en la página de lista (raro pero posible)
            ld_events = self._parse_json_ld(soup, url)
            if ld_events:
                return ld_events  # Si hay JSON-LD, úsalo y no pagines más

            # Recolectar enlaces a fichas
            links = (
                soup.find_all("a", href=re.compile(r"ficha\.php"))
                or soup.find_all("a", href=re.compile(r"table_id="))
            )
            if not links:
                if page == 1:
                    # Intentar parsear filas de tabla HTML
                    return self._parse_table_rows(soup, url, table)
                break   # Sin más links → fin de paginación

            for link in links:
                href = link.get("href", "")
                ficha_url = href if href.startswith("http") else urljoin(SIC_BASE, href)
                if ficha_url not in all_links:
                    all_links.append(ficha_url)

            if len(all_links) >= MAX_FICHAS:
                break

            await asyncio.sleep(self.delay)

        # Visitar fichas con concurrencia controlada
        all_links = all_links[:MAX_FICHAS]
        events    = await self._fetch_fichas_concurrent(client, all_links, table)
        return events

    async def _fetch_fichas_concurrent(
        self,
        client: httpx.AsyncClient,
        urls: list[str],
        table: str,
    ) -> list[dict]:
        """Descarga fichas en paralelo usando un semáforo de concurrencia."""
        async def _fetch_one(url: str) -> dict | None:
            async with self._sem:
                await asyncio.sleep(self.delay * 0.3)
                return await self._fetch_ficha(client, url, table)

        tasks   = [_fetch_one(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        events = []
        for r in results:
            if isinstance(r, dict) and r:
                events.append(r)
        return events

    def _parse_json_ld(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        events = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in (
                    "Event", "Festival", "MusicEvent", "ExhibitionEvent"
                ):
                    continue
                evt = self._normalize_json_ld(item, page_url)
                if evt:
                    events.append(evt)
        return events

    def _normalize_json_ld(self, item: dict, page_url: str) -> dict | None:
        title = item.get("name", "").strip()
        if not title:
            return None

        date_start = item.get("startDate")
        date_end   = item.get("endDate")

        location_data = item.get("location", {})
        location_name = ""
        lat = lon = None
        if isinstance(location_data, dict):
            location_name = location_data.get("name", "")
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
            "price":       None,
            "tags":        ["cultura", "sic"],
            "source_id":   "sic_jalisco",
        }

    def _parse_table_rows(
        self, soup: BeautifulSoup, page_url: str, table: str
    ) -> list[dict]:
        events = []
        for row in soup.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            title = cells[0].get_text(strip=True)
            if not title or len(title) < 3:
                continue

            date_start = None
            location   = ""
            for cell in cells[1:]:
                text = cell.get_text(strip=True)
                if re.search(r"\d{4}", text) and date_start is None:
                    date_start = text
                if any(kw in text.lower() for kw in ["jalisco", "guadalajara", "vallarta"]):
                    location = text

            link_el = row.find("a", href=True)
            url     = ""
            if link_el:
                href = link_el["href"]
                url  = href if href.startswith("http") else urljoin(SIC_BASE, href)

            events.append({
                "title":       title,
                "description": "",
                "date_start":  date_start,
                "date_end":    None,
                "location":    location or "Jalisco",
                "latitude":    None,
                "longitude":   None,
                "image_url":   None,
                "url":         url or page_url,
                "price":       None,
                "tags":        ["cultura", "sic", table],
                "source_id":   "sic_jalisco",
            })
        return events

    async def _fetch_ficha(
        self, client: httpx.AsyncClient, url: str, table: str, retries: int = 2
    ) -> dict | None:
        for attempt in range(retries):
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (404, 410):
                    return None
                if attempt < retries - 1:
                    await asyncio.sleep(2)
                else:
                    return None
            except Exception as e:
                logger.debug("SIC ficha error %s: %s", url, e)
                return None

        soup = BeautifulSoup(resp.text, "html.parser")

        ld_events = self._parse_json_ld(soup, url)
        if ld_events:
            return ld_events[0]

        return self._parse_ficha_html(soup, url, table)

    def _parse_ficha_html(
        self, soup: BeautifulSoup, url: str, table: str
    ) -> dict | None:
        title_el = soup.find(["h1", "h2", "h3"])
        title    = title_el.get_text(strip=True) if title_el else ""
        if not title:
            return None

        data: dict[str, str] = {}
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) == 2:
                key      = cells[0].get_text(strip=True).lower().rstrip(":")
                val      = cells[1].get_text(strip=True)
                data[key] = val

        date_start = (
            data.get("fecha de inicio") or data.get("fecha inicio")
            or data.get("inicio") or data.get("fecha")
        )
        date_end = (
            data.get("fecha de término") or data.get("fecha término")
            or data.get("término") or data.get("fin")
        )
        location = (
            data.get("municipio") or data.get("lugar") or data.get("sede")
            or data.get("recinto") or "Jalisco"
        )
        description = (
            data.get("descripción") or data.get("descripcion")
            or data.get("resumen") or ""
        )

        lat = lon = None
        map_el = soup.find(attrs={"data-lat": True})
        if map_el:
            try:
                lat = float(map_el["data-lat"])
                lon = float(map_el.get("data-lng") or map_el.get("data-lon", 0))
            except (ValueError, TypeError):
                pass

        img_el    = soup.find("img", src=re.compile(r"\.(jpg|jpeg|png|webp)", re.I))
        image_url = None
        if img_el:
            src       = img_el.get("src", "")
            image_url = src if src.startswith("http") else urljoin(SIC_BASE, src)

        return {
            "title":       title,
            "description": description,
            "date_start":  date_start,
            "date_end":    date_end,
            "location":    location,
            "latitude":    lat,
            "longitude":   lon,
            "image_url":   image_url,
            "url":         url,
            "price":       data.get("costo") or data.get("precio"),
            "tags":        ["cultura", "sic", table],
            "source_id":   "sic_jalisco",
        }