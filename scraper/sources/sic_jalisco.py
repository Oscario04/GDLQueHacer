"""
scraper/sources/sic_jalisco.py
Scraper para el Sistema de Información Cultural (SIC) de la Secretaría de Cultura.
URL: https://sic.cultura.gob.mx

Extrae festivales, ferias y eventos culturales de Jalisco (estado_id=14).
Sin API key — scraping HTML público del gobierno federal.

Tablas disponibles en SIC:
  - festival         → Festivales
  - festival_otros   → Muestras y otros eventos
  - feria            → Ferias
"""
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

SIC_BASE = "https://sic.cultura.gob.mx"

# Estado 14 = Jalisco en el SIC
JALISCO_ESTADO_ID = 14

# Tablas de eventos en el SIC
SIC_TABLES = [
    "festival",
    "festival_otros",
    "feria",
]

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
    """
    Scraper asíncrono para el SIC — eventos culturales de Jalisco.
    Extrae festivales, ferias y muestras culturales.
    """

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    async def fetch_events(self) -> list[dict[str, Any]]:
        """Punto de entrada principal. Retorna lista de eventos normalizados."""
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
        """Descarga la lista de eventos de una tabla del SIC para Jalisco."""
        url = (
            f"{SIC_BASE}/lista.php"
            f"?table={table}&estado_id={JALISCO_ESTADO_ID}"
        )
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.warning("SIC HTTP error %s tabla '%s': %s", e.response.status_code, table, url)
            return []
        except Exception as e:
            logger.warning("SIC error accediendo tabla '%s': %s", table, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # ── Estrategia 1: JSON-LD ─────────────────────────────────────
        events = self._parse_json_ld(soup, url)
        if events:
            return events

        # ── Estrategia 2: Lista de fichas del SIC ─────────────────────
        events = await self._parse_ficha_list(client, soup, url, table)
        return events

    def _parse_json_ld(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[dict[str, Any]]:
        """Extrae eventos de bloques JSON-LD (si el SIC los incluye)."""
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

    def _normalize_json_ld(
        self, item: dict, page_url: str
    ) -> dict[str, Any] | None:
        title = item.get("name", "").strip()
        if not title:
            return None

        date_start = item.get("startDate")
        date_end = item.get("endDate")

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
            "title": title,
            "description": item.get("description", ""),
            "date_start": date_start,
            "date_end": date_end,
            "location": location_name,
            "latitude": lat,
            "longitude": lon,
            "image_url": image,
            "url": item.get("url") or page_url,
            "price": None,
            "tags": ["cultura", "sic"],
            "source_id": "sic_jalisco",
        }

    async def _parse_ficha_list(
        self,
        client: httpx.AsyncClient,
        soup: BeautifulSoup,
        list_url: str,
        table: str,
    ) -> list[dict[str, Any]]:
        """
        Parsea la lista de fichas del SIC.
        Cada fila tiene un enlace a ficha.php?table=...&table_id=...
        Extrae datos básicos de la lista, opcionalmente visita fichas individuales.
        """
        events: list[dict] = []

        # El SIC lista eventos en tablas HTML o listas con enlaces a ficha.php
        links = soup.find_all("a", href=re.compile(r"ficha\.php"))
        if not links:
            # Intentar cualquier enlace con table_id
            links = soup.find_all("a", href=re.compile(r"table_id="))

        if not links:
            logger.debug("SIC: sin enlaces de fichas en %s", list_url)
            return self._parse_table_rows(soup, list_url, table)

        # Visitar cada ficha individualmente (con límite para no abusar)
        MAX_FICHAS = 50
        for link in links[:MAX_FICHAS]:
            href = link.get("href", "")
            ficha_url = href if href.startswith("http") else urljoin(SIC_BASE, href)

            try:
                evt = await self._fetch_ficha(client, ficha_url, table)
                if evt:
                    events.append(evt)
                await asyncio.sleep(self.delay * 0.5)
            except Exception as e:
                logger.debug("SIC error en ficha %s: %s", ficha_url, e)

        return events

    def _parse_table_rows(
        self, soup: BeautifulSoup, page_url: str, table: str
    ) -> list[dict[str, Any]]:
        """Extrae eventos directamente de filas de tabla HTML."""
        events = []
        rows = soup.find_all("tr")

        for row in rows[1:]:  # Saltar header
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            title = cells[0].get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Buscar fecha en las celdas
            date_start = None
            location = ""
            for cell in cells[1:]:
                text = cell.get_text(strip=True)
                # Detectar fechas
                if re.search(r"\d{4}", text) and date_start is None:
                    date_start = text
                # Detectar ubicación (si menciona Jalisco)
                if any(
                    kw in text.lower()
                    for kw in ["jalisco", "guadalajara", "vallarta"]
                ):
                    location = text

            link_el = row.find("a", href=True)
            url = ""
            if link_el:
                href = link_el["href"]
                url = href if href.startswith("http") else urljoin(SIC_BASE, href)

            events.append({
                "title": title,
                "description": "",
                "date_start": date_start,
                "date_end": None,
                "location": location or "Jalisco",
                "latitude": None,
                "longitude": None,
                "image_url": None,
                "url": url or page_url,
                "price": None,
                "tags": ["cultura", "sic", table],
                "source_id": "sic_jalisco",
            })

        return events

    async def _fetch_ficha(
        self, client: httpx.AsyncClient, url: str, table: str
    ) -> dict[str, Any] | None:
        """Descarga y parsea una ficha individual del SIC."""
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.debug("SIC ficha error %s: %s", url, e)
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # ── JSON-LD primero ───────────────────────────────────────────
        ld_events = self._parse_json_ld(soup, url)
        if ld_events:
            return ld_events[0]

        # ── Parseo de ficha estructurada del SIC ─────────────────────
        return self._parse_ficha_html(soup, url, table)

    def _parse_ficha_html(
        self, soup: BeautifulSoup, url: str, table: str
    ) -> dict[str, Any] | None:
        """Extrae datos de una ficha HTML del SIC."""
        # Título principal
        title_el = soup.find(["h1", "h2", "h3"])
        title = title_el.get_text(strip=True) if title_el else ""

        if not title:
            return None

        # La ficha del SIC usa una tabla de datos label-valor
        data: dict[str, str] = {}
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).lower().rstrip(":")
                val = cells[1].get_text(strip=True)
                data[key] = val

        # Mappings comunes del SIC
        date_start = (
            data.get("fecha de inicio")
            or data.get("fecha inicio")
            or data.get("inicio")
            or data.get("fecha")
        )
        date_end = (
            data.get("fecha de término")
            or data.get("fecha término")
            or data.get("término")
            or data.get("fin")
        )
        location = (
            data.get("municipio")
            or data.get("lugar")
            or data.get("sede")
            or data.get("recinto")
            or "Jalisco"
        )
        description = (
            data.get("descripción")
            or data.get("descripcion")
            or data.get("resumen")
            or ""
        )

        # Coordenadas del mapa (si existen en la ficha)
        lat = lon = None
        map_el = soup.find(attrs={"data-lat": True})
        if map_el:
            try:
                lat = float(map_el["data-lat"])
                lon = float(map_el.get("data-lng") or map_el.get("data-lon", 0))
            except (ValueError, TypeError):
                pass

        # Imagen
        img_el = soup.find("img", src=re.compile(r"\.(jpg|jpeg|png|webp)", re.I))
        image_url = None
        if img_el:
            src = img_el.get("src", "")
            image_url = src if src.startswith("http") else urljoin(SIC_BASE, src)

        return {
            "title": title,
            "description": description,
            "date_start": date_start,
            "date_end": date_end,
            "location": location,
            "latitude": lat,
            "longitude": lon,
            "image_url": image_url,
            "url": url,
            "price": data.get("costo") or data.get("precio"),
            "tags": ["cultura", "sic", table],
            "source_id": "sic_jalisco",
        }