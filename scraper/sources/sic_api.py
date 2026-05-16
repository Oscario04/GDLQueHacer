"""
scraper/sources/sic_api.py  — v3
Scraper para el SIC usando su API REST JSON oficial.

CAMBIOS v3:
  - Sin filtro estado_id — descarga TODO MÉXICO (el front filtra por ciudad/estado).
  - Más tablas: agrega festividad, feria_libro, festival_otros, convocatoria.
  - Campos nuevos: `estado` y `ciudad` para filtrado en el front.
  - Paginación mejorada: detecta last page por conteo real vs total.
  - No filtra eventos pasados en el scraper — lo hace el pipeline de ingestión.

Endpoint base: https://sic.cultura.gob.mx/api/v1/
Sin autenticación requerida.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

SIC_API_BASE = "https://sic.cultura.gob.mx/api/v1"
PAGE_SIZE    = 100
MAX_PAGES    = 50       # 50 × 100 = 5,000 por tabla
CONCURRENCY  = 4

SIC_API_TABLES = [
    {"id": "festival",       "category": "cultural",     "tags": ["festival"]},
    {"id": "festividad",     "category": "cultural",     "tags": ["festividad", "tradicion"]},
    {"id": "feria",          "category": "gastronomico", "tags": ["feria"]},
    {"id": "feria_libro",    "category": "cultural",     "tags": ["libro", "literatura"]},
    {"id": "espectaculo",    "category": "entretenimiento", "tags": ["espectaculo"]},
    {"id": "exposicion",     "category": "cultural",     "tags": ["exposicion", "arte"]},
    {"id": "curso",          "category": "talleres",     "tags": ["curso", "taller"]},
    {"id": "festival_otros", "category": "cultural",     "tags": ["muestra", "evento"]},
    {"id": "museo",          "category": "cultural",     "tags": ["museo"]},
    {"id": "cine",           "category": "entretenimiento", "tags": ["cine", "pelicula"]},
    {"id": "biblioteca",     "category": "cultural",     "tags": ["biblioteca"]},
    {"id": "convocatoria",   "category": "talleres",     "tags": ["convocatoria"]},
]

HEADERS = {
    "User-Agent": "GDLQueHacer/4.0 (contacto@gdlquehacer.mx)",
    "Accept": "application/json",
}


class SICAPIScaper:
    """
    Consulta la API REST oficial del SIC para obtener eventos de TODO MÉXICO.
    El filtrado por estado/ciudad se hace en el front-end.
    """

    def __init__(self, delay: float = 0.3):
        self.delay = delay
        self._sem  = asyncio.Semaphore(CONCURRENCY)

    async def fetch_events(self) -> list[dict[str, Any]]:
        all_events: list[dict] = []

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
        ) as client:
            tasks = [self._fetch_table(client, table) for table in SIC_API_TABLES]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for table, result in zip(SIC_API_TABLES, results):
            if isinstance(result, Exception):
                logger.error("SIC API tabla '%s': %s", table["id"], result)
            else:
                logger.info("SIC API tabla '%s': %d eventos", table["id"], len(result))
                all_events.extend(result)

        # Deduplicar por external_id
        unique: list[dict] = []
        seen: set[str] = set()
        for evt in all_events:
            key = evt.get("external_id") or evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("SIC API total: %d eventos únicos (todo México)", len(unique))
        return unique

    async def _fetch_table(
        self, client: httpx.AsyncClient, table: dict
    ) -> list[dict]:
        events: list[dict] = []
        table_id = table["id"]

        for page in range(1, MAX_PAGES + 1):
            async with self._sem:
                # Sin estado_id → todo México
                params = {
                    "per_page": PAGE_SIZE,
                    "page":     page,
                }
                url = f"{SIC_API_BASE}/{table_id}"

                try:
                    resp = await client.get(url, params=params)

                    if resp.status_code == 404:
                        logger.debug("SIC API: tabla '%s' no disponible (404)", table_id)
                        break
                    if resp.status_code == 429:
                        logger.warning("SIC API rate limit tabla '%s', esperando 15s", table_id)
                        await asyncio.sleep(15)
                        continue

                    resp.raise_for_status()

                    content_type = resp.headers.get("content-type", "")
                    if "json" not in content_type:
                        logger.debug("SIC API tabla '%s' no devolvió JSON", table_id)
                        break

                    data = resp.json()

                except httpx.HTTPError as exc:
                    logger.warning("SIC API HTTP error '%s' pág %d: %s", table_id, page, exc)
                    break
                except Exception as exc:
                    logger.warning("SIC API error '%s' pág %d: %s", table_id, page, exc)
                    break

            # Normalizar estructura de respuesta
            if isinstance(data, list):
                raw_list = data
                total = None
            elif isinstance(data, dict):
                raw_list = (
                    data.get("data") or data.get("results") or
                    data.get("items") or data.get("records") or []
                )
                total = data.get("total") or data.get("count") or data.get("totalCount")
            else:
                break

            if not raw_list:
                break

            for item in raw_list:
                mapped = self._map_event(item, table)
                if mapped:
                    events.append(mapped)

            # Condición de parada
            fetched = len(events)
            if total and fetched >= int(total):
                break
            if len(raw_list) < PAGE_SIZE:
                break  # última página

            await asyncio.sleep(self.delay)

        return events

    def _map_event(self, item: dict, table: dict) -> Optional[dict]:
        try:
            title = (
                item.get("nombre") or item.get("name") or
                item.get("titulo") or ""
            ).strip()

            if not title or len(title) < 3:
                return None

            date_start_raw = (
                item.get("fecha_inicio") or item.get("fecha_inicio_display") or
                item.get("startDate") or item.get("fecha")
            )
            date_end_raw = (
                item.get("fecha_fin") or item.get("fecha_termino") or
                item.get("endDate")
            )

            date_start = self._parse_dt(date_start_raw)
            date_end   = self._parse_dt(date_end_raw)

            # Ubicación — sin filtro, guardamos todo
            municipio = (item.get("municipio") or item.get("ciudad") or "").strip()
            estado    = (item.get("estado") or item.get("entidad") or "").strip()
            recinto   = (
                item.get("recinto") or item.get("venue") or
                item.get("lugar") or ""
            ).strip()
            location  = ", ".join(p for p in [recinto, municipio, estado] if p)

            # Geo
            lat = self._to_float(item.get("latitud") or item.get("lat"))
            lon = self._to_float(item.get("longitud") or item.get("lon") or item.get("lng"))

            # Imagen
            image_url = item.get("imagen") or item.get("image_url") or item.get("foto")
            if image_url and isinstance(image_url, str) and image_url.startswith("/"):
                image_url = f"https://sic.cultura.gob.mx{image_url}"

            # URL ficha
            slug = item.get("slug") or item.get("id") or item.get("id_registro")
            url  = item.get("url") or (
                f"https://sic.cultura.gob.mx/ficha.php?table={table['id']}&id={slug}"
                if slug else ""
            )

            # Precio
            precio = item.get("costo") or item.get("precio") or item.get("cost")
            if isinstance(precio, str):
                precio = precio.replace("$", "").replace(",", "").strip()
                if precio.lower() in ("gratis", "gratuito", "free", "0"):
                    precio = 0.0
                else:
                    try:
                        precio = float(precio) if precio else None
                    except ValueError:
                        precio = None

            # Descripción
            description = (
                item.get("descripcion") or item.get("description") or
                item.get("resumen") or item.get("sinopsis") or ""
            )

            # Tags
            categoria_raw = item.get("categoria") or item.get("genero") or table["id"]
            tags = list(table.get("tags", []))
            tags.append("sic")
            if municipio:
                tags.append(municipio.lower()[:30])
            if estado:
                tags.append(estado.lower()[:30])

            ext_id = str(item.get("id") or item.get("id_registro") or "")

            return {
                "source_id":   "sic_api",
                "external_id": f"sic_{table['id']}_{ext_id}" if ext_id else "",
                "title":       title,
                "description": description,
                "category":    self._map_category(table["id"], str(categoria_raw)),
                "tags":        tags,
                "image_url":   image_url,
                "date_start":  date_start,
                "date_end":    date_end,
                "price":       precio,
                "currency":    "MXN",
                "url":         url,
                "location":    location or estado or "México",
                # Campos extra para filtrado en el front:
                "estado":      estado,
                "ciudad":      municipio,
                "latitude":    lat,
                "longitude":   lon,
            }

        except Exception as exc:
            logger.debug("SIC API error mapeando item: %s | %s", exc, item)
            return None

    @staticmethod
    def _map_category(tabla: str, categoria: str) -> str:
        tabla     = tabla.lower()
        categoria = (categoria or "").lower()

        if tabla in ("espectaculo", "cine"):
            return "entretenimiento"
        if tabla in ("exposicion", "museo"):
            return "cultural"
        if tabla in ("curso", "convocatoria"):
            return "talleres"
        if tabla in ("feria",):
            if any(w in categoria for w in ["gastronom", "comida", "food"]):
                return "gastronomico"
            return "cultural"
        if tabla in ("festival", "festival_otros", "festividad", "feria_libro"):
            if any(w in categoria for w in ["musica", "música", "rock", "jazz"]):
                return "entretenimiento"
            if any(w in categoria for w in ["gastronom", "comida"]):
                return "gastronomico"
            return "cultural"
        if tabla in ("biblioteca",):
            return "cultural"
        return "cultural"

    @staticmethod
    def _parse_dt(value) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        s = str(value).strip()
        if s.lower() in ("nan", "none", "null", "", "0"):
            return None
        for fmt in [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
        ]:
            try:
                dt = datetime.strptime(s[:len(fmt)], fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None