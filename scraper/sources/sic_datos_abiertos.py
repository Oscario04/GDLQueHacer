"""
scraper/sources/sic_datos_abiertos.py
Scraper para el SIC usando sus archivos CSV de datos abiertos.

El SIC publica CSVs con todos sus registros en:
  https://sic.cultura.gob.mx/datos_abiertos.php

Los archivos están disponibles sin autenticación.
Filtramos por estado_id=14 (Jalisco).

Esta fuente reemplaza sic_api.py (cuya API no existe en /api/v1/).

Estimado: 800-2,000 eventos de Jalisco entre todos los CSVs.
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# CSVs públicos del SIC
# Formato: https://sic.cultura.gob.mx/datos_abiertos/{tabla}.csv
SIC_CSV_BASE = "https://sic.cultura.gob.mx/datos_abiertos"

SIC_TABLES = [
    {"table": "festival",        "category": "entretenimiento"},
    {"table": "festival_otros",  "category": "cultural"},
    {"table": "feria",           "category": "cultural"},
    {"table": "espectaculo",     "category": "entretenimiento"},
    {"table": "exposicion",      "category": "cultural"},
    {"table": "museo",           "category": "cultural"},
    {"table": "curso",           "category": "talleres"},
    {"table": "cine",            "category": "entretenimiento"},
    {"table": "biblioteca",      "category": "cultural"},
    {"table": "teatro",          "category": "entretenimiento"},
    {"table": "centro_cultural", "category": "cultural"},
]

JALISCO_ID = "14"

HEADERS = {
    "User-Agent": "GDLQueHacer/4.0 (datos abiertos SIC)",
    "Accept": "text/csv,text/plain,*/*",
    "Referer": "https://sic.cultura.gob.mx/datos_abiertos.php",
}


class SICDatosAbiertos:
    """
    Descarga los CSVs de datos abiertos del SIC y filtra por Jalisco.
    Mucho más eficiente y fiable que el scraper HTML de fichas.
    """

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    async def fetch_events(self) -> list[dict[str, Any]]:
        all_events: list[dict] = []

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=httpx.Timeout(60.0),
            follow_redirects=True,
        ) as client:
            tasks = [self._fetch_table(client, tbl) for tbl in SIC_TABLES]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for tbl, result in zip(SIC_TABLES, results):
            name = tbl["table"]
            if isinstance(result, Exception):
                logger.error("SIC CSV '%s': %s", name, result)
            elif result:
                logger.info("SIC CSV '%s': %d eventos de Jalisco", name, len(result))
                all_events.extend(result)
            else:
                logger.debug("SIC CSV '%s': 0 eventos (tabla vacía o no disponible)", name)

        # Deduplicar por external_id
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("external_id") or evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("SIC datos abiertos total: %d eventos de Jalisco", len(unique))
        return unique

    async def _fetch_table(
        self, client: httpx.AsyncClient, tbl: dict
    ) -> list[dict]:
        """Descarga un CSV del SIC y filtra por estado_id=14."""
        table_name = tbl["table"]
        category   = tbl["category"]

        # Intentar varias URLs posibles
        urls_to_try = [
            f"{SIC_CSV_BASE}/{table_name}.csv",
            f"{SIC_CSV_BASE}/{table_name}_datos_abiertos.csv",
            f"https://sic.cultura.gob.mx/datos_abiertos.php?table={table_name}&type=csv",
        ]

        for url in urls_to_try:
            try:
                resp = await client.get(url)

                if resp.status_code == 404:
                    continue

                if resp.status_code != 200:
                    logger.debug("SIC CSV '%s' → %s", table_name, resp.status_code)
                    continue

                content_type = resp.headers.get("content-type", "")
                # Rechazar si devuelve HTML (página de error del SIC)
                if "text/html" in content_type and b"<html" in resp.content[:50].lower():
                    logger.debug("SIC CSV '%s' devolvió HTML, no CSV", table_name)
                    continue

                # Intentar decodificar
                raw = resp.content
                text = None
                for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]:
                    try:
                        text = raw.decode(enc)
                        break
                    except Exception:
                        continue

                if not text:
                    continue

                # Verificar que sea realmente un CSV
                first_line = text.split("\n")[0] if text else ""
                if not first_line or "<" in first_line:
                    # Es HTML, no CSV
                    continue

                events = self._parse_csv(text, table_name, category)
                if events is not None:
                    return events

            except httpx.RequestError as exc:
                logger.debug("SIC CSV '%s' request error: %s", table_name, exc)
            except Exception as exc:
                logger.debug("SIC CSV '%s' error: %s", table_name, exc)

            await asyncio.sleep(self.delay * 0.5)

        return []

    def _parse_csv(
        self, text: str, table_name: str, category: str
    ) -> list[dict] | None:
        """Parsea el CSV y filtra por Jalisco (estado_id=14)."""
        try:
            reader = csv.DictReader(io.StringIO(text))
            if not reader.fieldnames:
                return None

            fields = [f.lower() for f in reader.fieldnames]
            logger.debug("SIC CSV '%s' campos: %s", table_name, fields[:10])

            events = []
            total = 0
            jal   = 0

            for record in reader:
                total += 1
                if not self._is_jalisco(record):
                    continue
                jal += 1
                mapped = self._map_record(record, table_name, category)
                if mapped:
                    events.append(mapped)

            if total == 0:
                return None  # CSV vacío o inválido

            logger.debug(
                "SIC CSV '%s': %d/%d registros son de Jalisco",
                table_name, jal, total
            )
            return events

        except Exception as exc:
            logger.debug("SIC CSV parse error '%s': %s", table_name, exc)
            return None

    @staticmethod
    def _is_jalisco(record: dict) -> bool:
        """True si el registro es de Jalisco."""
        # Campos típicos del SIC para el estado
        state_fields = [
            "estado_id", "ESTADO_ID",
            "id_estado", "ID_ESTADO",
            "estado", "ESTADO",
            "entidad", "ENTIDAD",
        ]
        for field in state_fields:
            val = str(record.get(field) or "").strip()
            if val == "14":
                return True
            if "jalisco" in val.lower():
                return True
        return False

    def _map_record(
        self, record: dict, table_name: str, default_category: str
    ) -> Optional[dict]:
        try:
            # Título — los CSVs del SIC usan "nombre" o "titulo"
            title = str(
                record.get("nombre") or record.get("NOMBRE")
                or record.get("titulo") or record.get("TITULO")
                or record.get("nombre_festival") or record.get("NOMBRE_FESTIVAL")
                or ""
            ).strip()

            if not title or len(title) < 3 or title in ("nan", "None"):
                return None

            # Descripción
            desc = str(
                record.get("descripcion") or record.get("DESCRIPCION")
                or record.get("resumen") or record.get("RESUMEN")
                or record.get("sinopsis") or record.get("SINOPSIS")
                or ""
            ).strip()
            if desc in ("nan", "None"):
                desc = ""

            # Fechas — el SIC usa fecha_inicio / fecha_fin o mes_inicio / anio
            date_start = self._parse_sic_date(
                record.get("fecha_inicio") or record.get("FECHA_INICIO"),
                record.get("mes_inicio"),
                record.get("anio") or record.get("año"),
            )
            date_end = self._parse_sic_date(
                record.get("fecha_fin") or record.get("FECHA_FIN")
                or record.get("fecha_termino") or record.get("FECHA_TERMINO"),
            )

            # Ubicación
            municipio = str(
                record.get("municipio") or record.get("MUNICIPIO")
                or record.get("ciudad") or record.get("CIUDAD") or ""
            ).strip()
            recinto = str(
                record.get("recinto") or record.get("RECINTO")
                or record.get("lugar") or record.get("LUGAR")
                or record.get("sede") or record.get("SEDE") or ""
            ).strip()
            for bad in ("nan", "None", ""):
                if municipio == bad:
                    municipio = ""
                if recinto == bad:
                    recinto = ""

            location = ", ".join(filter(None, [recinto, municipio, "Jalisco"]))

            # Geo (raro en los CSVs pero a veces aparece)
            lat = self._to_float(record.get("latitud") or record.get("LATITUD"))
            lon = self._to_float(record.get("longitud") or record.get("LONGITUD"))

            # Precio
            precio_str = str(
                record.get("costo") or record.get("COSTO")
                or record.get("precio") or record.get("PRECIO") or ""
            ).strip()
            precio: Optional[float] = None
            if precio_str.lower() in ("gratuito", "gratis", "libre", "0", "free"):
                precio = 0.0
            elif precio_str and precio_str not in ("nan", "None"):
                precio = self._to_float(precio_str)

            # URL
            url = str(
                record.get("url") or record.get("URL")
                or record.get("pagina_web") or record.get("PAGINA_WEB") or ""
            ).strip()
            if url in ("nan", "None"):
                url = ""

            # Si no hay URL, construir ficha
            rec_id = str(
                record.get("id") or record.get("ID")
                or record.get("clave") or record.get("CLAVE") or ""
            ).strip()
            if not url and rec_id and rec_id not in ("nan", "None"):
                url = f"https://sic.cultura.gob.mx/ficha.php?table={table_name}&table_id={rec_id}"

            # Imagen
            img = str(record.get("imagen") or record.get("IMAGEN") or "").strip()
            if img and img not in ("nan", "None"):
                if img.startswith("/"):
                    img = f"https://sic.cultura.gob.mx{img}"
            else:
                img = None

            # Categoría (refinar con género si está disponible)
            genero = str(
                record.get("genero") or record.get("GENERO")
                or record.get("tipo") or record.get("TIPO") or ""
            ).lower()
            category = self._refine_category(default_category, genero)

            # Tags
            tags = ["sic", table_name, "cultura"]
            if municipio:
                tags.append(municipio.lower()[:20])

            ext_id = f"sic_{table_name}_{rec_id}" if rec_id else ""

            return {
                "source_id":   "sic_datos_abiertos",
                "external_id": ext_id,
                "title":       title,
                "description": desc,
                "category":    category,
                "tags":        tags,
                "image_url":   img,
                "date_start":  date_start,
                "date_end":    date_end,
                "price":       precio,
                "currency":    "MXN",
                "url":         url,
                "location":    location,
                "latitude":    lat,
                "longitude":   lon,
            }

        except Exception as exc:
            logger.debug("SIC map_record error: %s", exc)
            return None

    @staticmethod
    def _parse_sic_date(
        fecha_str=None,
        mes_str=None,
        anio_str=None,
    ) -> Optional[datetime]:
        """Parsea fechas del SIC que pueden venir en varios formatos."""
        if fecha_str and str(fecha_str).strip() not in ("nan", "None", ""):
            s = str(fecha_str).strip()
            for fmt in [
                "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
            ]:
                try:
                    return datetime.strptime(s[:len(fmt)], fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

        # Reconstruir desde mes + año (fechas parciales del SIC)
        if anio_str and str(anio_str).strip() not in ("nan", "None", ""):
            try:
                year = int(str(anio_str).strip())
                month = 1
                if mes_str and str(mes_str).strip() not in ("nan", "None", ""):
                    # El SIC a veces usa nombres de mes en español
                    meses = {
                        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
                        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
                        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
                    }
                    mes_low = str(mes_str).lower().strip()
                    month = meses.get(mes_low) or int(mes_low) if mes_low.isdigit() else 1
                return datetime(year, month, 1, tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass

        return None

    @staticmethod
    def _refine_category(base: str, genero: str) -> str:
        if any(w in genero for w in ["músic", "music", "concierto", "rock", "jazz", "folk"]):
            return "entretenimiento"
        if any(w in genero for w in ["gastro", "comida", "vino", "cerveza"]):
            return "gastronomico"
        if any(w in genero for w in ["deport", "atletism"]):
            return "deportivo"
        if any(w in genero for w in ["taller", "curso", "seminario"]):
            return "talleres"
        return base

    @staticmethod
    def _to_float(value) -> Optional[float]:
        if not value:
            return None
        s = str(value).replace(",", "").replace("$", "").strip()
        if s in ("nan", "None", ""):
            return None
        try:
            return float(s)
        except (ValueError, TypeError):
            return None