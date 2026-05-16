"""
scraper/sources/datos_gob_mx.py  — v4
Scraper para datos abiertos del gobierno mexicano.

FIXES v4:
  - URL correcta del SIC: /descarga.php?table=X&type=csv
  - Encoding fix: latin-1 mal parseado como UTF-8 (ÃÂ©→é)
  - Columnas reales de los CSVs confirmadas con archivos del usuario:
      eventos INAOE:   tipo, no, evento, dirigido_a, fecha, numero_asistentes, lugar_enlace
      bioseguridad:    nombre_event, tipo_evento, objetivo_ev, enlace, fecha
      feria_libro SIC: feria_libro_nombre, nom_ent, nom_mun, latitud, longitud, link_sic
  - Sin filtro geográfico: todo México.
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

SIC_DESCARGA = "https://sic.cultura.gob.mx/descarga.php"

SIC_TABLES = [
    {"id": "festival",       "category": "cultural",  "tags": ["festival"]},
    {"id": "festividad",     "category": "cultural",  "tags": ["festividad", "tradicion"]},
    {"id": "feria_libro",    "category": "cultural",  "tags": ["libro", "literatura"]},
    {"id": "festival_otros", "category": "cultural",  "tags": ["muestra", "evento"]},
    {"id": "convocatoria",   "category": "talleres",  "tags": ["convocatoria"]},
]

CKAN_API = "https://www.datos.gob.mx/api/3/action"

CKAN_DATASETS = [
    {
        "id":          "eventos_divulgacion_ext",
        "name":        "Eventos divulgación externos INAOE",
        "resource_id": "f37bc0da-5960-4c30-9b7c-379bb54e905b",
        "direct_url":  "https://repodatos.atdt.gob.mx/api_update/inaoe/eventos_divulgacion_externos/08_eventos-externos-2025.csv",
        "category":    "cultural",
        "col_title":   "evento",
        "col_date":    "fecha",
        "col_desc":    None,
        "col_url":     "lugar_enlace",
        "col_type":    "tipo",
    },
    {
        "id":          "eventos_divulgacion_int",
        "name":        "Eventos divulgación internos INAOE",
        "resource_id": "5e393437-93ce-4e9e-a967-37eef4b45033",
        "direct_url":  "https://repodatos.atdt.gob.mx/api_update/inaoe/eventos_internos_divulgacion/09_eventos-internos-2025.csv",
        "category":    "cultural",
        "col_title":   "evento",
        "col_date":    "fecha",
        "col_desc":    None,
        "col_url":     "lugar_enlace",
        "col_type":    "tipo",
    },
    {
        "id":          "eventos_bioseguridad",
        "name":        "Eventos bioseguridad Cibiogem",
        "resource_id": "bc3fa0a8-3c26-4e39-b9e5-22f77a34c4ac",
        "direct_url":  None,
        "category":    "cultural",
        "col_title":   "nombre_event",
        "col_date":    "fecha",
        "col_desc":    "objetivo_ev",
        "col_url":     "enlace",
        "col_type":    "tipo_evento",
    },
]

HEADERS = {
    "User-Agent": "GDLQueHacer/4.0 (contacto@gdlquehacer.mx)",
    "Accept":     "text/csv,application/json,*/*",
    "Referer":    "https://sic.cultura.gob.mx/datos.php",
}


class DatosGobMxScraper:

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    async def fetch_events(self) -> list[dict[str, Any]]:
        all_events: list[dict] = []

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=httpx.Timeout(120.0),
            follow_redirects=True,
        ) as client:

            # 1. SIC CSV
            sic_tasks = [self._fetch_sic_table(client, t) for t in SIC_TABLES]
            sic_results = await asyncio.gather(*sic_tasks, return_exceptions=True)
            for table, result in zip(SIC_TABLES, sic_results):
                if isinstance(result, Exception):
                    logger.error("SIC CSV '%s': %s", table["id"], result)
                else:
                    logger.info("SIC CSV '%s': %d eventos", table["id"], len(result))
                    all_events.extend(result)

            await asyncio.sleep(self.delay)

            # 2. CKAN
            await self._resolve_missing_urls(client)
            ckan_tasks = [self._fetch_ckan_dataset(client, ds) for ds in CKAN_DATASETS]
            ckan_results = await asyncio.gather(*ckan_tasks, return_exceptions=True)
            for ds, result in zip(CKAN_DATASETS, ckan_results):
                if isinstance(result, Exception):
                    logger.error("CKAN '%s': %s", ds["id"], result)
                else:
                    logger.info("CKAN '%s': %d eventos", ds["id"], len(result))
                    all_events.extend(result)

        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("external_id") or evt.get("url") or evt.get("title", "")
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("DatosGobMx total: %d eventos (todo México)", len(unique))
        return unique

    # ── SIC CSV ──────────────────────────────────────────────────────────

    async def _fetch_sic_table(self, client: httpx.AsyncClient, table: dict) -> list[dict]:
        table_id = table["id"]
        url = f"{SIC_DESCARGA}?table={table_id}&type=csv"

        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("SIC /descarga.php '%s' HTTP %s", table_id, resp.status_code)
                return []

            content_type = resp.headers.get("content-type", "")
            if "html" in content_type.lower():
                logger.warning("SIC '%s': devolvió HTML en vez de CSV", table_id)
                return []

            text = self._decode(resp.content)
            if not text or not text.strip():
                return []

            reader = csv.DictReader(io.StringIO(text))
            events = []
            mapper = self._map_sic_feria_libro if table_id == "feria_libro" else self._map_sic_record
            for record in reader:
                mapped = mapper(record, table)
                if mapped:
                    events.append(mapped)
            return events

        except Exception as exc:
            logger.warning("SIC CSV '%s' error: %s", table_id, exc)
            return []

    def _map_sic_feria_libro(self, record: dict, table: dict) -> Optional[dict]:
        rec = {self._fix_enc(k).lower().strip(): self._fix_enc(str(v)) for k, v in record.items()}

        title = self._clean(rec.get("feria_libro_nombre") or "")
        if not title or len(title) < 3:
            return None

        estado   = self._clean(rec.get("nom_ent") or rec.get("nom_ent_corto") or "")
        ciudad   = self._clean(rec.get("nom_mun") or "")
        lugar    = self._clean(rec.get("nom_loc") or "")
        location = ", ".join(p for p in [lugar, ciudad, estado] if p)

        lat = self._to_float(rec.get("latitud"))
        lon = self._to_float(rec.get("longitud"))
        if lat == 0.0: lat = None
        if lon == 0.0: lon = None

        url    = self._clean(rec.get("link_sic") or "")
        rec_id = self._clean(rec.get("feria_libro_id") or "")
        date_start = self._parse_dt(rec.get("fecha_mod"))

        tags = list(table.get("tags", []))
        if estado:
            tags.append(estado.lower()[:30])

        return {
            "source_id":   "sic_csv",
            "external_id": f"sic_feria_libro_{rec_id}" if rec_id else "",
            "title":       title,
            "description": "",
            "category":    table.get("category", "cultural"),
            "tags":        tags,
            "image_url":   None,
            "date_start":  date_start,
            "date_end":    None,
            "price":       0.0,
            "currency":    "MXN",
            "url":         url,
            "location":    location or estado or "México",
            "estado":      estado,
            "ciudad":      ciudad,
            "latitude":    lat,
            "longitude":   lon,
        }

    def _map_sic_record(self, record: dict, table: dict) -> Optional[dict]:
        try:
            rec = {self._fix_enc(k).lower().strip(): self._fix_enc(str(v)) for k, v in record.items()}

            title = self._clean(
                rec.get("nombre") or rec.get("nombre_festival") or
                rec.get("titulo") or rec.get("name") or ""
            )
            if not title or len(title) < 3:
                return None

            description = self._clean(
                rec.get("descripcion") or rec.get("objetivo") or
                rec.get("resumen") or ""
            )
            date_start = self._parse_dt(rec.get("fecha_inicio") or rec.get("fecha"))
            date_end   = self._parse_dt(rec.get("fecha_fin") or rec.get("fecha_termino"))

            municipio = self._clean(rec.get("municipio") or rec.get("ciudad") or "")
            estado    = self._clean(rec.get("estado") or rec.get("nom_ent") or "")
            recinto   = self._clean(rec.get("recinto") or rec.get("lugar") or "")
            location  = ", ".join(p for p in [recinto, municipio, estado] if p)

            lat = self._to_float(rec.get("latitud") or rec.get("lat"))
            lon = self._to_float(rec.get("longitud") or rec.get("lon"))
            if lat == 0.0: lat = None
            if lon == 0.0: lon = None

            precio = self._parse_price(rec.get("costo") or rec.get("precio") or "")
            url    = self._clean(rec.get("url") or rec.get("link_sic") or "")
            rec_id = self._clean(rec.get("id") or rec.get("id_registro") or "")

            imagen = self._clean(rec.get("imagen") or "")
            if imagen and imagen.startswith("/"):
                imagen = f"https://sic.cultura.gob.mx{imagen}"

            tipo = self._clean(rec.get("tipo") or rec.get("genero") or "")
            tags = list(table.get("tags", []))
            if tipo: tags.append(tipo[:40])
            if estado: tags.append(estado.lower()[:30])

            return {
                "source_id":   "sic_csv",
                "external_id": f"sic_{table['id']}_{rec_id}" if rec_id else "",
                "title":       title,
                "description": description,
                "category":    table.get("category", "cultural"),
                "tags":        tags,
                "image_url":   imagen or None,
                "date_start":  date_start,
                "date_end":    date_end,
                "price":       precio,
                "currency":    "MXN",
                "url":         url,
                "location":    location or estado or "México",
                "estado":      estado,
                "ciudad":      municipio,
                "latitude":    lat,
                "longitude":   lon,
            }
        except Exception as exc:
            logger.debug("SIC map_record error: %s", exc)
            return None

    # ── CKAN ─────────────────────────────────────────────────────────────

    async def _resolve_missing_urls(self, client: httpx.AsyncClient) -> None:
        for ds in CKAN_DATASETS:
            if ds.get("direct_url"):
                continue
            try:
                resp = await client.get(f"{CKAN_API}/resource_show", params={"id": ds["resource_id"]})
                if resp.status_code == 200:
                    res_data = resp.json().get("result", {})
                    ds["direct_url"] = res_data.get("url") or res_data.get("download_url")
                await asyncio.sleep(self.delay * 0.5)
            except Exception as exc:
                logger.debug("resolve URL '%s': %s", ds["id"], exc)

    async def _fetch_ckan_dataset(self, client: httpx.AsyncClient, ds: dict) -> list[dict]:
        if ds.get("direct_url"):
            result = await self._download_csv(client, ds["direct_url"], ds)
            if result is not None:
                return result
        return await self._paginated_search(client, ds)

    async def _download_csv(self, client: httpx.AsyncClient, url: str, ds: dict) -> Optional[list[dict]]:
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                return None
            if "html" in resp.headers.get("content-type", "").lower():
                return None

            text = self._decode(resp.content)
            if not text:
                return None

            reader = csv.DictReader(io.StringIO(text))
            events = []
            for record in reader:
                mapped = self._map_divulgacion_record(record, ds)
                if mapped:
                    events.append(mapped)
            return events
        except Exception as exc:
            logger.warning("CKAN download_csv '%s': %s", ds.get("id"), exc)
            return None

    async def _paginated_search(self, client: httpx.AsyncClient, ds: dict) -> list[dict]:
        events, offset = [], 0
        rid = ds["resource_id"]
        for _ in range(200):
            try:
                resp = await client.get(
                    f"{CKAN_API}/datastore_search",
                    params={"resource_id": rid, "limit": 1000, "offset": offset},
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
                if not data.get("success"):
                    break
                records = data["result"].get("records", [])
                if not records:
                    break
                for rec in records:
                    m = self._map_divulgacion_record(rec, ds)
                    if m:
                        events.append(m)
                total   = data["result"].get("total", 0)
                offset += len(records)
                if offset >= total or len(records) < 1000:
                    break
                await asyncio.sleep(self.delay * 0.5)
            except Exception as exc:
                logger.warning("paginated_search '%s': %s", ds["id"], exc)
                break
        return events

    def _map_divulgacion_record(self, record: dict, ds: dict) -> Optional[dict]:
        try:
            rec = {self._fix_enc(k).lower().strip(): self._fix_enc(str(v)) for k, v in record.items()}

            col_title = ds.get("col_title", "evento")
            title = self._clean(
                rec.get(col_title) or rec.get("nombre_event") or
                rec.get("evento") or rec.get("title") or ""
            )
            if not title or len(title) < 3:
                return None

            col_desc = ds.get("col_desc")
            description = self._clean(rec.get(col_desc, "") if col_desc else "")
            if not description:
                description = self._clean(rec.get("objetivo_ev") or rec.get("objetivo") or "")

            col_date   = ds.get("col_date", "fecha")
            date_start = self._parse_dt(rec.get(col_date) or rec.get("fecha"))
            date_end   = self._parse_dt(rec.get("fecha_fin"))

            col_url = ds.get("col_url", "lugar_enlace")
            url = self._clean(rec.get(col_url) or rec.get("enlace") or rec.get("lugar_enlace") or "")
            # Si hay dos columnas de enlace (lugar_enlace_01, lugar_enlace_02), tomar la primera
            if not url:
                url = self._clean(rec.get("lugar_enlace_01") or "")

            col_type = ds.get("col_type", "tipo")
            tipo     = self._clean(rec.get(col_type) or rec.get("tipo_evento") or rec.get("tipo") or "")
            category = self._map_tipo_to_category(tipo)

            dirigido = self._clean(rec.get("dirigido_a") or rec.get("ambi") or "")
            ambito   = self._clean(rec.get("ambi") or rec.get("ambito") or "Nacional")

            tags = ["divulgacion", ds["id"]]
            if tipo:     tags.append(tipo.lower()[:40])
            if dirigido: tags.append(dirigido.lower()[:30])

            rec_id = self._clean(rec.get("no") or rec.get("id") or "")

            return {
                "source_id":   "datos_gob_mx",
                "external_id": f"{ds['id']}_{rec_id}" if rec_id else "",
                "title":       title,
                "description": description,
                "category":    category,
                "tags":        tags,
                "image_url":   None,
                "date_start":  date_start,
                "date_end":    date_end,
                "price":       0.0,
                "currency":    "MXN",
                "url":         url,
                "location":    ambito or "México",
                "estado":      "",
                "ciudad":      "",
                "latitude":    None,
                "longitude":   None,
            }
        except Exception as exc:
            logger.debug("CKAN map_record error: %s", exc)
            return None

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _fix_enc(text: str) -> str:
        try:
            return text.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text

    @staticmethod
    def _decode(raw: bytes) -> str:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                text = raw.decode(enc)
                if enc in ("utf-8", "utf-8-sig") and text.count("Ã") > 3:
                    return raw.decode("latin-1")
                return text
            except (UnicodeDecodeError, LookupError):
                continue
        return raw.decode("utf-8", errors="replace")

    @staticmethod
    def _clean(value) -> str:
        s = str(value or "").strip()
        skip = {"nan", "none", "null", "n/a", "sin dato", "no aplica", ""}
        if s.lower() in skip:
            return ""
        try:
            return s.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return s

    @staticmethod
    def _parse_price(value) -> Optional[float]:
        if not value:
            return None
        s = str(value).replace("$", "").replace(",", "").strip()
        if s.lower() in ("gratis", "free", "0", "gratuito", "nan", "none", ""):
            return 0.0
        try:
            return float(s)
        except ValueError:
            return None

    @staticmethod
    def _parse_dt(value) -> Optional[datetime]:
        if not value:
            return None
        s = str(value).strip()
        if s.lower() in ("nan", "none", "null", "", "0", "sin dato"):
            return None
        for fmt in [
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
            "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d",
        ]:
            try:
                return datetime.strptime(s[:len(fmt)], fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(str(value).strip()) if value else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _map_tipo_to_category(tipo: str) -> str:
        t = tipo.lower()
        if any(w in t for w in ["taller", "laboratorio", "curso"]):
            return "talleres"
        if any(w in t for w in ["concierto", "festival", "musica"]):
            return "entretenimiento"
        if any(w in t for w in ["deporte", "atletismo"]):
            return "deportivo"
        return "cultural"