"""
ml/pipeline.py — v4

Funciones:
  - process_raw_event()  Normaliza un evento crudo y lo prepara para Mongo.
  - ingest_events()      Procesa una lista de crudos y los guarda en Mongo.

CAMBIOS v4:
  - Se agrega ingest_events() que faltaba y causaba ImportError en scraper.py.
  - Eventos sin fecha ya no se descartan: se asigna fecha estimada y quedan
    en 'pendiente_revision' con date_estimated=True.
  - classify_event() recibe un solo dict (no argumentos separados).
  - date_start siempre se pasa al ml_input (incluso cuando es estimada),
    para que date_is_future() no siempre devuelva 0.0.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Helpers de texto
# ──────────────────────────────────────────────────────────────────────

def _fix_encoding(text: str) -> str:
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _clean_text(text: str | None) -> str:
    if not text:
        return ""
    text = str(text)
    text = _fix_encoding(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = "".join(c for c in text if c.isprintable() or c == "\n")
    return text.strip()


# ──────────────────────────────────────────────────────────────────────
# Parseo de fechas
# ──────────────────────────────────────────────────────────────────────

def _parse_date(date_str: Any) -> datetime | None:
    if isinstance(date_str, datetime):
        if date_str.tzinfo is not None:
            return date_str.astimezone(timezone.utc).replace(tzinfo=None)
        return date_str

    if not date_str:
        return None

    date_str = str(date_str).strip()
    if date_str.lower() in ("nan", "none", "null", "", "0"):
        return None

    try:
        normalized = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except (ValueError, AttributeError):
        pass

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%B %d, %Y",
        "%d de %B de %Y",
        "%d %b %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str[:19], fmt)
        except ValueError:
            continue

    match = re.search(r"\b(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})\b", date_str)
    if match:
        try:
            d, m, y = match.groups()
            return datetime(int(y), int(m), int(d))
        except ValueError:
            pass

    match = re.search(r"\b(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})\b", date_str)
    if match:
        try:
            y, m, d = match.groups()
            return datetime(int(y), int(m), int(d))
        except ValueError:
            pass

    logger.debug("No se pudo parsear la fecha: %r", date_str)
    return None


def _estimated_date(days_ahead: int = 7) -> datetime:
    return datetime.utcnow() + timedelta(days=days_ahead)


# ──────────────────────────────────────────────────────────────────────
# process_raw_event
# ──────────────────────────────────────────────────────────────────────

def process_raw_event(raw_event: dict[str, Any]) -> dict[str, Any]:
    """
    Procesa un evento crudo del scraper y lo prepara para almacenamiento.

    Returns:
        Dict normalizado listo para Mongo, o {} si el evento se descarta
        (título muy corto o vacío).
    """
    from api.services.ml_service import classify_event

    # 1. Limpieza de texto
    title       = _clean_text(raw_event.get("title", ""))
    description = _clean_text(raw_event.get("description", ""))

    if not title or len(title) < 3:
        return {}

    # 2. Parsear fechas
    date_start = _parse_date(
        raw_event.get("date_start") or raw_event.get("start_date")
    )
    date_end = _parse_date(
        raw_event.get("date_end") or raw_event.get("end_date")
    )

    date_estimated = False
    if date_start is None:
        date_start     = _estimated_date(days_ahead=7)
        date_estimated = True
        logger.debug("Evento sin fecha, usando estimada: %r", title[:60])

    # 3. Clasificación ML
    # classify_event(title, description, event) — tres argumentos separados
    event_context = {
        "image_url":  raw_event.get("image_url") or raw_event.get("image"),
        "latitude":   raw_event.get("latitude")  or raw_event.get("lat"),
        "longitude":  raw_event.get("longitude") or raw_event.get("lon"),
        "price":      raw_event.get("price"),
        "category":   raw_event.get("category"),
        # Siempre pasamos date_start (estimada o real) para que
        # date_is_future() no devuelva siempre 0.0
        "date_start": date_start.isoformat(),
    }

    ml_result     = classify_event(title, description, event_context)
    quality_ml    = ml_result.get("quality_ml", 0.0)
    ml_category   = ml_result.get("category", "otro")
    ml_confidence = ml_result.get("category_confidence", 0.0)
    models_active = ml_result.get("models_active", False)

    # 4. Categoría final — si ML no aporta nada, respetar la del scraper
    if not models_active or (ml_category == "otro" and ml_confidence == 0.0):
        ml_category = raw_event.get("category") or "otro"

    # 5. Estado de publicación
    if date_estimated:
        status = "pendiente_revision"
    elif models_active:
        from api.config.settings import get_settings
        threshold = get_settings().ML_QUALITY_THRESHOLD
        status = "publicado" if quality_ml >= threshold else "pendiente_revision"
    else:
        status = "publicado"

    # 6. Coordenadas GeoJSON
    coordinates = None
    lat = raw_event.get("latitude") or raw_event.get("lat")
    lon = raw_event.get("longitude") or raw_event.get("lon")
    if lat is not None and lon is not None:
        try:
            coordinates = {
                "type": "Point",
                "coordinates": [float(lon), float(lat)],
            }
        except (ValueError, TypeError):
            pass

    # 7. Campos de ubicación
    estado = _clean_text(raw_event.get("estado", ""))
    ciudad = _clean_text(raw_event.get("ciudad", ""))

    # 8. Documento final
    from bson import ObjectId

    return {
        "_id":                 str(ObjectId()),
        "title":               title,
        "description":         description,
        "category":            ml_category,
        "category_confidence": ml_confidence,
        "date_start":          date_start,
        "date_end":            date_end,
        "date_estimated":      date_estimated,
        "location":            _clean_text(
            raw_event.get("location") or raw_event.get("venue", "")
        ),
        "estado":              estado,
        "ciudad":              ciudad,
        "coordinates":         coordinates,
        "image_url":           raw_event.get("image_url") or raw_event.get("image"),
        "url_source":          raw_event.get("url") or raw_event.get("url_source"),
        "price":               raw_event.get("price"),
        "currency":            raw_event.get("currency", "MXN"),
        "tags":                raw_event.get("tags", []),
        "source_id":           raw_event.get("source_id"),
        "external_id":         raw_event.get("external_id", ""),
        "fingerprint":         raw_event.get("fingerprint", ""),
        "quality_ml":          quality_ml,
        "tfidf_vector":        ml_result.get("tfidf_vector", []),
        "status":              status,
        "models_active":       models_active,
        "created_at":          datetime.utcnow(),
        "updated_at":          datetime.utcnow(),
    }


# ──────────────────────────────────────────────────────────────────────
# ingest_events  ← función que faltaba y causaba el ImportError
# ──────────────────────────────────────────────────────────────────────

async def ingest_events(
    raw_events: list[dict[str, Any]],
    db,                          # motor.motor_asyncio.AsyncIOMotorDatabase
) -> dict[str, int]:
    """
    Procesa una lista de eventos crudos y los guarda en MongoDB.

    Estrategia de upsert:
      - Clave primaria: fingerprint (cross-source) si existe,
        si no: (source_id + external_id).
      - $setOnInsert: created_at  (no se sobreescribe en updates).
      - $set: todo lo demás (actualiza descripción, fechas, etc.).

    Returns:
        {
            "total":          int,   # eventos procesados
            "published":      int,   # guardados con status=publicado
            "pending_review": int,   # guardados con status=pendiente_revision
            "skipped":        int,   # descartados (título vacío, etc.)
            "errors":         int,   # errores al guardar en Mongo
        }
    """
    from pymongo import UpdateOne
    from pymongo.errors import BulkWriteError

    col = db["events"]
    stats = {"total": 0, "published": 0, "pending_review": 0, "skipped": 0, "errors": 0}
    ops: list[UpdateOne] = []

    for raw in raw_events:
        stats["total"] += 1

        # Procesar evento
        try:
            processed = process_raw_event(raw)
        except Exception as exc:
            logger.warning("process_raw_event error: %s | raw=%s", exc, str(raw)[:120])
            stats["errors"] += 1
            continue

        if not processed:
            stats["skipped"] += 1
            continue

        # Construir filtro de upsert
        fp = processed.get("fingerprint") or raw.get("fingerprint", "")
        if fp:
            upsert_filter = {"fingerprint": fp}
        else:
            upsert_filter = {
                "source_id":   processed.get("source_id", ""),
                "external_id": processed.get("external_id", ""),
            }

        # Separar created_at para $setOnInsert
        created_at  = processed.pop("created_at", datetime.utcnow())
        doc_id      = processed.pop("_id", None)

        ops.append(
            UpdateOne(
                upsert_filter,
                {
                    "$set": processed,
                    "$setOnInsert": {
                        "created_at": created_at,
                        "_id": doc_id,
                    },
                },
                upsert=True,
            )
        )

        # Contadores por status
        if processed.get("status") == "publicado":
            stats["published"] += 1
        else:
            stats["pending_review"] += 1

        # Escribir en lotes de 500 para no saturar Mongo
        if len(ops) >= 500:
            try:
                await col.bulk_write(ops, ordered=False)
            except BulkWriteError as bwe:
                stats["errors"] += len(bwe.details.get("writeErrors", []))
                logger.warning("BulkWriteError: %d errores", stats["errors"])
            ops = []

    # Último lote
    if ops:
        try:
            await col.bulk_write(ops, ordered=False)
        except BulkWriteError as bwe:
            stats["errors"] += len(bwe.details.get("writeErrors", []))
            logger.warning("BulkWriteError (último lote): %d errores", stats["errors"])

    logger.info(
        "ingest_events: total=%d pub=%d rev=%d skip=%d err=%d",
        stats["total"], stats["published"],
        stats["pending_review"], stats["skipped"], stats["errors"],
    )
    return stats