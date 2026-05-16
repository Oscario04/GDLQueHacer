"""
ml/pipeline.py
Pipeline completo de procesamiento ML para eventos recolectados.

FIXES:
  - Modo degradado (sin modelos): publica eventos directamente en vez de skipearlos.
    Un evento sin ML score sigue siendo útil; el umbral solo aplica cuando hay modelo.
  - Deduplicación por (source_id + url_source), no solo por url_source.
    Evita que el mismo evento de Ticketmaster se salte en runs subsecuentes.
  - Encoding fix en texto: decodifica latin-1 mal parseado como UTF-8 (Ã©→é, etc.)
  - Fecha: acepta date_start / start_date y date_end / end_date indistintamente.
"""
import logging
import re
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers de texto
# ---------------------------------------------------------------------------

def _fix_encoding(text: str) -> str:
    """
    Intenta corregir texto con encoding roto (latin-1 leído como UTF-8).
    Ej: "ObservaciÃ³n" → "Observación"
    """
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _clean_text(text: str | None) -> str:
    """Limpieza básica: elimina HTML, caracteres extra, normaliza espacios."""
    if not text:
        return ""
    text = str(text)
    # Intentar fix de encoding roto
    text = _fix_encoding(text)
    # Eliminar tags HTML básicos
    text = re.sub(r"<[^>]+>", " ", text)
    # Normalizar espacios
    text = re.sub(r"\s+", " ", text)
    # Eliminar caracteres no imprimibles
    text = "".join(c for c in text if c.isprintable() or c == "\n")
    return text.strip()


# ---------------------------------------------------------------------------
# Parseo de fechas
# ---------------------------------------------------------------------------

def _parse_date(date_str: Any) -> datetime | None:
    """
    Intenta parsear una fecha en múltiples formatos.
    Normaliza siempre a UTC naive (sin tzinfo) para consistencia en MongoDB.
    """
    if isinstance(date_str, datetime):
        if date_str.tzinfo is not None:
            return date_str.astimezone(timezone.utc).replace(tzinfo=None)
        return date_str

    if not date_str:
        return None

    date_str = str(date_str).strip()
    if date_str.lower() in ("nan", "none", "null", "", "0"):
        return None

    # fromisoformat — maneja +00:00, espacios, T, etc.
    try:
        normalized = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except (ValueError, AttributeError):
        pass

    # Formatos manuales
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str[:len(fmt)], fmt)
        except ValueError:
            continue

    logger.debug("No se pudo parsear la fecha: %r", date_str)
    return None


# ---------------------------------------------------------------------------
# Procesamiento de un evento
# ---------------------------------------------------------------------------

def process_raw_event(raw_event: dict[str, Any]) -> dict[str, Any]:
    """
    Procesa un evento crudo del scraper y lo prepara para almacenamiento.

    Returns:
        Dict con campos normalizados, o {} si el evento debe descartarse.
    """
    from api.services.ml_service import classify_event

    # ── 1. Limpieza de texto ──────────────────────────────────────────
    title       = _clean_text(raw_event.get("title", ""))
    description = _clean_text(raw_event.get("description", ""))

    if not title or len(title) < 3:
        return {}

    # ── 2. Parsear fechas ─────────────────────────────────────────────
    date_start = _parse_date(
        raw_event.get("date_start") or raw_event.get("start_date")
    )
    date_end = _parse_date(
        raw_event.get("date_end") or raw_event.get("end_date")
    )

    if date_start is None:
        logger.warning("Evento sin fecha válida, descartado: %r", title[:60])
        return {}

    # ── 3. Clasificación ML ───────────────────────────────────────────
    ml_result      = classify_event(title, description)
    quality_ml     = ml_result.get("quality_ml", 0.0)
    ml_category    = ml_result.get("category", "otro")
    ml_confidence  = ml_result.get("category_confidence", 0.0)
    models_active  = ml_result.get("models_active", False)

    # ── 4. Categoría final ────────────────────────────────────────────
    # Si no hay modelos activos (modo degradado), usar la del scraper
    if not models_active or (ml_category == "otro" and ml_confidence == 0.0):
        ml_category = raw_event.get("category") or "otro"

    # ── 5. Estado de publicación ──────────────────────────────────────
    #
    # REGLA CLAVE:
    #   - Con modelos activos  → publicar si quality_ml >= threshold
    #   - Sin modelos (degradado) → publicar SIEMPRE (no tener ML no es
    #     razón para tirar el evento; se puede re-clasificar después)
    #
    if models_active:
        from api.config.settings import get_settings
        threshold = get_settings().ML_QUALITY_THRESHOLD
        status = "publicado" if quality_ml >= threshold else "pendiente_revision"
    else:
        status = "publicado"

    # ── 6. Coordenadas ────────────────────────────────────────────────
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

    # ── 7. Campos extra de ubicación (para filtrado en front) ─────────
    estado = _clean_text(raw_event.get("estado", ""))
    ciudad = _clean_text(raw_event.get("ciudad", ""))

    # ── 8. Construir documento final ──────────────────────────────────
    from bson import ObjectId

    processed = {
        "_id":                  str(ObjectId()),
        "title":                title,
        "description":          description,
        "category":             ml_category,
        "category_confidence":  ml_confidence,
        "date_start":           date_start,
        "date_end":             date_end,
        "location":             _clean_text(
            raw_event.get("location") or raw_event.get("venue", "")
        ),
        "estado":               estado,
        "ciudad":               ciudad,
        "coordinates":          coordinates,
        "image_url":            raw_event.get("image_url") or raw_event.get("image"),
        "url_source":           raw_event.get("url") or raw_event.get("url_source"),
        "price":                raw_event.get("price"),
        "currency":             raw_event.get("currency", "MXN"),
        "tags":                 raw_event.get("tags", []),
        "source_id":            raw_event.get("source_id"),
        "external_id":          raw_event.get("external_id", ""),
        "quality_ml":           quality_ml,
        "tfidf_vector":         ml_result.get("tfidf_vector", []),
        "status":               status,
        "models_active":        models_active,
        "created_at":           datetime.utcnow(),
        "updated_at":           datetime.utcnow(),
    }
    return processed


# ---------------------------------------------------------------------------
# Ingestión batch
# ---------------------------------------------------------------------------

async def ingest_events(
    raw_events: list[dict[str, Any]],
    db,
) -> dict[str, int]:
    """
    Procesa e ingresa una lista de eventos crudos en la base de datos.

    Deduplicación:
      1. Por external_id + source_id (si existen) — clave de negocio
      2. Por url_source — evita duplicados de distinta fuente misma URL

    Returns:
        {"total": N, "published": N, "pending_review": N, "skipped": N, "errors": N}
    """
    stats = {
        "total":          len(raw_events),
        "published":      0,
        "pending_review": 0,
        "skipped":        0,
        "errors":         0,
    }

    for raw in raw_events:
        try:
            processed = process_raw_event(raw)

            if not processed:
                stats["skipped"] += 1
                continue

            # ── Deduplicación ─────────────────────────────────────────
            # Primero por external_id + source_id (más preciso)
            ext_id    = processed.get("external_id", "")
            source_id = processed.get("source_id", "")
            url       = processed.get("url_source", "")

            existing = None

            if ext_id and source_id:
                existing = await db.events.find_one(
                    {"external_id": ext_id, "source_id": source_id},
                    {"_id": 1},
                )

            # Si no encontró por external_id, buscar por URL
            if not existing and url:
                existing = await db.events.find_one(
                    {"url_source": url},
                    {"_id": 1},
                )

            if existing:
                # Actualizar updated_at y campos que pueden cambiar
                await db.events.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "updated_at": datetime.utcnow(),
                        "date_start":  processed["date_start"],
                        "date_end":    processed["date_end"],
                        "price":       processed["price"],
                        "image_url":   processed["image_url"],
                    }},
                )
                stats["skipped"] += 1
                continue

            # ── Insertar evento nuevo ─────────────────────────────────
            await db.events.insert_one(processed)

            if processed["status"] == "publicado":
                stats["published"] += 1
            else:
                stats["pending_review"] += 1
                # Añadir a cola de revisión manual
                await db.reviews_manual.update_one(
                    {"event_id": processed["_id"]},
                    {"$setOnInsert": {
                        "event_id":   processed["_id"],
                        "quality_ml": processed["quality_ml"],
                        "status":     "pendiente",
                        "created_at": datetime.utcnow(),
                    }},
                    upsert=True,
                )

        except Exception as exc:
            logger.error(
                "Error procesando evento '%s': %s",
                raw.get("title", "?")[:50], exc,
            )
            stats["errors"] += 1

    logger.info(
        "📥  Ingestión completada: %d total | %d publicados | %d en revisión | %d skipped | %d errores",
        stats["total"],
        stats["published"],
        stats["pending_review"],
        stats["skipped"],
        stats["errors"],
    )
    return stats