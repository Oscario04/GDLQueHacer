"""
ml/pipeline.py
Pipeline completo de procesamiento ML para eventos recolectados.
Se ejecuta desde el scraper (GitHub Actions) o manualmente.

Flujo:
  raw_event → clean_text → classify_category → score_quality
           → vectorize → decide(publish | manual_review)
           → save_to_mongodb
"""
import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _clean_text(text: str | None) -> str:
    """Limpieza básica de texto: elimina HTML, caracteres extra, normaliza espacios."""
    if not text:
        return ""
    # Eliminar tags HTML básicos
    text = re.sub(r"<[^>]+>", " ", text)
    # Normalizar espacios
    text = re.sub(r"\s+", " ", text)
    # Eliminar caracteres no imprimibles
    text = "".join(c for c in text if c.isprintable() or c == "\n")
    return text.strip()


def _parse_date(date_str: Any) -> datetime | None:
    """Intenta parsear una fecha en múltiples formatos."""
    if isinstance(date_str, datetime):
        return date_str
    if not date_str:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    return None


def process_raw_event(raw_event: dict[str, Any]) -> dict[str, Any]:
    """
    Procesa un evento crudo del scraper y lo prepara para almacenamiento.

    Args:
        raw_event: Diccionario con datos crudos del scraper.

    Returns:
        Diccionario con todos los campos normalizados y resultado del ML.
    """
    from api.services.ml_service import classify_event

    # ── 1. Limpieza de texto ──────────────────────────────────────────
    title = _clean_text(raw_event.get("title", ""))
    description = _clean_text(raw_event.get("description", ""))

    # ── 2. Clasificación ML + score de calidad ────────────────────────
    ml_result = classify_event(title, description)

    # ── 3. Determinar estado según quality_ml ────────────────────────
    from api.config.settings import get_settings
    settings = get_settings()
    threshold = settings.ML_QUALITY_THRESHOLD

    if ml_result["quality_ml"] >= threshold:
        status = "publicado"
    else:
        status = "pendiente_revision"

    # ── 4. Parsear fechas ─────────────────────────────────────────────
    date_start = _parse_date(raw_event.get("date_start") or raw_event.get("start_date"))
    date_end = _parse_date(raw_event.get("date_end") or raw_event.get("end_date"))

    # Si no hay fecha, descartar evento
    if date_start is None:
        logger.warning("Evento sin fecha válida descartado: %s", title[:50])
        return {}

    # ── 5. Coordenadas ────────────────────────────────────────────────
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

    # ── 6. Construir documento final ──────────────────────────────────
    from bson import ObjectId

    processed = {
        "_id": str(ObjectId()),
        "title": title,
        "description": description,
        "category": ml_result.get("category", "otro"),
        "category_confidence": ml_result.get("category_confidence", 0.0),
        "date_start": date_start,
        "date_end": date_end,
        "location": _clean_text(raw_event.get("location") or raw_event.get("venue", "")),
        "coordinates": coordinates,
        "image_url": raw_event.get("image_url") or raw_event.get("image"),
        "url_source": raw_event.get("url") or raw_event.get("url_source"),
        "price": raw_event.get("price"),
        "tags": raw_event.get("tags", []),
        "source_id": raw_event.get("source_id"),
        "quality_ml": ml_result["quality_ml"],
        "tfidf_vector": ml_result.get("tfidf_vector", []),
        "status": status,
        "metadata_raw": raw_event,  # Guardar original para reprocessing
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    return processed


async def ingest_events(
    raw_events: list[dict[str, Any]],
    db,
) -> dict[str, int]:
    """
    Procesa e ingresa una lista de eventos crudos en la base de datos.
    Retorna estadísticas del proceso.

    Args:
        raw_events: Lista de eventos del scraper.
        db: Instancia AsyncIOMotorDatabase.

    Returns:
        {"total": N, "published": N, "pending_review": N, "skipped": N, "errors": N}
    """
    stats = {"total": len(raw_events), "published": 0,
              "pending_review": 0, "skipped": 0, "errors": 0}

    for raw in raw_events:
        try:
            processed = process_raw_event(raw)

            if not processed:
                stats["skipped"] += 1
                continue

            # Evitar duplicados por URL fuente
            if processed.get("url_source"):
                existing = await db.events.find_one(
                    {"url_source": processed["url_source"]},
                    {"_id": 1}
                )
                if existing:
                    stats["skipped"] += 1
                    continue

            # Insertar evento
            await db.events.insert_one(processed)

            if processed["status"] == "publicado":
                stats["published"] += 1
            else:
                stats["pending_review"] += 1
                # Añadir a la cola de revisión manual
                await db.reviews_manual.update_one(
                    {"event_id": processed["_id"]},
                    {"$setOnInsert": {
                        "event_id": processed["_id"],
                        "quality_ml": processed["quality_ml"],
                        "status": "pendiente",
                        "created_at": datetime.utcnow(),
                    }},
                    upsert=True,
                )

        except Exception as exc:
            logger.error("Error procesando evento: %s", exc)
            stats["errors"] += 1

    logger.info(
        "📥  Ingestión completada: %d total | %d publicados | %d en revisión | %d skipped | %d errores",
        stats["total"], stats["published"], stats["pending_review"],
        stats["skipped"], stats["errors"]
    )
    return stats