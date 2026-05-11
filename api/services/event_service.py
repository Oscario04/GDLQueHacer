"""
services/event_service.py
Lógica de negocio para consulta, creación y gestión de eventos.
"""
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from datetime import datetime
from typing import Any

from api.models.event import EventFilter, EventCreate, EventStatus, EventCategory
from api.config.settings import get_settings

settings = get_settings()

# Campos a proyectar en respuestas públicas (excluye tfidf_vector y metadata_raw)
_PUBLIC_PROJECTION = {
    "_id": 1, "title": 1, "description": 1, "category": 1,
    "date_start": 1, "date_end": 1, "location": 1, "coordinates": 1,
    "quality_ml": 1, "status": 1, "image_url": 1, "url_source": 1,
    "price": 1, "tags": 1, "created_at": 1, "updated_at": 1,
}


def _build_filter_query(f: EventFilter) -> dict[str, Any]:
    """Construye el documento de filtro MongoDB a partir de los parámetros."""
    query: dict[str, Any] = {"status": EventStatus.publicado.value}

    if f.category:
        query["category"] = f.category.value

    # Rango de fechas
    date_filter: dict = {}
    if f.date_from:
        date_filter["$gte"] = f.date_from
    if f.date_to:
        date_filter["$lte"] = f.date_to
    if date_filter:
        query["date_start"] = date_filter

    # Búsqueda textual
    if f.q and f.q.strip():
        query["$text"] = {"$search": f.q.strip()}

    # Filtro geográfico
    if f.lat is not None and f.lon is not None and f.radius_km:
        query["coordinates"] = {
            "$near": {
                "$geometry": {"type": "Point", "coordinates": [f.lon, f.lat]},
                "$maxDistance": f.radius_km * 1000,  # metros
            }
        }

    return query


async def list_events(
    filters: EventFilter,
    db: AsyncIOMotorDatabase,
) -> dict[str, Any]:
    """
    Lista eventos públicos con filtros, paginación y búsqueda textual.
    """
    query = _build_filter_query(filters)
    skip = (filters.page - 1) * filters.limit

    total = await db.events.count_documents(query)
    cursor = (
        db.events.find(query, _PUBLIC_PROJECTION)
        .sort("date_start", 1)
        .skip(skip)
        .limit(filters.limit)
    )
    items = await cursor.to_list(length=filters.limit)

    # Convertir ObjectId a str si es necesario
    for item in items:
        item["_id"] = str(item["_id"])

    return {
        "total": total,
        "page": filters.page,
        "limit": filters.limit,
        "has_next": skip + len(items) < total,
        "items": items,
    }


async def get_event_by_id(
    event_id: str,
    db: AsyncIOMotorDatabase,
) -> dict[str, Any]:
    """Retorna el detalle completo de un evento publicado."""
    event = await db.events.find_one(
        {"_id": event_id, "status": EventStatus.publicado.value},
        {**_PUBLIC_PROJECTION, "source_id": 1},
    )
    if not event:
        raise HTTPException(status_code=404, detail="Evento no encontrado.")
    event["_id"] = str(event["_id"])
    return event


async def create_event_manual(
    data: EventCreate,
    admin_id: str,
    db: AsyncIOMotorDatabase,
) -> dict[str, Any]:
    """Crea un evento manualmente (sólo admins). Se publica directo."""
    doc = {
        "_id": str(ObjectId()),
        **data.model_dump(exclude_none=True),
        "category": data.category.value,
        "status": EventStatus.publicado.value,
        "quality_ml": 1.0,  # Creado por admin = calidad máxima
        "tfidf_vector": [],
        "created_by": admin_id,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    if doc.get("coordinates"):
        doc["coordinates"] = doc["coordinates"].model_dump() if hasattr(
            doc["coordinates"], "model_dump"
        ) else doc["coordinates"]

    await db.events.insert_one(doc)
    return {k: v for k, v in doc.items() if k not in ("tfidf_vector", "metadata_raw")}


async def update_event_status(
    event_id: str,
    new_status: EventStatus,
    db: AsyncIOMotorDatabase,
) -> None:
    """Actualiza el estado de un evento (usado por review manual)."""
    result = await db.events.update_one(
        {"_id": event_id},
        {"$set": {"status": new_status.value, "updated_at": datetime.utcnow()}},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Evento no encontrado.")


async def get_events_for_ml(
    db: AsyncIOMotorDatabase,
    limit: int = 500,
) -> list[dict]:
    """
    Retorna eventos publicados con sus vectores TF-IDF para el ML pipeline.
    Usado internamente por el servicio de recomendaciones.
    """
    cursor = db.events.find(
        {"status": EventStatus.publicado.value, "tfidf_vector": {"$ne": []}},
        {"_id": 1, "title": 1, "category": 1, "tfidf_vector": 1,
         "date_start": 1, "quality_ml": 1},
    ).limit(limit)
    return await cursor.to_list(length=limit)