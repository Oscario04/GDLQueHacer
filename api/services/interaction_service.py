"""
services/interaction_service.py
Registra interacciones de usuario y dispara actualización de perfil ML.
"""
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException
from datetime import datetime
from bson import ObjectId

from api.models.interaction import InteractionCreate, InteractionType
from api.models.event import EventStatus

# Pesos para calcular el vector de preferencias
INTERACTION_WEIGHTS = {
    InteractionType.view: 1.0,
    InteractionType.save: 3.0,
    InteractionType.interested: 5.0,
    InteractionType.uninterested: -2.0,
}


async def log_interaction(
    user_id: str,
    data: InteractionCreate,
    db: AsyncIOMotorDatabase,
) -> dict:
    """
    Registra una interacción usuario-evento.
    Si ya existe una interacción del mismo tipo, la actualiza (upsert).
    Retorna el documento guardado.
    """
    # Verificar que el evento existe y está publicado
    event = await db.events.find_one(
        {"_id": data.event_id, "status": EventStatus.publicado.value},
        {"_id": 1, "category": 1, "tfidf_vector": 1},
    )
    if not event:
        raise HTTPException(status_code=404, detail="Evento no encontrado.")

    now = datetime.utcnow()
    interaction_doc = {
        "user_id": user_id,
        "event_id": data.event_id,
        "type": data.type.value,
        "created_at": now,
    }

    # Upsert: reemplaza si ya existe la combinación user+event+type
    result = await db.user_interactions.find_one_and_replace(
        {"user_id": user_id, "event_id": data.event_id, "type": data.type.value},
        {**interaction_doc, "_id": str(ObjectId())},
        upsert=True,
        return_document=True,
    )
    if result is None:
        # Si fue insert nuevo, buscarlo
        result = await db.user_interactions.find_one(
            {"user_id": user_id, "event_id": data.event_id, "type": data.type.value}
        )

    # Actualizar perfil de preferencias del usuario de forma ligera
    await _update_user_preferences_lightweight(
        user_id=user_id,
        event=event,
        interaction_type=data.type,
        db=db,
    )

    return result


async def _update_user_preferences_lightweight(
    user_id: str,
    event: dict,
    interaction_type: InteractionType,
    db: AsyncIOMotorDatabase,
) -> None:
    """
    Actualización ligera del perfil: actualiza top_categories e interaction_count.
    El vector TF-IDF se recalcula en el pipeline ML completo (nightly).
    """
    category = event.get("category", "otro")
    weight = INTERACTION_WEIGHTS.get(interaction_type, 1.0)

    if weight > 0:
        # Incrementar contador de interacciones y actualizar categorías top
        await db.user_preferences.update_one(
            {"user_id": user_id},
            {
                "$inc": {"interaction_count": 1},
                "$set": {"updated_at": datetime.utcnow()},
                # Asegurar que la categoría esté en top_categories (máx 5)
                "$addToSet": {"top_categories": category},
            },
            upsert=True,
        )
    else:
        # Interacción negativa — solo actualizar timestamp
        await db.user_preferences.update_one(
            {"user_id": user_id},
            {"$set": {"updated_at": datetime.utcnow()}},
            upsert=True,
        )


async def get_user_interactions(
    user_id: str,
    db: AsyncIOMotorDatabase,
    limit: int = 50,
) -> list[dict]:
    """Retorna las últimas N interacciones de un usuario."""
    cursor = (
        db.user_interactions
        .find({"user_id": user_id})
        .sort("created_at", -1)
        .limit(limit)
    )
    interactions = await cursor.to_list(length=limit)
    for i in interactions:
        i["_id"] = str(i["_id"])
    return interactions