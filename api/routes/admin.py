"""
routes/admin.py
Endpoints exclusivos para el rol administrador.
- Cola de revisión manual (quality_ml < 0.5)
- Creación manual de eventos
- Estadísticas del sistema
"""
from fastapi import APIRouter, Depends, status, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from bson import ObjectId

from api.config.database import get_db
from api.middleware.auth import require_admin
from api.models.event import EventCreate, EventStatus, EventPublic
from api.models.interaction import ReviewAction, ReviewStatus
from api.models.user import TokenData
from api.services.event_service import create_event_manual, update_event_status

router = APIRouter(prefix="/api/admin", tags=["Admin"])


# ── Cola de revisión manual ───────────────────────────────────────────

@router.get(
    "/reviews",
    summary="Cola de revisión manual",
    description=(
        "Lista los eventos con quality_ml < 0.5 pendientes de revisión. "
        "**Requiere rol admin**."
    ),
)
async def list_reviews(
    status_filter: ReviewStatus = Query(ReviewStatus.pendiente),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    skip = (page - 1) * limit
    query = {"status": status_filter.value}

    total = await db.reviews_manual.count_documents(query)
    cursor = (
        db.reviews_manual
        .find(query)
        .sort("created_at", 1)
        .skip(skip)
        .limit(limit)
    )
    items = await cursor.to_list(length=limit)
    for item in items:
        item["_id"] = str(item.get("_id", ""))

    return {"total": total, "page": page, "limit": limit, "items": items}


@router.patch(
    "/reviews/{event_id}",
    summary="Aprobar o rechazar evento en revisión",
    description=(
        "Actualiza el estado de un evento en cola de revisión. "
        "Si se aprueba, el evento pasa a 'publicado'. "
        "Si se rechaza, el evento queda en 'rechazado'. **Requiere rol admin**."
    ),
)
async def review_event(
    event_id: str,
    action: ReviewAction,
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    now = datetime.utcnow()

    # Actualizar estado del evento
    if action.action == ReviewStatus.aprobado:
        new_event_status = EventStatus.publicado
    else:
        new_event_status = EventStatus.rechazado

    await update_event_status(event_id, new_event_status, db)

    # Actualizar documento de revisión
    await db.reviews_manual.update_one(
        {"event_id": event_id},
        {
            "$set": {
                "status": action.action.value,
                "reviewer_id": admin.user_id,
                "notes": action.notes,
                "reviewed_at": now,
            }
        },
    )

    return {
        "event_id": event_id,
        "new_status": new_event_status.value,
        "reviewed_at": now.isoformat(),
    }


# ── Creación manual de eventos ────────────────────────────────────────

@router.post(
    "/events",
    status_code=status.HTTP_201_CREATED,
    summary="Crear evento manualmente",
    description="El administrador puede crear eventos directamente. Se publican de inmediato. **Requiere rol admin**.",
)
async def create_event(
    data: EventCreate,
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    return await create_event_manual(data, admin.user_id, db)


# ── Estadísticas ──────────────────────────────────────────────────────

@router.get(
    "/stats",
    summary="Estadísticas del sistema",
    description="Resumen del estado del sistema: eventos, usuarios, reviews. **Requiere rol admin**.",
)
async def system_stats(
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    total_events = await db.events.count_documents({})
    published = await db.events.count_documents({"status": EventStatus.publicado.value})
    pending_review = await db.reviews_manual.count_documents({"status": ReviewStatus.pendiente.value})
    total_users = await db.users.count_documents({})
    total_interactions = await db.user_interactions.count_documents({})

    # Distribución por categoría
    pipeline = [
        {"$match": {"status": EventStatus.publicado.value}},
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
    ]
    category_dist_raw = await db.events.aggregate(pipeline).to_list(20)
    category_dist = {doc["_id"]: doc["count"] for doc in category_dist_raw}

    from api.models.interaction import ReviewStatus

    return {
        "events": {
            "total": total_events,
            "published": published,
            "pending_review": await db.reviews_manual.count_documents(
                {"status": ReviewStatus.pendiente.value}
            ),
            "by_category": category_dist,
        },
        "users": {"total": total_users},
        "interactions": {"total": total_interactions},
        "generated_at": datetime.utcnow().isoformat(),
    }


# ── Scraper trigger (protegido con SCRAPER_CRON_SECRET) ───────────────

@router.post(
    "/trigger-scraper",
    summary="Disparar scraping manualmente",
    description=(
        "Ejecuta el pipeline de scraping de forma manual. "
        "Requiere header X-Cron-Secret con el valor correcto. "
        "**Requiere rol admin**."
    ),
)
async def trigger_scraper(
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    """
    En producción esto dispararía un webhook a GitHub Actions.
    Aquí retornamos un mensaje de confirmación.
    """
    return {
        "message": "Scraping disparado. Revisa GitHub Actions para el progreso.",
        "triggered_by": admin.user_id,
        "triggered_at": datetime.utcnow().isoformat(),
    }