"""
routes/events.py
Endpoints públicos de eventos: listado, detalle y recomendaciones.
"""
from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from typing import Optional

from api.config.database import get_db
from api.config.settings import get_settings
from api.middleware.auth import get_current_user, get_optional_user
from api.models.event import (
    EventListResponse, EventDetail, EventFilter,
    EventCategory, EventRecommendation,
)
from api.models.user import TokenData
from api.services import event_service
from api.services.recommendation_service import get_recommendations

router = APIRouter(prefix="/api/events", tags=["Events"])
settings = get_settings()


@router.get(
    "",
    response_model=EventListResponse,
    summary="Listar eventos públicos",
    description=(
        "Retorna eventos publicados con filtros opcionales por categoría, "
        "fechas, búsqueda textual y rango geográfico. Paginado."
    ),
)
async def list_events(
    category: Optional[EventCategory] = Query(None, description="Filtrar por categoría"),
    date_from: Optional[datetime] = Query(None, description="Fecha inicio (ISO 8601)"),
    date_to: Optional[datetime] = Query(None, description="Fecha fin (ISO 8601)"),
    lat: Optional[float] = Query(None, ge=-90, le=90, description="Latitud del centro"),
    lon: Optional[float] = Query(None, ge=-180, le=180, description="Longitud del centro"),
    radius_km: Optional[float] = Query(None, gt=0, le=200, description="Radio en km"),
    q: Optional[str] = Query(None, max_length=200, description="Búsqueda textual"),
    page: int = Query(1, ge=1, description="Página"),
    limit: int = Query(20, ge=1, le=100, description="Resultados por página"),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    filters = EventFilter(
        category=category, date_from=date_from, date_to=date_to,
        lat=lat, lon=lon, radius_km=radius_km, q=q,
        page=page, limit=limit,
    )
    return await event_service.list_events(filters, db)


@router.get(
    "/recommended",
    response_model=list[EventRecommendation],
    summary="Recomendaciones personalizadas",
    description=(
        "Retorna eventos recomendados para el usuario autenticado usando KNN + SVM. "
        "Los usuarios anónimos reciben los eventos más populares recientes. "
        "**Requiere JWT** para recomendaciones personalizadas."
    ),
)
async def recommended_events(
    limit: int = Query(20, ge=1, le=50),
    current_user: Optional[TokenData] = Depends(get_optional_user),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> list:
    user_id = current_user.user_id if current_user else None

    if user_id is None:
        # Usuario anónimo → cold start (eventos populares recientes)
        from api.services.recommendation_service import _cold_start_recommendations
        return await _cold_start_recommendations(db, limit)

    return await get_recommendations(user_id=user_id, db=db, limit=limit)


@router.get(
    "/{event_id}",
    response_model=EventDetail,
    summary="Detalle de un evento",
    description="Retorna todos los campos de un evento publicado por su ID.",
)
async def get_event(
    event_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    return await event_service.get_event_by_id(event_id, db)