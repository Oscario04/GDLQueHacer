"""
routes/interactions.py
Endpoints para registrar y consultar interacciones de usuario con eventos.
Todos requieren JWT.
"""
from fastapi import APIRouter, Depends, status, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from api.config.database import get_db
from api.middleware.auth import get_current_user
from api.models.interaction import InteractionCreate, InteractionResponse
from api.models.user import TokenData
from api.services.interaction_service import log_interaction, get_user_interactions

router = APIRouter(prefix="/api/interactions", tags=["Interactions"])


@router.post(
    "",
    response_model=InteractionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar interacción con un evento",
    description=(
        "Registra que el usuario autenticado realizó una acción sobre un evento "
        "(view, save, interested, uninterested). "
        "Actualiza el perfil de preferencias del usuario. **Requiere JWT**."
    ),
)
async def create_interaction(
    data: InteractionCreate,
    current_user: TokenData = Depends(get_current_user),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    return await log_interaction(
        user_id=current_user.user_id,
        data=data,
        db=db,
    )


@router.get(
    "/my",
    response_model=list[InteractionResponse],
    summary="Mis interacciones recientes",
    description="Retorna las últimas interacciones del usuario autenticado. **Requiere JWT**.",
)
async def my_interactions(
    limit: int = Query(50, ge=1, le=200),
    current_user: TokenData = Depends(get_current_user),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> list:
    return await get_user_interactions(
        user_id=current_user.user_id,
        db=db,
        limit=limit,
    )