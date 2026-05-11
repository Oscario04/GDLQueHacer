"""
routes/auth.py
Endpoints de autenticación: registro, login y perfil del usuario.
"""
from fastapi import APIRouter, Depends, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from api.config.database import get_db
from api.middleware.auth import get_current_user
from api.models.user import (
    RegisterRequest, LoginRequest, TokenResponse,
    UserPublic, UserProfile, TokenData,
)
from api.services import auth_service

router = APIRouter(prefix="/api/auth", tags=["Auth"])


@router.post(
    "/register",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar nuevo usuario",
    description=(
        "Crea una cuenta nueva con nombre, correo y contraseña. "
        "Retorna un JWT listo para usar (expiración 24 h). "
        "La contraseña se almacena como hash bcrypt (factor 12)."
    ),
)
async def register(
    data: RegisterRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> TokenResponse:
    return await auth_service.register_user(data, db)


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Iniciar sesión",
    description="Autentica con email/contraseña y retorna un JWT con expiración de 24 h.",
)
async def login(
    data: LoginRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> TokenResponse:
    return await auth_service.login_user(data, db)


@router.get(
    "/me",
    response_model=UserProfile,
    summary="Perfil del usuario autenticado",
    description="Retorna el perfil completo incluyendo categorías top y conteo de interacciones.",
)
async def get_me(
    current_user: TokenData = Depends(get_current_user),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    return await auth_service.get_user_profile(current_user.user_id, db)