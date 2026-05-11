"""
services/auth_service.py
Lógica de negocio para registro, login y generación de JWT.
"""
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from passlib.context import CryptContext
from jose import jwt
from datetime import datetime, timedelta, timezone
from bson import ObjectId

from api.config.settings import get_settings
from api.models.user import (
    RegisterRequest, LoginRequest, TokenResponse,
    UserPublic, UserRole,
)

settings = get_settings()

# Contexto bcrypt — factor de costo 12 (>= 10 según SRS)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__rounds=12)


def _hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def _verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def _create_access_token(user_id: str, email: str, role: str) -> str:
    """Genera un JWT firmado con HS256 y expiración de 24 h."""
    expire = datetime.now(timezone.utc) + timedelta(hours=settings.JWT_EXPIRE_HOURS)
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


async def register_user(
    data: RegisterRequest,
    db: AsyncIOMotorDatabase,
) -> TokenResponse:
    """
    Crea una cuenta nueva. Lanza 409 si el correo ya existe.
    Retorna JWT listo para usar.
    """
    existing = await db.users.find_one({"email": data.email.lower()})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El correo electrónico ya está registrado.",
        )

    user_doc = {
        "_id": str(ObjectId()),
        "name": data.name.strip(),
        "email": data.email.lower(),
        "password_hash": _hash_password(data.password),
        "role": UserRole.user.value,
        "created_at": datetime.utcnow(),
    }
    await db.users.insert_one(user_doc)

    # Crear documento de preferencias vacío
    await db.user_preferences.insert_one({
        "_id": str(ObjectId()),
        "user_id": user_doc["_id"],
        "user_preference_vector": [],
        "top_categories": [],
        "interaction_count": 0,
        "updated_at": datetime.utcnow(),
    })

    token = _create_access_token(
        user_id=user_doc["_id"],
        email=user_doc["email"],
        role=user_doc["role"],
    )
    return TokenResponse(
        access_token=token,
        expires_in=settings.JWT_EXPIRE_HOURS * 3600,
    )


async def login_user(
    data: LoginRequest,
    db: AsyncIOMotorDatabase,
) -> TokenResponse:
    """
    Autentica con email/contraseña. Retorna JWT o lanza 401.
    El mensaje de error es genérico para no revelar si el email existe.
    """
    user = await db.users.find_one({"email": data.email.lower()})

    # Siempre verificar hash (timing-safe) aunque no exista el usuario
    dummy_hash = "$2b$12$abcdefghijklmnopqrstuuabcdefghijklmnopqrstuuabcdefghijkl"
    stored_hash = user["password_hash"] if user else dummy_hash

    if not _verify_password(data.password, stored_hash) or user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas.",
        )

    token = _create_access_token(
        user_id=user["_id"],
        email=user["email"],
        role=user.get("role", "user"),
    )
    return TokenResponse(
        access_token=token,
        expires_in=settings.JWT_EXPIRE_HOURS * 3600,
    )


async def get_user_profile(user_id: str, db: AsyncIOMotorDatabase) -> dict:
    """Retorna el perfil completo del usuario autenticado."""
    user = await db.users.find_one({"_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado.")

    prefs = await db.user_preferences.find_one({"user_id": user_id}) or {}
    interaction_count = await db.user_interactions.count_documents({"user_id": user_id})

    return {
        "_id": user["_id"],
        "name": user["name"],
        "email": user["email"],
        "role": user.get("role", "user"),
        "created_at": user["created_at"],
        "top_categories": prefs.get("top_categories", []),
        "total_interactions": interaction_count,
        "preferences_updated_at": prefs.get("updated_at"),
    }