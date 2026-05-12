"""
services/auth_service.py
Lógica de negocio para registro, login y generación de JWT.
"""
import bcrypt
from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from jose import jwt
from datetime import datetime, timedelta, timezone
from bson import ObjectId

from api.config.settings import get_settings
from api.models.user import (
    RegisterRequest, LoginRequest, TokenResponse,
    UserPublic, UserRole,
)

settings = get_settings()


def _hash_password(plain: str) -> str:
    return bcrypt.hashpw(
        plain.encode("utf-8"),
        bcrypt.gensalt(rounds=12)
    ).decode("utf-8")


def _verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(
        plain.encode("utf-8"),
        hashed.encode("utf-8")
    )


def _create_access_token(user_id: str, email: str, role: str) -> str:
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
    existing = await db.users.find_one({"email": data.email.lower()})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El correo electrónico ya está registrado.",
        )

    user_id = str(ObjectId())
    user_doc = {
        "_id": user_id,
        "name": data.name.strip(),
        "email": data.email.lower(),
        "password_hash": _hash_password(data.password),
        "role": UserRole.user.value,
        "created_at": datetime.now(timezone.utc),
    }
    await db.users.insert_one(user_doc)

    await db.user_preferences.insert_one({
        "_id": str(ObjectId()),
        "user_id": user_id,
        "user_preference_vector": [],
        "top_categories": [],
        "interaction_count": 0,
        "updated_at": datetime.now(timezone.utc),
    })

    token = _create_access_token(
        user_id=user_id,
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
    user = await db.users.find_one({"email": data.email.lower()})

    dummy_hash = bcrypt.hashpw(b"dummy", bcrypt.gensalt()).decode("utf-8")
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


async def update_user_preferences(
    user_id: str,
    data,
    db: AsyncIOMotorDatabase,
) -> dict:
    """Guarda las categorías preferidas del usuario en orden de prioridad."""
    await db.user_preferences.update_one(
        {"user_id": user_id},
        {"$set": {
            "preferred_categories": data.preferred_categories,
            "top_categories": data.preferred_categories,
            "updated_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )
    return {"preferred_categories": data.preferred_categories}
