"""
models/user.py
Modelos Pydantic para usuarios, autenticación y preferencias.
"""
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import Optional
from datetime import datetime
from enum import Enum
import re


class UserRole(str, Enum):
    user = "user"
    admin = "admin"


# ── Auth ─────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)

    @field_validator("password")
    @classmethod
    def validate_password_strength(cls, v: str) -> str:
        if not re.search(r"[A-Za-z]", v):
            raise ValueError("La contraseña debe contener al menos una letra.")
        if not re.search(r"\d", v):
            raise ValueError("La contraseña debe contener al menos un número.")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("El nombre no puede estar vacío.")
        return v


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # segundos


class TokenData(BaseModel):
    """Payload decodificado del JWT."""
    user_id: str
    email: str
    role: UserRole
    exp: Optional[int] = None


# ── User public schemas ───────────────────────────────────────────────

class UserPublic(BaseModel):
    """Schema de respuesta pública — sin contraseña ni campos sensibles."""
    id: str = Field(alias="_id")
    name: str
    email: EmailStr
    role: UserRole
    created_at: datetime

    model_config = {"populate_by_name": True}


class UserProfile(UserPublic):
    """Perfil ampliado del usuario autenticado."""
    top_categories: list[str] = Field(default_factory=list)
    total_interactions: int = 0
    preferences_updated_at: Optional[datetime] = None


# ── User preferences (interno ML) ────────────────────────────────────

class UserPreferencesDB(BaseModel):
    """Documento guardado en la colección user_preferences."""
    user_id: str
    user_preference_vector: list[float] = Field(default_factory=list)
    top_categories: list[str] = Field(default_factory=list)
    interaction_count: int = 0
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class PreferencesRequest(BaseModel):
    """Categorías preferidas en orden de prioridad (máximo 3)."""
    preferred_categories: list[str] = Field(
        ..., min_length=1, max_length=3,
        description="Lista ordenada de categorías: índice 0 = mayor prioridad"
    )

    @field_validator("preferred_categories")
    @classmethod
    def validate_categories(cls, v: list[str]) -> list[str]:
        valid = {"cultural", "deportivo", "gastronomico", "entretenimiento", "otro"}
        for cat in v:
            if cat not in valid:
                raise ValueError(f"Categoría inválida: {cat}")
        if len(v) != len(set(v)):
            raise ValueError("No puedes repetir categorías.")
        return v