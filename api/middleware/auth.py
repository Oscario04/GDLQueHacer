"""
middleware/auth.py
Middleware y dependencias FastAPI para autenticación JWT.
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase

from api.config.settings import get_settings
from api.config.database import get_db
from api.models.user import TokenData, UserRole

settings = get_settings()
bearer_scheme = HTTPBearer(auto_error=False)


def _decode_token(token: str) -> TokenData:
    """Decodifica y valida un JWT. Lanza HTTPException si es inválido."""
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
        user_id: str = payload.get("sub")
        email: str = payload.get("email")
        role: str = payload.get("role", "user")

        if user_id is None or email is None:
            raise ValueError("Payload incompleto")

        return TokenData(user_id=user_id, email=email, role=UserRole(role))

    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> TokenData:
    """
    Dependencia FastAPI: extrae y valida el JWT del header Authorization.
    Uso:  current_user: TokenData = Depends(get_current_user)
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticación requerido.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token_data = _decode_token(credentials.credentials)

    # Verificar que el usuario sigue activo en la BD
    user = await db.users.find_one(
        {"_id": token_data.user_id},
        {"_id": 1, "role": 1},
    )
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario no encontrado.",
        )
    return token_data


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> TokenData | None:
    """
    Dependencia opcional: retorna TokenData si hay JWT válido, o None si no.
    Uso para endpoints accesibles tanto anónimos como autenticados.
    """
    if credentials is None:
        return None
    try:
        return _decode_token(credentials.credentials)
    except HTTPException:
        return None


async def require_admin(
    current_user: TokenData = Depends(get_current_user),
) -> TokenData:
    """Dependencia que exige rol 'admin'. Lanza 403 si es usuario normal."""
    if current_user.role != UserRole.admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requieren permisos de administrador.",
        )
    return current_user