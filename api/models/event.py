"""
models/event.py
Modelos Pydantic para eventos — request/response validation y serialización.
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Any
from datetime import datetime
from enum import Enum


class EventCategory(str, Enum):
    cultural = "cultural"
    deportivo = "deportivo"
    gastronomico = "gastronomico"
    entretenimiento = "entretenimiento"
    otro = "otro"


class EventStatus(str, Enum):
    recolectado = "recolectado"
    normalizado = "normalizado"
    pendiente_revision = "pendiente_revision"
    publicado = "publicado"
    rechazado = "rechazado"


class GeoCoordinates(BaseModel):
    """GeoJSON Point para coordenadas geográficas."""
    type: str = "Point"
    coordinates: list[float] = Field(
        ...,
        description="[longitud, latitud] — formato GeoJSON estándar",
        min_length=2,
        max_length=2,
    )

    @field_validator("coordinates")
    @classmethod
    def validate_coordinates(cls, v: list[float]) -> list[float]:
        lon, lat = v
        if not (-180 <= lon <= 180):
            raise ValueError(f"Longitud inválida: {lon}")
        if not (-90 <= lat <= 90):
            raise ValueError(f"Latitud inválida: {lat}")
        return v


# ── Schemas de respuesta ──────────────────────────────────────────────

class EventBase(BaseModel):
    title: str = Field(..., min_length=3, max_length=300)
    description: Optional[str] = Field(None, max_length=5000)
    category: EventCategory
    date_start: datetime
    date_end: Optional[datetime] = None
    location: Optional[str] = Field(None, max_length=500)
    coordinates: Optional[GeoCoordinates] = None
    image_url: Optional[str] = None
    url_source: Optional[str] = None
    price: Optional[float] = Field(None, ge=0)
    tags: list[str] = Field(default_factory=list)


class EventPublic(EventBase):
    """Schema para respuestas públicas — sin campos internos de ML."""
    id: str = Field(alias="_id")
    quality_ml: float = Field(..., ge=0, le=1)
    status: EventStatus
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = {"populate_by_name": True}


class EventDetail(EventPublic):
    """Schema extendido con todos los campos, incluyendo fuente."""
    source_id: Optional[str] = None


class EventCreate(EventBase):
    """Schema para crear un evento manualmente (admin)."""
    source_id: Optional[str] = None
    metadata_raw: Optional[dict[str, Any]] = None


class EventListResponse(BaseModel):
    """Respuesta paginada de lista de eventos."""
    total: int
    page: int
    limit: int
    has_next: bool
    items: list[EventPublic]


class EventRecommendation(EventPublic):
    """Evento con score de recomendación adjunto."""
    recommendation_score: float = Field(..., ge=0, le=1)
    recommendation_reason: Optional[str] = None


class EventFilter(BaseModel):
    """Parámetros de filtrado para GET /api/events."""
    category: Optional[EventCategory] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    lat: Optional[float] = Field(None, ge=-90, le=90)
    lon: Optional[float] = Field(None, ge=-180, le=180)
    radius_km: Optional[float] = Field(None, gt=0, le=200)
    q: Optional[str] = Field(None, max_length=200)  # Búsqueda textual
    page: int = Field(1, ge=1)
    limit: int = Field(20, ge=1, le=100)