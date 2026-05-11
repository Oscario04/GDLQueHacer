"""
models/interaction.py
Modelos para interacciones de usuario con eventos.
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from enum import Enum


class InteractionType(str, Enum):
    view = "view"           # Usuario vio el detalle del evento
    save = "save"           # Usuario guardó el evento
    interested = "interested"  # Usuario marcó "me interesa"
    uninterested = "uninterested"  # Feedback negativo


class InteractionCreate(BaseModel):
    event_id: str
    type: InteractionType


class InteractionDB(BaseModel):
    """Documento guardado en user_interactions."""
    user_id: str
    event_id: str
    type: InteractionType
    created_at: datetime = Field(default_factory=datetime.utcnow)


class InteractionResponse(BaseModel):
    id: str = Field(alias="_id")
    user_id: str
    event_id: str
    type: InteractionType
    created_at: datetime

    model_config = {"populate_by_name": True}


# ── Manual review ─────────────────────────────────────────────────────

class ReviewStatus(str, Enum):
    pendiente = "pendiente"
    aprobado = "aprobado"
    rechazado = "rechazado"


class ReviewAction(BaseModel):
    action: ReviewStatus  # aprobado | rechazado
    notes: Optional[str] = Field(None, max_length=1000)


class ManualReviewDB(BaseModel):
    """Documento en la cola reviews_manual."""
    event_id: str
    quality_ml: float
    status: ReviewStatus = ReviewStatus.pendiente
    reviewer_id: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    reviewed_at: Optional[datetime] = None