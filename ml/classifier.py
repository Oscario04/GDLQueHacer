"""
classifier.py
Calcula el score de calidad (quality_ml) para cada evento recolectado.
Si quality_ml >= 0.5  → el evento pasa directo a "publicado".
Si quality_ml <  0.5  → queda en "pendiente_revision" para revisión manual.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pymongo
from pymongo import MongoClient

from .utils import get_mongo_client


# ---------------------------------------------------------------------------
# Extracción de features
# ---------------------------------------------------------------------------

def _has_image(event: dict) -> float:
    return 1.0 if event.get("image_url") else 0.0


def _description_length_score(event: dict) -> float:
    desc = event.get("description", "") or ""
    length = len(desc.strip())
    if length >= 200:
        return 1.0
    if length >= 80:
        return 0.6
    if length > 0:
        return 0.3
    return 0.0


def _has_location(event: dict) -> float:
    loc = event.get("location", {}) or {}
    return 1.0 if (loc.get("lat") and loc.get("lon")) else 0.0


def _has_price(event: dict) -> float:
    return 1.0 if event.get("price") is not None else 0.0


def _has_category(event: dict) -> float:
    return 1.0 if event.get("category") else 0.0


def _date_is_future(event: dict) -> float:
    raw = event.get("start_date")
    if not raw:
        return 0.0
    try:
        if isinstance(raw, datetime):
            dt = raw
        else:
            dt = datetime.fromisoformat(str(raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return 1.0 if dt > datetime.now(timezone.utc) else 0.0
    except (ValueError, TypeError):
        return 0.0


def _title_quality(event: dict) -> float:
    title = event.get("title", "") or ""
    title = title.strip()
    if len(title) < 5:
        return 0.0
    # Penaliza títulos en mayúsculas o con muchos símbolos
    upper_ratio = sum(1 for c in title if c.isupper()) / max(len(title), 1)
    special = len(re.findall(r"[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑ\s,.\-!?]", title))
    if upper_ratio > 0.7 or special > 3:
        return 0.4
    return 1.0


# ---------------------------------------------------------------------------
# Score final (promedio ponderado)
# ---------------------------------------------------------------------------

WEIGHTS: list[tuple[Any, float]] = [
    (_has_image,              0.20),
    (_description_length_score, 0.25),
    (_has_location,           0.20),
    (_has_price,              0.10),
    (_has_category,           0.10),
    (_date_is_future,         0.10),
    (_title_quality,          0.05),
]


def compute_quality_score(event: dict) -> float:
    """Devuelve un float en [0, 1] representando la calidad del evento."""
    score = sum(fn(event) * weight for fn, weight in WEIGHTS)
    return round(score, 4)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def classify_event(event: dict) -> dict:
    """
    Recibe un dict de evento y devuelve el mismo dict enriquecido con:
      - quality_ml: float
      - status: 'publicado' | 'pendiente_revision'
    """
    score = compute_quality_score(event)
    event["quality_ml"] = score
    event["status"] = "publicado" if score >= 0.5 else "pendiente_revision"
    return event


def run_pipeline() -> None:
    """
    Corre el clasificador sobre todos los eventos en estado 'normalizado'
    y actualiza su status en MongoDB.
    """
    client = get_mongo_client()
    db = client["gdlquehacer"]
    events_col = db["events"]

    query = {"status": "normalizado"}
    total = events_col.count_documents(query)
    print(f"[classifier] Eventos a clasificar: {total}")

    updated = 0
    for event in events_col.find(query):
        enriched = classify_event(event)
        events_col.update_one(
            {"_id": event["_id"]},
            {
                "$set": {
                    "quality_ml": enriched["quality_ml"],
                    "status": enriched["status"],
                    "classified_at": datetime.now(timezone.utc),
                }
            },
        )
        updated += 1

    print(f"[classifier] Clasificados: {updated} eventos")
    client.close()


if __name__ == "__main__":
    run_pipeline()