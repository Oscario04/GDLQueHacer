"""
services/recommendation_service.py
Sistema de recomendaciones híbrido:
  • KNN content-based: encuentra eventos similares al perfil del usuario
  • SVM ranking:       puntúa y re-rankea los candidatos KNN
"""
import numpy as np
import logging
from motor.motor_asyncio import AsyncIOMotorDatabase
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import normalize
from datetime import datetime, timedelta
from typing import Any

from api.config.settings import get_settings
from api.models.event import EventStatus

logger = logging.getLogger(__name__)
settings = get_settings()

# Pesos para las interacciones al calcular vector de preferencia
INTERACTION_WEIGHTS = {"view": 1.0, "save": 3.0, "interested": 5.0, "uninterested": -2.0}


async def get_recommendations(
    user_id: str,
    db: AsyncIOMotorDatabase,
    limit: int = 20,
) -> list[dict[str, Any]]:
    # ── 1. Vector de preferencias del usuario ────────────────────────
    user_vector = await _build_user_vector(user_id, db)

    # ── 2. Cargar preferencias de categorías ─────────────────────────
    prefs_doc = await db.user_preferences.find_one({"user_id": user_id}) or {}
    preferred_categories = prefs_doc.get("preferred_categories", [])

    # Pesos por posición: 1ª = 1.0, 2ª = 0.6, 3ª = 0.3
    CATEGORY_WEIGHTS = {cat: round(1.0 - i * 0.35, 2)
                        for i, cat in enumerate(preferred_categories)}

    if user_vector is None or len(user_vector) == 0:
        cold = await _cold_start_recommendations(db, limit)
        return _apply_category_boost(cold, CATEGORY_WEIGHTS)

    # ── 3. Cargar eventos con vectores TF-IDF ────────────────────────
    events_with_vectors = await _load_events_with_vectors(db)
    if not events_with_vectors:
        cold = await _cold_start_recommendations(db, limit)
        return _apply_category_boost(cold, CATEGORY_WEIGHTS)

    event_matrix = np.array([e["tfidf_vector"] for e in events_with_vectors])
    user_vec = np.array(user_vector)

    if user_vec.shape[0] != event_matrix.shape[1]:
        user_vec, event_matrix = _align_dimensions(user_vec, event_matrix)

    event_matrix_norm = normalize(event_matrix, norm="l2")
    user_vec_norm = normalize(user_vec.reshape(1, -1), norm="l2")[0]

    # ── 4. KNN ────────────────────────────────────────────────────────
    n_neighbors = min(settings.KNN_N_NEIGHBORS * 3, len(events_with_vectors))
    knn = NearestNeighbors(n_neighbors=n_neighbors, metric="cosine", algorithm="brute")
    knn.fit(event_matrix_norm)
    distances, indices = knn.kneighbors(user_vec_norm.reshape(1, -1))
    distances, indices = distances[0], indices[0]
    knn_scores = 1.0 - distances

    # ── 5. SVM re-ranking ─────────────────────────────────────────────
    candidate_events = [events_with_vectors[i] for i in indices]
    candidate_scores = _svm_rerank(
        user_vec=user_vec_norm,
        candidate_vectors=event_matrix_norm[indices],
        knn_scores=knn_scores,
    )

    # ── 6. Boost por categorías preferidas ────────────────────────────
    boosted_scores = []
    for event, score in zip(candidate_events, candidate_scores):
        cat = event.get("category", "")
        boost = CATEGORY_WEIGHTS.get(cat, 0.0)
        # Boost máximo de +0.15 para la 1ª categoría
        final_score = min(1.0, score + boost * 0.15)
        boosted_scores.append(final_score)

    # ── 7. Filtrar vistos y construir respuesta ───────────────────────
    seen_ids = await _get_seen_event_ids(user_id, db)
    results = []

    for event, score in sorted(
        zip(candidate_events, boosted_scores),
        key=lambda x: x[1], reverse=True
    ):
        if event["_id"] in seen_ids:
            continue
        event["recommendation_score"] = round(float(score), 4)
        event["recommendation_reason"] = _get_reason_with_category(
            score, event.get("category", ""), CATEGORY_WEIGHTS
        )
        event["_id"] = str(event["_id"])
        results.append(event)
        if len(results) >= limit:
            break

    if len(results) < limit // 2:
        cold = await _cold_start_recommendations(db, limit - len(results))
        cold = _apply_category_boost(cold, CATEGORY_WEIGHTS)
        existing_ids = {r["_id"] for r in results}
        for e in cold:
            if e["_id"] not in existing_ids:
                results.append(e)

    return results[:limit]


def _apply_category_boost(
    events: list[dict], category_weights: dict[str, float]
) -> list[dict]:
    """Aplica boost de categoría a una lista de eventos (usado en cold start)."""
    for event in events:
        cat = event.get("category", "")
        boost = category_weights.get(cat, 0.0)
        base = event.get("recommendation_score", 0.5)
        event["recommendation_score"] = round(min(1.0, base + boost * 0.15), 4)
        event["recommendation_reason"] = _get_reason_with_category(
            event["recommendation_score"], cat, category_weights
        )
    return sorted(events, key=lambda x: x["recommendation_score"], reverse=True)


def _get_reason_with_category(
    score: float, category: str, category_weights: dict[str, float]
) -> str:
    CATEGORY_LABELS = {
        "cultural": "cultural", "deportivo": "deportivo",
        "gastronomico": "gastronómico", "entretenimiento": "entretenimiento",
        "otro": "variado",
    }
    if category in category_weights:
        label = CATEGORY_LABELS.get(category, category)
        rank = list(category_weights.keys()).index(category) + 1
        if rank == 1:
            return f"Tu categoría favorita: {label}"
        elif rank == 2:
            return f"Una de tus categorías preferidas: {label}"
        else:
            return f"Dentro de tus intereses: {label}"
    return _get_reason(score)

async def _build_user_vector(user_id: str, db: AsyncIOMotorDatabase) -> list[float] | None:
    """
    Construye el vector de preferencias del usuario a partir de sus interacciones.
    Vector = promedio ponderado de los vectores TF-IDF de los eventos con los que interactuó.
    """
    # Obtener últimas 100 interacciones
    interactions = await db.user_interactions.find(
        {"user_id": user_id}
    ).sort("created_at", -1).limit(100).to_list(100)

    if not interactions:
        return None

    # Obtener vectores de esos eventos
    event_ids = list({i["event_id"] for i in interactions})
    events = await db.events.find(
        {"_id": {"$in": event_ids}, "tfidf_vector": {"$ne": []}},
        {"_id": 1, "tfidf_vector": 1},
    ).to_list(len(event_ids))

    event_vectors = {e["_id"]: e["tfidf_vector"] for e in events}

    # Promediar ponderado por tipo de interacción
    weighted_sum = None
    total_weight = 0.0

    for interaction in interactions:
        event_id = interaction["event_id"]
        itype = interaction.get("type", "view")
        weight = INTERACTION_WEIGHTS.get(itype, 1.0)

        if event_id in event_vectors:
            vec = np.array(event_vectors[event_id])
            if weighted_sum is None:
                weighted_sum = weight * vec
            else:
                if vec.shape[0] == weighted_sum.shape[0]:
                    weighted_sum += weight * vec
            total_weight += abs(weight)

    if weighted_sum is None or total_weight == 0:
        return None

    preference_vector = (weighted_sum / total_weight).tolist()

    # Guardar vector actualizado en BD
    await db.user_preferences.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_preference_vector": preference_vector,
            "updated_at": datetime.utcnow(),
        }},
        upsert=True,
    )
    return preference_vector


async def _load_events_with_vectors(db: AsyncIOMotorDatabase) -> list[dict]:
    """Carga eventos publicados que tienen vector TF-IDF calculado."""
    # Solo eventos de los próximos 60 días (relevancia temporal)
    date_limit = datetime.utcnow() - timedelta(days=1)
    cursor = db.events.find(
        {
            "status": EventStatus.publicado.value,
            "tfidf_vector": {"$ne": [], "$exists": True},
            "date_start": {"$gte": date_limit},
        },
        {
            "_id": 1, "title": 1, "category": 1, "tfidf_vector": 1,
            "date_start": 1, "quality_ml": 1, "description": 1,
            "location": 1, "coordinates": 1, "image_url": 1,
            "url_source": 1, "price": 1, "tags": 1,
            "status": 1, "created_at": 1,
        },
    ).limit(1000)
    return await cursor.to_list(1000)


async def _get_seen_event_ids(user_id: str, db: AsyncIOMotorDatabase) -> set[str]:
    """Retorna IDs de eventos que el usuario ya vio/guardó recientemente."""
    cutoff = datetime.utcnow() - timedelta(days=30)
    interactions = await db.user_interactions.find(
        {
            "user_id": user_id,
            "type": {"$in": ["view", "save", "interested"]},
            "created_at": {"$gte": cutoff},
        },
        {"event_id": 1},
    ).to_list(500)
    return {i["event_id"] for i in interactions}


def _svm_rerank(
    user_vec: np.ndarray,
    candidate_vectors: np.ndarray,
    knn_scores: np.ndarray,
) -> np.ndarray:
    """
    Re-ranking SVM simple: calcula similitud coseno directa con el user_vector
    y la combina con el score KNN para producir un ranking final.

    En producción, aquí se usaría el SVM entrenado offline (joblib).
    Por ahora usamos SVM analítico para no depender del modelo pre-entrenado.
    """
    # Similitud coseno directa (SVM-proxy)
    dot_products = candidate_vectors @ user_vec
    norms = np.linalg.norm(candidate_vectors, axis=1) * np.linalg.norm(user_vec)
    norms = np.where(norms == 0, 1e-9, norms)
    cosine_scores = dot_products / norms
    cosine_scores = np.clip(cosine_scores, 0, 1)

    # Score híbrido: 60% KNN + 40% coseno directo
    hybrid = 0.6 * knn_scores + 0.4 * cosine_scores
    return hybrid


def _align_dimensions(user_vec: np.ndarray, event_matrix: np.ndarray):
    """Alinea dimensiones si hay discrepancia (puede ocurrir tras reentrenamiento)."""
    u_dim = user_vec.shape[0]
    e_dim = event_matrix.shape[1]

    if u_dim < e_dim:
        user_vec = np.pad(user_vec, (0, e_dim - u_dim))
    elif u_dim > e_dim:
        user_vec = user_vec[:e_dim]

    return user_vec, event_matrix


async def _cold_start_recommendations(
    db: AsyncIOMotorDatabase,
    limit: int,
) -> list[dict]:
    """
    Recomendaciones para usuarios sin historial: eventos recientes de alta calidad.
    """
    date_limit = datetime.utcnow() - timedelta(days=1)
    cursor = db.events.find(
        {
            "status": EventStatus.publicado.value,
            "date_start": {"$gte": date_limit},
            "quality_ml": {"$gte": 0.7},
        },
        {
            "_id": 1, "title": 1, "description": 1, "category": 1,
            "date_start": 1, "date_end": 1, "location": 1, "coordinates": 1,
            "quality_ml": 1, "status": 1, "image_url": 1, "url_source": 1,
            "price": 1, "tags": 1, "created_at": 1,
        },
    ).sort("quality_ml", -1).limit(limit)

    events = await cursor.to_list(limit)
    for e in events:
        e["_id"] = str(e["_id"])
        e["recommendation_score"] = round(float(e.get("quality_ml", 0.5)), 4)
        e["recommendation_reason"] = "Evento destacado en Guadalajara"
    return events


def _get_reason(score: float) -> str:
    if score >= 0.85:
        return "Muy relevante según tus preferencias"
    elif score >= 0.70:
        return "Relacionado con tus categorías favoritas"
    elif score >= 0.55:
        return "Podría interesarte"
    return "Evento popular en tu zona"