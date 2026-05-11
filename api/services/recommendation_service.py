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
    """
    Pipeline de recomendación completo:
    1. Obtiene el vector de preferencias del usuario
    2. Carga los vectores TF-IDF de eventos disponibles
    3. KNN: encuentra los K más cercanos al perfil del usuario
    4. SVM scoring: re-rankea los candidatos
    5. Filtra eventos ya vistos y devuelve los top-N
    """
    # ── 1. Vector de preferencias del usuario ────────────────────────
    user_vector = await _build_user_vector(user_id, db)

    if user_vector is None or len(user_vector) == 0:
        # Usuario sin historial → eventos recientes de alta calidad
        return await _cold_start_recommendations(db, limit)

    # ── 2. Cargar eventos con vectores TF-IDF ────────────────────────
    events_with_vectors = await _load_events_with_vectors(db)

    if not events_with_vectors:
        return await _cold_start_recommendations(db, limit)

    event_ids = [e["_id"] for e in events_with_vectors]
    event_matrix = np.array([e["tfidf_vector"] for e in events_with_vectors])

    # Alinear dimensiones si el vector del usuario difiere
    user_vec = np.array(user_vector)
    if user_vec.shape[0] != event_matrix.shape[1]:
        user_vec, event_matrix = _align_dimensions(user_vec, event_matrix)

    # Normalizar para similitud coseno
    event_matrix_norm = normalize(event_matrix, norm="l2")
    user_vec_norm = normalize(user_vec.reshape(1, -1), norm="l2")[0]

    # ── 3. KNN: K vecinos más cercanos al perfil ──────────────────────
    n_neighbors = min(settings.KNN_N_NEIGHBORS * 3, len(event_ids))
    knn = NearestNeighbors(n_neighbors=n_neighbors, metric="cosine", algorithm="brute")
    knn.fit(event_matrix_norm)

    distances, indices = knn.kneighbors(user_vec_norm.reshape(1, -1))
    distances = distances[0]
    indices = indices[0]

    # Similitud coseno = 1 - distancia
    knn_scores = 1.0 - distances

    # ── 4. SVM re-ranking sobre candidatos KNN ────────────────────────
    candidate_events = [events_with_vectors[i] for i in indices]
    candidate_scores = _svm_rerank(
        user_vec=user_vec_norm,
        candidate_vectors=event_matrix_norm[indices],
        knn_scores=knn_scores,
    )

    # ── 5. Filtrar vistos y construir respuesta ───────────────────────
    seen_ids = await _get_seen_event_ids(user_id, db)
    results = []

    sorted_pairs = sorted(
        zip(candidate_events, candidate_scores),
        key=lambda x: x[1],
        reverse=True,
    )

    for event, score in sorted_pairs:
        if event["_id"] in seen_ids:
            continue
        event["recommendation_score"] = round(float(score), 4)
        event["recommendation_reason"] = _get_reason(score)
        event["_id"] = str(event["_id"])
        results.append(event)

        if len(results) >= limit:
            break

    # Si no hay suficientes, completar con cold start
    if len(results) < limit // 2:
        cold = await _cold_start_recommendations(db, limit - len(results))
        existing_ids = {r["_id"] for r in results}
        for e in cold:
            if e["_id"] not in existing_ids:
                results.append(e)

    return results[:limit]


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