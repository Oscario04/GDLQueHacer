"""
recommender.py
Sistema de recomendaciones basado en contenido.
Dado un user_id (o un perfil de preferencias), devuelve eventos
ordenados por relevancia usando TF-IDF + similitud coseno.
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Optional

from .utils import get_mongo_client


# ---------------------------------------------------------------------------
# Helpers de texto
# ---------------------------------------------------------------------------

_STOP_WORDS = {
    "de", "la", "el", "en", "y", "a", "los", "las", "un", "una",
    "con", "del", "al", "es", "se", "por", "para", "que", "su",
    "lo", "más", "como", "this", "the", "and", "for", "are",
}


def _tokenize(text: str) -> list[str]:
    text = (text or "").lower()
    tokens = re.findall(r"[a-záéíóúñ]+", text)
    return [t for t in tokens if t not in _STOP_WORDS and len(t) > 2]


def _event_to_text(event: dict) -> str:
    parts = [
        event.get("title", ""),
        event.get("description", ""),
        event.get("category", ""),
        " ".join(event.get("tags", [])),
    ]
    return " ".join(filter(None, parts))


# ---------------------------------------------------------------------------
# TF-IDF en memoria
# ---------------------------------------------------------------------------

def _compute_tfidf(documents: list[list[str]]) -> list[dict[str, float]]:
    N = len(documents)
    if N == 0:
        return []

    # IDF
    df: Counter = Counter()
    for doc in documents:
        for term in set(doc):
            df[term] += 1
    idf = {term: math.log((N + 1) / (count + 1)) + 1 for term, count in df.items()}

    # TF-IDF vectors
    vectors = []
    for doc in documents:
        tf: Counter = Counter(doc)
        total = max(len(doc), 1)
        vec = {term: (count / total) * idf.get(term, 0) for term, count in tf.items()}
        vectors.append(vec)

    return vectors


def _cosine_similarity(vec_a: dict, vec_b: dict) -> float:
    shared = set(vec_a) & set(vec_b)
    if not shared:
        return 0.0
    dot = sum(vec_a[t] * vec_b[t] for t in shared)
    norm_a = math.sqrt(sum(v ** 2 for v in vec_a.values()))
    norm_b = math.sqrt(sum(v ** 2 for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def get_recommendations(
    user_id: str,
    limit: int = 10,
    category_filter: Optional[str] = None,
) -> list[dict]:
    """
    Devuelve una lista de eventos recomendados para el usuario.

    Estrategia:
      1. Recupera los eventos que el usuario ha visto / guardado como perfil.
      2. Construye un vector de preferencias del usuario.
      3. Calcula similitud coseno contra todos los eventos publicados.
      4. Devuelve los `limit` eventos más similares que el usuario no ha visto.

    Si el usuario no tiene historial, devuelve los eventos más recientes
    con mejor quality_ml (cold-start).
    """
    client = get_mongo_client()
    db = client["gdlquehacer"]
    events_col = db["events"]
    users_col = db["users"]

    now = datetime.now(timezone.utc)

    # ── Filtro base: solo eventos publicados y futuros ──
    base_filter: dict = {
        "status": "publicado",
        "start_date": {"$gt": now},
    }
    if category_filter:
        base_filter["category"] = category_filter

    published = list(events_col.find(base_filter))
    if not published:
        client.close()
        return []

    # ── Recuperar historial del usuario ──
    user = users_col.find_one({"_id": user_id}) or {}
    seen_ids = set(str(eid) for eid in user.get("seen_events", []))
    liked_ids = set(str(eid) for eid in user.get("liked_events", []))

    # ── Cold-start: sin historial → mejores por quality_ml ──
    if not liked_ids:
        cold = sorted(
            [e for e in published if str(e["_id"]) not in seen_ids],
            key=lambda e: e.get("quality_ml", 0),
            reverse=True,
        )
        client.close()
        return _serialize(cold[:limit])

    # ── Construir perfil del usuario (promedio de vectores de eventos likeados) ──
    liked_events = [e for e in published if str(e["_id"]) in liked_ids]
    candidate_events = [e for e in published if str(e["_id"]) not in seen_ids]

    all_events = liked_events + candidate_events
    docs = [_tokenize(_event_to_text(e)) for e in all_events]
    vectors = _compute_tfidf(docs)

    liked_vecs = vectors[: len(liked_events)]
    candidate_vecs = vectors[len(liked_events):]

    # Perfil = promedio de los vectores likeados
    profile: dict[str, float] = defaultdict(float)
    for vec in liked_vecs:
        for term, val in vec.items():
            profile[term] += val
    if liked_vecs:
        for term in profile:
            profile[term] /= len(liked_vecs)

    # ── Calcular similitudes ──
    scored = []
    for event, vec in zip(candidate_events, candidate_vecs):
        sim = _cosine_similarity(dict(profile), vec)
        # Bonus ligero por quality_ml
        final_score = sim * 0.8 + event.get("quality_ml", 0) * 0.2
        scored.append((final_score, event))

    scored.sort(key=lambda x: x[0], reverse=True)
    client.close()
    return _serialize([e for _, e in scored[:limit]])


def _serialize(events: list[dict]) -> list[dict]:
    """Convierte ObjectId y datetime a tipos serializables."""
    result = []
    for e in events:
        e = dict(e)
        e["_id"] = str(e["_id"])
        if isinstance(e.get("start_date"), datetime):
            e["start_date"] = e["start_date"].isoformat()
        if isinstance(e.get("end_date"), datetime):
            e["end_date"] = e["end_date"].isoformat()
        result.append(e)
    return result