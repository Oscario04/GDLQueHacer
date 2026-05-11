"""
services/ml_service.py
Servicio ML para clasificación de categoría, score de calidad y normalización.
Carga los modelos entrenados desde disco (joblib) y los mantiene en memoria.
"""
import joblib
import numpy as np
import logging
import os
import re
from pathlib import Path
from typing import Any

from api.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Directorio de modelos guardados
MODELS_DIR = Path(settings.ML_MODELS_PATH)

# ── Singleton de modelos (cargados una vez al iniciar la app) ─────────
_models: dict[str, Any] = {}


def load_ml_models() -> None:
    """
    Carga los modelos entrenados desde disco.
    Llamar durante el startup de FastAPI.
    """
    global _models
    required = ["tfidf_vectorizer", "category_classifier", "svm_quality_scorer"]

    for name in required:
        path = MODELS_DIR / f"{name}.joblib"
        if path.exists():
            _models[name] = joblib.load(path)
            logger.info("🤖  Modelo cargado: %s", name)
        else:
            logger.warning("⚠️   Modelo no encontrado: %s — usando modo degradado", path)

    if not _models:
        logger.warning("⚠️   Sin modelos ML. Ejecuta ml/training/train_models.py primero.")


def _preprocess_text(text: str) -> str:
    """Limpieza básica de texto para vectorización."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\sáéíóúüñ]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def classify_event(title: str, description: str = "") -> dict[str, Any]:
    """
    Clasifica la categoría de un evento y calcula su quality_ml score.

    Returns:
        {
            "category": str,
            "category_confidence": float,
            "quality_ml": float,
            "tfidf_vector": list[float],
        }
    """
    text = _preprocess_text(f"{title} {description}")

    # ── Fallback si no hay modelos ────────────────────────────────────
    if "tfidf_vectorizer" not in _models:
        return {
            "category": "otro",
            "category_confidence": 0.0,
            "quality_ml": _heuristic_quality(title, description),
            "tfidf_vector": [],
        }

    # ── Vectorización TF-IDF ──────────────────────────────────────────
    vectorizer = _models["tfidf_vectorizer"]
    tfidf_matrix = vectorizer.transform([text])
    tfidf_dense = tfidf_matrix.toarray()[0]

    # ── Clasificación de categoría (LogReg o SVM) ─────────────────────
    classifier = _models["category_classifier"]
    category_pred = classifier.predict(tfidf_matrix)[0]

    if hasattr(classifier, "predict_proba"):
        probas = classifier.predict_proba(tfidf_matrix)[0]
        confidence = float(np.max(probas))
    else:
        # SVM con decision_function
        scores = classifier.decision_function(tfidf_matrix)[0]
        if scores.ndim == 0:
            confidence = float(abs(scores) / (abs(scores) + 1))
        else:
            confidence = float(np.max(scores) / (np.sum(np.abs(scores)) + 1e-9))

    # ── Score de calidad SVM ──────────────────────────────────────────
    quality_score = _compute_quality_score(
        title=title,
        description=description,
        tfidf_matrix=tfidf_matrix,
        confidence=confidence,
    )

    return {
        "category": category_pred,
        "category_confidence": round(confidence, 4),
        "quality_ml": round(quality_score, 4),
        "tfidf_vector": tfidf_dense.tolist(),
    }


def _compute_quality_score(
    title: str,
    description: str,
    tfidf_matrix,
    confidence: float,
) -> float:
    """
    Calcula quality_ml combinando:
    1. SVM de calidad (si está disponible)
    2. Heurística de completitud de campos
    3. Confianza del clasificador de categoría

    Score final = 0.5 * svm_score + 0.3 * completitud + 0.2 * confidence
    """
    # 1. SVM scorer
    svm_score = 0.5  # Valor neutro por defecto
    if "svm_quality_scorer" in _models:
        try:
            scorer = _models["svm_quality_scorer"]
            raw = scorer.decision_function(tfidf_matrix)[0]
            # Normalizar a [0, 1] via sigmoid
            svm_score = float(1 / (1 + np.exp(-raw)))
        except Exception:
            pass

    # 2. Heurística de completitud
    completitud = _heuristic_quality(title, description)

    # 3. Ponderación final
    quality = 0.5 * svm_score + 0.3 * completitud + 0.2 * confidence
    return float(np.clip(quality, 0.0, 1.0))


def _heuristic_quality(title: str, description: str = "") -> float:
    """Score heurístico basado en completitud y longitud del contenido."""
    score = 0.0

    # Título no vacío y razonable
    if title and len(title.strip()) >= 5:
        score += 0.4
    if title and len(title.strip()) >= 15:
        score += 0.1

    # Descripción
    desc_len = len(description.strip()) if description else 0
    if desc_len >= 30:
        score += 0.3
    elif desc_len >= 10:
        score += 0.15

    # Penalizar contenido claramente spam/vacío
    if title and re.search(r"(test|prueba|lorem ipsum|undefined|null)", title.lower()):
        score -= 0.3

    return float(np.clip(score, 0.0, 1.0))


def vectorize_text(text: str) -> list[float]:
    """Vectoriza texto usando el TF-IDF cargado. Retorna lista vacía si no hay modelo."""
    if "tfidf_vectorizer" not in _models:
        return []
    text_clean = _preprocess_text(text)
    vec = _models["tfidf_vectorizer"].transform([text_clean])
    return vec.toarray()[0].tolist()