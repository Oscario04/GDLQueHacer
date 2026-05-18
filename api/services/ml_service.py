"""
api/services/ml_service.py
Servicio ML para clasificación de eventos y scoring de calidad.

v3 — cambios respecto a v2:
  - Añade load_ml_models() como función pública (requerida por api/main.py).
  - Quality Scorer usa features ESTRUCTURADAS (imagen, descripción, coords, etc.)
    en lugar de TF-IDF. Esto alinea inferencia con entrenamiento.
  - Compatibilidad hacia atrás: carga 'quality_scorer.joblib' primero,
    y si no existe busca 'svm_quality_scorer.joblib' (nombre anterior).
  - reload_models() limpia la caché de lru_cache para forzar recarga
    después de un reentrenamiento.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

MODELS_DIR = Path("ml/saved_models")


# ═══════════════════════════════════════════════════════════════════════
# Carga de modelos
# ═══════════════════════════════════════════════════════════════════════

def _try_load(filename: str) -> Any | None:
    """Carga un joblib o devuelve None logueando el warning."""
    import joblib
    path = MODELS_DIR / filename
    if not path.exists():
        return None
    try:
        model = joblib.load(path)
        logger.info("🤖  Modelo cargado: %s", filename.replace(".joblib", ""))
        return model
    except Exception as exc:
        logger.error("❌  Error cargando %s: %s", filename, exc)
        return None


@lru_cache(maxsize=1)
def _load_models() -> dict:
    """
    Carga todos los modelos ML una sola vez (lazy + thread-safe con lru_cache).
    Compatibilidad hacia atrás para nombres de archivo anteriores.
    """
    models: dict[str, Any] = {"active": False}

    vectorizer = _try_load("tfidf_vectorizer.joblib")
    classifier = _try_load("category_classifier.joblib")

    # Compatibilidad: nuevo nombre → nombre anterior
    quality = (
        _try_load("quality_scorer.joblib")
        or _try_load("svm_quality_scorer.joblib")
    )

    if vectorizer is None:
        logger.warning("⚠️   Modelo no encontrado: tfidf_vectorizer.joblib — usando modo degradado")
    if classifier is None:
        logger.warning("⚠️   Modelo no encontrado: category_classifier.joblib — usando modo degradado")
    if quality is None:
        logger.warning("⚠️   Modelo no encontrado: quality_scorer.joblib — usando modo degradado")

    if vectorizer and classifier and quality:
        models.update({
            "vectorizer": vectorizer,
            "classifier": classifier,
            "quality":    quality,
            "active":     True,
        })
    else:
        logger.warning("⚠️   Sin modelos ML. Ejecuta ml/training/train_models.py primero.")

    return models


def load_ml_models() -> dict:
    """
    Función PÚBLICA requerida por api/main.py en el startup de la aplicación.
    Retorna el dict de modelos cargados (o vacío en modo degradado).
    """
    return _load_models()


def reload_models() -> dict:
    """
    Fuerza recarga completa de modelos desde disco.
    Llamar después de un reentrenamiento exitoso.
    """
    _load_models.cache_clear()
    logger.info("🔄  Caché de modelos limpiada. Recargando...")
    return _load_models()


# ═══════════════════════════════════════════════════════════════════════
# Extracción de features estructuradas (Quality Scorer v3)
# ═══════════════════════════════════════════════════════════════════════

def _extract_quality_features(
    title: str,
    description: str,
    event: dict | None = None,
) -> np.ndarray:
    """
    Extrae las mismas 8 features que usa train_models._extract_quality_features().
    Debe mantenerse sincronizado con generate_dataset._compute_quality_score().

    Features:
      0  has_image
      1  desc_len_score_full    (>= 200 chars)
      2  desc_len_score_medium  (>= 80 chars)
      3  desc_len_raw_norm      (normalizada a [0, 1], cap 500)
      4  has_location
      5  has_price
      6  has_category
      7  date_is_future
    """
    event = event or {}
    now   = datetime.now(timezone.utc)

    img       = event.get("image_url") or event.get("image", "")
    has_image = 1.0 if (img and not _is_nan(img)) else 0.0

    desc      = (description or "").strip()
    desc_len  = len(desc)
    desc_full   = 1.0 if desc_len >= 200 else 0.0
    desc_medium = 1.0 if desc_len >= 80 else 0.0
    desc_norm   = min(desc_len / 500.0, 1.0)

    lat = event.get("latitude") or event.get("lat")
    lon = event.get("longitude") or event.get("lon")
    has_location = 1.0 if (lat is not None and lon is not None
                            and not _is_nan(lat) and not _is_nan(lon)) else 0.0

    price     = event.get("price")
    has_price = 0.0 if (price is None or _is_nan(price)) else 1.0

    cat          = event.get("category", "") or ""
    has_category = 1.0 if cat else 0.0

    raw_date = event.get("date_start") or event.get("start_date")
    future   = 0.0
    if raw_date:
        try:
            dt = datetime.fromisoformat(str(raw_date))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            future = 1.0 if dt > now else 0.0
        except (ValueError, TypeError):
            pass

    return np.array([[
        has_image,
        desc_full,
        desc_medium,
        desc_norm,
        has_location,
        has_price,
        has_category,
        future,
    ]], dtype=np.float32)


def _is_nan(val: Any) -> bool:
    """Verifica si un valor es NaN (float o pandas NaT)."""
    try:
        import math
        return math.isnan(float(val))
    except (TypeError, ValueError):
        return False


# ═══════════════════════════════════════════════════════════════════════
# classify_event — función pública principal
# ═══════════════════════════════════════════════════════════════════════

def classify_event(
    title: str,
    description: str,
    event: dict | None = None,
) -> dict:
    """
    Clasifica un evento y devuelve:
      {
        "category":            str,
        "category_confidence": float,
        "quality_ml":          float,
        "tfidf_vector":        list[float],
        "models_active":       bool,
      }

    Parámetros:
        title       — título del evento (ya limpio)
        description — descripción del evento (ya limpia)
        event       — dict con campos opcionales para quality scoring:
                      image_url, latitude, longitude, price,
                      category (si ya se conoce), date_start / start_date.
    """
    models = _load_models()

    # ── Modo degradado: sin modelos ───────────────────────────────────
    if not models["active"]:
        return {
            "category":            (event or {}).get("category", "otro"),
            "category_confidence": 0.0,
            "quality_ml":          0.0,
            "tfidf_vector":        [],
            "models_active":       False,
        }

    vectorizer = models["vectorizer"]
    classifier = models["classifier"]
    quality_m  = models["quality"]

    text  = f"{title} {description}".strip()
    X_vec = vectorizer.transform([text])

    # ── Clasificación de categoría ────────────────────────────────────
    category   = classifier.predict(X_vec)[0]
    confidence = _get_confidence(classifier, X_vec)

    # ── Quality Score ─────────────────────────────────────────────────
    # Detectar si el scorer es el nuevo (features estructuradas) o el
    # viejo (TF-IDF). El viejo acepta sparse matrix; el nuevo ndarray de 8 cols.
    quality_ml = _predict_quality(quality_m, X_vec, title, description,
                                  {**(event or {}), "category": category})

    return {
        "category":            str(category),
        "category_confidence": round(float(confidence), 4),
        "quality_ml":          round(float(quality_ml), 4),
        "tfidf_vector":        X_vec.toarray()[0].tolist(),
        "models_active":       True,
    }


def _get_confidence(classifier: Any, X_vec: Any) -> float:
    if hasattr(classifier, "predict_proba"):
        proba = classifier.predict_proba(X_vec)[0]
        return float(proba.max())
    if hasattr(classifier, "decision_function"):
        df = classifier.decision_function(X_vec)[0]
        exp_s = np.exp(df - df.max())
        return float(exp_s.max() / exp_s.sum())
    return 0.0


def _predict_quality(
    quality_m: Any,
    X_tfidf,
    title: str,
    description: str,
    event: dict,
) -> float:
    """
    Detecta si el scorer es v3 (8 features estructuradas) o v2 (TF-IDF)
    y llama al método apropiado.

    Detección: si el modelo fue entrenado con n_features == 8, es v3.
    GradientBoostingClassifier → n_features_in_ == 8 → scorer v3.
    SVC con TF-IDF → n_features_in_ >> 8 → scorer v2 (legacy).
    """
    n_features = getattr(quality_m, "n_features_in_", None)
    use_structured = (n_features is not None and n_features <= 16)

    if use_structured:
        # Scorer v3: features estructuradas
        X_q = _extract_quality_features(title, description, event)
    else:
        # Scorer v2 (legacy): usa TF-IDF directamente
        X_q = X_tfidf

    if hasattr(quality_m, "predict_proba"):
        return float(quality_m.predict_proba(X_q)[0][1])
    return float(quality_m.predict(X_q)[0])