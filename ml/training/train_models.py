"""
ml/training/train_models.py
Entrena y guarda todos los modelos ML del sistema:

1. TF-IDF Vectorizer      — vectorización de texto
2. Clasificador de categoría (LogReg + LinearSVC comparado)
3. Quality Scorer         — GradientBoosting sobre features ESTRUCTURADAS
4. KNN Recommender        — recomendaciones por similitud con SVD
5. Ranker                 — re-ranking de candidatos KNN

Mejoras v4:
- KNN: SVD sube de 50 → 100 componentes para capturar más varianza semántica.
- Ranker: cambia de LogisticRegression → GradientBoostingClassifier
  (más robusto con features de alta dimensión y señal débil).
- Ranker: se añade quality_ml del evento como feature extra,
  capturando la calidad objetiva del evento además de la similitud usuario-evento.
- Quality Scorer: threshold dinámico basado en la mediana real del dataset
  en lugar de 0.5 fijo, para manejar mejor el desbalance de clases.

Uso:
    python -m ml.training.train_models
    python -m ml.training.train_models --use-kaggle
"""
import argparse
import joblib
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.neighbors import NearestNeighbors
from sklearn.decomposition import TruncatedSVD
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import LabelEncoder, normalize
from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import Pipeline
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR   = Path("ml/training/data")
MODELS_DIR = Path("ml/saved_models")
MODELS_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════
# 1. CARGA DE DATOS
# ═══════════════════════════════════════════════════════════════════════

def load_synthetic_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Generando dataset sintético v4...")
    from ml.training.generate_dataset import (
        generate_events_dataset, generate_interactions_dataset
    )
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    events_df       = generate_events_dataset(600)
    interactions_df = generate_interactions_dataset()
    events_df.to_csv(DATA_DIR / "events_synthetic.csv", index=False)
    interactions_df.to_csv(DATA_DIR / "interactions_synthetic.csv", index=False)

    logger.info("Eventos cargados: %d", len(events_df))
    logger.info("Interacciones cargadas: %d", len(interactions_df))
    return events_df, interactions_df


def load_kaggle_data(kaggle_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga el dataset de Kaggle Event Recommendation Engine Challenge."""
    kaggle_path = Path(kaggle_dir)

    events_raw = pd.read_csv(kaggle_path / "events.csv", nrows=10000)
    train_raw  = pd.read_csv(kaggle_path / "train.csv")

    events_df = events_raw[["event_id", "description"]].copy()
    events_df = events_df.dropna(subset=["description"])
    events_df["text"] = events_df["description"].fillna("")

    def infer_category(text: str) -> str:
        t = str(text).lower()
        if any(w in t for w in ["music", "concert", "band", "jazz", "rock"]):
            return "entretenimiento"
        if any(w in t for w in ["food", "drink", "wine", "beer", "cook"]):
            return "gastronomico"
        if any(w in t for w in ["sport", "run", "yoga", "fitness", "bike"]):
            return "deportivo"
        if any(w in t for w in ["art", "museum", "exhibit", "film", "book"]):
            return "cultural"
        return "otro"

    events_df["category"]   = events_df["text"].apply(infer_category)
    events_df["quality_ml"] = np.random.beta(3, 2, len(events_df)).round(4)

    interactions_df = train_raw.copy()
    interactions_df["label"]            = interactions_df["interested"].fillna(0).astype(int)
    interactions_df                     = interactions_df.rename(columns={"user": "user_id", "event": "event_id"})
    interactions_df["interaction_type"] = interactions_df["label"].map({1: "interested", 0: "view"})

    logger.info("Kaggle eventos: %d | interacciones: %d", len(events_df), len(interactions_df))
    return events_df, interactions_df


# ═══════════════════════════════════════════════════════════════════════
# 2. CLASIFICADOR DE CATEGORÍA
# ═══════════════════════════════════════════════════════════════════════

def train_category_classifier(events_df: pd.DataFrame) -> dict:
    """
    Entrena TF-IDF + LogisticRegression y LinearSVC. Guarda el mejor.
    Con el overlap léxico de generate_dataset v4 se espera accuracy ~0.75-0.88.
    """
    logger.info("\n━━━ Entrenando clasificador de categorías ━━━")

    X = events_df["text"].fillna("").astype(str)
    y = events_df["category"]

    vectorizer = TfidfVectorizer(
        max_features=5000,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.95,
        sublinear_tf=True,
        strip_accents="unicode",
        analyzer="word",
    )
    X_vec = vectorizer.fit_transform(X)

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    X_train, X_test, y_train, y_test = train_test_split(
        X_vec, y, test_size=0.2, random_state=42, stratify=y
    )

    results = {}

    lr = LogisticRegression(max_iter=1000, C=1.0, solver="lbfgs", random_state=42)
    lr.fit(X_train, y_train)
    lr_acc = accuracy_score(y_test, lr.predict(X_test))
    results["LogisticRegression"] = {"model": lr, "accuracy": lr_acc}
    logger.info("LogisticRegression accuracy: %.4f", lr_acc)

    svm = LinearSVC(C=1.0, max_iter=2000, random_state=42)
    svm.fit(X_train, y_train)
    svm_acc = accuracy_score(y_test, svm.predict(X_test))
    results["LinearSVC"] = {"model": svm, "accuracy": svm_acc}
    logger.info("LinearSVC accuracy: %.4f", svm_acc)

    best_name  = max(results, key=lambda k: results[k]["accuracy"])
    best_model = results[best_name]["model"]
    logger.info("✅  Mejor clasificador: %s (acc=%.4f)", best_name, results[best_name]["accuracy"])

    y_pred = best_model.predict(X_test)
    logger.info("\n%s", classification_report(y_test, y_pred))

    cv_scores = cross_val_score(lr, X_vec, y, cv=cv, scoring="f1_weighted")
    logger.info("CV F1 (weighted) — scores: %s | Mean: %.4f", cv_scores.round(4), cv_scores.mean())
    if cv_scores.mean() > 0.98:
        logger.warning("⚠️  CV F1 > 0.98: posible separación trivial en el dataset.")

    joblib.dump(vectorizer, MODELS_DIR / "tfidf_vectorizer.joblib")
    joblib.dump(best_model, MODELS_DIR / "category_classifier.joblib")
    logger.info("💾  Guardados: tfidf_vectorizer.joblib + category_classifier.joblib")

    return {"vectorizer": vectorizer, "classifier": best_model}


# ═══════════════════════════════════════════════════════════════════════
# 3. QUALITY SCORER — features estructuradas + threshold dinámico
# ═══════════════════════════════════════════════════════════════════════

def _extract_quality_features(events_df: pd.DataFrame) -> np.ndarray:
    """
    Extrae las MISMAS features que usa compute_quality_score() en classifier.py.

    Columnas (8 features):
      0  has_image
      1  desc_len_score_full    — 1.0 si desc >= 200 chars
      2  desc_len_score_medium  — 1.0 si desc >= 80 chars
      3  desc_len_raw_norm      — longitud normalizada [0,1] (cap 500)
      4  has_location
      5  has_price
      6  has_category
      7  date_is_future
    """
    now = datetime.now(timezone.utc)

    def parse_date(raw):
        if pd.isna(raw) or not raw:
            return None
        try:
            dt = datetime.fromisoformat(str(raw))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None

    rows = []
    for _, row in events_df.iterrows():
        desc  = str(row.get("description", "") or "").strip()
        dt    = parse_date(row.get("date_start"))
        lat   = row.get("latitude")
        lon   = row.get("longitude")
        price = row.get("price")
        cat   = row.get("category", "")
        img   = row.get("image_url", "")

        has_image    = 1.0 if img and not pd.isna(img) else 0.0
        desc_len     = len(desc)
        desc_full    = 1.0 if desc_len >= 200 else 0.0
        desc_medium  = 1.0 if desc_len >= 80 else 0.0
        desc_norm    = min(desc_len / 500.0, 1.0)
        has_location = 1.0 if (lat is not None and lon is not None
                                and not pd.isna(lat) and not pd.isna(lon)) else 0.0
        has_price    = 0.0 if (price is None or pd.isna(price)) else 1.0
        has_category = 1.0 if cat else 0.0
        future       = 1.0 if (dt is not None and dt > now) else 0.0

        rows.append([has_image, desc_full, desc_medium, desc_norm,
                     has_location, has_price, has_category, future])

    return np.array(rows, dtype=np.float32)


QUALITY_FEATURE_NAMES = [
    "has_image",
    "desc_len_score_full",
    "desc_len_score_medium",
    "desc_len_raw_norm",
    "has_location",
    "has_price",
    "has_category",
    "date_is_future",
]


def train_quality_scorer(events_df: pd.DataFrame) -> None:
    """
    Entrena GradientBoostingClassifier sobre features estructuradas.

    v4: threshold dinámico (mediana del dataset) en lugar de 0.5 fijo.
    Maneja mejor distribuciones desbalanceadas.
    """
    logger.info("\n━━━ Entrenando Quality Scorer (features estructuradas) ━━━")

    X = _extract_quality_features(events_df)

    # Threshold dinámico: mediana de quality_ml del dataset real
    threshold = float(np.median(events_df["quality_ml"]))
    logger.info("Threshold dinámico (mediana quality_ml): %.4f", threshold)
    y = (events_df["quality_ml"] >= threshold).astype(int).values

    high = y.sum()
    low  = (y == 0).sum()
    logger.info("Distribución de calidad — alta: %d | baja: %d", high, low)

    if abs(high - low) / len(y) > 0.4:
        logger.warning(
            "⚠️  Desbalance fuerte (%.0f%% alta). Ajusta tiers en generate_dataset.py.",
            100 * high / len(y),
        )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    gb = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        random_state=42,
    )
    gb.fit(X_train, y_train)

    acc = accuracy_score(y_test, gb.predict(X_test))
    logger.info("GradientBoosting accuracy: %.4f", acc)
    logger.info("\n%s", classification_report(y_test, gb.predict(X_test),
                                              target_names=["baja_calidad", "alta_calidad"]))

    cv_scores = cross_val_score(gb, X, y, cv=5, scoring="f1")
    logger.info("CV F1 scores: %s | Mean: %.4f", cv_scores.round(4), cv_scores.mean())

    logger.info("\nImportancia de features:")
    for name, imp in sorted(
        zip(QUALITY_FEATURE_NAMES, gb.feature_importances_),
        key=lambda x: -x[1],
    ):
        bar = "█" * int(imp * 30)
        logger.info("  %-25s %.4f  %s", name, imp, bar)

    joblib.dump(gb,        MODELS_DIR / "quality_scorer.joblib")
    joblib.dump(QUALITY_FEATURE_NAMES, MODELS_DIR / "quality_feature_names.joblib")
    joblib.dump(threshold, MODELS_DIR / "quality_threshold.joblib")
    logger.info("💾  Guardados: quality_scorer.joblib + quality_feature_names.joblib + quality_threshold.joblib")


# ═══════════════════════════════════════════════════════════════════════
# 4. KNN RECOMMENDER — SVD 100 componentes
# ═══════════════════════════════════════════════════════════════════════

def train_knn_recommender(
    events_df: pd.DataFrame,
    interactions_df: pd.DataFrame,
    vectorizer: TfidfVectorizer,
) -> None:
    """
    KNN sobre vectores TF-IDF reducidos con SVD + one-hot de categoría.

    v4: SVD sube de 50 → 100 componentes para capturar más varianza semántica.
    """
    logger.info("\n━━━ Entrenando KNN Recommender (SVD 100 componentes) ━━━")

    X_text  = events_df["text"].fillna("").astype(str)
    X_tfidf = vectorizer.transform(X_text)

    # v4: n_components sube a 100
    n_components = min(100, X_tfidf.shape[1] - 1, X_tfidf.shape[0] - 1)
    svd = TruncatedSVD(n_components=n_components, random_state=42)
    X_svd     = svd.fit_transform(X_tfidf)
    explained = svd.explained_variance_ratio_.sum()
    logger.info(
        "SVD: %d → %d dims | varianza explicada: %.2f%%",
        X_tfidf.shape[1], n_components, explained * 100,
    )

    le = LabelEncoder()
    cat_encoded = le.fit_transform(events_df["category"])
    cat_matrix  = np.zeros((len(cat_encoded), len(le.classes_)))
    for i, c in enumerate(cat_encoded):
        cat_matrix[i, c] = 1.0

    X_combined   = np.hstack([X_svd, cat_matrix * 0.5])
    X_normalized = normalize(X_combined, norm="l2")

    knn = NearestNeighbors(
        n_neighbors=10,
        metric="cosine",
        algorithm="brute",
        n_jobs=-1,
    )
    knn.fit(X_normalized)
    logger.info("KNN entrenado sobre %d eventos, %d features", *X_normalized.shape)

    joblib.dump(knn, MODELS_DIR / "knn_recommender.joblib")
    joblib.dump(le,  MODELS_DIR / "category_label_encoder.joblib")
    joblib.dump(svd, MODELS_DIR / "tfidf_svd.joblib")
    joblib.dump(
        {
            "event_ids":          events_df.index.tolist(),
            "n_features":         X_normalized.shape[1],
            "n_components":       n_components,
            "explained_variance": float(explained),
        },
        MODELS_DIR / "knn_metadata.joblib",
    )
    logger.info("💾  Guardados: knn_recommender.joblib + tfidf_svd.joblib + category_label_encoder.joblib")


# ═══════════════════════════════════════════════════════════════════════
# 5. RANKER — GradientBoosting + quality_ml como feature
# ═══════════════════════════════════════════════════════════════════════

def train_svm_ranker(
    events_df: pd.DataFrame,
    interactions_df: pd.DataFrame,
    vectorizer: TfidfVectorizer,
) -> None:
    """
    Clasificador de relevancia usuario-evento.

    v4 mejoras:
    - GradientBoostingClassifier en lugar de LogisticRegression.
    - quality_ml del evento como feature escalar extra.
    - Features: [diff, prod, |diff|, quality_ml]
    """
    logger.info("\n━━━ Entrenando Ranker de Recomendaciones (GradientBoosting v4) ━━━")

    X_text        = events_df["text"].fillna("").astype(str)
    event_vectors = vectorizer.transform(X_text).toarray()
    quality_scores = events_df["quality_ml"].values

    positive_types = {"save", "interested"}
    user_vectors: dict = {}

    for user_id, group in interactions_df.groupby("user_id"):
        positive_events = group[
            group["interaction_type"].isin(positive_types)
        ]["event_id"].tolist()

        valid_ids = [
            int(eid) for eid in positive_events
            if isinstance(eid, (int, np.integer)) and 0 <= int(eid) < len(event_vectors)
        ]
        if valid_ids:
            user_vectors[user_id] = np.mean(event_vectors[valid_ids], axis=0)

    if not user_vectors:
        logger.warning("Sin interacciones positivas suficientes para el ranker.")
        return

    X_pairs, y_labels = [], []

    for _, row in interactions_df.iterrows():
        uid = row["user_id"]
        eid = row["event_id"]

        if uid not in user_vectors:
            continue
        if not isinstance(eid, (int, np.integer)) or int(eid) >= len(event_vectors):
            continue

        user_vec  = user_vectors[uid]
        event_vec = event_vectors[int(eid)]
        quality   = quality_scores[int(eid)]

        diff     = user_vec - event_vec
        prod     = user_vec * event_vec
        abs_diff = np.abs(diff)

        # v4: quality_ml como feature extra
        X_pairs.append(np.concatenate([diff, prod, abs_diff, [quality]]))
        y_labels.append(int(row["label"]))

    if len(X_pairs) < 100:
        logger.warning("Muy pocas muestras (%d) para el ranker. Se omite.", len(X_pairs))
        return

    X_pairs  = np.array(X_pairs)
    y_labels = np.array(y_labels)

    logger.info(
        "Dataset ranker: %d muestras | positivas: %d | negativas: %d",
        len(y_labels), y_labels.sum(), (y_labels == 0).sum(),
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X_pairs, y_labels, test_size=0.2, random_state=42, stratify=y_labels
    )

    # v4: GradientBoosting
    ranker = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.8,
        random_state=42,
    )
    ranker.fit(X_train, y_train)

    acc = accuracy_score(y_test, ranker.predict(X_test))
    logger.info("Ranker accuracy: %.4f", acc)
    logger.info("\n%s", classification_report(y_test, ranker.predict(X_test)))

    cv_scores = cross_val_score(ranker, X_pairs, y_labels, cv=5, scoring="f1")
    logger.info("CV F1 scores: %s | Mean: %.4f", cv_scores.round(4), cv_scores.mean())

    joblib.dump(ranker, MODELS_DIR / "svm_ranker.joblib")
    logger.info("💾  Guardado: svm_ranker.joblib")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Entrenar modelos ML — GDL Qué Hacer v4")
    parser.add_argument("--use-kaggle", action="store_true")
    parser.add_argument("--kaggle-dir", default="ml/training/data/kaggle")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  GDL Qué Hacer — Pipeline de Entrenamiento ML v4")
    logger.info("=" * 60)

    if args.use_kaggle:
        logger.info("📦  Cargando dataset de Kaggle desde %s", args.kaggle_dir)
        events_df, interactions_df = load_kaggle_data(args.kaggle_dir)
    else:
        logger.info("📦  Usando dataset sintético v4")
        events_df, interactions_df = load_synthetic_data()

    models     = train_category_classifier(events_df)
    vectorizer = models["vectorizer"]

    train_quality_scorer(events_df)
    train_knn_recommender(events_df, interactions_df, vectorizer)
    train_svm_ranker(events_df, interactions_df, vectorizer)

    logger.info("\n" + "=" * 60)
    logger.info("  ✅  Entrenamiento completado. Modelos en:")
    logger.info("  %s", MODELS_DIR.resolve())
    logger.info("=" * 60)

    for f in sorted(MODELS_DIR.glob("*.joblib")):
        size_kb = f.stat().st_size / 1024
        logger.info("  📁  %s (%.1f KB)", f.name, size_kb)


if __name__ == "__main__":
    main()