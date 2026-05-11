"""
ml/training/train_models.py
Entrena y guarda todos los modelos ML del sistema:

1. TF-IDF Vectorizer  — vectorización de texto
2. Clasificador de categoría (LogReg + SVM comparado)
3. SVM Quality Scorer — predice si un evento tiene alta/baja calidad
4. KNN Recommender    — base para recomendaciones por similitud

Datasets soportados:
  A) Sintético generado por generate_dataset.py
  B) Kaggle Event Recommendation Engine Challenge (Meetup.com)
     Descarga: https://www.kaggle.com/c/event-recommendation-engine-challenge/data

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
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC, LinearSVC
from sklearn.neighbors import NearestNeighbors, KNeighborsClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder, normalize
from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import Pipeline
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# Directorios
DATA_DIR = Path("ml/training/data")
MODELS_DIR = Path("ml/saved_models")
MODELS_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════
# 1. CARGA DE DATOS
# ═══════════════════════════════════════════════════════════════════════

def load_synthetic_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga los datasets sintéticos generados por generate_dataset.py."""
    events_path = DATA_DIR / "events_synthetic.csv"
    interactions_path = DATA_DIR / "interactions_synthetic.csv"

    if not events_path.exists():
        logger.info("Generando dataset sintético...")
        from ml.training.generate_dataset import (
            generate_events_dataset, generate_interactions_dataset
        )
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        events_df = generate_events_dataset(600)
        interactions_df = generate_interactions_dataset()
        events_df.to_csv(events_path, index=False)
        interactions_df.to_csv(interactions_path, index=False)
    else:
        events_df = pd.read_csv(events_path)
        interactions_df = pd.read_csv(interactions_path)

    logger.info("Eventos cargados: %d", len(events_df))
    logger.info("Interacciones cargadas: %d", len(interactions_df))
    return events_df, interactions_df


def load_kaggle_data(kaggle_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carga el dataset de Kaggle Event Recommendation Engine Challenge.

    Estructura esperada en kaggle_dir/:
      events.csv     — event_id, description, ...
      train.csv      — user, event, invited, timestamp, interested, not_interested
      users.csv      — user_id, locale, ...

    Descarga en: https://www.kaggle.com/c/event-recommendation-engine-challenge/data
    """
    kaggle_path = Path(kaggle_dir)

    events_raw = pd.read_csv(kaggle_path / "events.csv", nrows=10000)
    train_raw = pd.read_csv(kaggle_path / "train.csv")

    # Preparar eventos
    events_df = events_raw[["event_id", "description"]].copy()
    events_df = events_df.dropna(subset=["description"])
    events_df["text"] = events_df["description"].fillna("")

    # Asignar categorías aproximadas via keywords (Kaggle no tiene categorías exactas)
    def infer_category(text: str) -> str:
        text_lower = str(text).lower()
        if any(w in text_lower for w in ["music", "concert", "band", "jazz", "rock"]):
            return "entretenimiento"
        if any(w in text_lower for w in ["food", "drink", "wine", "beer", "cook"]):
            return "gastronomico"
        if any(w in text_lower for w in ["sport", "run", "yoga", "fitness", "bike"]):
            return "deportivo"
        if any(w in text_lower for w in ["art", "museum", "exhibit", "film", "book"]):
            return "cultural"
        return "entretenimiento"

    events_df["category"] = events_df["text"].apply(infer_category)
    events_df["quality_ml"] = np.random.beta(5, 2, len(events_df)).round(4)

    # Preparar interacciones
    interactions_df = train_raw.copy()
    interactions_df["label"] = (
        interactions_df["interested"].fillna(0).astype(int)
    )
    interactions_df = interactions_df.rename(
        columns={"user": "user_id", "event": "event_id"}
    )
    interactions_df["interaction_type"] = interactions_df["label"].map(
        {1: "interested", 0: "view"}
    )

    logger.info("Kaggle eventos: %d | interacciones: %d", len(events_df), len(interactions_df))
    return events_df, interactions_df


# ═══════════════════════════════════════════════════════════════════════
# 2. ENTRENAMIENTO TF-IDF + CLASIFICADOR DE CATEGORÍA
# ═══════════════════════════════════════════════════════════════════════

def train_category_classifier(events_df: pd.DataFrame) -> dict:
    """
    Entrena TF-IDF + LogisticRegression como clasificador principal.
    También entrena LinearSVC y compara — guarda el mejor.
    """
    logger.info("\n━━━ Entrenando clasificador de categorías ━━━")

    X = events_df["text"].fillna("").astype(str)
    y = events_df["category"]

    # TF-IDF Vectorizer
    vectorizer = TfidfVectorizer(
        max_features=5000,
        ngram_range=(1, 2),       # Unigramas y bigramas
        min_df=2,
        max_df=0.95,
        sublinear_tf=True,         # TF logarítmico
        strip_accents="unicode",
        analyzer="word",
    )

    X_vec = vectorizer.fit_transform(X)
    X_train, X_test, y_train, y_test = train_test_split(
        X_vec, y, test_size=0.2, random_state=42, stratify=y
    )

    results = {}

    # ── Logistic Regression ──────────────────────────────────────────
    lr = LogisticRegression(
        max_iter=1000,
        C=1.0,
        multi_class="multinomial",
        solver="lbfgs",
        random_state=42,
    )
    lr.fit(X_train, y_train)
    lr_acc = accuracy_score(y_test, lr.predict(X_test))
    results["LogisticRegression"] = {"model": lr, "accuracy": lr_acc}
    logger.info("LogisticRegression accuracy: %.4f", lr_acc)

    # ── SVM (LinearSVC — más rápido para texto) ──────────────────────
    svm = LinearSVC(
        C=1.0,
        max_iter=2000,
        random_state=42,
    )
    svm.fit(X_train, y_train)
    svm_acc = accuracy_score(y_test, svm.predict(X_test))
    results["LinearSVC"] = {"model": svm, "accuracy": svm_acc}
    logger.info("LinearSVC accuracy: %.4f", svm_acc)

    # ── Seleccionar el mejor ─────────────────────────────────────────
    best_name = max(results, key=lambda k: results[k]["accuracy"])
    best_model = results[best_name]["model"]
    logger.info("✅  Mejor clasificador: %s (acc=%.4f)", best_name, results[best_name]["accuracy"])

    # Classification report del mejor
    y_pred = best_model.predict(X_test)
    logger.info("\n%s", classification_report(y_test, y_pred))

    # Guardar
    joblib.dump(vectorizer, MODELS_DIR / "tfidf_vectorizer.joblib")
    joblib.dump(best_model, MODELS_DIR / "category_classifier.joblib")
    logger.info("💾  Guardados: tfidf_vectorizer.joblib + category_classifier.joblib")

    return {"vectorizer": vectorizer, "classifier": best_model}


# ═══════════════════════════════════════════════════════════════════════
# 3. ENTRENAMIENTO SVM QUALITY SCORER
# ═══════════════════════════════════════════════════════════════════════

def train_quality_scorer(
    events_df: pd.DataFrame,
    vectorizer: TfidfVectorizer,
) -> None:
    """
    Entrena un SVM binario que predice si un evento tiene alta calidad
    (quality_ml >= 0.5) basándose en el vector TF-IDF de su texto.
    """
    logger.info("\n━━━ Entrenando SVM Quality Scorer ━━━")

    X_text = events_df["text"].fillna("").astype(str)
    y_quality = (events_df["quality_ml"] >= 0.5).astype(int)

    X_vec = vectorizer.transform(X_text)
    X_train, X_test, y_train, y_test = train_test_split(
        X_vec, y_quality, test_size=0.2, random_state=42
    )

    # SVC con kernel RBF para mejor separación no lineal
    svm_quality = SVC(
        kernel="linear",    # Linear es más eficiente para alta dimensión
        C=1.0,
        probability=False,  # decision_function es suficiente para scoring
        random_state=42,
        class_weight="balanced",
    )
    svm_quality.fit(X_train, y_train)

    acc = accuracy_score(y_test, svm_quality.predict(X_test))
    logger.info("SVM Quality Scorer accuracy: %.4f", acc)

    # Cross-validation
    cv_scores = cross_val_score(svm_quality, X_vec, y_quality, cv=5, scoring="f1")
    logger.info("CV F1 scores: %s | Mean: %.4f", cv_scores.round(4), cv_scores.mean())

    joblib.dump(svm_quality, MODELS_DIR / "svm_quality_scorer.joblib")
    logger.info("💾  Guardado: svm_quality_scorer.joblib")


# ═══════════════════════════════════════════════════════════════════════
# 4. ENTRENAMIENTO KNN PARA RECOMENDACIONES
# ═══════════════════════════════════════════════════════════════════════

def train_knn_recommender(
    events_df: pd.DataFrame,
    interactions_df: pd.DataFrame,
    vectorizer: TfidfVectorizer,
) -> None:
    """
    Construye y guarda el modelo KNN para recomendaciones de contenido.

    Estrategia:
    - Feature matrix: vectores TF-IDF de eventos + codificación de categoría
    - KNN entrenado sobre esta matrix para encontrar eventos similares
    - En inferencia: el vector del usuario (promedio de eventos vistos)
      se busca en este espacio
    """
    logger.info("\n━━━ Entrenando KNN Recommender ━━━")

    X_text = events_df["text"].fillna("").astype(str)
    X_tfidf = vectorizer.transform(X_text)

    # Codificar categorías como features adicionales
    le = LabelEncoder()
    cat_encoded = le.fit_transform(events_df["category"])
    cat_matrix = np.zeros((len(cat_encoded), len(le.classes_)))
    for i, c in enumerate(cat_encoded):
        cat_matrix[i, c] = 1.0

    # Combinar TF-IDF + categoría codificada (ponderado)
    from scipy.sparse import hstack, csr_matrix
    X_combined = hstack([X_tfidf, csr_matrix(cat_matrix * 0.5)])
    X_normalized = normalize(X_combined, norm="l2")

    # KNN con métrica coseno (brute force para alta dimensionalidad)
    knn = NearestNeighbors(
        n_neighbors=10,
        metric="cosine",
        algorithm="brute",
        n_jobs=-1,
    )
    knn.fit(X_normalized)

    logger.info("KNN entrenado sobre %d eventos, %d features", *X_normalized.shape)

    # Guardar modelos
    joblib.dump(knn, MODELS_DIR / "knn_recommender.joblib")
    joblib.dump(le, MODELS_DIR / "category_label_encoder.joblib")
    joblib.dump(
        {"event_ids": events_df.index.tolist(), "n_features": X_normalized.shape[1]},
        MODELS_DIR / "knn_metadata.joblib"
    )
    logger.info("💾  Guardados: knn_recommender.joblib + category_label_encoder.joblib")


# ═══════════════════════════════════════════════════════════════════════
# 5. ENTRENAMIENTO SVM PARA RANKING DE RECOMENDACIONES
# ═══════════════════════════════════════════════════════════════════════

def train_svm_ranker(
    events_df: pd.DataFrame,
    interactions_df: pd.DataFrame,
    vectorizer: TfidfVectorizer,
) -> None:
    """
    Entrena un SVM binario para predecir si un usuario interactuará
    positivamente con un evento dado (relevance ranking).

    Features: [user_preference_vector ⊕ event_tfidf_vector]
    Label:    1 si hay interacción positiva (save/interested), 0 si no
    """
    logger.info("\n━━━ Entrenando SVM Ranker de Recomendaciones ━━━")

    X_text = events_df["text"].fillna("").astype(str)
    event_vectors = vectorizer.transform(X_text).toarray()

    # Construir vectores de usuario como promedio de eventos con los que interactuó
    positive_types = {"save", "interested"}
    user_vectors = {}

    for user_id, group in interactions_df.groupby("user_id"):
        positive_events = group[
            group["interaction_type"].isin(positive_types)
        ]["event_id"].tolist()

        valid_event_ids = [
            eid for eid in positive_events
            if isinstance(eid, int) and 0 <= eid < len(event_vectors)
        ]
        if valid_event_ids:
            user_vec = np.mean(event_vectors[valid_event_ids], axis=0)
            user_vectors[user_id] = user_vec

    if not user_vectors:
        logger.warning("No hay suficientes interacciones positivas para entrenar SVM Ranker.")
        return

    # Construir dataset de entrenamiento
    X_pairs, y_labels = [], []

    for _, row in interactions_df.iterrows():
        uid = row["user_id"]
        eid = row["event_id"]

        if uid not in user_vectors:
            continue
        if not isinstance(eid, int) or eid >= len(event_vectors):
            continue

        user_vec = user_vectors[uid]
        event_vec = event_vectors[eid]

        # Feature: concatenación de vectores usuario + evento
        feature = np.concatenate([user_vec, event_vec])
        X_pairs.append(feature)
        y_labels.append(int(row["label"]))

    if len(X_pairs) < 100:
        logger.warning("Muy pocas muestras (%d) para SVM Ranker. Se omite.", len(X_pairs))
        return

    X_pairs = np.array(X_pairs)
    y_labels = np.array(y_labels)

    X_train, X_test, y_train, y_test = train_test_split(
        X_pairs, y_labels, test_size=0.2, random_state=42
    )

    # SVM con kernel lineal (eficiente para alta dimensión)
    svm_ranker = LinearSVC(
        C=0.1,
        max_iter=2000,
        random_state=42,
        class_weight="balanced",
    )
    svm_ranker.fit(X_train, y_train)

    acc = accuracy_score(y_test, svm_ranker.predict(X_test))
    logger.info("SVM Ranker accuracy: %.4f", acc)
    logger.info("\n%s", classification_report(y_test, svm_ranker.predict(X_test)))

    joblib.dump(svm_ranker, MODELS_DIR / "svm_ranker.joblib")
    logger.info("💾  Guardado: svm_ranker.joblib")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Entrenar modelos ML de GDL Qué Hacer")
    parser.add_argument(
        "--use-kaggle",
        action="store_true",
        help="Usar dataset de Kaggle en lugar del sintético",
    )
    parser.add_argument(
        "--kaggle-dir",
        default="ml/training/data/kaggle",
        help="Directorio con los archivos CSV de Kaggle",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  GDL Qué Hacer — Pipeline de Entrenamiento ML")
    logger.info("=" * 60)

    # Cargar datos
    if args.use_kaggle:
        logger.info("📦  Cargando dataset de Kaggle desde %s", args.kaggle_dir)
        events_df, interactions_df = load_kaggle_data(args.kaggle_dir)
    else:
        logger.info("📦  Usando dataset sintético")
        events_df, interactions_df = load_synthetic_data()

    # 1. Clasificador de categorías (incluye TF-IDF)
    models = train_category_classifier(events_df)
    vectorizer = models["vectorizer"]

    # 2. SVM Quality Scorer
    train_quality_scorer(events_df, vectorizer)

    # 3. KNN Recommender
    train_knn_recommender(events_df, interactions_df, vectorizer)

    # 4. SVM Ranker
    train_svm_ranker(events_df, interactions_df, vectorizer)

    logger.info("\n" + "=" * 60)
    logger.info("  ✅  Entrenamiento completado. Modelos guardados en:")
    logger.info("  %s", MODELS_DIR.resolve())
    logger.info("=" * 60)

    # Listar modelos guardados
    for f in sorted(MODELS_DIR.glob("*.joblib")):
        size_kb = f.stat().st_size / 1024
        logger.info("  📁  %s (%.1f KB)", f.name, size_kb)


if __name__ == "__main__":
    main()