"""
train_classifier.py
Script de entrenamiento / evaluación del clasificador de calidad.

Por ahora el clasificador es basado en reglas (no requiere entrenamiento
supervisado), pero este script:
  1. Descarga una muestra de eventos etiquetados manualmente desde MongoDB.
  2. Evalúa el accuracy de las reglas actuales.
  3. Reporta métricas y guarda un resumen en `ml/reports/classifier_report.json`.

Uso:
    python -m ml.train_classifier
    python ml/train_classifier.py
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .classifier import compute_quality_score
from .utils import get_mongo_client


REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)


def _load_labeled_events() -> list[dict]:
    """
    Recupera eventos que ya tienen una decisión manual (aprobado/rechazado)
    para usarlos como ground-truth.
    """
    client = get_mongo_client()
    db = client["gdlquehacer"]
    events = list(
        db["events"].find(
            {"status": {"$in": ["publicado", "rechazado"]}},
            {"_id": 1, "title": 1, "description": 1, "image_url": 1,
             "location": 1, "price": 1, "category": 1, "start_date": 1,
             "status": 1},
        ).limit(2000)
    )
    client.close()
    return events


def evaluate(events: list[dict]) -> dict:
    """Calcula métricas básicas del clasificador."""
    tp = fp = tn = fn = 0

    for event in events:
        score = compute_quality_score(event)
        predicted_pub = score >= 0.5
        actual_pub = event["status"] == "publicado"

        if predicted_pub and actual_pub:
            tp += 1
        elif predicted_pub and not actual_pub:
            fp += 1
        elif not predicted_pub and not actual_pub:
            tn += 1
        else:
            fn += 1

    total = len(events)
    accuracy = (tp + tn) / total if total else 0
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) else 0)

    return {
        "total": total,
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def main() -> None:
    print("[train_classifier] Cargando eventos etiquetados…")
    events = _load_labeled_events()

    if not events:
        print("[train_classifier] No hay eventos etiquetados. Saliendo.")
        return

    print(f"[train_classifier] {len(events)} eventos encontrados.")
    metrics = evaluate(events)

    print("\n=== Métricas del clasificador ===")
    for k, v in metrics.items():
        print(f"  {k:12}: {v}")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
    }
    report_path = REPORT_DIR / "classifier_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\n[train_classifier] Reporte guardado en {report_path}")


if __name__ == "__main__":
    main()