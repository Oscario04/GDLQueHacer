"""
train_recommender.py
Evaluación offline del recomendador (leave-one-out sobre historial de usuarios).

Métricas calculadas:
  - Precision@K
  - Recall@K
  - Hit Rate@K

Uso:
    python -m ml.train_recommender
    python ml/train_recommender.py --k 10
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .recommender import _tokenize, _event_to_text, _compute_tfidf, _cosine_similarity
from .utils import get_mongo_client


REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)


def _load_data():
    client = get_mongo_client()
    db = client["gdlquehacer"]
    users = list(db["users"].find(
        {"liked_events": {"$exists": True, "$not": {"$size": 0}}}
    ))
    events_raw = list(db["events"].find({"status": "publicado"}))
    client.close()

    events_by_id = {str(e["_id"]): e for e in events_raw}
    return users, events_by_id


def evaluate_recommender(k: int = 10) -> dict:
    users, events_by_id = _load_data()

    if not users or not events_by_id:
        return {"error": "Sin datos suficientes para evaluar."}

    all_event_ids = list(events_by_id.keys())
    all_events = [events_by_id[eid] for eid in all_event_ids]
    docs = [_tokenize(_event_to_text(e)) for e in all_events]
    vectors = _compute_tfidf(docs)
    event_vec_map = {eid: vec for eid, vec in zip(all_event_ids, vectors)}

    hits = 0
    precision_sum = 0.0
    recall_sum = 0.0
    evaluated = 0

    for user in users:
        liked = [str(eid) for eid in user.get("liked_events", [])]
        if len(liked) < 2:
            continue

        # Leave-one-out: el último like es el ground truth
        train_liked = liked[:-1]
        held_out = liked[-1]

        # Perfil del usuario con los likes de entrenamiento
        train_vecs = [event_vec_map[eid] for eid in train_liked if eid in event_vec_map]
        if not train_vecs:
            continue

        from collections import defaultdict
        profile: dict = defaultdict(float)
        for vec in train_vecs:
            for term, val in vec.items():
                profile[term] += val
        for term in profile:
            profile[term] /= len(train_vecs)

        # Score contra todos los eventos (excepto los ya likeados en train)
        candidates = [
            eid for eid in all_event_ids
            if eid not in train_liked and eid in event_vec_map
        ]
        scored = sorted(
            candidates,
            key=lambda eid: _cosine_similarity(dict(profile), event_vec_map[eid]),
            reverse=True,
        )

        top_k = scored[:k]
        is_hit = held_out in top_k

        hits += int(is_hit)
        precision_sum += int(is_hit) / k
        recall_sum += int(is_hit) / 1  # solo 1 ítem de ground truth
        evaluated += 1

    if evaluated == 0:
        return {"error": "No hay usuarios con suficiente historial."}

    return {
        "k": k,
        "users_evaluated": evaluated,
        "hit_rate": round(hits / evaluated, 4),
        "precision_at_k": round(precision_sum / evaluated, 4),
        "recall_at_k": round(recall_sum / evaluated, 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--k", type=int, default=10, help="Top-K para métricas")
    args = parser.parse_args()

    print(f"[train_recommender] Evaluando recomendador con K={args.k}…")
    metrics = evaluate_recommender(k=args.k)

    print("\n=== Métricas del Recomendador ===")
    for key, val in metrics.items():
        print(f"  {key:20}: {val}")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
    }
    report_path = REPORT_DIR / "recommender_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\n[train_recommender] Reporte guardado en {report_path}")


if __name__ == "__main__":
    main()