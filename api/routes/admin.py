"""
routes/admin.py
Endpoints exclusivos para el rol administrador.
"""
from fastapi import APIRouter, Depends, status, Query
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime
from bson import ObjectId
import asyncio
import json
import uuid

from api.config.database import get_db
from api.middleware.auth import require_admin, _decode_token
from api.models.event import EventCreate, EventStatus, EventPublic
from api.models.interaction import ReviewAction, ReviewStatus
from api.models.user import TokenData
from api.services.event_service import create_event_manual, update_event_status

router = APIRouter(prefix="/api/admin", tags=["Admin"])

# Almacén en memoria de logs por job_id
_job_logs: dict[str, list[str]] = {}
_job_status: dict[str, str] = {}  # "running" | "done" | "error"


# ── Cola de revisión manual ───────────────────────────────────────────

@router.get("/reviews")
async def list_reviews(
    status_filter: ReviewStatus = Query(ReviewStatus.pendiente),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    skip = (page - 1) * limit
    query = {"status": status_filter.value}
    total = await db.reviews_manual.count_documents(query)
    cursor = (
        db.reviews_manual.find(query)
        .sort("created_at", 1).skip(skip).limit(limit)
    )
    items = await cursor.to_list(length=limit)
    for item in items:
        item["_id"] = str(item.get("_id", ""))
    return {"total": total, "page": page, "limit": limit, "items": items}


@router.patch("/reviews/{event_id}")
async def review_event(
    event_id: str,
    action: ReviewAction,
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    now = datetime.utcnow()
    new_event_status = (
        EventStatus.publicado if action.action == ReviewStatus.aprobado
        else EventStatus.rechazado
    )
    await update_event_status(event_id, new_event_status, db)
    await db.reviews_manual.update_one(
        {"event_id": event_id},
        {"$set": {
            "status": action.action.value,
            "reviewer_id": admin.user_id,
            "notes": action.notes,
            "reviewed_at": now,
        }},
    )
    return {
        "event_id": event_id,
        "new_status": new_event_status.value,
        "reviewed_at": now.isoformat(),
    }


# ── Creación manual de eventos ────────────────────────────────────────

@router.post("/events", status_code=status.HTTP_201_CREATED)
async def create_event(
    data: EventCreate,
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    return await create_event_manual(data, admin.user_id, db)


# ── Estadísticas ──────────────────────────────────────────────────────

@router.get("/stats")
async def system_stats(
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> dict:
    total_events = await db.events.count_documents({})
    published = await db.events.count_documents({"status": EventStatus.publicado.value})
    total_users = await db.users.count_documents({})
    total_interactions = await db.user_interactions.count_documents({})

    pipeline = [
        {"$match": {"status": EventStatus.publicado.value}},
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
    ]
    category_dist_raw = await db.events.aggregate(pipeline).to_list(20)
    category_dist = {doc["_id"]: doc["count"] for doc in category_dist_raw}

    return {
        "events": {
            "total": total_events,
            "published": published,
            "pending_review": await db.reviews_manual.count_documents(
                {"status": ReviewStatus.pendiente.value}
            ),
            "by_category": category_dist,
        },
        "users": {"total": total_users},
        "interactions": {"total": total_interactions},
        "generated_at": datetime.utcnow().isoformat(),
    }


# ── Helpers de jobs ───────────────────────────────────────────────────

def _make_logger(job_id: str):
    """Retorna una función log y un logging.Handler para capturar logs externos."""
    import logging

    def log(msg: str):
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        _job_logs[job_id].append(line)

    class ListHandler(logging.Handler):
        def emit(self, record):
            _job_logs[job_id].append(
                f"[{datetime.now().strftime('%H:%M:%S')}] {record.getMessage()}"
            )

    return log, ListHandler()


# ── Scraper trigger con logs en tiempo real ───────────────────────────

@router.post("/trigger-scraper")
async def trigger_scraper(
    admin: TokenData = Depends(require_admin),
) -> dict:
    job_id = str(uuid.uuid4())
    _job_logs[job_id] = []
    _job_status[job_id] = "running"

    async def run():
        import logging
        log, handler = _make_logger(job_id)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            log("🕷️  Iniciando scraper...")
            from scraper.scraper import run_scraper
            stats = await run_scraper(sources=None, dry_run=False)
            log("━" * 40)
            log(f"✅  Scraping completado")
            log(f"   Total recolectados : {stats.get('total', 0)}")
            log(f"   Publicados         : {stats.get('published', 0)}")
            log(f"   En revisión manual : {stats.get('pending_review', 0)}")
            log(f"   Skipped            : {stats.get('skipped', 0)}")
            log(f"   Errores            : {stats.get('errors', 0)}")
            _job_status[job_id] = "done"
        except Exception as exc:
            log(f"❌  Error: {exc}")
            _job_status[job_id] = "error"
        finally:
            root_logger.removeHandler(handler)

    asyncio.create_task(run())
    return {"job_id": job_id, "message": "Scraper iniciado."}


# ── Reentrenamiento ML ────────────────────────────────────────────────

@router.post("/trigger-retrain")
async def trigger_retrain(
    admin: TokenData = Depends(require_admin),
) -> dict:
    job_id = str(uuid.uuid4())
    _job_logs[job_id] = []
    _job_status[job_id] = "running"

    async def run():
        import logging
        log, handler = _make_logger(job_id)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            log("🧠  Iniciando reentrenamiento de modelos ML...")
            log("   Cargando dataset sintético...")

            def _train():
                from ml.training.train_models import (
                    load_synthetic_data,
                    train_category_classifier,
                    train_quality_scorer,
                    train_knn_recommender,
                    train_svm_ranker,
                )
                events_df, interactions_df = load_synthetic_data()
                models = train_category_classifier(events_df)
                vectorizer = models["vectorizer"]
                train_quality_scorer(events_df)           # ✅ solo events_df
                train_knn_recommender(events_df, interactions_df, vectorizer)
                train_svm_ranker(events_df, interactions_df, vectorizer)

            await asyncio.get_event_loop().run_in_executor(None, _train)

            log("━" * 40)
            log("✅  Reentrenamiento completado.")
            log("   Modelos guardados en ml/saved_models/")
            log("   Recarga uvicorn para aplicar los nuevos modelos.")
            _job_status[job_id] = "done"
        except Exception as exc:
            log(f"❌  Error: {exc}")
            _job_status[job_id] = "error"
        finally:
            root_logger.removeHandler(handler)

    asyncio.create_task(run())
    return {"job_id": job_id, "message": "Reentrenamiento iniciado."}


# ── SSE: stream de logs por job_id ────────────────────────────────────

@router.get("/logs/{job_id}")
async def stream_logs(
    job_id: str,
    token: str = Query(...),
) -> StreamingResponse:
    # Validar token manualmente (EventSource no soporta headers)
    try:
        token_data = _decode_token(token)
        if token_data.role.value != "admin":
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail="Se requiere rol admin.")
    except Exception:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Token inválido.")

    async def event_generator():
        sent = 0
        while True:
            logs = _job_logs.get(job_id, [])
            while sent < len(logs):
                line = logs[sent]
                yield f"data: {json.dumps({'line': line})}\n\n"
                sent += 1

            job_st = _job_status.get(job_id, "running")
            if job_st in ("done", "error"):
                yield f"data: {json.dumps({'status': job_st})}\n\n"
                break

            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ── Export CSV de eventos ─────────────────────────────────────────────

@router.get("/export/events-csv")
async def export_events_csv(
    admin: TokenData = Depends(require_admin),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> StreamingResponse:
    import csv
    import io

    cursor = db.events.find(
        {},
        {
            "_id": 1, "title": 1, "description": 1, "category": 1,
            "status": 1, "date_start": 1, "date_end": 1, "location": 1,
            "price": 1, "quality_ml": 1, "tags": 1,
            "url_source": 1, "image_url": 1, "created_at": 1,
        }
    ).sort("created_at", -1)

    events = await cursor.to_list(length=10_000)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "title", "description", "category", "status",
        "date_start", "date_end", "location", "price",
        "quality_ml", "tags", "url_source", "image_url", "created_at",
    ])
    for e in events:
        writer.writerow([
            str(e.get("_id", "")),
            e.get("title", ""),
            e.get("description", ""),
            e.get("category", ""),
            e.get("status", ""),
            e.get("date_start", ""),
            e.get("date_end", ""),
            e.get("location", ""),
            e.get("price", ""),
            e.get("quality_ml", ""),
            ",".join(e.get("tags", [])),
            e.get("url_source", ""),
            e.get("image_url", ""),
            e.get("created_at", ""),
        ])

    output.seek(0)
    filename = f"eventos_gdl_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )