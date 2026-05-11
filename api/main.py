"""
api/main.py
Punto de entrada principal de la API FastAPI.
Compatible con Vercel serverless (Mangum) y uvicorn local.
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mangum import Mangum

from api.config.settings import get_settings
from api.config.database import connect_db, disconnect_db
from api.services.ml_service import load_ml_models
from api.routes import auth, events, interactions, admin

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)
settings = get_settings()


# ── Lifespan (startup / shutdown) ─────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Eventos de ciclo de vida de la aplicación."""
    logger.info("🚀  Iniciando GDL Qué Hacer API...")
    await connect_db()
    load_ml_models()
    logger.info("✅  API lista.")
    yield
    logger.info("🛑  Cerrando API...")
    await disconnect_db()


# ── Aplicación FastAPI ────────────────────────────────────────────────
app = FastAPI(
    title="GDL Qué Hacer — API",
    description=(
        "API REST para la plataforma de descubrimiento de eventos urbanos "
        "en la Zona Metropolitana de Guadalajara. "
        "Incluye scraping automatizado, pipeline ML de clasificación y calidad, "
        "autenticación JWT y recomendaciones personalizadas con KNN + SVM."
    ),
    version="2.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Cron-Secret"],
)

# ── Routers ───────────────────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(events.router)
app.include_router(interactions.router)
app.include_router(admin.router)


# ── Manejador global de errores ───────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Error no manejado: %s — %s", type(exc).__name__, exc, exc_info=True)
    if settings.ENVIRONMENT == "development":
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": str(exc), "type": type(exc).__name__},
        )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error interno del servidor."},
    )


# ── Health check ──────────────────────────────────────────────────────
@app.get("/api/health", tags=["System"], summary="Health check")
async def health():
    return {
        "status": "ok",
        "version": "2.1.0",
        "environment": settings.ENVIRONMENT,
    }


# ── Vercel serverless handler ─────────────────────────────────────────
handler = Mangum(app, lifespan="off")

# ── Desarrollo local ──────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)