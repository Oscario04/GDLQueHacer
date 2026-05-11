"""
config/database.py
Gestión de la conexión asíncrona a MongoDB Atlas con motor.
Patrón de singleton para reutilizar la conexión entre requests.
"""
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, GEOSPHERE, TEXT
from .settings import get_settings
import logging

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


async def connect_db() -> None:
    """Inicializa el cliente Motor y crea índices necesarios."""
    global _client, _db
    settings = get_settings()

    _client = AsyncIOMotorClient(
        settings.MONGODB_URI,
        serverSelectionTimeoutMS=5000,
        maxPoolSize=10,
    )
    _db = _client[settings.DB_NAME]

    # Verificar conexión
    await _client.admin.command("ping")
    logger.info("✅  Conectado a MongoDB Atlas — base: %s", settings.DB_NAME)

    # Crear índices (idempotentes)
    await _create_indexes()


async def disconnect_db() -> None:
    """Cierra el cliente Motor al apagar la aplicación."""
    global _client
    if _client:
        _client.close()
        logger.info("🔌  Conexión a MongoDB cerrada.")


def get_db() -> AsyncIOMotorDatabase:
    """Retorna la instancia de la base de datos. Usar como dependencia FastAPI."""
    if _db is None:
        raise RuntimeError("Base de datos no inicializada. Llama connect_db() primero.")
    return _db


async def _create_indexes() -> None:
    """Crea todos los índices necesarios para las colecciones."""
    db = get_db()

    # ── events ───────────────────────────────────────────────────────
    await db.events.create_indexes([
        IndexModel([("status", ASCENDING)]),
        IndexModel([("category", ASCENDING)]),
        IndexModel([("date_start", ASCENDING)]),
        IndexModel([("coordinates", GEOSPHERE)]),
        IndexModel([("title", TEXT), ("description", TEXT)]),
        IndexModel([("quality_ml", ASCENDING)]),
        IndexModel([("source_id", ASCENDING)]),
    ])

    # ── users ─────────────────────────────────────────────────────────
    await db.users.create_indexes([
        IndexModel([("email", ASCENDING)], unique=True),
        IndexModel([("role", ASCENDING)]),
    ])

    # ── user_interactions ─────────────────────────────────────────────
    await db.user_interactions.create_indexes([
        IndexModel([("user_id", ASCENDING), ("event_id", ASCENDING)]),
        IndexModel([("user_id", ASCENDING), ("created_at", ASCENDING)]),
        IndexModel([("event_id", ASCENDING)]),
    ])

    # ── user_preferences ─────────────────────────────────────────────
    await db.user_preferences.create_indexes([
        IndexModel([("user_id", ASCENDING)], unique=True),
    ])

    # ── reviews_manual ────────────────────────────────────────────────
    await db.reviews_manual.create_indexes([
        IndexModel([("status", ASCENDING)]),
        IndexModel([("event_id", ASCENDING)], unique=True),
        IndexModel([("created_at", ASCENDING)]),
    ])

    logger.info("📑  Índices de MongoDB verificados/creados.")