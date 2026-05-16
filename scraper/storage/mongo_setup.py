"""
scraper/storage/mongo_setup.py
Configuración de índices MongoDB para el proyecto GDL Qué Hacer.

Ejecutar una sola vez (o en cada deploy para que sea idempotente):
    python -m scraper.storage.mongo_setup

Índices creados:
  events:
    - (source, source_id) unique     — deduplicación exacta por fuente
    - fingerprint unique             — deduplicación cross-source
    - start_date                     — filtrado y ordenamiento por fecha
    - estado                         — filtrado geográfico
    - ciudad                         — filtrado geográfico
    - category                       — filtrado por categoría
    - location (2dsphere)            — búsquedas geoespaciales

  geocache:
    - address unique                 — lookup O(1)
"""
from __future__ import annotations

import logging
import os

from pymongo import MongoClient, ASCENDING, GEOSPHERE

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")


def setup_indexes(mongo_uri: str | None = None) -> None:
    uri = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    db = client["gdlquehacer"]

    # ── Colección events ─────────────────────────────────────────────
    events = db["events"]

    # Deduplicación exacta
    events.create_index(
        [("source", ASCENDING), ("source_id", ASCENDING)],
        unique=True,
        name="idx_source_unique",
    )

    # Deduplicación cross-source (fingerprint)
    events.create_index(
        "fingerprint",
        unique=True,
        sparse=True,          # sparse porque registros viejos pueden no tenerlo
        name="idx_fingerprint",
    )

    # Filtrado por fecha
    events.create_index([("start_date", ASCENDING)], name="idx_start_date")
    events.create_index(
        [("start_date", ASCENDING), ("end_date", ASCENDING)],
        name="idx_date_range",
    )

    # Filtrado geográfico
    events.create_index([("estado", ASCENDING)], name="idx_estado")
    events.create_index([("ciudad", ASCENDING)], name="idx_ciudad")
    events.create_index(
        [("estado", ASCENDING), ("ciudad", ASCENDING)],
        name="idx_estado_ciudad",
    )

    # Filtrado por categoría
    events.create_index([("category", ASCENDING)], name="idx_category")

    # Filtrado combinado (el más común en la API)
    events.create_index(
        [("estado", ASCENDING), ("start_date", ASCENDING), ("category", ASCENDING)],
        name="idx_estado_fecha_cat",
    )

    # Geoespacial 2dsphere para búsquedas por coordenadas
    events.create_index(
        [("location", GEOSPHERE)],
        name="idx_location_geo",
        sparse=True,
    )

    # Estado del evento para moderar
    events.create_index([("status", ASCENDING)], name="idx_status")

    # TTL automático: eliminar eventos pasados después de 90 días
    # (comentado por defecto — descomentar si lo quieres activo)
    # events.create_index(
    #     [("end_date", ASCENDING)],
    #     expireAfterSeconds=7776000,  # 90 días
    #     name="idx_ttl_end_date",
    # )

    logger.info("✅  Índices 'events' configurados")

    # ── Colección geocache ───────────────────────────────────────────
    geocache = db["geocache"]
    geocache.create_index("address", unique=True, name="idx_geocache_address")
    logger.info("✅  Índices 'geocache' configurados")

    # ── Colección admin_logs (opcional) ─────────────────────────────
    db["admin_logs"].create_index([("created_at", ASCENDING)], name="idx_log_date")
    logger.info("✅  Índices 'admin_logs' configurados")

    client.close()
    logger.info("🎉  Setup de MongoDB completo")


def print_index_report(mongo_uri: str | None = None) -> None:
    """Muestra los índices actuales de cada colección."""
    uri = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    db = client["gdlquehacer"]

    for col_name in ["events", "geocache", "admin_logs"]:
        col = db[col_name]
        indexes = list(col.list_indexes())
        logger.info("\n── %s (%d índices) ──", col_name, len(indexes))
        for idx in indexes:
            logger.info("   %s: %s", idx["name"], idx["key"])

    client.close()


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from dotenv import load_dotenv
    load_dotenv()

    setup_indexes()
    print_index_report()
