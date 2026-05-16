"""
scraper/scraper.py  — v6
Motor de scraping para GDL Qué Hacer — cobertura nacional completa.

Fuentes activas (objetivo: 5,000–10,000 eventos):
  1.  Ticketmaster Nacional v2  — 50+ ciudades + 25 geo + géneros   → ~3,000
  2.  Ticketmaster Jalisco      — multi-radio + segmentos (legacy)   → ~500
  3.  Eventbrite Nacional v2    — 25 geo + 50 queries + categorías   → ~1,500
  4.  Songkick                  — ★ NUEVO: 20 metro areas México      → ~500
  5.  Fuentes Nacionales        — ★ NUEVO: CDMX, INBA, UNAM, etc.   → ~800
  6.  SIC HTML                  — scraper HTML original              → ~300
  7.  SIC API REST              — API REST oficial del SIC           → ~400
  8.  SIC Datos Abiertos        — CSVs abiertos del SIC             → ~300
  9.  datos.gob.mx              — datasets CKAN del gobierno         → ~200
  10. GDL Nuevas Fuentes        — Meetup, AllEvents, Superboletos    → ~300
  11. GDL Local                 — sitios locales (ITESO, UdeG, etc.) → ~200

Total estimado: 5,000-10,000 eventos únicos.

Uso:
    python -m scraper.scraper                                # todas las fuentes
    python -m scraper.scraper --source ticketmaster_nacional
    python -m scraper.scraper --source eventbrite_nacional
    python -m scraper.scraper --source songkick
    python -m scraper.scraper --source fuentes_nacionales
    python -m scraper.scraper --dry-run
    python -m scraper.scraper --setup-db     # crear índices MongoDB
"""
import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

ALL_SOURCES = [
    "ticketmaster_nacional",
    "ticketmaster_jalisco",
    "eventbrite_nacional",
    "eventbrite",
    "songkick",              # ★ nuevo
    "fuentes_nacionales",    # ★ nuevo
    "sic",
    "sic_api",
    "ticketmaster_latam",
    "sic_datos_abiertos",
    "massive_scraper",
    "datos_gob_mx",
    "nuevas",
    "local",
    "all",
]


async def run_scraper(
    sources: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    from ml.pipeline import ingest_events

    eventbrite_token = os.getenv("EVENTBRITE_TOKEN")
    ticketmaster_key = os.getenv("TICKETMASTER_API_KEY")

    run_all = not sources or "all" in sources
    all_raw_events: list[dict] = []

    # ── 1. Ticketmaster Nacional v2 ───────────────────────────────────
    if run_all or "ticketmaster_nacional" in (sources or []):
        if not ticketmaster_key:
            logger.warning("TICKETMASTER_API_KEY no configurada.")
        else:
            try:
                from scraper.sources.ticketmaster_nacional import TicketmasterNacionalScraper
                scraper = TicketmasterNacionalScraper(api_key=ticketmaster_key)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Ticketmaster Nacional: %d eventos", len(events))
            except Exception as exc:
                logger.error("❌  Ticketmaster Nacional: %s", exc)

    # ── 2. Ticketmaster Jalisco (legacy) ─────────────────────────────
    if run_all or "ticketmaster_jalisco" in (sources or []):
        if not ticketmaster_key:
            logger.warning("TICKETMASTER_API_KEY no configurada.")
        else:
            try:
                from scraper.sources.ticketmaster_jalisco import TicketmasterJaliscoScraper
                scraper = TicketmasterJaliscoScraper(api_key=ticketmaster_key)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Ticketmaster Jalisco: %d eventos", len(events))
            except Exception as exc:
                logger.error("❌  Ticketmaster Jalisco: %s", exc)

    # ── 3. Eventbrite Nacional v2 ─────────────────────────────────────
    if run_all or "eventbrite_nacional" in (sources or []):
        if not eventbrite_token:
            logger.warning("EVENTBRITE_TOKEN no configurado.")
        else:
            try:
                from scraper.sources.eventbrite_nacional import EventbriteNacionalScraper
                scraper = EventbriteNacionalScraper(token=eventbrite_token)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Eventbrite Nacional: %d eventos", len(events))
            except Exception as exc:
                logger.error("❌  Eventbrite Nacional: %s", exc)

    # ── 4. Eventbrite GDL (legacy, solo si se pide explícitamente) ────
    if "eventbrite" in (sources or []):
        if not eventbrite_token:
            logger.warning("EVENTBRITE_TOKEN no configurado.")
        else:
            try:
                from scraper.sources.eventbrite import EventbriteScraper
                scraper = EventbriteScraper(token=eventbrite_token)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Eventbrite GDL: %d eventos", len(events))
            except Exception as exc:
                logger.error("❌  Eventbrite GDL: %s", exc)

    # ── 5. Songkick ★ ─────────────────────────────────────────────────
    if run_all or "songkick" in (sources or []):
        try:
            from scraper.sources.songkick import SongkickScraper
            scraper = SongkickScraper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  Songkick: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  Songkick: %s", exc)

    # ── 6. Fuentes Nacionales ★ ───────────────────────────────────────
    if run_all or "fuentes_nacionales" in (sources or []):
        try:
            from scraper.sources.fuentes_nacionales import FuentesNacionalesScraper
            scraper = FuentesNacionalesScraper()
            events = await scraper.fetch_all()
            all_raw_events.extend(events)
            logger.info("✅  Fuentes Nacionales: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  Fuentes Nacionales: %s", exc)

    # ── 7. SIC Jalisco (HTML) ─────────────────────────────────────────
    if run_all or "sic" in (sources or []):
        try:
            from scraper.sources.sic_jalisco import SICJaliscoScraper
            scraper = SICJaliscoScraper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  SIC Jalisco (HTML): %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  SIC Jalisco (HTML): %s", exc)

    # ── 8. SIC API REST ───────────────────────────────────────────────
    if run_all or "sic_api" in (sources or []):
        try:
            from scraper.sources.sic_api import SICAPIScaper
            scraper = SICAPIScaper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  SIC API REST: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  SIC API REST: %s", exc)

    # ── 9. SIC Datos Abiertos ─────────────────────────────────────────
    if run_all or "sic_datos_abiertos" in (sources or []):
        try:
            from scraper.sources.sic_datos_abiertos import SICDatosAbiertos
            scraper = SICDatosAbiertos()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  SIC Datos Abiertos: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  SIC Datos Abiertos: %s", exc)

    # ── 10. datos.gob.mx CKAN ─────────────────────────────────────────
    if run_all or "datos_gob_mx" in (sources or []):
        try:
            from scraper.sources.datos_gob_mx import DatosGobMxScraper
            scraper = DatosGobMxScraper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  datos.gob.mx: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  datos.gob.mx: %s", exc)

    # ── 11. GDL Nuevas Fuentes ────────────────────────────────────────
    if run_all or "nuevas" in (sources or []):
        try:
            from scraper.sources.gdl_nuevas_fuentes import GDLNuevasFuentesScraper
            scraper = GDLNuevasFuentesScraper()
            events = await scraper.fetch_all()
            all_raw_events.extend(events)
            logger.info("✅  GDL Nuevas Fuentes: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  GDL Nuevas Fuentes: %s", exc)

    # ── 12. GDL Local ─────────────────────────────────────────────────
    if run_all or "local" in (sources or []):
        try:
            from scraper.sources.gdl_local import GDLLocalScraper
            scraper = GDLLocalScraper()
            events = await scraper.fetch_all()
            all_raw_events.extend(events)
            logger.info("✅  GDL Local: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  GDL Local: %s", exc)

    # ── Resumen bruto ─────────────────────────────────────────────────
    logger.info("━" * 60)
    logger.info("📦  Total bruto recolectado: %d eventos", len(all_raw_events))

    if not all_raw_events:
        logger.info("Sin eventos nuevos.")
        return {"total": 0, "published": 0, "pending_review": 0, "skipped": 0, "errors": 0}

    # ── Deduplicación en memoria (fingerprint SHA-256) ─────────────────
    try:
        from scraper.pipelines.deduplicate import deduplicate_events
        all_raw_events, dupes = deduplicate_events(all_raw_events)
        logger.info(
            "🔁  Deduplicación: %d únicos (%d eliminados)",
            len(all_raw_events), dupes,
        )
    except Exception as exc:
        logger.warning("Deduplicación falló, continuando: %s", exc)

    logger.info("📊  Total después de deduplicar: %d eventos", len(all_raw_events))

    if dry_run:
        logger.info("🔍  Dry-run — muestra de los primeros 10:")
        for i, evt in enumerate(all_raw_events[:10]):
            logger.info(
                "  [%d] %s | fuente=%-20s | fecha=%s",
                i + 1,
                evt.get("title", "Sin título")[:50],
                evt.get("source_id", "?"),
                str(evt.get("date_start", "?"))[:10],
            )
        if len(all_raw_events) > 10:
            logger.info("  … y %d eventos más", len(all_raw_events) - 10)
        return {"total": len(all_raw_events), "dry_run": True}

    # ── Ingestión en MongoDB ───────────────────────────────────────────
    from api.config.database import connect_db, get_db
    await connect_db()
    db = get_db()

    from api.services.ml_service import load_ml_models
    load_ml_models()

    logger.info("📥  Ingestando en MongoDB…")
    stats = await ingest_events(all_raw_events, db)

    logger.info("━" * 60)
    logger.info(
        "✅  Completado: total=%d pub=%d rev=%d skip=%d err=%d",
        stats["total"], stats["published"], stats["pending_review"],
        stats["skipped"], stats["errors"],
    )
    logger.info("━" * 60)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Scraper GDL Qué Hacer v6")
    parser.add_argument(
        "--source",
        nargs="+",
        choices=ALL_SOURCES,
        default=["all"],
        help="Fuentes a ejecutar (default: todas)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra eventos sin guardar en MongoDB",
    )
    parser.add_argument(
        "--setup-db",
        action="store_true",
        help="Crea índices MongoDB y sale",
    )
    args = parser.parse_args()

    if args.setup_db:
        from scraper.storage.mongo_setup import setup_indexes
        setup_indexes()
        return

    sources = None if "all" in args.source else args.source

    logger.info("━" * 60)
    logger.info("🕷️   GDL Qué Hacer — Scraper v6")
    logger.info("   Modo    : %s", "DRY RUN" if args.dry_run else "PRODUCCIÓN")
    logger.info("   Fuentes : %s", sources or "TODAS (11 fuentes)")
    logger.info("   Hora    : %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    logger.info("━" * 60)

    stats = asyncio.run(run_scraper(sources=sources, dry_run=args.dry_run))
    logger.info("Finalizado: %s", stats)


if __name__ == "__main__":
    main()