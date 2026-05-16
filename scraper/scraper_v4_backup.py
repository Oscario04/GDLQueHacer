"""
scraper/scraper.py  — v4
Motor de scraping para GDL Qué Hacer.

Fuentes activas:
  1. Ticketmaster GDL      — eventos ciudad exacta
  2. Ticketmaster Jalisco  — todo Jalisco, multi-búsqueda
  3. SIC HTML              — scraper HTML original (sic_jalisco.py)
  4. SIC API               — ★ NUEVA: API REST oficial del SIC (más rápida y completa)
  5. datos.gob.mx          — ★ NUEVA: datasets CKAN abiertos del gobierno
  6. Eventbrite            — requiere EVENTBRITE_TOKEN
  7. GDL Nuevas Fuentes    — Meetup, AllEvents, Superboletos, Songkick, BandsInTown
  8. GDL Local             — sitios locales (ITESO, UdeG, El Informador, etc.)
  9. Boletia               — DESACTIVADO (404 en todas sus URLs a mayo 2026)

Uso:
    python -m scraper.scraper                              # todas las fuentes
    python -m scraper.scraper --source ticketmaster
    python -m scraper.scraper --source sic_api
    python -m scraper.scraper --source datos_gob_mx
    python -m scraper.scraper --source nuevas
    python -m scraper.scraper --dry-run
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
    "ticketmaster",
    "ticketmaster_jalisco",
    "sic",
    "sic_api",          # ★ nuevo
    "sic_datos_abiertos", # ★ nuevo
    "datos_gob_mx",     # ★ nuevo
    "eventbrite",
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

    # ── 1. Ticketmaster GDL ───────────────────────────────────────────
    if run_all or "ticketmaster" in sources:
        if not ticketmaster_key:
            logger.warning("TICKETMASTER_API_KEY no configurada.")
        else:
            try:
                from scraper.sources.ticketmaster import TicketmasterScraper
                scraper = TicketmasterScraper(api_key=ticketmaster_key)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Ticketmaster GDL: %d eventos", len(events))
            except Exception as exc:
                logger.error("❌  Ticketmaster GDL: %s", exc)

    # ── 2. Ticketmaster Jalisco ───────────────────────────────────────
    if run_all or "ticketmaster_jalisco" in sources:
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

    # ── 3. SIC Jalisco (HTML) ─────────────────────────────────────────
    if run_all or "sic" in sources:
        try:
            from scraper.sources.sic_jalisco import SICJaliscoScraper
            scraper = SICJaliscoScraper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  SIC Jalisco (HTML): %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  SIC Jalisco (HTML): %s", exc)

    # ── 4. SIC API REST (★ nueva fuente) ──────────────────────────────
    if run_all or "sic_api" in sources:
        try:
            from scraper.sources.sic_api import SICAPIScaper
            scraper = SICAPIScaper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  SIC API REST: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  SIC API REST: %s", exc)

    # ── 5. datos.gob.mx CKAN (★ nueva fuente) ────────────────────────
    if run_all or "datos_gob_mx" in sources:
        try:
            from scraper.sources.datos_gob_mx import DatosGobMxScraper
            scraper = DatosGobMxScraper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  datos.gob.mx: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  datos.gob.mx: %s", exc)

    # ── 6. Eventbrite ─────────────────────────────────────────────────
    if run_all or "eventbrite" in sources:
        if not eventbrite_token:
            logger.warning("EVENTBRITE_TOKEN no configurado.")
        else:
            try:
                from scraper.sources.eventbrite import EventbriteScraper
                scraper = EventbriteScraper(token=eventbrite_token)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Eventbrite: %d eventos", len(events))
            except Exception as exc:
                logger.error("❌  Eventbrite: %s", exc)

    # ── 7. GDL Nuevas Fuentes ─────────────────────────────────────────
    if run_all or "nuevas" in sources:
        try:
            from scraper.sources.gdl_nuevas_fuentes import GDLNuevasFuentesScraper
            scraper = GDLNuevasFuentesScraper()
            events = await scraper.fetch_all()
            all_raw_events.extend(events)
            logger.info("✅  GDL Nuevas Fuentes: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  GDL Nuevas Fuentes: %s", exc)

    # ── 8. GDL Local ──────────────────────────────────────────────────
    if run_all or "local" in sources:
        try:
            from scraper.sources.gdl_local import GDLLocalScraper
            scraper = GDLLocalScraper()
            events = await scraper.fetch_all()
            all_raw_events.extend(events)
            logger.info("✅  GDL Local: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  GDL Local: %s", exc)

    # ── Resumen ───────────────────────────────────────────────────────
    logger.info("━" * 50)
    logger.info("📦  Total eventos recolectados: %d", len(all_raw_events))

    if not all_raw_events:
        logger.info("Sin eventos nuevos.")
        return {"total": 0, "published": 0, "pending_review": 0, "skipped": 0, "errors": 0}

    if dry_run:
        logger.info("🔍  Dry-run — muestra:")
        for i, evt in enumerate(all_raw_events[:5]):
            logger.info(
                "  [%d] %s | fuente=%s | fecha=%s",
                i + 1,
                evt.get("title", "Sin título")[:60],
                evt.get("source_id", "?"),
                evt.get("date_start", "?"),
            )
        if len(all_raw_events) > 5:
            logger.info("  ... y %d eventos más", len(all_raw_events) - 5)
        return {"total": len(all_raw_events), "dry_run": True}

    from api.config.database import connect_db, get_db
    await connect_db()
    db = get_db()

    from api.services.ml_service import load_ml_models
    load_ml_models()

    logger.info("📥  Ingestando en MongoDB...")
    stats = await ingest_events(all_raw_events, db)

    logger.info("━" * 50)
    logger.info("✅  Completado: total=%d pub=%d rev=%d skip=%d err=%d",
                stats["total"], stats["published"], stats["pending_review"],
                stats["skipped"], stats["errors"])
    logger.info("━" * 50)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Scraper GDL Qué Hacer v4")
    parser.add_argument("--source", nargs="+", choices=ALL_SOURCES, default=["all"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sources = None if "all" in args.source else args.source
    logger.info("━" * 50)
    logger.info("🕷️   GDL Qué Hacer — Scraper v4")
    logger.info("   Modo    : %s", "DRY RUN" if args.dry_run else "PRODUCCIÓN")
    logger.info("   Fuentes : %s", sources or "todas")
    logger.info("   Hora    : %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    logger.info("━" * 50)

    stats = asyncio.run(run_scraper(sources=sources, dry_run=args.dry_run))
    logger.info("Finalizado: %s", stats)


if __name__ == "__main__":
    main()