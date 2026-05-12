"""
scraper/scraper.py
Motor de scraping para GDL Qué Hacer.
Ejecutado por GitHub Actions cada 6 horas.

Fuentes:
  1. Ticketmaster GDL  — ciudad Guadalajara (rápido, pocos resultados)
  2. Ticketmaster JAL  — todo Jalisco por geopoint + multi-ciudad (NUEVO)
  3. Boletia           — plataforma MX de tickets, scraping HTML (NUEVO)
  4. SIC Jalisco       — Sistema de Información Cultural, eventos culturales (NUEVO)
  5. Eventbrite        — requiere EVENTBRITE_TOKEN en .env
  6. Sitios locales GDL— sin API key (JSON-LD + HTML scraping)

Uso:
    python -m scraper.scraper                           # todas las fuentes
    python -m scraper.scraper --source ticketmaster
    python -m scraper.scraper --source ticketmaster_jalisco
    python -m scraper.scraper --source boletia
    python -m scraper.scraper --source sic
    python -m scraper.scraper --source eventbrite
    python -m scraper.scraper --source local
    python -m scraper.scraper --source ticketmaster boletia sic
    python -m scraper.scraper --dry-run               # sin guardar en BD
"""
import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Cargar .env ANTES de cualquier import de api/ml ──────────────────
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

# Fuentes disponibles (para --source choices)
ALL_SOURCES = [
    "ticketmaster",
    "ticketmaster_jalisco",
    "boletia",
    "sic",
    "eventbrite",
    "local",
    "all",
]


async def run_scraper(
    sources: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Ejecuta los scrapers configurados e ingesta los eventos en MongoDB.

    Args:
        sources: Lista de fuentes a usar. None = todas.
        dry_run: Si True, procesa eventos pero NO guarda en BD.

    Returns:
        Estadísticas del proceso.
    """
    from ml.pipeline import ingest_events

    eventbrite_token = os.getenv("EVENTBRITE_TOKEN")
    ticketmaster_key = os.getenv("TICKETMASTER_API_KEY")

    run_all = not sources or "all" in sources
    all_raw_events: list[dict] = []

    # ── 1. Ticketmaster GDL (original, rápido) ────────────────────────
    if run_all or "ticketmaster" in sources:
        if not ticketmaster_key:
            logger.warning("TICKETMASTER_API_KEY no configurada. Saltando Ticketmaster.")
        else:
            try:
                from scraper.sources.ticketmaster import TicketmasterScraper
                scraper = TicketmasterScraper(api_key=ticketmaster_key)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Ticketmaster GDL: %d eventos obtenidos", len(events))
            except Exception as exc:
                logger.error("❌  Error en Ticketmaster GDL: %s", exc)

    # ── 2. Ticketmaster Jalisco (expandido, todo el estado) ───────────
    if run_all or "ticketmaster_jalisco" in sources:
        if not ticketmaster_key:
            logger.warning("TICKETMASTER_API_KEY no configurada. Saltando TM Jalisco.")
        else:
            try:
                from scraper.sources.ticketmaster_jalisco import TicketmasterJaliscoScraper
                scraper = TicketmasterJaliscoScraper(api_key=ticketmaster_key)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Ticketmaster Jalisco: %d eventos obtenidos", len(events))
            except Exception as exc:
                logger.error("❌  Error en Ticketmaster Jalisco: %s", exc)

    # ── 3. Boletia ────────────────────────────────────────────────────
    if run_all or "boletia" in sources:
        try:
            from scraper.sources.boletia import BoletiaScraper
            scraper = BoletiaScraper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  Boletia: %d eventos obtenidos", len(events))
        except Exception as exc:
            logger.error("❌  Error en Boletia: %s", exc)

    # ── 4. SIC Jalisco ────────────────────────────────────────────────
    if run_all or "sic" in sources:
        try:
            from scraper.sources.sic_jalisco import SICJaliscoScraper
            scraper = SICJaliscoScraper()
            events = await scraper.fetch_events()
            all_raw_events.extend(events)
            logger.info("✅  SIC Jalisco: %d eventos obtenidos", len(events))
        except Exception as exc:
            logger.error("❌  Error en SIC Jalisco: %s", exc)

    # ── 5. Eventbrite ─────────────────────────────────────────────────
    if run_all or "eventbrite" in sources:
        if not eventbrite_token:
            logger.warning("EVENTBRITE_TOKEN no configurado. Saltando Eventbrite.")
        else:
            try:
                from scraper.sources.eventbrite import EventbriteScraper
                scraper = EventbriteScraper(token=eventbrite_token)
                events = await scraper.fetch_events()
                all_raw_events.extend(events)
                logger.info("✅  Eventbrite: %d eventos obtenidos", len(events))
            except Exception as exc:
                logger.error("❌  Error en Eventbrite: %s", exc)

    # ── 6. Sitios locales GDL ─────────────────────────────────────────
    if run_all or "local" in sources:
        try:
            from scraper.sources.gdl_local import GDLLocalScraper
            scraper = GDLLocalScraper()
            events = await scraper.fetch_all()
            all_raw_events.extend(events)
            logger.info("✅  GDL Local: %d eventos obtenidos", len(events))
        except Exception as exc:
            logger.error("❌  Error en GDL Local: %s", exc)

    # ── Resumen de recolección ────────────────────────────────────────
    logger.info("━" * 50)
    logger.info("📦  Total eventos recolectados: %d", len(all_raw_events))

    if not all_raw_events:
        logger.info("Sin eventos nuevos. Finalizando.")
        return {"total": 0, "published": 0, "pending_review": 0, "skipped": 0, "errors": 0}

    # ── Modo dry-run: solo mostrar muestra, no guardar ────────────────
    if dry_run:
        logger.info("🔍  Modo dry-run: mostrando muestra de eventos...")
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

    # ── Conectar a MongoDB ────────────────────────────────────────────
    from api.config.database import connect_db, get_db
    await connect_db()
    db = get_db()

    # ── Cargar modelos ML antes de ingestar ───────────────────────────
    from api.services.ml_service import load_ml_models
    load_ml_models()

    # ── Ingestión en MongoDB ──────────────────────────────────────────
    logger.info("📥  Iniciando ingestión en MongoDB...")
    stats = await ingest_events(all_raw_events, db)

    logger.info("━" * 50)
    logger.info("✅  Scraping completado:")
    logger.info("   Total recolectados : %d", stats["total"])
    logger.info("   Publicados         : %d", stats["published"])
    logger.info("   En revisión manual : %d", stats["pending_review"])
    logger.info("   Skipped (duplicado): %d", stats["skipped"])
    logger.info("   Errores            : %d", stats["errors"])
    logger.info("━" * 50)

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Scraper de GDL Qué Hacer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python -m scraper.scraper                              # todas las fuentes
  python -m scraper.scraper --source ticketmaster        # solo TM ciudad GDL
  python -m scraper.scraper --source ticketmaster_jalisco # todo Jalisco
  python -m scraper.scraper --source boletia             # solo Boletia
  python -m scraper.scraper --source sic                 # solo SIC cultural
  python -m scraper.scraper --source boletia sic ticketmaster_jalisco
  python -m scraper.scraper --dry-run                    # sin guardar en BD
        """,
    )
    parser.add_argument(
        "--source",
        nargs="+",
        choices=ALL_SOURCES,
        default=["all"],
        help="Fuentes a scrapear (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta sin guardar en BD. Útil para pruebas.",
    )
    args = parser.parse_args()

    sources = None if "all" in args.source else args.source
    mode = "DRY RUN" if args.dry_run else "PRODUCCIÓN"

    logger.info("━" * 50)
    logger.info("🕷️   GDL Qué Hacer — Scraper")
    logger.info("   Modo    : %s", mode)
    logger.info("   Fuentes : %s", sources or "todas")
    logger.info("   Hora    : %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    logger.info("━" * 50)

    stats = asyncio.run(run_scraper(sources=sources, dry_run=args.dry_run))
    logger.info("Finalizado: %s", stats)


if __name__ == "__main__":
    main()