"""
scraper/scraper.py
Motor de scraping para GDL Qué Hacer.
Ejecutado por GitHub Actions cada 6 horas.

Fuentes:
  1. Eventbrite API  — eventos con API pública
  2. Scrapers HTML   — sitios sin API (con BeautifulSoup)

Uso:
    python -m scraper.scraper
    python -m scraper.scraper --source eventbrite
"""
import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Añadir directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s — %(message)s")


# ── Fuente: Eventbrite API ────────────────────────────────────────────

async def scrape_eventbrite(api_key: str, location: str = "Guadalajara, México") -> list[dict]:
    """
    Obtiene eventos de Eventbrite API para Guadalajara.
    Docs: https://www.eventbrite.com/platform/api
    """
    import httpx

    if not api_key:
        logger.warning("EVENTBRITE_API_KEY no configurada. Saltando fuente Eventbrite.")
        return []

    events = []
    url = "https://www.eventbriteapi.com/v3/events/search/"
    headers = {"Authorization": f"Bearer {api_key}"}

    params = {
        "location.address": location,
        "location.within": "50km",
        "expand": "venue,category",
        "sort_by": "date",
        "start_date.range_start": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "page_size": 50,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            page = 1
            while page <= 5:  # Máximo 5 páginas = 250 eventos
                params["page"] = page
                resp = await client.get(url, headers=headers, params=params)

                if resp.status_code == 429:
                    logger.warning("Rate limit Eventbrite. Esperando 60s...")
                    await asyncio.sleep(60)
                    continue

                resp.raise_for_status()
                data = resp.json()

                for evt in data.get("events", []):
                    venue = evt.get("venue") or {}
                    address = venue.get("address") or {}

                    raw = {
                        "title": evt.get("name", {}).get("text", ""),
                        "description": evt.get("description", {}).get("text", ""),
                        "date_start": evt.get("start", {}).get("local"),
                        "date_end": evt.get("end", {}).get("local"),
                        "location": venue.get("name", ""),
                        "latitude": address.get("latitude"),
                        "longitude": address.get("longitude"),
                        "url": evt.get("url", ""),
                        "image_url": (evt.get("logo") or {}).get("url"),
                        "price": 0 if evt.get("is_free") else None,
                        "source_id": "eventbrite",
                        "external_id": evt.get("id"),
                    }
                    events.append(raw)

                if not data.get("pagination", {}).get("has_more_items", False):
                    break
                page += 1

        except httpx.HTTPError as e:
            logger.error("Error en Eventbrite API: %s", e)

    logger.info("Eventbrite: %d eventos obtenidos", len(events))
    return events


# ── Fuente: Scraping HTML de secciones de agenda local ───────────────

async def scrape_generic_html(source_url: str, source_id: str) -> list[dict]:
    """
    Scraper genérico para sitios web de agenda de GDL.
    Extrae eventos de páginas con estructura semántica básica.
    """
    import httpx
    from bs4 import BeautifulSoup

    events = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        try:
            resp = await client.get(source_url, headers=headers)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Intentar extraer eventos de estructuras JSON-LD
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    import json
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get("@type") == "Event":
                        events.append(_parse_jsonld_event(data, source_id))
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get("@type") == "Event":
                                events.append(_parse_jsonld_event(item, source_id))
                except Exception:
                    pass

        except httpx.HTTPError as e:
            logger.error("Error scraping %s: %s", source_url, e)

    logger.info("%s: %d eventos extraídos", source_id, len(events))
    return events


def _parse_jsonld_event(data: dict, source_id: str) -> dict:
    """Parsea un evento en formato JSON-LD (schema.org/Event)."""
    location = data.get("location") or {}
    if isinstance(location, dict):
        address = location.get("address") or {}
        location_name = location.get("name") or (
            address.get("streetAddress", "") if isinstance(address, dict) else ""
        )
    else:
        location_name = str(location)

    return {
        "title": data.get("name", ""),
        "description": data.get("description", ""),
        "date_start": data.get("startDate"),
        "date_end": data.get("endDate"),
        "location": location_name,
        "url": data.get("url", ""),
        "image_url": data.get("image"),
        "price": 0,
        "source_id": source_id,
    }


# ── Orquestador principal ─────────────────────────────────────────────

async def run_scraper(sources: list[str] | None = None) -> dict:
    """
    Ejecuta todos los scrapers configurados e ingesta los eventos en MongoDB.
    """
    from api.config.database import connect_db, get_db
    from api.config.settings import get_settings
    from ml.pipeline import ingest_events

    settings = get_settings()
    await connect_db()
    db = get_db()

    all_raw_events = []

    # Fuentes habilitadas
    run_all = not sources

    if run_all or "eventbrite" in sources:
        eb_events = await scrape_eventbrite(settings.EVENTBRITE_API_KEY)
        all_raw_events.extend(eb_events)

    # Aquí puedes agregar más fuentes:
    # if run_all or "ocesa" in sources:
    #     events = await scrape_generic_html("https://www.ocesa.com.mx/...", "ocesa")
    #     all_raw_events.extend(events)

    logger.info("Total eventos recolectados: %d", len(all_raw_events))

    if not all_raw_events:
        logger.info("Sin eventos nuevos. Finalizando.")
        return {"total": 0}

    # Procesar e ingestar
    stats = await ingest_events(all_raw_events, db)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Scraper de GDL Qué Hacer")
    parser.add_argument(
        "--source",
        nargs="+",
        choices=["eventbrite", "all"],
        default=["all"],
        help="Fuentes a scrapear",
    )
    args = parser.parse_args()

    sources = None if "all" in args.source else args.source

    logger.info("🕷️   Iniciando scraper | Fuentes: %s", sources or "todas")
    stats = asyncio.run(run_scraper(sources))
    logger.info("✅  Scraping finalizado: %s", stats)


if __name__ == "__main__":
    main()