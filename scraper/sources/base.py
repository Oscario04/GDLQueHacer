"""
base.py
Orquestador principal del scraper.
  1. Llama a cada fuente (Eventbrite, etc.)
  2. Normaliza los eventos al esquema unificado
  3. Geocodifica la dirección con Nominatim si no hay coords
  4. Guarda en MongoDB con status='recolectado' → 'normalizado'
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from pymongo import MongoClient, UpdateOne

from .eventbrite import EventbriteScraper


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "GDLQueHacer/1.0 (contacto@gdlquehacer.mx)"}


# ---------------------------------------------------------------------------
# Geocodificación
# ---------------------------------------------------------------------------

def geocode(address: str) -> Optional[dict]:
    """Llama a Nominatim y devuelve {'lat': float, 'lon': float} o None."""
    if not address:
        return None
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"q": address, "format": "json", "limit": 1},
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            return {"lat": float(results[0]["lat"]), "lon": float(results[0]["lon"])}
    except Exception as exc:
        print(f"[geocode] Error para '{address}': {exc}")
    time.sleep(1)  # Respetar rate-limit de Nominatim
    return None


# ---------------------------------------------------------------------------
# Normalización del esquema de evento
# ---------------------------------------------------------------------------

def normalize_event(raw: dict, source: str) -> dict:
    """
    Convierte el dict crudo de cualquier fuente al esquema unificado:

    {
        source_id: str,          # ID original de la fuente
        source: str,             # "eventbrite" | "manual" | ...
        title: str,
        description: str,
        category: str,
        tags: list[str],
        image_url: str | None,
        start_date: datetime,
        end_date: datetime | None,
        price: float | None,     # 0.0 = gratis
        currency: str,
        url: str,
        location: {
            address: str,
            lat: float | None,
            lon: float | None,
        },
        status: "normalizado",
        quality_ml: None,        # se calcula después por el clasificador
        created_at: datetime,
        updated_at: datetime,
    }
    """
    now = datetime.now(timezone.utc)

    location = raw.get("location", {})
    lat = location.get("lat")
    lon = location.get("lon")
    address = location.get("address", "")

    # Geocodificar si faltan coordenadas
    if address and (lat is None or lon is None):
        coords = geocode(f"{address}, Guadalajara, Mexico")
        if coords:
            lat, lon = coords["lat"], coords["lon"]

    return {
        "source_id": str(raw.get("source_id", "")),
        "source": source,
        "title": (raw.get("title") or "").strip(),
        "description": (raw.get("description") or "").strip(),
        "category": (raw.get("category") or "").strip(),
        "tags": raw.get("tags", []),
        "image_url": raw.get("image_url"),
        "start_date": raw.get("start_date"),
        "end_date": raw.get("end_date"),
        "price": raw.get("price"),
        "currency": raw.get("currency", "MXN"),
        "url": raw.get("url", ""),
        "location": {
            "address": address,
            "lat": lat,
            "lon": lon,
        },
        "status": "normalizado",
        "quality_ml": None,
        "created_at": now,
        "updated_at": now,
    }


# ---------------------------------------------------------------------------
# Guardado en MongoDB (upsert por source + source_id)
# ---------------------------------------------------------------------------

def save_events(events: list[dict]) -> None:
    uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    col = client["gdlquehacer"]["events"]

    ops = []
    for event in events:
        ops.append(
            UpdateOne(
                {"source": event["source"], "source_id": event["source_id"]},
                {
                    "$set": {k: v for k, v in event.items() if k != "created_at"},
                    "$setOnInsert": {"created_at": event["created_at"]},
                },
                upsert=True,
            )
        )

    if ops:
        result = col.bulk_write(ops, ordered=False)
        print(
            f"[save_events] Upserts: {result.upserted_count} nuevos, "
            f"{result.modified_count} actualizados"
        )
    client.close()


# ---------------------------------------------------------------------------
# Orquestador
# ---------------------------------------------------------------------------

def run_all_scrapers() -> None:
    token = os.environ.get("EVENTBRITE_TOKEN", "")
    all_events: list[dict] = []

    # ── Eventbrite ──
    print("[scraper] Iniciando Eventbrite…")
    try:
        eb = EventbriteScraper(token=token)
        raw_events = eb.fetch_events()
        for raw in raw_events:
            normalized = normalize_event(raw, source="eventbrite")
            all_events.append(normalized)
        print(f"[scraper] Eventbrite: {len(raw_events)} eventos obtenidos")
    except Exception as exc:
        print(f"[scraper] Error Eventbrite: {exc}")

    # ── Aquí puedes agregar más scrapers en el futuro ──
    # from .otra_fuente import OtraFuenteScraper
    # ...

    print(f"[scraper] Total eventos a guardar: {len(all_events)}")
    if all_events:
        save_events(all_events)


if __name__ == "__main__":
    run_all_scrapers()