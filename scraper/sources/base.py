"""
base.py  — v2
Orquestador principal del scraper.
  1. Llama a cada fuente (Ticketmaster, SIC, Eventbrite, etc.)
  2. Normaliza los eventos al esquema unificado
  3. Geocodifica la dirección con Nominatim si no hay coords
  4. Guarda en MongoDB con upsert (source + source_id)

CAMBIOS v2:
  - Esquema normalizado agrega `estado` y `ciudad` para filtrado en front.
  - Geocodificación preserva `estado`/`ciudad` que vienen del raw.
  - Sin filtros geográficos — el scraper captura TODO México.
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
NOMINATIM_HEADERS = {"User-Agent": "GDLQueHacer/2.0 (contacto@gdlquehacer.mx)"}


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
    Convierte el dict crudo de cualquier fuente al esquema unificado.

    Esquema:
    {
        source_id: str,
        source: str,
        title: str,
        description: str,
        category: str,
        tags: list[str],
        image_url: str | None,
        start_date: datetime,
        end_date: datetime | None,
        price: float | None,
        currency: str,
        url: str,
        location: {
            address: str,
            lat: float | None,
            lon: float | None,
        },
        # Campos para filtrado geográfico en el front:
        estado: str,          # "Jalisco", "CDMX", etc.
        ciudad: str,          # "Guadalajara", "Zapopan", etc.
        status: "normalizado",
        quality_ml: None,
        created_at: datetime,
        updated_at: datetime,
    }
    """
    now = datetime.now(timezone.utc)

    location = raw.get("location", {})
    lat = raw.get("latitude") or location.get("lat")
    lon = raw.get("longitude") or location.get("lon")
    address = raw.get("location") if isinstance(raw.get("location"), str) else location.get("address", "")

    # Geocodificar si faltan coordenadas y hay dirección
    if address and isinstance(address, str) and (lat is None or lon is None):
        coords = geocode(f"{address}, México")
        if coords:
            lat, lon = coords["lat"], coords["lon"]

    # Campos de ubicación estructurada (vienen de scrapers que los separan)
    estado = (raw.get("estado") or "").strip()
    ciudad = (raw.get("ciudad") or "").strip()

    return {
        "source_id":   str(raw.get("source_id", "") or raw.get("external_id", "")),
        "source":      source,
        "title":       (raw.get("title") or "").strip(),
        "description": (raw.get("description") or "").strip(),
        "category":    (raw.get("category") or "").strip(),
        "tags":        raw.get("tags", []),
        "image_url":   raw.get("image_url"),
        "start_date":  raw.get("date_start") or raw.get("start_date"),
        "end_date":    raw.get("date_end") or raw.get("end_date"),
        "price":       raw.get("price"),
        "currency":    raw.get("currency", "MXN"),
        "url":         raw.get("url", ""),
        "location": {
            "address": address if isinstance(address, str) else "",
            "lat":     float(lat) if lat is not None else None,
            "lon":     float(lon) if lon is not None else None,
        },
        "estado":      estado,
        "ciudad":      ciudad,
        "status":      "normalizado",
        "quality_ml":  None,
        "created_at":  now,
        "updated_at":  now,
    }


# ---------------------------------------------------------------------------
# Guardado en MongoDB
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
# Orquestador (versión sync legacy — usa scraper.py para el async completo)
# ---------------------------------------------------------------------------

def run_all_scrapers() -> None:
    token = os.environ.get("EVENTBRITE_TOKEN", "")
    all_events: list[dict] = []

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

    print(f"[scraper] Total eventos a guardar: {len(all_events)}")
    if all_events:
        save_events(all_events)


if __name__ == "__main__":
    run_all_scrapers()