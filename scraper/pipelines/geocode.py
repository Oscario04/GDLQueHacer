"""
scraper/pipelines/geocode.py
Geocodificación con caché persistente en MongoDB.

Reduce llamadas a Nominatim en ~95%. El caché vive en la colección
`geocache` de MongoDB y persiste entre corridas.

Esquema de geocache:
    {
        address: str,       # clave primaria
        lat: float,
        lon: float,
        city: str,          # ciudad inferida por Nominatim
        state: str,         # estado inferido
        created_at: datetime
    }
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from pymongo import MongoClient

logger = logging.getLogger(__name__)

NOMINATIM_URL     = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "GDLQueHacer/3.0 (contacto@gdlquehacer.mx)"}

# Máximo de requests a Nominatim por corrida (para no ser baneado)
MAX_NOMINATIM_CALLS = 200


class GeoCache:
    """Caché de geocodificación respaldado por MongoDB."""

    def __init__(self, mongo_uri: Optional[str] = None) -> None:
        uri = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self._client = MongoClient(uri)
        self._col = self._client["gdlquehacer"]["geocache"]
        self._ensure_indexes()
        self._nominatim_calls = 0

    def _ensure_indexes(self) -> None:
        self._col.create_index("address", unique=True)

    def get(self, address: str) -> Optional[dict]:
        """Busca en caché. Retorna {lat, lon, city, state} o None."""
        doc = self._col.find_one({"address": address})
        if doc:
            return {
                "lat":   doc.get("lat"),
                "lon":   doc.get("lon"),
                "city":  doc.get("city", ""),
                "state": doc.get("state", ""),
            }
        return None

    def set(self, address: str, lat: float, lon: float, city: str = "", state: str = "") -> None:
        """Guarda en caché."""
        try:
            self._col.update_one(
                {"address": address},
                {
                    "$set": {
                        "lat": lat, "lon": lon,
                        "city": city, "state": state,
                    },
                    "$setOnInsert": {
                        "address": address,
                        "created_at": datetime.now(timezone.utc),
                    },
                },
                upsert=True,
            )
        except Exception as exc:
            logger.warning("Geocache write error: %s", exc)

    def lookup(self, address: str) -> Optional[dict]:
        """
        Busca en caché; si no está, llama a Nominatim y guarda el resultado.
        Respeta el rate-limit de 1 req/s y el máximo de llamadas por corrida.
        """
        if not address or not address.strip():
            return None

        # 1. Intento desde caché
        cached = self.get(address)
        if cached:
            return cached

        # 2. Límite de llamadas Nominatim por corrida
        if self._nominatim_calls >= MAX_NOMINATIM_CALLS:
            logger.warning("Geocache: límite de Nominatim alcanzado (%d), omitiendo '%s'",
                           MAX_NOMINATIM_CALLS, address[:60])
            return None

        # 3. Llamada a Nominatim
        try:
            resp = requests.get(
                NOMINATIM_URL,
                params={"q": address, "format": "json", "limit": 1, "addressdetails": 1},
                headers=NOMINATIM_HEADERS,
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json()
            self._nominatim_calls += 1
            time.sleep(1)  # Respetar 1 req/s de Nominatim

            if not results:
                # Guardar miss para no volver a intentar
                self.set(address, 0.0, 0.0, "", "")
                return None

            r = results[0]
            lat   = float(r["lat"])
            lon   = float(r["lon"])
            addr_details = r.get("address", {})
            city  = addr_details.get("city") or addr_details.get("town") or addr_details.get("village", "")
            state = addr_details.get("state", "")

            self.set(address, lat, lon, city, state)
            return {"lat": lat, "lon": lon, "city": city, "state": state}

        except Exception as exc:
            logger.warning("Nominatim error para '%s': %s", address[:60], exc)
            return None

    def close(self) -> None:
        self._client.close()


# ── Función standalone para usar desde normalize_event ───────────────

_cache_instance: Optional[GeoCache] = None

def get_geocache() -> GeoCache:
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = GeoCache()
    return _cache_instance


def geocode_with_cache(address: str) -> Optional[dict]:
    """
    Geocodifica una dirección usando el caché persistente.
    Drop-in replacement de la función geocode() original en base.py.
    """
    return get_geocache().lookup(address)


def geocode_events_batch(events: list[dict]) -> list[dict]:
    """
    Geocodifica en batch una lista de eventos normalizados que les falten coords.
    Modifica los eventos in-place y retorna la lista.
    """
    cache = get_geocache()
    needs_geocode = [
        e for e in events
        if (e.get("location", {}).get("lat") is None
            or e.get("location", {}).get("lon") is None)
        and e.get("location", {}).get("address")
    ]

    logger.info("Geocodificando %d eventos sin coords (caché activo)…", len(needs_geocode))
    hits = 0
    misses = 0

    for evt in needs_geocode:
        address = evt["location"]["address"]
        # Agregar "México" si no viene ya para mejorar precisión
        query = address if "méxico" in address.lower() or "mexico" in address.lower() \
                else f"{address}, México"

        result = cache.lookup(query)
        if result and result.get("lat") and result.get("lon"):
            evt["location"]["lat"] = result["lat"]
            evt["location"]["lon"] = result["lon"]
            if not evt.get("ciudad") and result.get("city"):
                evt["ciudad"] = result["city"]
            if not evt.get("estado") and result.get("state"):
                evt["estado"] = result["state"]
            hits += 1
        else:
            misses += 1

    logger.info("Geocodificación: %d hits, %d misses (de %d sin coords)",
                hits, misses, len(needs_geocode))
    return events
