"""
scraper/pipelines/deduplicate.py
Deduplicación robusta de eventos con fingerprint SHA-256.

Dos niveles:
  1. source + source_id  (deduplicación exacta por fuente)
  2. fingerprint         (título + fecha + ciudad — captura el mismo evento
                          publicado en Ticketmaster, Eventbrite y Boletia)
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)


def _normalize_title(title: str) -> str:
    """Normaliza título para comparación."""
    t = title.lower().strip()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t)
    return t


def _date_key(dt) -> str:
    """Extrae solo la fecha (YYYY-MM-DD) para tolerar diferencias de hora."""
    if dt is None:
        return ""
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d")
    if isinstance(dt, str):
        return dt[:10]
    return str(dt)


def fingerprint(event: dict) -> str:
    """
    Genera un hash SHA-256 reproducible para un evento.
    Combina: título normalizado + fecha inicio + ciudad.
    """
    title  = _normalize_title(event.get("title", ""))
    date   = _date_key(event.get("date_start") or event.get("start_date"))
    ciudad = (event.get("ciudad") or "").lower().strip()

    # Fallback de ciudad desde el campo location
    if not ciudad:
        loc = event.get("location", "")
        if isinstance(loc, str):
            parts = [p.strip() for p in loc.split(",")]
            ciudad = parts[0].lower() if parts else ""
        elif isinstance(loc, dict):
            ciudad = (loc.get("city") or loc.get("address") or "").lower()

    raw = f"{title}|{date}|{ciudad}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def deduplicate_events(events: list[dict]) -> tuple[list[dict], int]:
    """
    Deduplica una lista de eventos en memoria (antes de guardar en Mongo).

    Prioridad de deduplicación:
      1. (source + external_id)  — exacta por fuente
      2. fingerprint             — mismos eventos cross-source

    Returns:
        (eventos_únicos, duplicados_eliminados)
    """
    seen_source_ids: set[str]    = set()
    seen_fingerprints: set[str]  = set()
    unique: list[dict]           = []
    duplicates                   = 0

    for evt in events:
        # Clave exacta por fuente
        source_key = f"{evt.get('source_id', '')}::{evt.get('external_id', '')}"
        if source_key in seen_source_ids and evt.get("external_id"):
            duplicates += 1
            continue

        # Fingerprint cross-source
        fp = fingerprint(evt)
        if fp in seen_fingerprints:
            duplicates += 1
            continue

        if evt.get("external_id"):
            seen_source_ids.add(source_key)
        seen_fingerprints.add(fp)

        # Almacenar el fingerprint en el evento para usarlo como índice en Mongo
        evt["fingerprint"] = fp
        unique.append(evt)

    logger.info(
        "Deduplicación: %d originales → %d únicos (%d duplicados eliminados)",
        len(events), len(unique), duplicates,
    )
    return unique, duplicates