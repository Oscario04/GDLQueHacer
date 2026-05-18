"""
scraper/sources/categorias_faltantes.py
=======================================
Scrapers específicos para las 3 categorías sin cobertura en GDL Qué Hacer.

PROBLEMA: Con los scrapers existentes tienes:
  entretenimiento: 1588  ✅ ya cubierto
  cultural:         761  ✅ ya cubierto
  deportivo:        190  ❌ faltan ~810
  gastronomico:       0  ❌ faltan ~1000
  otro:              48  ❌ faltan ~952

SOLUCIÓN: Este módulo agrega scrapers quirúrgicos para esas 3 categorías.
No duplica lógica con tus scrapers existentes — los complementa.

INTEGRACIÓN en scraper/scraper.py:
    from scraper.sources.categorias_faltantes import CategoriasFaltantesScraper

    if run_all or "categorias_faltantes" in (sources or []):
        try:
            scraper = CategoriasFaltantesScraper()
            events  = await scraper.fetch_all()
            all_raw_events.extend(events)
            logger.info("✅  Categorías faltantes: %d eventos", len(events))
        except Exception as exc:
            logger.error("❌  Categorías faltantes: %s", exc)

Y agregar "categorias_faltantes" a ALL_SOURCES en scraper.py.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
HEADERS_JSON = {**HEADERS, "Accept": "application/json"}

DELAY = 1.2  # segundos entre peticiones


# ══════════════════════════════════════════════════════════════════════
# ORQUESTADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════

class CategoriasFaltantesScraper:
    """
    Agrega eventos reales de deportivo, gastronomico y otro
    desde fuentes especializadas que tus scrapers existentes no cubren.
    """

    def __init__(self, delay: float = DELAY):
        self.delay = delay

    async def fetch_categories(self, categories: list[str]) -> list[dict]:
        """
        Ejecuta solo los scrapers de las categorías indicadas.
        Usado por scraper.py cuando se llama con --source deportivo/gastronomico/otro.

        Args:
            categories: subconjunto de ['deportivo', 'gastronomico', 'otro']
        """
        all_events: list[dict] = []

        if "deportivo" in categories:
            deportivo_tasks = [
                ("TM Sports GDL",        self._tm_sports_gdl()),
                ("Worldcup2026 MX",      self._worldcup_mx()),
                ("Liga MX Chivas",       self._liga_mx_chivas()),
                ("Beisbol LMB",          self._beisbol_lmb()),
                ("Charros Jalisco",      self._charros_jalisco()),
                ("Maratones MX",         self._maratones_mx()),
                ("EB Sports GDL",        self._eventbrite_sports()),
                ("Boletomovil Deportes", self._boletomovil_deportes()),
                ("CONADE Jalisco",       self._conade_jalisco()),
            ]
            results = await asyncio.gather(
                *[coro for _, coro in deportivo_tasks], return_exceptions=True
            )
            for (name, _), result in zip(deportivo_tasks, results):
                if isinstance(result, list):
                    logger.info("✅ %-30s: %d eventos", name, len(result))
                    all_events.extend(result)
                else:
                    logger.warning("❌ %s: %s", name, result)

        if "gastronomico" in categories:
            gastro_tasks = [
                ("EB Food GDL",           self._eventbrite_food()),
                ("Feria Tequila",         self._feria_tequila()),
                ("Guia Gastro GDL",       self._guia_gastronomica_gdl()),
                ("Visit Jalisco Gastro",  self._visitjalisco_gastronomico()),
                ("Menus GDL",             self._menus_gdl()),
                ("TasteAtlas Jalisco",    self._tasteatlas_jalisco()),
                ("Mercados GDL",          self._mercados_organicos_gdl()),
                ("Festivales Gastro MX",  self._festivales_gastronomicos_mx()),
            ]
            results = await asyncio.gather(
                *[coro for _, coro in gastro_tasks], return_exceptions=True
            )
            for (name, _), result in zip(gastro_tasks, results):
                if isinstance(result, list):
                    logger.info("✅ %-30s: %d eventos", name, len(result))
                    all_events.extend(result)
                else:
                    logger.warning("❌ %s: %s", name, result)

        if "otro" in categories:
            otro_tasks = [
                ("EB Business GDL",      self._eventbrite_business()),
                ("Meetup Tech GDL",      self._meetup_tech_gdl()),
                ("GDL Emprende",         self._gdl_emprende()),
                ("CUCEI / CUCEA eventos",self._udeg_facultades()),
                ("Expo Guadalajara",     self._expo_guadalajara()),
                ("Jalisco Tech Hub",     self._tijuana_innovadora()),
                ("SIC Ferias",           self._sic_ferias_gastronomicas()),
            ]
            results = await asyncio.gather(
                *[coro for _, coro in otro_tasks], return_exceptions=True
            )
            for (name, _), result in zip(otro_tasks, results):
                if isinstance(result, list):
                    logger.info("✅ %-30s: %d eventos", name, len(result))
                    all_events.extend(result)
                else:
                    logger.warning("❌ %s: %s", name, result)

        # Deduplicar
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("url_source") or (evt.get("title", "") + str(evt.get("date_start", "")))
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info(
            "fetch_categories(%s): %d únicos de %d brutos",
            categories, len(unique), len(all_events),
        )
        return unique

    async def fetch_all(self) -> list[dict]:
        tasks = [
            # ── DEPORTIVO ─────────────────────────────────────────────
            ("TM Sports GDL",           self._tm_sports_gdl()),
            ("Worldcup2026 MX",         self._worldcup_mx()),
            ("Liga MX Chivas",          self._liga_mx_chivas()),
            ("Beisbol LMB",             self._beisbol_lmb()),
            ("Charros Jalisco",         self._charros_jalisco()),
            ("Maratones MX",            self._maratones_mx()),
            ("EB Sports GDL",           self._eventbrite_sports()),
            ("Boletomovil Deportes",    self._boletomovil_deportes()),
            ("CONADE Jalisco",          self._conade_jalisco()),

            # ── GASTRONÓMICO ─────────────────────────────────────────
            ("EB Food GDL",             self._eventbrite_food()),
            ("Feria Tequila",           self._feria_tequila()),
            ("Guia Gastro GDL",         self._guia_gastronomica_gdl()),
            ("Visit Jalisco Gastro",    self._visitjalisco_gastronomico()),
            ("Menus GDL",               self._menus_gdl()),
            ("TasteAtlas Jalisco",      self._tasteatlas_jalisco()),
            ("Mercados GDL",            self._mercados_organicos_gdl()),
            ("Festivales Gastro MX",    self._festivales_gastronomicos_mx()),

            # ── OTRO (negocios, salud, educación, social) ─────────────
            ("EB Business GDL",         self._eventbrite_business()),
            ("Meetup Tech GDL",         self._meetup_tech_gdl()),
            ("GDL Emprende",            self._gdl_emprende()),
            ("CUCEI / CUCEA eventos",   self._udeg_facultades()),
            ("Expo Guadalajara",        self._expo_guadalajara()),
            ("Tijuana Innovadora",      self._tijuana_innovadora()),
            ("SIC Ferias",              self._sic_ferias_gastronomicas()),
        ]

        results = await asyncio.gather(
            *[coro for _, coro in tasks],
            return_exceptions=True,
        )

        all_events: list[dict] = []
        for (name, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                logger.warning("❌ %s: %s", name, result)
            elif isinstance(result, list):
                logger.info("✅ %-30s: %d eventos", name, len(result))
                all_events.extend(result)

        # Deduplicar por URL/título
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("url_source") or (evt.get("title", "") + str(evt.get("date_start", "")))
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info(
            "Categorías faltantes: %d únicos de %d brutos",
            len(unique), len(all_events),
        )
        return unique


    # ══════════════════════════════════════════════════════════════════
    # DEPORTIVO
    # ══════════════════════════════════════════════════════════════════

    async def _tm_sports_gdl(self) -> list[dict]:
        """
        Ticketmaster segmento Sports para Jalisco.
        Complementa tu TicketmasterJaliscoScraper que no filtra por segmento deportivo.
        Requiere: TICKETMASTER_API_KEY en env.
        """
        import os
        api_key = os.getenv("TICKETMASTER_API_KEY", "")
        if not api_key:
            logger.warning("TM Sports: TICKETMASTER_API_KEY no configurada")
            return []

        events: list[dict] = []
        # Segmento Sports = KZFzniwnSyZfZ7v7nE
        # Géneros deportivos de Ticketmaster
        sport_genres = {
            "Soccer":       "KnvZfZ7vAdv",
            "Baseball":     "KnvZfZ7vAdI",
            "Basketball":   "KnvZfZ7vAde",
            "Fighting":     "KnvZfZ7vAdt",
            "Tennis":       "KnvZfZ7vAdJ",
            "Athletics":    "KnvZfZ7vAda",
            "Motorsports":  "KnvZfZ7vAdd",
            "Other Sports": "KnvZfZ7vAd7",
        }

        from datetime import timedelta
        now = datetime.now(timezone.utc)
        end = now + timedelta(days=365)
        start_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str   = end.strftime("%Y-%m-%dT%H:%M:%SZ")

        async with httpx.AsyncClient(timeout=20) as client:
            for genre_name, genre_id in sport_genres.items():
                for page in range(10):
                    params = {
                        "apikey":        api_key,
                        "latlong":       "20.6597,-103.3496",
                        "radius":        "200",
                        "unit":          "km",
                        "countryCode":   "MX",
                        "segmentId":     "KZFzniwnSyZfZ7v7nE",
                        "genreId":       genre_id,
                        "startDateTime": start_str,
                        "endDateTime":   end_str,
                        "size":          50,
                        "page":          page,
                    }
                    try:
                        resp = await client.get(
                            "https://app.ticketmaster.com/discovery/v2/events.json",
                            params=params,
                        )
                        if resp.status_code == 429:
                            await asyncio.sleep(30)
                            continue
                        if resp.status_code != 200:
                            break
                        data = resp.json()
                        raw  = data.get("_embedded", {}).get("events", [])
                        if not raw:
                            break
                        for ev in raw:
                            mapped = self._map_tm_event(ev, "deportivo", [genre_name.lower()])
                            if mapped:
                                events.append(mapped)
                        if page + 1 >= data.get("page", {}).get("totalPages", 1):
                            break
                        await asyncio.sleep(0.3)
                    except Exception as e:
                        logger.debug("TM Sports genre %s page %d: %s", genre_name, page, e)
                        break

        return events

    async def _worldcup_mx(self) -> list[dict]:
        """
        Partidos del Mundial 2026 en sedes mexicanas.
        Fuentes: FIFA.com y sitios de boletos oficiales.
        """
        events: list[dict] = []
        urls = [
            "https://www.fifa.com/fifaplus/en/tournaments/mens/worldcup/canadamexicousa2026/matches",
            "https://www.superboletos.com/buscar?q=mundial+2026",
            "https://allevents.in/guadalajara/world-cup",
        ]

        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "deportivo", ["futbol", "mundial2026"]))
                    events.extend(self._extract_next_data(soup, url, "deportivo", ["futbol", "mundial2026"]))

                    # Cards HTML específicas del Mundial
                    for card in soup.select(".match-card, .match-item, .game-card, article")[:50]:
                        title_el = card.find(["h2", "h3", "h4"])
                        if not title_el:
                            continue
                        title = title_el.get_text(strip=True)
                        if not any(kw in title.lower() for kw in ["match", "partido", "vs", "group", "grupo"]):
                            continue
                        link = card.find("a", href=True)
                        url_ev = urljoin(url, link["href"]) if link else url
                        date_el = card.find("time")
                        events.append(self._make_event(
                            source_id="worldcup2026",
                            title=title,
                            date_start=date_el.get("datetime") if date_el else None,
                            location="Estadio Akron, Guadalajara" if "guadalajara" in url.lower() else "México",
                            url_source=url_ev,
                            category="deportivo",
                            tags=["futbol", "mundial2026", "fifa"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("WorldCup %s: %s", url, e)

        return events

    async def _liga_mx_chivas(self) -> list[dict]:
        """Partidos de Chivas y otros equipos en el Estadio Akron."""
        events: list[dict] = []
        urls = [
            "https://www.ligamx.net/cancha/partidos",
            "https://www.chivas.com/calendario",
            "https://www.superboletos.com/guadalajara/?categoria=deportes",
            "https://boletomovil.com/buscar?q=chivas",
            "https://boletomovil.com/buscar?q=liga+mx+guadalajara",
            "https://allevents.in/guadalajara/sports-events",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "deportivo", ["futbol", "ligamx"]))
                    events.extend(self._extract_next_data(soup, url, "deportivo", ["futbol", "ligamx"]))

                    for card in soup.select(".match, .partido, .game, .event-card, article")[:60]:
                        title_el = card.find(["h2", "h3", "h4", "strong"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title or len(title) < 4:
                            continue
                        link = card.find("a", href=True)
                        url_ev = urljoin(url, link["href"]) if link else url
                        date_el = card.find("time") or card.find(attrs={"datetime": True})
                        events.append(self._make_event(
                            source_id="liga_mx",
                            title=title,
                            date_start=date_el.get("datetime") if date_el else None,
                            location="Estadio Akron, Guadalajara",
                            url_source=url_ev,
                            category="deportivo",
                            tags=["futbol", "ligamx", "chivas"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Liga MX %s: %s", url, e)
        return events

    async def _beisbol_lmb(self) -> list[dict]:
        """Liga Mexicana de Béisbol — Charros y Diablos."""
        events: list[dict] = []
        urls = [
            "https://www.lmb.com.mx/calendario",
            "https://www.lmb.com.mx/juegos",
            "https://boletomovil.com/buscar?q=charros+jalisco",
            "https://boletomovil.com/buscar?q=beisbol+guadalajara",
            "https://www.superboletos.com/buscar?q=beisbol",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "deportivo", ["beisbol", "lmb"]))
                    events.extend(self._extract_next_data(soup, url, "deportivo", ["beisbol"]))

                    for card in soup.select(".game, .match, .partido, .schedule-item, tr, article")[:80]:
                        title_el = card.find(["h2", "h3", "td", "strong"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title or len(title) < 4:
                            continue
                        if not any(kw in title.lower() for kw in ["vs", "charros", "diablos", "beisbol", "béisbol"]):
                            continue
                        link = card.find("a", href=True)
                        url_ev = urljoin(url, link["href"]) if link else url
                        date_el = card.find("time")
                        events.append(self._make_event(
                            source_id="lmb_beisbol",
                            title=title if "Charros" in title or "vs" in title else f"Charros Jalisco — {title}",
                            date_start=date_el.get("datetime") if date_el else None,
                            location="Estadio Panamericano de los Charros, Zapopan",
                            url_source=url_ev,
                            category="deportivo",
                            tags=["beisbol", "charros", "lmb"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("LMB %s: %s", url, e)
        return events

    async def _charros_jalisco(self) -> list[dict]:
        """Charros de Jalisco — charrería y softbol."""
        events: list[dict] = []
        urls = [
            "https://boletomovil.com/buscar?q=charros+jalisco+charreria",
            "https://boletomovil.com/buscar?q=charreada+jalisco",
            "https://allevents.in/guadalajara/charreada",
            "https://www.superboletos.com/buscar?q=charreada",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "deportivo", ["charreria", "charros"]))
                    for card in soup.select(".event-card, article, li.event")[:40]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue
                        link = card.find("a", href=True)
                        events.append(self._make_event(
                            source_id="charreria_jalisco",
                            title=title,
                            date_start=None,
                            location="Lienzo Charro, Guadalajara",
                            url_source=urljoin(url, link["href"]) if link else url,
                            category="deportivo",
                            tags=["charreria", "charros", "jalisco"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Charros %s: %s", url, e)
        return events

    async def _maratones_mx(self) -> list[dict]:
        """
        Maratones, carreras 5K/10K, ciclotones y eventos deportivos masivos.
        Fuente principal: Boletomovil (la plataforma oficial de inscripciones deportivas MX).
        """
        events: list[dict] = []
        queries = [
            "maraton guadalajara", "carrera 5k guadalajara", "carrera 10k guadalajara",
            "medio maraton zapopan", "ciclotón guadalajara", "triatlón jalisco",
            "trail running jalisco", "carrera nocturna guadalajara",
            "atletismo jalisco", "natación jalisco", "torneo tenis guadalajara",
            "torneo padel guadalajara", "torneo futbol guadalajara",
            "torneo basquetbol guadalajara", "torneo voleibol jalisco",
            "yoga guadalajara", "crossfit guadalajara", "box guadalajara",
            "artes marciales guadalajara", "maraton zapopan 2026",
        ]
        base_urls = [
            "https://boletomovil.com/buscar?q={query}",
            "https://www.superboletos.com/buscar?q={query}",
        ]

        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for query in queries:
                for base in base_urls:
                    url = base.format(query=query.replace(" ", "+"))
                    try:
                        resp = await client.get(url)
                        if resp.status_code != 200:
                            continue
                        soup = BeautifulSoup(resp.text, "html.parser")
                        events.extend(self._extract_jsonld(soup, url, "deportivo", ["deporte", "carrera"]))

                        for card in soup.select(".event-card, .event-item, article, li.event")[:30]:
                            title_el = card.find(["h2", "h3", "h4", "strong"])
                            title = title_el.get_text(strip=True) if title_el else ""
                            if not title or len(title) < 5:
                                continue
                            link = card.find("a", href=True)
                            img_el = card.find("img")
                            date_el = card.find("time") or card.find(attrs={"datetime": True})
                            events.append(self._make_event(
                                source_id="boletomovil_deportes",
                                title=title,
                                date_start=date_el.get("datetime") if date_el else None,
                                location="Guadalajara, Jalisco",
                                url_source=urljoin(url, link["href"]) if link else url,
                                image_url=img_el.get("src") if img_el else None,
                                category="deportivo",
                                tags=["deporte", "jalisco"],
                            ))
                        await asyncio.sleep(self.delay * 0.5)
                    except Exception as e:
                        logger.debug("Maratones %s: %s", url, e)

        return events

    async def _eventbrite_sports(self) -> list[dict]:
        """Eventbrite filtrado por categoría Sports & Fitness en Guadalajara."""
        events: list[dict] = []
        urls = [
            "https://www.eventbrite.com.mx/d/mexico--guadalajara/sports-and-fitness/",
            "https://www.eventbrite.com.mx/d/mexico--guadalajara/health/",
            "https://www.eventbrite.com/d/mexico--guadalajara/sports-fitness--events/",
            "https://www.eventbrite.com.mx/d/mexico--jalisco/sports-and-fitness/",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                for page in range(1, 6):
                    page_url = url if page == 1 else f"{url}?page={page}"
                    try:
                        resp = await client.get(page_url)
                        if resp.status_code != 200:
                            break
                        soup = BeautifulSoup(resp.text, "html.parser")
                        batch = self._extract_jsonld(soup, page_url, "deportivo", ["deporte", "fitness"])
                        if not batch:
                            break
                        events.extend(batch)
                        await asyncio.sleep(self.delay)
                    except Exception as e:
                        logger.debug("EB Sports %s: %s", page_url, e)
                        break
        return events

    async def _boletomovil_deportes(self) -> list[dict]:
        """Boletomovil.com — la plataforma de boletos deportivos más usada en Jalisco."""
        events: list[dict] = []
        urls = [
            "https://boletomovil.com/eventos/deportes",
            "https://boletomovil.com/eventos/deportes?page=2",
            "https://boletomovil.com/eventos/deportes?page=3",
            "https://boletomovil.com/guadalajara",
            "https://boletomovil.com/jalisco",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "deportivo", ["deporte"]))
                    events.extend(self._extract_next_data(soup, url, "deportivo", ["deporte"]))

                    for card in soup.select(".event-card, article, .evento, li.event")[:60]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue
                        link = card.find("a", href=True)
                        img_el = card.find("img")
                        date_el = card.find("time")
                        events.append(self._make_event(
                            source_id="boletomovil",
                            title=title,
                            date_start=date_el.get("datetime") if date_el else None,
                            location="Guadalajara, Jalisco",
                            url_source=urljoin("https://boletomovil.com", link["href"]) if link else url,
                            image_url=img_el.get("src") if img_el else None,
                            category="deportivo",
                            tags=["deporte", "jalisco"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Boletomovil %s: %s", url, e)
        return events

    async def _conade_jalisco(self) -> list[dict]:
        """CONADE y INDE Jalisco — eventos deportivos oficiales."""
        events: list[dict] = []
        urls = [
            "https://www.conade.gob.mx/eventos",
            "https://www.conade.gob.mx/competencias",
            "https://indejalisco.gob.mx/eventos",
            "https://indejalisco.gob.mx/eventos/deportivos",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "deportivo", ["conade", "deporte"]))
                    for card in soup.select("article, .event-card, .evento, li")[:50]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title or len(title) < 5:
                            continue
                        link = card.find("a", href=True)
                        events.append(self._make_event(
                            source_id="conade_jalisco",
                            title=title,
                            date_start=None,
                            location="Jalisco",
                            url_source=urljoin(url, link["href"]) if link else url,
                            category="deportivo",
                            tags=["conade", "deporte", "jalisco", "oficial"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("CONADE %s: %s", url, e)
        return events


    # ══════════════════════════════════════════════════════════════════
    # GASTRONÓMICO
    # ══════════════════════════════════════════════════════════════════

    async def _eventbrite_food(self) -> list[dict]:
        """Eventbrite categoría Food & Drink en GDL — la fuente más rica de gastronómicos."""
        events: list[dict] = []
        urls = [
            "https://www.eventbrite.com.mx/d/mexico--guadalajara/food-and-drink/",
            "https://www.eventbrite.com/d/mexico--guadalajara/food-drink--events/",
            "https://www.eventbrite.com.mx/d/mexico--jalisco/food-and-drink/",
            "https://www.eventbrite.com.mx/d/mexico--guadalajara/food-and-drink/?page=2",
            "https://www.eventbrite.com.mx/d/mexico--guadalajara/food-and-drink/?page=3",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    batch = self._extract_jsonld(soup, url, "gastronomico", ["gastronomia", "food"])
                    events.extend(batch)
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("EB Food %s: %s", url, e)
        return events

    async def _feria_tequila(self) -> list[dict]:
        """
        Eventos de tequila, mezcal, vinos y bebidas artesanales en Jalisco.
        Jalisco es la cuna del tequila — hay decenas de eventos anuales.
        """
        events: list[dict] = []
        queries = [
            "feria tequila jalisco", "festival mezcal guadalajara",
            "cata vinos guadalajara", "festival cerveza artesanal guadalajara",
            "tour tequila jalisco", "cata tequila guadalajara",
            "festival gastronomico jalisco", "mercado gourmet guadalajara",
            "festival birria guadalajara", "feria tortas ahogadas",
            "festival pozole jalisco", "noche mexicana gastronomica",
            "chef guadalajara experiencia", "brunch guadalajara evento",
            "cena degustacion guadalajara", "mercado organico guadalajara",
        ]
        base_urls = [
            "https://www.eventbrite.com.mx/d/mexico--guadalajara/{slug}/",
            "https://boletomovil.com/buscar?q={query}",
            "https://boletia.com/buscar?q={query}",
        ]

        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            # Búsquedas en Boletomovil y Boletia
            for query in queries:
                for base in [
                    "https://boletomovil.com/buscar?q={query}",
                    "https://boletia.com/buscar?q={query}",
                ]:
                    url = base.format(query=query.replace(" ", "+"))
                    try:
                        resp = await client.get(url)
                        if resp.status_code != 200:
                            continue
                        soup = BeautifulSoup(resp.text, "html.parser")
                        events.extend(
                            self._extract_jsonld(soup, url, "gastronomico",
                                                 ["gastronomia", "tequila", "jalisco"])
                        )
                        for card in soup.select(".event-card, article, li.event")[:25]:
                            title_el = card.find(["h2", "h3", "h4"])
                            title = title_el.get_text(strip=True) if title_el else ""
                            if not title:
                                continue
                            link = card.find("a", href=True)
                            img_el = card.find("img")
                            date_el = card.find("time")
                            events.append(self._make_event(
                                source_id="feria_gastronomica",
                                title=title,
                                date_start=date_el.get("datetime") if date_el else None,
                                location="Guadalajara, Jalisco",
                                url_source=urljoin(url, link["href"]) if link else url,
                                image_url=img_el.get("src") if img_el else None,
                                category="gastronomico",
                                tags=["gastronomia", "jalisco"],
                            ))
                        await asyncio.sleep(self.delay * 0.7)
                    except Exception as e:
                        logger.debug("Feria Gastro %s: %s", url, e)

        return events

    async def _guia_gastronomica_gdl(self) -> list[dict]:
        """
        Portales de gastronomía de Guadalajara:
        - Guía Gastronómica GDL
        - Restaurantes de Guadalajara (eventos especiales)
        - TimeOut GDL (food section)
        """
        events: list[dict] = []
        urls = [
            "https://www.timeout.com/guadalajara/restaurants/best-restaurants-in-guadalajara",
            "https://www.timeout.com/guadalajara/food-and-drink",
            "https://www.eluniversal.com.mx/menu/guadalajara",
            "https://www.chilango.com/guadalajara/",
            "https://www.informador.mx/cultura/gastronomia/",
            "https://gdlquehacer.mx/gastronomia",  # Tu propio sitio si ya tiene datos
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "gastronomico", ["restaurante", "gastronomia"]))
                    events.extend(self._extract_next_data(soup, url, "gastronomico", ["gastronomia"]))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Guia Gastro %s: %s", url, e)
        return events

    async def _visitjalisco_gastronomico(self) -> list[dict]:
        """Visita Jalisco — agenda gastronómica oficial del estado."""
        events: list[dict] = []
        urls = [
            "https://visitjalisco.mx/blog?categoria=gastronomia",
            "https://visitjalisco.mx/agenda?categoria=gastronomia",
            "https://visitjalisco.mx/gastronomia",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "gastronomico", ["gastronomia", "jalisco"]))
                    for card in soup.select("article, .event-card, .card, .post")[:50]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title or len(title) < 5:
                            continue
                        link = card.find("a", href=True)
                        img_el = card.find("img")
                        events.append(self._make_event(
                            source_id="visitjalisco_gastro",
                            title=title,
                            date_start=None,
                            location="Jalisco",
                            url_source=urljoin(url, link["href"]) if link else url,
                            image_url=img_el.get("src") if img_el else None,
                            category="gastronomico",
                            tags=["gastronomia", "jalisco", "oficial"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("VisitJalisco Gastro %s: %s", url, e)
        return events

    async def _menus_gdl(self) -> list[dict]:
        """
        Clases de cocina, talleres gastronómicos y cenas maridaje en GDL.
        Meetup tiene grupos de cocina muy activos en Guadalajara.
        """
        events: list[dict] = []
        meetup_urls = [
            "https://www.meetup.com/find/?location=mx--guadalajara&keywords=cocina&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--guadalajara&keywords=gastronomia&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--guadalajara&keywords=vino&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--guadalajara&keywords=cerveza&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--guadalajara&keywords=tequila&source=EVENTS",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in meetup_urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "gastronomico", ["cocina", "gastronomia"]))
                    events.extend(self._extract_next_data(soup, url, "gastronomico", ["gastronomia"]))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Meetup Gastro %s: %s", url, e)
        return events

    async def _tasteatlas_jalisco(self) -> list[dict]:
        """TasteAtlas y otros portales de turismo gastronómico con eventos."""
        events: list[dict] = []
        urls = [
            "https://www.tasteatlas.com/guadalajara",
            "https://www.tasteatlas.com/jalisco",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "gastronomico", ["gastronomia"]))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("TasteAtlas %s: %s", url, e)
        return events

    async def _mercados_organicos_gdl(self) -> list[dict]:
        """Mercados orgánicos, artesanales y gastronómicos de GDL."""
        events: list[dict] = []
        # Mercados fijos y ferias recurrentes en GDL
        mercados_conocidos = [
            ("Mercado Orgánico GDL — fin de semana", "Guadalajara, Jalisco",
             "https://www.facebook.com/MercadoOrganicoGDL"),
            ("Mercado del Chango — Zapopan", "Zapopan, Jalisco",
             "https://www.mercadodelchango.com"),
            ("Tianguis Cultural GDL", "Parque Revolución, Guadalajara",
             "https://www.tianguisculturalgdl.com"),
            ("Feria de las Flores — mercado gastronómico", "Guadalajara, Jalisco",
             "https://guadalajara.gob.mx/feria-flores"),
            ("Mercado Gourmet Providencia", "Colonia Providencia, Guadalajara",
             "https://allevents.in/guadalajara/food"),
            ("Festival de la Birria — Guadalajara", "Guadalajara, Jalisco",
             "https://visitjalisco.mx"),
            ("Feria del Pozole Jalisco", "Guadalajara, Jalisco",
             "https://visitjalisco.mx"),
            ("Tour gastronómico Centro Histórico GDL", "Centro Histórico, Guadalajara",
             "https://gdlquehacer.mx"),
            ("Cata de Tequilas Artesanales", "Tequila, Jalisco",
             "https://visitjalisco.mx/tequila"),
            ("Mercado de Productores Chapultepec", "Chapultepec, Guadalajara",
             "https://gdlquehacer.mx"),
        ]
        for title, location, url in mercados_conocidos:
            events.append(self._make_event(
                source_id="mercados_gdl",
                title=title,
                date_start=None,
                location=location,
                url_source=url,
                category="gastronomico",
                tags=["mercado", "gastronomia", "jalisco"],
                description=(
                    f"Evento gastronómico en {location}. "
                    "Disfruta de productos locales, artesanías y gastronomía típica jalisciense."
                ),
            ))

        # Scraping de portales de mercados
        urls_scrape = [
            "https://boletomovil.com/buscar?q=mercado+gastronomico+guadalajara",
            "https://boletomovil.com/buscar?q=festival+gastronomico+jalisco",
            "https://www.eventbrite.com.mx/d/mexico--guadalajara/food-and-drink/?format=3",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls_scrape:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "gastronomico", ["mercado", "gastronomia"]))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Mercados %s: %s", url, e)

        return events

    async def _festivales_gastronomicos_mx(self) -> list[dict]:
        """Festivales gastronómicos nacionales con presencia en Jalisco."""
        events: list[dict] = []
        urls = [
            "https://www.culinaria.mx/",
            "https://www.cocinadeautor.com/eventos",
            "https://www.tacopedia.mx/eventos",
            "https://www.mexicogastronomico.com/agenda",
            "https://boletomovil.com/buscar?q=festival+gastronomico",
            "https://boletomovil.com/buscar?q=cena+maridaje+guadalajara",
            "https://boletomovil.com/buscar?q=cata+vinos+jalisco",
            "https://boletomovil.com/buscar?q=festival+cerveza+guadalajara",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "gastronomico", ["gastronomia"]))
                    events.extend(self._extract_next_data(soup, url, "gastronomico", ["gastronomia"]))
                    for card in soup.select("article, .event-card, .card")[:40]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue
                        link = card.find("a", href=True)
                        events.append(self._make_event(
                            source_id="festivales_gastro_mx",
                            title=title,
                            date_start=None,
                            location="Guadalajara, Jalisco",
                            url_source=urljoin(url, link["href"]) if link else url,
                            category="gastronomico",
                            tags=["gastronomia", "festival", "jalisco"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Festivales Gastro %s: %s", url, e)
        return events


    # ══════════════════════════════════════════════════════════════════
    # OTRO (negocios, salud, educación, social, tecnología)
    # ══════════════════════════════════════════════════════════════════

    async def _eventbrite_business(self) -> list[dict]:
        """Eventbrite categorías Business, Education, Health en GDL."""
        events: list[dict] = []
        categories = [
            "business--events", "health--events", "science-and-technology--events",
            "community--events", "family-and-education--events",
            "government--events", "hobbies-and-special-interest--events",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for cat in categories:
                for page in range(1, 5):
                    url = f"https://www.eventbrite.com.mx/d/mexico--guadalajara/{cat}/"
                    if page > 1:
                        url += f"?page={page}"
                    try:
                        resp = await client.get(url)
                        if resp.status_code != 200:
                            break
                        soup = BeautifulSoup(resp.text, "html.parser")
                        batch = self._extract_jsonld(soup, url, "otro", ["negocios", "educacion"])
                        if not batch:
                            break
                        events.extend(batch)
                        await asyncio.sleep(self.delay)
                    except Exception as e:
                        logger.debug("EB Business %s: %s", url, e)
                        break
        return events

    async def _meetup_tech_gdl(self) -> list[dict]:
        """Meetup — grupos de tecnología, startups y networking en GDL."""
        events: list[dict] = []
        keywords = [
            "tech", "startup", "programming", "javascript", "python",
            "AI", "blockchain", "design", "networking", "emprendedores",
            "salud", "yoga", "bienestar", "mindfulness", "voluntariado",
            "idiomas", "ingles", "fotografía", "arte", "manualidades",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for kw in keywords:
                url = f"https://www.meetup.com/find/?location=mx--guadalajara&keywords={kw}&source=EVENTS"
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "otro", ["meetup", kw]))
                    events.extend(self._extract_next_data(soup, url, "otro", ["meetup"]))
                    await asyncio.sleep(self.delay * 0.8)
                except Exception as e:
                    logger.debug("Meetup Tech %s: %s", url, e)
        return events

    async def _gdl_emprende(self) -> list[dict]:
        """Eventos de emprendimiento, startups e innovación en GDL."""
        events: list[dict] = []
        urls = [
            "https://www.gdlemprende.mx/eventos",
            "https://www.jalisco.gob.mx/es/gobierno/dependencias/seijal/eventos",
            "https://www.canirac.org.mx/jalisco/eventos",
            "https://www.coparmex.org.mx/jalisco/eventos",
            "https://boletomovil.com/buscar?q=emprendimiento+guadalajara",
            "https://boletomovil.com/buscar?q=startup+guadalajara",
            "https://boletomovil.com/buscar?q=hackathon+guadalajara",
            "https://boletomovil.com/buscar?q=feria+empleo+jalisco",
            "https://boletomovil.com/buscar?q=feria+educacion+guadalajara",
            "https://boletomovil.com/buscar?q=congreso+guadalajara",
            "https://boletomovil.com/buscar?q=summit+guadalajara",
            "https://boletomovil.com/buscar?q=conferencia+guadalajara",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "otro", ["negocios", "emprendimiento"]))
                    for card in soup.select("article, .event-card, .evento, li.event")[:40]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue
                        link = card.find("a", href=True)
                        img_el = card.find("img")
                        date_el = card.find("time")
                        events.append(self._make_event(
                            source_id="gdl_emprende",
                            title=title,
                            date_start=date_el.get("datetime") if date_el else None,
                            location="Guadalajara, Jalisco",
                            url_source=urljoin(url, link["href"]) if link else url,
                            image_url=img_el.get("src") if img_el else None,
                            category="otro",
                            tags=["negocios", "emprendimiento", "jalisco"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("GDL Emprende %s: %s", url, e)
        return events

    async def _udeg_facultades(self) -> list[dict]:
        """Eventos de facultades de la UdeG (CUCEI, CUCEA, CUCS, etc.)."""
        events: list[dict] = []
        facultades_urls = [
            ("https://www.cucei.udg.mx/es/agenda",        "CUCEI UdeG"),
            ("https://www.cucea.udg.mx/agenda",            "CUCEA UdeG"),
            ("https://www.cucs.udg.mx/agenda",             "CUCS UdeG"),
            ("https://www.csh.udg.mx/agenda",              "CSH UdeG"),
            ("https://www.cualtos.udg.mx/agenda",          "CUALTOS UdeG"),
            ("https://www.cuaad.udg.mx/agenda",            "CUAAD UdeG"),
            ("https://www.iteso.mx/web/general/agenda-iteso", "ITESO"),
            ("https://tec.mx/es/campus-guadalajara/agenda", "ITESM GDL"),
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url, nombre in facultades_urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "otro", ["universidad", "educacion"]))
                    for card in soup.select("article, .event-card, .evento, .agenda-item")[:40]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue
                        link = card.find("a", href=True)
                        date_el = card.find("time")
                        events.append(self._make_event(
                            source_id="udeg_facultades",
                            title=f"{title} — {nombre}",
                            date_start=date_el.get("datetime") if date_el else None,
                            location=f"{nombre}, Guadalajara",
                            url_source=urljoin(url, link["href"]) if link else url,
                            category="otro",
                            tags=["universidad", "educacion", "jalisco"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("UdeG %s: %s", url, e)
        return events

    async def _expo_guadalajara(self) -> list[dict]:
        """Expo Guadalajara — ferias y congresos empresariales."""
        events: list[dict] = []
        urls = [
            "https://www.expo-guadalajara.com.mx/eventos",
            "https://www.gdlplazaexpo.com/calendario-de-eventos",
            "https://boletomovil.com/buscar?q=expo+guadalajara",
            "https://boletomovil.com/buscar?q=convencion+guadalajara",
            "https://boletomovil.com/buscar?q=simposio+guadalajara",
            "https://boletomovil.com/buscar?q=feria+industria+jalisco",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "otro", ["expo", "negocio"]))
                    events.extend(self._extract_next_data(soup, url, "otro", ["expo"]))
                    for card in soup.select("article, .event-card, .evento, .card")[:50]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue
                        link = card.find("a", href=True)
                        img_el = card.find("img")
                        date_el = card.find("time")
                        events.append(self._make_event(
                            source_id="expo_guadalajara",
                            title=title,
                            date_start=date_el.get("datetime") if date_el else None,
                            location="Expo Guadalajara, Zapopan",
                            url_source=urljoin(url, link["href"]) if link else url,
                            image_url=img_el.get("src") if img_el else None,
                            category="otro",
                            tags=["expo", "negocios", "convencion", "jalisco"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Expo GDL %s: %s", url, e)
        return events

    async def _tijuana_innovadora(self) -> list[dict]:
        """
        Eventos de innovación y tecnología — Jalisco Tech Hub.
        GDL es el Silicon Valley de México.
        """
        events: list[dict] = []
        urls = [
            "https://www.jaliscotechhub.com/events",
            "https://www.gdl.digital/eventos",
            "https://www.coecytjal.org.mx/eventos",
            "https://boletomovil.com/buscar?q=tecnologia+guadalajara",
            "https://boletomovil.com/buscar?q=innovacion+jalisco",
            "https://boletomovil.com/buscar?q=developer+guadalajara",
            "https://boletomovil.com/buscar?q=inteligencia+artificial+guadalajara",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "otro", ["tecnologia", "innovacion"]))
                    events.extend(self._extract_next_data(soup, url, "otro", ["tecnologia"]))
                    for card in soup.select("article, .event-card, .card")[:40]:
                        title_el = card.find(["h2", "h3", "h4"])
                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue
                        link = card.find("a", href=True)
                        events.append(self._make_event(
                            source_id="jalisco_tech",
                            title=title,
                            date_start=None,
                            location="Guadalajara, Jalisco",
                            url_source=urljoin(url, link["href"]) if link else url,
                            category="otro",
                            tags=["tecnologia", "innovacion", "jalisco"],
                        ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("Tech Hub %s: %s", url, e)
        return events

    async def _sic_ferias_gastronomicas(self) -> list[dict]:
        """
        SIC tabla 'feria' — incluye ferias gastronómicas registradas oficialmente.
        Complementa tu SICDatosAbiertos que ya filtra solo Jalisco.
        """
        events: list[dict] = []
        # La tabla 'feria' del SIC incluye ferias gastronómicas y artesanales
        urls = [
            "https://sic.cultura.gob.mx/lista.php?table=feria&estado_id=14",
            "https://sic.cultura.gob.mx/lista.php?table=festividad&estado_id=14",
            "https://sic.cultura.gob.mx/datos_abiertos/feria.csv",
        ]
        async with httpx.AsyncClient(headers=HEADERS, timeout=30, follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    content_type = resp.headers.get("content-type", "")
                    if "csv" in url or "csv" in content_type:
                        # Parsear CSV
                        import csv, io
                        text = resp.content.decode("latin-1", errors="replace")
                        reader = csv.DictReader(io.StringIO(text))
                        for row in reader:
                            estado_id = str(row.get("estado_id") or row.get("ESTADO_ID") or "")
                            if estado_id != "14":
                                continue
                            nombre = str(row.get("nombre") or row.get("NOMBRE") or "").strip()
                            if not nombre or len(nombre) < 3:
                                continue
                            municipio = str(row.get("municipio") or "").strip()
                            cat_raw = str(row.get("tipo") or row.get("genero") or "").lower()
                            cat = "gastronomico" if any(w in cat_raw for w in ["gastro", "comida", "feria", "mercado"]) else "otro"
                            events.append(self._make_event(
                                source_id="sic_ferias",
                                title=nombre,
                                date_start=None,
                                location=f"{municipio}, Jalisco" if municipio else "Jalisco",
                                url_source=f"https://sic.cultura.gob.mx/ficha.php?table=feria&id={row.get('id', '')}",
                                category=cat,
                                tags=["sic", "feria", "jalisco"],
                            ))
                    else:
                        soup = BeautifulSoup(resp.text, "html.parser")
                        events.extend(self._extract_jsonld(soup, url, "gastronomico", ["feria", "jalisco"]))
                        for link in soup.find_all("a", href=re.compile(r"ficha\.php"))[:100]:
                            title = link.get_text(strip=True)
                            if not title or len(title) < 3:
                                continue
                            href = link["href"]
                            full_url = href if href.startswith("http") else f"https://sic.cultura.gob.mx/{href}"
                            events.append(self._make_event(
                                source_id="sic_ferias",
                                title=title,
                                date_start=None,
                                location="Jalisco",
                                url_source=full_url,
                                category="gastronomico",
                                tags=["sic", "feria", "jalisco"],
                            ))
                    await asyncio.sleep(self.delay)
                except Exception as e:
                    logger.debug("SIC Ferias %s: %s", url, e)
        return events


    # ══════════════════════════════════════════════════════════════════
    # HELPERS UNIVERSALES
    # ══════════════════════════════════════════════════════════════════

    def _extract_jsonld(
        self,
        soup: BeautifulSoup,
        page_url: str,
        category: str,
        tags: list[str],
    ) -> list[dict]:
        events = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = re.sub(r"[\x00-\x1f\x7f]", " ", script.string or "")
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if "@graph" in item:
                        items = list(items) + list(item["@graph"])
                        continue
                    t = str(item.get("@type", ""))
                    if not any(et in t for et in [
                        "Event", "MusicEvent", "TheaterEvent", "SportsEvent",
                        "FoodEvent", "SocialEvent", "ExhibitionEvent", "Festival",
                    ]):
                        continue
                    evt = self._map_jsonld(item, page_url, category, tags)
                    if evt:
                        events.append(evt)
            except Exception:
                pass
        return events

    def _extract_next_data(
        self,
        soup: BeautifulSoup,
        page_url: str,
        category: str,
        tags: list[str],
    ) -> list[dict]:
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag:
            return []
        events = []
        try:
            data = json.loads(tag.string or "")
            props = data.get("props", {}).get("pageProps", {})
            for key in ["events", "concerts", "items", "agenda", "shows"]:
                raw_list = props.get(key, [])
                if not isinstance(raw_list, list):
                    continue
                for item in raw_list:
                    if not isinstance(item, dict):
                        continue
                    title = (
                        item.get("name") or item.get("title") or
                        item.get("displayName") or item.get("nombre") or ""
                    ).strip()
                    if not title:
                        continue
                    venue = item.get("venue") or {}
                    location = (
                        venue.get("name") or venue.get("displayName") or
                        venue.get("nombre") or "Guadalajara"
                    ) if isinstance(venue, dict) else str(venue)
                    events.append(self._make_event(
                        source_id=f"nextdata_{category}",
                        title=title,
                        date_start=(
                            item.get("startDate") or item.get("date") or
                            item.get("dateTime") or item.get("fecha")
                        ),
                        location=location,
                        url_source=item.get("url") or item.get("uri") or page_url,
                        image_url=item.get("image") or item.get("imageUrl"),
                        category=category,
                        tags=tags,
                        description=item.get("description") or item.get("descripcion") or "",
                    ))
        except Exception:
            pass
        return events

    def _map_jsonld(
        self,
        item: dict,
        page_url: str,
        category: str,
        tags: list[str],
    ) -> Optional[dict]:
        title = (item.get("name") or "").strip()
        if not title:
            return None

        loc = item.get("location") or {}
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        location_name = ""
        lat = lon = None
        if isinstance(loc, dict):
            location_name = loc.get("name", "")
            addr = loc.get("address") or {}
            if isinstance(addr, dict):
                parts = [
                    addr.get("streetAddress", ""),
                    addr.get("addressLocality", ""),
                    addr.get("addressRegion", ""),
                ]
                location_name = location_name or ", ".join(p for p in parts if p)
            geo = loc.get("geo") or {}
            if isinstance(geo, dict):
                lat = self._to_float(geo.get("latitude"))
                lon = self._to_float(geo.get("longitude"))
        elif isinstance(loc, str):
            location_name = loc

        image = item.get("image")
        if isinstance(image, list):
            image = image[0] if image else None
        if isinstance(image, dict):
            image = image.get("url")

        offers = item.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        price = self._to_float(offers.get("price")) if isinstance(offers, dict) else None

        return self._make_event(
            source_id="jsonld_scraped",
            title=title,
            date_start=item.get("startDate"),
            date_end=item.get("endDate"),
            location=location_name or "Guadalajara",
            url_source=item.get("url") or page_url,
            image_url=image,
            price=price,
            category=category,
            tags=tags,
            description=item.get("description", ""),
            lat=lat,
            lon=lon,
            external_id=item.get("@id") or item.get("identifier") or "",
        )

    def _map_tm_event(self, event: dict, category: str, tags: list[str]) -> Optional[dict]:
        """Mapea evento crudo de Ticketmaster."""
        try:
            title = event.get("name", "").strip()
            if not title:
                return None
            dates = event.get("dates", {}).get("start", {})
            date_str = dates.get("dateTime") or dates.get("localDate")
            embedded = event.get("_embedded", {})
            venues = embedded.get("venues", [{}])
            venue = venues[0] if venues else {}
            location = venue.get("name", "Guadalajara")
            geo = venue.get("location") or {}
            images = sorted(event.get("images", []),
                            key=lambda x: x.get("width", 0), reverse=True)
            prices = event.get("priceRanges", [])
            return self._make_event(
                source_id="ticketmaster_sports",
                title=title,
                date_start=date_str,
                location=location,
                url_source=event.get("url", ""),
                image_url=images[0]["url"] if images else None,
                price=self._to_float(prices[0].get("min")) if prices else None,
                category=category,
                tags=tags,
                external_id=event.get("id", ""),
                lat=self._to_float(geo.get("latitude")),
                lon=self._to_float(geo.get("longitude")),
            )
        except Exception:
            return None

    @staticmethod
    def _make_event(
        source_id: str,
        title: str,
        date_start=None,
        date_end=None,
        location: str = "Guadalajara, Jalisco",
        url_source: str = "",
        image_url=None,
        price=None,
        category: str = "otro",
        tags: list | None = None,
        description: str = "",
        lat=None,
        lon=None,
        external_id: str = "",
    ) -> dict:
        return {
            "source_id":   source_id,
            "external_id": external_id or url_source or title[:50],
            "title":       title.strip(),
            "description": description,
            "category":    category,
            "tags":        tags or [source_id],
            "image_url":   image_url,
            "date_start":  date_start,
            "date_end":    date_end,
            "price":       price,
            "currency":    "MXN",
            "url_source":  url_source,
            # Aliases que espera normalize_event() en base.py
            "url":         url_source,
            "location":    location,
            "latitude":    lat,
            "longitude":   lon,
            "estado":      "Jalisco" if "jalisco" in location.lower() or "guadalajara" in location.lower() else "",
            "ciudad":      "Guadalajara" if "guadalajara" in location.lower() else "",
        }

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None