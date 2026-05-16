"""
scraper/sources/massive_scraper.py
Scraper masivo multi-fuente para alcanzar 5,000+ eventos únicos.

Fuentes:
  1.  LastFM Events           — API pública, conciertos internacionales en MX
  2.  Ticketmaster MX extra   — más géneros y ciudades pequeñas
  3.  Concerts.com            — scraping HTML + JSON-LD
  4.  RA (Resident Advisor)   — eventos electrónicos GDL/CDMX
  5.  Bandsintown scraping    — agenda de artistas
  6.  AXS.com                 — ticketera con cobertura MX
  7.  StubHub                 — reventa con buenos datos de eventos
  8.  Fever                   — app de experiencias MX
  9.  Facebook Events público — mbasic.facebook.com sin login
  10. Google Events scraping  — resultados enriquecidos de Google
  11. Yelp Events             — eventos de restaurantes/bares GDL
  12. Meetup scraping HTML    — grupos locales sin API
  13. Eventful / Bandsintown  — feeds RSS públicos
  14. Time Out México         — agenda editorial curada
  15. TicketNetwork           — red secundaria de tickets
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urljoin, urlencode, quote_plus

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS_DESKTOP = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
}

HEADERS_MOBILE = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

HEADERS_JSON = {
    **HEADERS_DESKTOP,
    "Accept": "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
}

# Ciudades clave de México para scraping
MX_CITIES = [
    "Guadalajara", "Ciudad de Mexico", "Monterrey", "Cancun", "Tijuana",
    "Puebla", "Queretaro", "Merida", "Leon", "San Luis Potosi",
    "Aguascalientes", "Morelia", "Chihuahua", "Culiacan", "Hermosillo",
    "Veracruz", "Oaxaca", "Mazatlan", "Puerto Vallarta", "Torreon",
    "Saltillo", "Acapulco", "Los Cabos", "Guanajuato", "Zapopan",
    "Tlaquepaque", "Tonala", "Tlajomulco",
]

MX_COORDS = [
    (20.6597, -103.3496, "Guadalajara"),
    (19.4326, -99.1332,  "CDMX"),
    (25.6866, -100.3161, "Monterrey"),
    (21.1619, -86.8515,  "Cancun"),
    (19.0414, -98.2063,  "Puebla"),
    (20.5888, -100.3899, "Queretaro"),
    (32.5149, -117.0382, "Tijuana"),
    (20.9674, -89.6237,  "Merida"),
    (21.8853, -102.2916, "Aguascalientes"),
    (22.1565, -100.9855, "SLP"),
    (19.7069, -101.1950, "Morelia"),
    (17.0669, -96.7203,  "Oaxaca"),
    (23.2494, -106.4111, "Mazatlan"),
    (20.6597, -105.2253, "PuertoVallarta"),
    (25.4232, -100.9734, "Saltillo"),
    (25.5428, -103.4068, "Torreon"),
]


class MassiveScraper:
    """
    Scraper masivo que agrega múltiples fuentes alternativas.
    Objetivo: 3,000+ eventos adicionales para sumar 5,000+ en total.
    """

    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self._timeout = httpx.Timeout(30.0, connect=10.0)

    async def fetch_all(self) -> list[dict]:
        """Ejecuta todos los sub-scrapers en paralelo."""
        tasks = [
            ("LastFM",          self._fetch_lastfm()),
            ("TimeOutMexico",   self._fetch_timeout_mexico()),
            ("FeverApp",        self._fetch_fever()),
            ("TicketNetwork",   self._fetch_ticketnetwork()),
            ("AlleventsExtra",  self._fetch_allevents_extra()),
            ("VivaAerobusEvents", self._fetch_vivaevents()),
            ("MeetupHTML",      self._fetch_meetup_html()),
            ("PredictHQ",       self._fetch_predicthq()),
            ("AxsMexico",       self._fetch_axs()),
            ("Xceed",           self._fetch_xceed()),
            ("RAMexico",        self._fetch_ra()),
            ("FacebookPublic",  self._fetch_facebook_public()),
            ("EventimMexico",   self._fetch_eventim()),
            ("Teleticket",      self._fetch_teleticket()),
            ("BoletosNet",      self._fetch_boletos_net()),
            ("Culturama",       self._fetch_culturama()),
            ("CinemexEventos",  self._fetch_cinemex()),
            ("CinepolisEventos",self._fetch_cinepolis()),
            ("LaListaEventos",  self._fetch_lalista()),
            ("SeatGeek",        self._fetch_seatgeek()),
            ("EventixMx",       self._fetch_eventix()),
            ("Ticketea",        self._fetch_ticketea()),
            ("TicketsToday",    self._fetch_ticketstoday()),
            ("MXConcerts",      self._fetch_mx_concerts_scrape()),
            ("Viberate",        self._fetch_viberate()),
        ]

        results = await asyncio.gather(
            *[task for _, task in tasks],
            return_exceptions=True
        )

        all_events: list[dict] = []
        for (name, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                logger.warning("MassiveScraper '%s' error: %s", name, result)
            elif isinstance(result, list):
                logger.info("MassiveScraper '%s': %d eventos", name, len(result))
                all_events.extend(result)

        # Deduplicar
        seen: set[str] = set()
        unique = []
        for evt in all_events:
            key = evt.get("url") or (evt.get("title", "") + str(evt.get("date_start", "")))
            if key and key not in seen:
                seen.add(key)
                unique.append(evt)

        logger.info("MassiveScraper total: %d eventos únicos", len(unique))
        return unique

    # ── 1. LastFM Events ────────────────────────────────────────────────
    async def _fetch_lastfm(self) -> list[dict]:
        """
        Last.fm tiene un endpoint público de eventos por ciudad.
        No requiere API key para consultas básicas.
        """
        events: list[dict] = []
        # Last.fm concerts endpoint (datos de Songkick/Bandsintown integrado)
        urls = [
            "https://www.last.fm/events/+Mexico",
            "https://www.last.fm/events/+Mexico?page=2",
            "https://www.last.fm/events/+Mexico?page=3",
            "https://www.last.fm/events/+Mexico?page=4",
            "https://www.last.fm/events/+Mexico?page=5",
        ]
        cities_urls = [
            "https://www.last.fm/events/+Guadalajara",
            "https://www.last.fm/events/+Ciudad_de_Mexico",
            "https://www.last.fm/events/+Monterrey",
            "https://www.last.fm/events/+Cancun",
            "https://www.last.fm/events/+Tijuana",
            "https://www.last.fm/events/+Puebla",
            "https://www.last.fm/events/+Merida",
            "https://www.last.fm/events/+Queretaro",
        ]
        all_urls = urls + cities_urls

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in all_urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # Last.fm usa JSON-LD en algunos eventos
                    ld = self._extract_jsonld(soup, url, "lastfm")
                    events.extend(ld)

                    # También scrapear las tarjetas de eventos
                    event_cards = soup.select("article.events-list-item, li.events-list-item, .event-item")
                    for card in event_cards[:50]:
                        try:
                            title_el = card.find(["h3", "h2", ".event-item--title"])
                            if not title_el:
                                title_el = card.find(class_=re.compile(r"title|heading|name", re.I))
                            title = title_el.get_text(strip=True) if title_el else ""
                            if not title or len(title) < 3:
                                continue

                            date_el = card.find("time")
                            date_str = date_el.get("datetime", "") if date_el else ""

                            link_el = card.find("a", href=True)
                            evt_url = ""
                            if link_el:
                                href = link_el["href"]
                                evt_url = href if href.startswith("http") else f"https://www.last.fm{href}"

                            loc_el = card.find(class_=re.compile(r"venue|location|lugar", re.I))
                            location = loc_el.get_text(strip=True) if loc_el else "México"

                            img_el = card.find("img")
                            img_url = img_el.get("src") if img_el else None

                            events.append(self._make_event(
                                source_id="lastfm",
                                title=title,
                                date_start=date_str,
                                location=location,
                                url=evt_url,
                                image_url=img_url,
                                category="entretenimiento",
                                tags=["concierto", "musica", "lastfm"],
                            ))
                        except Exception:
                            pass
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("LastFM error %s: %s", url, exc)

        return events

    # ── 2. Time Out México ──────────────────────────────────────────────
    async def _fetch_timeout_mexico(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.timeout.com/mexico-city/events",
            "https://www.timeout.com/mexico-city/events?page=2",
            "https://www.timeout.com/mexico-city/events?page=3",
            "https://www.timeout.com/mexico-city/music",
            "https://www.timeout.com/mexico-city/nightlife",
            "https://www.timeout.com/mexico-city/art",
            "https://www.timeout.com/mexico-city/things-to-do",
            "https://www.timeout.com/guadalajara/eventos",
            "https://www.timeout.com/guadalajara/events",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # JSON-LD
                    events.extend(self._extract_jsonld(soup, url, "timeout_mexico"))

                    # __NEXT_DATA__
                    events.extend(self._extract_next_data(soup, url, "timeout_mexico"))

                    # HTML cards
                    cards = soup.select(
                        "article[class*='tile'], div[class*='tile'], "
                        "li[class*='card'], div[class*='card-article']"
                    )
                    for card in cards[:30]:
                        evt = self._extract_generic_card(card, url, "timeout_mexico", "México")
                        if evt:
                            events.append(evt)

                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("TimeOut error %s: %s", url, exc)

        return events

    # ── 3. Fever App ────────────────────────────────────────────────────
    async def _fetch_fever(self) -> list[dict]:
        """Fever tiene API JSON pública para exploración de eventos."""
        events: list[dict] = []
        # Fever API pública (sin auth, solo para exploración)
        api_urls = [
            "https://api.feverup.com/api/discovery/v2/plans?city_id=21&limit=100&offset=0",    # CDMX
            "https://api.feverup.com/api/discovery/v2/plans?city_id=21&limit=100&offset=100",
            "https://api.feverup.com/api/discovery/v2/plans?city_id=21&limit=100&offset=200",
            "https://api.feverup.com/api/discovery/v2/plans?city_id=196&limit=100&offset=0",   # GDL
            "https://api.feverup.com/api/discovery/v2/plans?city_id=196&limit=100&offset=100",
        ]
        # Fallback HTML
        html_urls = [
            "https://feverup.com/en/mexico-city/events",
            "https://feverup.com/en/guadalajara/events",
            "https://feverup.com/en/monterrey/events",
            "https://feverup.com/en/cancun/events",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_JSON, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in api_urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        plans = data.get("plans") or data.get("data") or data.get("results") or []
                        for item in plans:
                            evt = self._map_fever_event(item)
                            if evt:
                                events.append(evt)
                    await asyncio.sleep(self.delay * 0.5)
                except Exception as exc:
                    logger.debug("Fever API error %s: %s", url, exc)

            if not events:
                for url in html_urls:
                    try:
                        resp = await client.get(url, headers=HEADERS_DESKTOP)
                        if resp.status_code != 200:
                            continue
                        soup = BeautifulSoup(resp.text, "html.parser")
                        events.extend(self._extract_jsonld(soup, url, "fever"))
                        events.extend(self._extract_next_data(soup, url, "fever"))
                        await asyncio.sleep(self.delay)
                    except Exception as exc:
                        logger.debug("Fever HTML error %s: %s", url, exc)

        return events

    def _map_fever_event(self, item: dict) -> Optional[dict]:
        title = (item.get("title") or item.get("name") or "").strip()
        if not title:
            return None
        return self._make_event(
            source_id="fever",
            title=title,
            date_start=item.get("start_date") or item.get("date"),
            location=item.get("venue_name") or item.get("location") or "México",
            url=item.get("url") or item.get("plan_url") or "",
            image_url=item.get("cover_image") or item.get("image_url"),
            price=item.get("min_price") or item.get("price"),
            category="entretenimiento",
            tags=["fever", "experiencias"],
            description=item.get("description") or "",
            lat=self._to_float(item.get("latitude")),
            lon=self._to_float(item.get("longitude")),
        )

    # ── 4. TicketNetwork ────────────────────────────────────────────────
    async def _fetch_ticketnetwork(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.ticketnetwork.com/events/mexico",
            "https://www.ticketnetwork.com/events/mexico-city-events.aspx",
            "https://www.ticketnetwork.com/events/guadalajara-events.aspx",
            "https://www.ticketnetwork.com/events/cancun-events.aspx",
            "https://www.ticketnetwork.com/events/monterrey-events.aspx",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "ticketnetwork"))

                    cards = soup.select("div.event, li.event, article, .event-listing")
                    for card in cards[:50]:
                        evt = self._extract_generic_card(card, url, "ticketnetwork", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("TicketNetwork error %s: %s", url, exc)
        return events

    # ── 5. AllEvents extra páginas ──────────────────────────────────────
    async def _fetch_allevents_extra(self) -> list[dict]:
        """Más páginas de AllEvents para ciudades no cubiertas antes."""
        events: list[dict] = []
        urls = [
            "https://allevents.in/monterrey",
            "https://allevents.in/monterrey/concerts",
            "https://allevents.in/monterrey/festivals",
            "https://allevents.in/cancun",
            "https://allevents.in/cancun/concerts",
            "https://allevents.in/tijuana",
            "https://allevents.in/puebla",
            "https://allevents.in/queretaro",
            "https://allevents.in/merida",
            "https://allevents.in/leon",
            "https://allevents.in/aguascalientes",
            "https://allevents.in/morelia",
            "https://allevents.in/chihuahua",
            "https://allevents.in/san-luis-potosi",
            "https://allevents.in/veracruz",
            "https://allevents.in/oaxaca",
            "https://allevents.in/mazatlan",
            "https://allevents.in/los-cabos",
            "https://allevents.in/torreon",
            "https://allevents.in/saltillo",
            "https://allevents.in/hermosillo",
            "https://allevents.in/culiacan",
            "https://allevents.in/acapulco",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "allevents"))

                    cards = soup.select("li.event-item, div.event-item, article.event")
                    for card in cards[:40]:
                        evt = self._extract_generic_card(card, url, "allevents", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay * 0.7)
                except Exception as exc:
                    logger.debug("AllEvents extra error %s: %s", url, exc)
        return events

    # ── 6. VivaAerobus Experiences (eventos ligados a vuelos MX) ────────
    async def _fetch_vivaevents(self) -> list[dict]:
        """Scraping de eventos culturales en sitios de turismo MX."""
        events: list[dict] = []
        urls = [
            "https://www.visitmexico.com/es/actividades-y-eventos",
            "https://www.visitmexico.com/es/fiestas-y-tradiciones",
            "https://www.mexicodesconocido.com.mx/eventos",
            "https://www.mexicodesconocido.com.mx/fiestas-y-tradiciones",
            "https://www.mexicodesconocido.com.mx/agenda-cultural.html",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "visitmexico"))
                    cards = soup.select("article, .event-card, .post, .card")
                    for card in cards[:30]:
                        evt = self._extract_generic_card(card, url, "visitmexico", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("VisitMexico error %s: %s", url, exc)
        return events

    # ── 7. Meetup HTML scraping ─────────────────────────────────────────
    async def _fetch_meetup_html(self) -> list[dict]:
        """Meetup HTML público sin API."""
        events: list[dict] = []
        search_urls = [
            "https://www.meetup.com/find/?location=mx--guadalajara&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--ciudad-de-mexico&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--monterrey&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--cancun&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--puebla&source=EVENTS",
            "https://www.meetup.com/find/?location=mx--queretaro&source=EVENTS",
            "https://www.meetup.com/find/?keywords=tech&location=mx--guadalajara&source=EVENTS",
            "https://www.meetup.com/find/?keywords=music&location=mx--guadalajara&source=EVENTS",
            "https://www.meetup.com/find/?keywords=fitness&location=mx--guadalajara&source=EVENTS",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in search_urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "meetup"))
                    events.extend(self._extract_next_data(soup, url, "meetup"))

                    cards = soup.select(
                        "div[data-event-id], article[data-event-id], "
                        "li[data-element-name='event-card'], div[class*='event-card']"
                    )
                    for card in cards[:30]:
                        evt = self._extract_generic_card(card, url, "meetup", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Meetup HTML error %s: %s", url, exc)
        return events

    # ── 8. PredictHQ Public ─────────────────────────────────────────────
    async def _fetch_predicthq(self) -> list[dict]:
        """
        PredictHQ tiene datos de eventos por API.
        El tier gratuito permite 1000 req/mes.
        """
        events: list[dict] = []
        # Sin API key usamos el scraping de su sitio público
        urls = [
            "https://www.predicthq.com/events?country=MX&category=concerts,festivals,performing-arts,sports,community,conferences,expos",
            "https://www.predicthq.com/events?country=MX&category=concerts",
            "https://www.predicthq.com/events?country=MX&category=festivals",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "predicthq"))
                    events.extend(self._extract_next_data(soup, url, "predicthq"))
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("PredictHQ error %s: %s", url, exc)
        return events

    # ── 9. AXS Mexico ───────────────────────────────────────────────────
    async def _fetch_axs(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.axs.com/mx/events",
            "https://www.axs.com/mx/events?q=guadalajara",
            "https://www.axs.com/mx/events?q=mexico+city",
            "https://www.axs.com/mx/events?q=monterrey",
            "https://www.axs.com/mx/events?q=cancun",
            # API pública de AXS
            "https://api.axs.com/v2/mx/events?countryCode=MX&size=100",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_JSON, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    ct = resp.headers.get("content-type", "")
                    if "json" in ct:
                        data = resp.json()
                        items = data.get("events") or data.get("data") or []
                        for item in items:
                            title = item.get("name") or item.get("title") or ""
                            if not title:
                                continue
                            events.append(self._make_event(
                                source_id="axs",
                                title=title,
                                date_start=item.get("dateTime") or item.get("date"),
                                location=item.get("venue", {}).get("name") or "México",
                                url=item.get("url") or "",
                                image_url=item.get("imageUrl"),
                                category="entretenimiento",
                                tags=["axs"],
                            ))
                    else:
                        soup = BeautifulSoup(resp.text, "html.parser")
                        events.extend(self._extract_jsonld(soup, url, "axs"))
                        events.extend(self._extract_next_data(soup, url, "axs"))
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("AXS error %s: %s", url, exc)
        return events

    # ── 10. Xceed (electrónica/nightlife) ───────────────────────────────
    async def _fetch_xceed(self) -> list[dict]:
        events: list[dict] = []
        api_urls = [
            "https://xceed.me/en/mexico/guadalajara--3/events",
            "https://xceed.me/en/mexico/mexico-city--2/events",
            "https://xceed.me/en/mexico/cancun--4/events",
            "https://xceed.me/en/mexico/monterrey--5/events",
            "https://xceed.me/en/mexico/tijuana--6/events",
            "https://xceed.me/en/mexico/playa-del-carmen--7/events",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in api_urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "xceed"))
                    events.extend(self._extract_next_data(soup, url, "xceed"))

                    cards = soup.select(".event-card, [class*='EventCard'], article")
                    for card in cards[:40]:
                        evt = self._extract_generic_card(card, url, "xceed", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Xceed error %s: %s", url, exc)
        return events

    # ── 11. Resident Advisor Mexico ─────────────────────────────────────
    async def _fetch_ra(self) -> list[dict]:
        events: list[dict] = []
        # RA tiene una API GraphQL pública
        api_url = "https://ra.co/graphql"
        # Ciudades de RA en México
        ra_areas = [
            {"id": "27", "name": "Mexico City"},
            {"id": "280", "name": "Guadalajara"},
            {"id": "374", "name": "Monterrey"},
            {"id": "437", "name": "Cancun"},
        ]

        query = """
        query GET_EVENTS_FOR_AREA($areaId: ID!, $page: Int!) {
          listing(
            filters: { areas: { id: $areaId }, event: { startDatetime: { gte: "NOW" } } }
            pageSize: 100
            page: $page
          ) {
            data {
              id
              title
              startTime
              endTime
              venue { name address }
              images { filename }
              artists { name }
              tickets { link }
            }
          }
        }
        """

        async with httpx.AsyncClient(
            headers={**HEADERS_JSON, "ra-content-language": "es"},
            timeout=self._timeout
        ) as client:
            for area in ra_areas:
                for page in range(1, 5):
                    try:
                        resp = await client.post(
                            api_url,
                            json={"query": query, "variables": {"areaId": area["id"], "page": page}},
                        )
                        if resp.status_code != 200:
                            break
                        data = resp.json()
                        items = (
                            data.get("data", {})
                            .get("listing", {})
                            .get("data", [])
                        )
                        if not items:
                            break
                        for item in items:
                            venue = item.get("venue") or {}
                            artists = item.get("artists") or []
                            artist_names = ", ".join(a.get("name", "") for a in artists[:3])
                            title = item.get("title") or (f"{artist_names} en {venue.get('name', area['name'])}" if artist_names else "")
                            if not title:
                                continue
                            tickets = item.get("tickets") or [{}]
                            url = tickets[0].get("link", "") if tickets else ""
                            images = item.get("images") or []
                            img = f"https://images.ra.co/b9/{images[0]['filename']}" if images else None
                            events.append(self._make_event(
                                source_id="resident_advisor",
                                title=title,
                                date_start=item.get("startTime"),
                                location=venue.get("name") or area["name"],
                                url=url,
                                image_url=img,
                                category="entretenimiento",
                                tags=["musica", "electronica", "ra"],
                            ))
                        await asyncio.sleep(self.delay)
                    except Exception as exc:
                        logger.debug("RA error area %s page %d: %s", area["name"], page, exc)
                        break

            # Fallback HTML
            if not events:
                for area in ra_areas:
                    try:
                        url = f"https://ra.co/events/mx/{area['name'].lower().replace(' ', '-')}"
                        resp = await client.get(url, headers=HEADERS_DESKTOP)
                        if resp.status_code == 200:
                            soup = BeautifulSoup(resp.text, "html.parser")
                            events.extend(self._extract_jsonld(soup, url, "resident_advisor"))
                            events.extend(self._extract_next_data(soup, url, "resident_advisor"))
                        await asyncio.sleep(self.delay)
                    except Exception:
                        pass

        return events

    # ── 12. Facebook Events público ─────────────────────────────────────
    async def _fetch_facebook_public(self) -> list[dict]:
        """mbasic.facebook.com permite ver eventos sin login."""
        events: list[dict] = []
        search_urls = [
            "https://mbasic.facebook.com/events/search/?q=guadalajara",
            "https://mbasic.facebook.com/events/search/?q=concierto+guadalajara",
            "https://mbasic.facebook.com/events/search/?q=festival+guadalajara",
            "https://mbasic.facebook.com/events/search/?q=evento+guadalajara",
            "https://mbasic.facebook.com/events/search/?q=evento+mexico",
            "https://mbasic.facebook.com/events/search/?q=concierto+mexico",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_MOBILE, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in search_urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # mbasic facebook tiene estructura muy simple
                    for link in soup.find_all("a", href=re.compile(r"/events/\d+")):
                        title_text = link.get_text(strip=True)
                        if not title_text or len(title_text) < 3:
                            continue
                        href = link["href"].split("?")[0]
                        evt_url = f"https://www.facebook.com{href}" if not href.startswith("http") else href

                        # Buscar fecha cercana al link
                        parent = link.find_parent(["div", "li", "td"])
                        date_text = ""
                        if parent:
                            time_el = parent.find("abbr") or parent.find("time")
                            if time_el:
                                date_text = time_el.get("title") or time_el.get_text(strip=True)

                        events.append(self._make_event(
                            source_id="facebook_events",
                            title=title_text,
                            date_start=date_text,
                            location="México",
                            url=evt_url,
                            category="social",
                            tags=["facebook"],
                        ))
                    await asyncio.sleep(self.delay * 2)
                except Exception as exc:
                    logger.debug("Facebook error %s: %s", url, exc)
        return events

    # ── 13. Eventim México ──────────────────────────────────────────────
    async def _fetch_eventim(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.eventim.mx/eventsearch?affiliate=EVX&fun=search&page=1",
            "https://www.eventim.mx/eventsearch?affiliate=EVX&fun=search&page=2",
            "https://www.eventim.mx/eventsearch?affiliate=EVX&fun=search&page=3",
            "https://www.eventim.mx/eventsearch?affiliate=EVX&fun=search&page=4",
            "https://www.eventim.mx/eventsearch?affiliate=EVX&fun=search&page=5",
            "https://www.eventim.mx/city/guadalajara/",
            "https://www.eventim.mx/city/mexico/",
            "https://www.eventim.mx/city/monterrey/",
            "https://www.eventim.mx/city/cancun/",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "eventim"))

                    cards = soup.select(
                        "li.result-item, div.event-item, article.event, "
                        ".search-result-item, [class*='EventCard']"
                    )
                    for card in cards[:50]:
                        evt = self._extract_generic_card(card, url, "eventim", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Eventim error %s: %s", url, exc)
        return events

    # ── 14. Teleticket ──────────────────────────────────────────────────
    async def _fetch_teleticket(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://teleticket.com.mx/",
            "https://teleticket.com.mx/conciertos",
            "https://teleticket.com.mx/teatro",
            "https://teleticket.com.mx/deportes",
            "https://teleticket.com.mx/shows",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "teleticket"))

                    cards = soup.select(".event-card, .show-card, article, li.event")
                    for card in cards[:50]:
                        evt = self._extract_generic_card(card, url, "teleticket", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Teleticket error %s: %s", url, exc)
        return events

    # ── 15. Boletos.Net ─────────────────────────────────────────────────
    async def _fetch_boletos_net(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.boletos.com/mx/conciertos/",
            "https://www.boletos.com/mx/teatro/",
            "https://www.boletos.com/mx/deportes/",
            "https://www.boletos.com/mx/guadalajara/",
            "https://www.boletos.com/mx/ciudad-de-mexico/",
            "https://www.boletos.com/mx/monterrey/",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "boletos_net"))
                    cards = soup.select(".event-card, article, li.event, .producto")
                    for card in cards[:50]:
                        evt = self._extract_generic_card(card, url, "boletos_net", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Boletos.Net error %s: %s", url, exc)
        return events

    # ── 16. Culturama ───────────────────────────────────────────────────
    async def _fetch_culturama(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.culturama.mx/agenda",
            "https://www.culturama.mx/conciertos",
            "https://www.culturama.mx/teatro",
            "https://www.culturama.mx/guadalajara",
            "https://www.culturama.mx/ciudad-de-mexico",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "culturama"))
                    cards = soup.select("article, .event-card, .agenda-item")
                    for card in cards[:40]:
                        evt = self._extract_generic_card(card, url, "culturama", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Culturama error %s: %s", url, exc)
        return events

    # ── 17. Cinemex Eventos ─────────────────────────────────────────────
    async def _fetch_cinemex(self) -> list[dict]:
        """Cinemex tiene funciones especiales (ópera, deportes, etc.)."""
        events: list[dict] = []
        urls = [
            "https://cinemex.com/eventos-especiales",
            "https://cinemex.com/alternativo",
            "https://cinemex.com/eventos",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "cinemex"))
                    cards = soup.select(".movie-card, .event-card, article, .pelicula")
                    for card in cards[:30]:
                        evt = self._extract_generic_card(card, url, "cinemex", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Cinemex error %s: %s", url, exc)
        return events

    # ── 18. Cinépolis Eventos Especiales ────────────────────────────────
    async def _fetch_cinepolis(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://cinepolis.com/eventos-especiales",
            "https://cinepolis.com/eventos",
            "https://www.cinepolisclub.com/eventos",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "cinepolis"))
                    events.extend(self._extract_next_data(soup, url, "cinepolis"))
                    cards = soup.select(".event-card, article, .movie-card, .swiper-slide")
                    for card in cards[:30]:
                        evt = self._extract_generic_card(card, url, "cinepolis", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Cinepolis error %s: %s", url, exc)
        return events

    # ── 19. La Lista Eventos ────────────────────────────────────────────
    async def _fetch_lalista(self) -> list[dict]:
        """La Lista es una guía cultural de CDMX muy completa."""
        events: list[dict] = []
        urls = [
            "https://www.lalista.mx/agenda",
            "https://www.lalista.mx/agenda?page=2",
            "https://www.lalista.mx/agenda?page=3",
            "https://www.lalista.mx/agenda?page=4",
            "https://www.lalista.mx/agenda?page=5",
            "https://www.lalista.mx/musica",
            "https://www.lalista.mx/teatro",
            "https://www.lalista.mx/arte",
            "https://www.lalista.mx/cine",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "lalista"))
                    events.extend(self._extract_next_data(soup, url, "lalista"))
                    cards = soup.select("article, .event-card, .agenda-item, .post-card")
                    for card in cards[:40]:
                        evt = self._extract_generic_card(card, url, "lalista", "Ciudad de México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("LaLista error %s: %s", url, exc)
        return events

    # ── 20. SeatGeek Mexico ─────────────────────────────────────────────
    async def _fetch_seatgeek(self) -> list[dict]:
        """SeatGeek tiene API pública con ID de cliente."""
        events: list[dict] = []
        # SeatGeek API pública (client_id público para frontend)
        base = "https://api.seatgeek.com/2/events"
        params_list = [
            {"venue.country": "MX", "per_page": 100, "page": 1, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 2, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 3, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 4, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 5, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 6, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 7, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 8, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 9, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
            {"venue.country": "MX", "per_page": 100, "page": 10, "client_id": "MjMyODk0OXwxNjE4ODU1NjMzLjU3"},
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_JSON, timeout=self._timeout, follow_redirects=True
        ) as client:
            for params in params_list:
                try:
                    resp = await client.get(base, params=params)
                    if resp.status_code != 200:
                        break
                    data = resp.json()
                    items = data.get("events") or []
                    if not items:
                        break
                    for item in items:
                        venue = item.get("venue") or {}
                        performers = item.get("performers") or [{}]
                        title = item.get("title") or item.get("short_title") or ""
                        if not title:
                            continue
                        events.append(self._make_event(
                            source_id="seatgeek",
                            title=title,
                            date_start=item.get("datetime_local") or item.get("datetime_utc"),
                            location=venue.get("name_v2") or venue.get("name") or "México",
                            url=item.get("url") or "",
                            image_url=performers[0].get("image") if performers else None,
                            price=item.get("stats", {}).get("lowest_price"),
                            category=self._map_seatgeek_category(item.get("type", "")),
                            tags=[item.get("type", ""), "seatgeek"],
                            lat=self._to_float(venue.get("location", {}).get("lat")),
                            lon=self._to_float(venue.get("location", {}).get("lon")),
                            external_id=str(item.get("id", "")),
                        ))
                    await asyncio.sleep(self.delay * 0.5)
                except Exception as exc:
                    logger.debug("SeatGeek error page %s: %s", params.get("page"), exc)
                    break
        return events

    def _map_seatgeek_category(self, type_str: str) -> str:
        t = type_str.lower()
        if "music" in t or "concert" in t:
            return "entretenimiento"
        if "sport" in t:
            return "deportivo"
        if "theater" in t or "comedy" in t or "dance" in t:
            return "cultural"
        if "family" in t:
            return "entretenimiento"
        return "entretenimiento"

    # ── 21. Eventix Mexico ──────────────────────────────────────────────
    async def _fetch_eventix(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://eventix.io/mx",
            "https://eventix.io/mx/guadalajara",
            "https://eventix.io/mx/mexico-city",
            "https://eventix.io/mx/monterrey",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "eventix"))
                    events.extend(self._extract_next_data(soup, url, "eventix"))
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Eventix error %s: %s", url, exc)
        return events

    # ── 22. Ticketea ────────────────────────────────────────────────────
    async def _fetch_ticketea(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.ticketea.com/es-mx/",
            "https://www.ticketea.com/es-mx/ciudad/guadalajara/",
            "https://www.ticketea.com/es-mx/ciudad/ciudad-de-mexico/",
            "https://www.ticketea.com/es-mx/ciudad/monterrey/",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "ticketea"))
                    cards = soup.select(".event-card, article, .evento")
                    for card in cards[:40]:
                        evt = self._extract_generic_card(card, url, "ticketea", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Ticketea error %s: %s", url, exc)
        return events

    # ── 23. TicketsToday ────────────────────────────────────────────────
    async def _fetch_ticketstoday(self) -> list[dict]:
        events: list[dict] = []
        urls = [
            "https://www.ticketstoday.com.mx/",
            "https://www.ticketstoday.com.mx/conciertos",
            "https://www.ticketstoday.com.mx/teatro",
            "https://www.ticketstoday.com.mx/deportes",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "ticketstoday"))
                    cards = soup.select(".event-card, article, li.event")
                    for card in cards[:40]:
                        evt = self._extract_generic_card(card, url, "ticketstoday", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("TicketsToday error %s: %s", url, exc)
        return events

    # ── 24. MX Concerts scraping masivo ─────────────────────────────────
    async def _fetch_mx_concerts_scrape(self) -> list[dict]:
        """Scraping de múltiples sitios de conciertos México."""
        events: list[dict] = []
        sources = [
            ("https://www.escenario.com.mx/agenda/guadalajara", "escenario_mx"),
            ("https://www.escenario.com.mx/agenda/guadalajara?page=2", "escenario_mx"),
            ("https://www.escenario.com.mx/agenda/guadalajara?page=3", "escenario_mx"),
            ("https://www.escenario.com.mx/agenda/mexico-df", "escenario_mx"),
            ("https://www.escenario.com.mx/agenda/monterrey", "escenario_mx"),
            ("https://www.ocesa.com.mx/eventos", "ocesa"),
            ("https://www.ocesa.com.mx/eventos?page=2", "ocesa"),
            ("https://www.ocesa.com.mx/eventos?page=3", "ocesa"),
            ("https://livenation.com.mx/eventos", "livenation_mx"),
            ("https://livenation.com.mx/eventos?page=2", "livenation_mx"),
            ("https://www.c3presents.com/events/", "c3presents"),
            ("https://www.foro-sol.com.mx/eventos", "forosol"),
            ("https://www.auditoriumnacional.mx/eventos", "auditorio"),
            ("https://www.auditoriumnacional.mx/conciertos", "auditorio"),
            ("https://www.palaciodelasbellesartes.mx/agenda", "bellas_artes"),
            ("https://www.arena.com.mx/eventos", "arena_mx"),
            ("https://www.arenamonterrey.com/eventos", "arena_mty"),
            ("https://www.arenagdl.com.mx/eventos", "arena_gdl"),
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url, source_id in sources:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")

                    ld = self._extract_jsonld(soup, url, source_id)
                    events.extend(ld)

                    nd = self._extract_next_data(soup, url, source_id)
                    events.extend(nd)

                    if not ld and not nd:
                        cards = soup.select(
                            "article, .event-card, .event-item, li.event, "
                            ".show, .concierto, [class*='event']"
                        )
                        for card in cards[:50]:
                            evt = self._extract_generic_card(card, url, source_id, "México")
                            if evt:
                                events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("MXConcerts %s error %s: %s", source_id, url, exc)
        return events

    # ── 25. Viberate ────────────────────────────────────────────────────
    async def _fetch_viberate(self) -> list[dict]:
        """Viberate tiene datos de eventos de artistas en México."""
        events: list[dict] = []
        urls = [
            "https://www.viberate.com/music-events/in-mexico/",
            "https://www.viberate.com/music-events/in-mexico/?page=2",
            "https://www.viberate.com/music-events/in-guadalajara/",
            "https://www.viberate.com/music-events/in-mexico-city/",
            "https://www.viberate.com/music-events/in-monterrey/",
            "https://www.viberate.com/music-events/in-cancun/",
        ]

        async with httpx.AsyncClient(
            headers=HEADERS_DESKTOP, timeout=self._timeout, follow_redirects=True
        ) as client:
            for url in urls:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "html.parser")
                    events.extend(self._extract_jsonld(soup, url, "viberate"))
                    events.extend(self._extract_next_data(soup, url, "viberate"))
                    cards = soup.select(".event-card, article, [class*='EventCard']")
                    for card in cards[:40]:
                        evt = self._extract_generic_card(card, url, "viberate", "México")
                        if evt:
                            events.append(evt)
                    await asyncio.sleep(self.delay)
                except Exception as exc:
                    logger.debug("Viberate error %s: %s", url, exc)
        return events

    # ── Helpers universales ─────────────────────────────────────────────

    def _extract_jsonld(
        self, soup: BeautifulSoup, page_url: str, source_id: str
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
                    t = item.get("@type", "")
                    if any(et in str(t) for et in [
                        "Event", "MusicEvent", "TheaterEvent", "SportsEvent",
                        "FoodEvent", "SocialEvent", "ExhibitionEvent", "Festival",
                        "ComedyEvent", "DanceEvent", "EducationEvent",
                    ]):
                        evt = self._map_jsonld(item, page_url, source_id)
                        if evt:
                            events.append(evt)
            except Exception:
                pass
        return events

    def _extract_next_data(
        self, soup: BeautifulSoup, page_url: str, source_id: str
    ) -> list[dict]:
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag:
            return []
        events = []
        try:
            data = json.loads(tag.string or "")
            props = data.get("props", {}).get("pageProps", {})
            # Buscar en múltiples paths posibles
            candidates = [
                props.get("events"),
                props.get("concerts"),
                props.get("shows"),
                props.get("items"),
                props.get("agenda"),
                props.get("initialData", {}).get("events") if isinstance(props.get("initialData"), dict) else None,
                props.get("data", {}).get("events") if isinstance(props.get("data"), dict) else None,
            ]
            for raw_list in candidates:
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
                    venue = item.get("venue") or item.get("lugar") or {}
                    if isinstance(venue, str):
                        location = venue
                    else:
                        location = (
                            venue.get("name") or venue.get("displayName") or
                            venue.get("nombre") or "México"
                        )
                    events.append(self._make_event(
                        source_id=source_id,
                        title=title,
                        date_start=(
                            item.get("startDate") or item.get("date") or
                            item.get("dateTime") or item.get("fecha") or
                            (item.get("start") or {}).get("datetime") if isinstance(item.get("start"), dict) else None
                        ),
                        location=location,
                        url=item.get("url") or item.get("uri") or item.get("link") or page_url,
                        image_url=item.get("imageUrl") or item.get("image") or item.get("imagen"),
                        category="entretenimiento",
                        tags=[source_id],
                        description=item.get("description") or item.get("descripcion") or "",
                    ))
        except Exception:
            pass
        return events

    def _extract_generic_card(
        self,
        card: Any,
        page_url: str,
        source_id: str,
        ciudad_default: str,
    ) -> Optional[dict]:
        try:
            # Título
            title_el = (
                card.find(["h1", "h2", "h3", "h4"])
                or card.find(class_=re.compile(r"title|titulo|name|nombre|heading", re.I))
            )
            title = title_el.get_text(strip=True) if title_el else ""
            if not title or len(title) < 3:
                return None

            # URL
            link_el = card.find("a", href=True)
            url = ""
            if link_el:
                href = link_el["href"]
                base = "/".join(page_url.split("/")[:3])
                url = href if href.startswith("http") else (base + href if href.startswith("/") else urljoin(page_url, href))

            # Fecha
            date_el = card.find("time") or card.find(attrs={"datetime": True})
            date_str = date_el.get("datetime", "") if date_el else ""
            if not date_str:
                date_el2 = card.find(class_=re.compile(r"date|fecha|when", re.I))
                date_str = date_el2.get_text(strip=True) if date_el2 else ""

            # Imagen
            img_el = card.find("img")
            image_url = None
            if img_el:
                src = img_el.get("src") or img_el.get("data-src") or img_el.get("data-lazy-src", "")
                if src and not src.startswith("data:"):
                    base = "/".join(page_url.split("/")[:3])
                    image_url = src if src.startswith("http") else (base + src if src.startswith("/") else src)

            # Ubicación
            loc_el = card.find(class_=re.compile(r"location|venue|lugar|sede|recinto|city|ciudad", re.I))
            location = loc_el.get_text(strip=True) if loc_el else ciudad_default

            return self._make_event(
                source_id=source_id,
                title=title,
                date_start=date_str or None,
                location=location,
                url=url,
                image_url=image_url,
                category="entretenimiento",
                tags=[source_id],
            )
        except Exception:
            return None

    def _map_jsonld(self, item: dict, page_url: str, source_id: str) -> Optional[dict]:
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
                    addr.get("addressCountry", ""),
                ]
                location_name = location_name or ", ".join(p for p in parts if p)
            elif isinstance(addr, str):
                location_name = location_name or addr
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
            source_id=source_id,
            title=title,
            date_start=item.get("startDate"),
            date_end=item.get("endDate"),
            location=location_name or "México",
            url=item.get("url") or page_url,
            image_url=image,
            price=price,
            category="entretenimiento",
            tags=[source_id],
            description=item.get("description", ""),
            lat=lat,
            lon=lon,
            external_id=item.get("@id") or item.get("identifier") or "",
        )

    def _make_event(
        self,
        source_id: str,
        title: str,
        date_start=None,
        date_end=None,
        location: str = "México",
        url: str = "",
        image_url=None,
        price=None,
        category: str = "entretenimiento",
        tags: list = None,
        description: str = "",
        lat=None,
        lon=None,
        external_id: str = "",
    ) -> dict:
        return {
            "source_id":   source_id,
            "external_id": external_id or url or title[:50],
            "title":       title.strip(),
            "description": description,
            "category":    category,
            "tags":        tags or [source_id],
            "image_url":   image_url,
            "date_start":  date_start,
            "date_end":    date_end,
            "price":       price,
            "currency":    "MXN",
            "url":         url,
            "location":    location,
            "latitude":    lat,
            "longitude":   lon,
            "estado":      "",
            "ciudad":      "",
        }

    @staticmethod
    def _to_float(value) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None