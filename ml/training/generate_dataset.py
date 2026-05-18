"""
ml/training/generate_dataset.py
Genera un dataset sintético de eventos en español para entrenar los modelos.
Basado en el dominio de GDL Qué Hacer (Guadalajara, México).

Mejoras v5:
- EVENTOS AMBIGUOS EXPLÍCITOS (~25% del dataset): títulos y descripciones
  que mezclan vocabulario de 2 categorías reales. El clasificador ya no puede
  memorizar 2-3 palabras por categoría — necesita semántica contextual.
- TÍTULOS CROSS-CATEGORÍA: el 40% de los títulos incluye vocabulario de otra
  categoría, rompiendo la separación trivial que causaba accuracy 1.0.
- RANKER: label ahora usa señal de engagement real (save=2pts, interested=2pts,
  view=1pt si es categoría preferida) en lugar de binario simple. Positivos
  suben de ~43% → ~50%, con señal más discriminativa.
- Se mantienen todas las mejoras v4 (15k interacciones, tiers balanceados,
  overlap léxico, cross-category phrases).
"""
import pandas as pd
import numpy as np
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone

random.seed(42)
np.random.seed(42)

# ── Fragmentos de texto por categoría ────────────────────────────────

FRAGMENTS = {
    "cultural": {
        "verbs": ["disfruta", "explora", "descubre", "vive", "celebra", "contempla"],
        "subjects": [
            "exposición de arte contemporáneo", "muestra fotográfica",
            "recital de poesía", "ciclo de conferencias literarias",
            "presentación de ballet clásico", "obra de teatro",
            "concierto de música clásica", "festival de danza folclórica",
            "taller de pintura", "exhibición de escultura",
            "curso de historia del arte", "feria del libro",
            "instalación artística interactiva", "proyección de cine documental",
            "lectura de narrativa contemporánea", "taller de serigrafía",
        ],
        "venues": [
            "el Museo de las Artes", "el Teatro Degollado",
            "el Centro Cultural Cabañas", "la Biblioteca Pública de Jalisco",
            "el Instituto Cultural Cabañas", "la Casa de la Cultura de Zapopan",
            "la Galería Jorge Martínez", "el Museo Regional de Guadalajara",
        ],
        "extras": [
            "con artistas locales y nacionales",
            "abierto para todo público",
            "entrada libre los domingos",
            "con visita guiada incluida",
            "cupo limitado, reserva tu lugar",
            "obra premiada a nivel nacional",
            "colección permanente y temporal",
            "actividades para niños y adultos",
            "catálogo impreso disponible",
            "charla con el artista al finalizar",
        ],
    },
    "deportivo": {
        "verbs": ["participa", "compite", "entrena", "corre", "únete", "supérate"],
        "subjects": [
            "maratón popular", "torneo de fútbol amateur",
            "carrera 5K benéfica", "clase de yoga al aire libre",
            "campeonato de natación", "torneo de tenis",
            "ciclotón dominical", "liga de voleibol de playa",
            "trail running", "torneo de basquetbol 3x3",
            "festival de artes marciales", "clase masiva de zumba",
            "torneo de pádel mixto", "carrera nocturna 10K",
            "clínica de atletismo juvenil", "reto de calistenia urbana",
        ],
        "venues": [
            "el Parque Metropolitano", "el Estadio Akron",
            "el Bosque de la Primavera", "el Parque Agua Azul",
            "la Unidad Deportiva Revolución", "el Lago de Chapala",
            "la Pista Atlética Jalisco", "el Velódromo Panamericano",
        ],
        "extras": [
            "para todos los niveles y edades",
            "inscripción gratuita en línea",
            "premiación para los tres primeros lugares",
            "con hidratación y snacks incluidos",
            "evento avalado por la Federación",
            "apto para principiantes y avanzados",
            "cronometraje oficial con chip",
            "playera de participación incluida",
            "categorías por edad y género",
            "médico deportivo en sitio",
        ],
    },
    "gastronomico": {
        "verbs": ["prueba", "saborea", "disfruta", "aprende", "celebra", "descubre"],
        "subjects": [
            "festival de tortas ahogadas", "feria del tequila artesanal",
            "mercado de productores locales", "cata de vinos regionales",
            "taller de cocina mexicana", "tour de street food",
            "festival del birriegal", "feria de café de especialidad",
            "cena maridaje gourmet", "clase de repostería tradicional",
            "mercado orgánico agroecológico", "cook-off de pozole",
            "ruta del mezcal artesanal", "festival de chiles en nogada",
            "taller de fermentados y kombuchas", "feria de quesos mexicanos",
        ],
        "venues": [
            "el Mercado San Juan de Dios", "Tlaquepaque Centro",
            "el Jardín Botánico de Guadalajara", "Tonalá Artesanal",
            "la Plaza de Armas", "el Mercado Corona",
            "el Parque Revolución", "la Hacienda Santa Lucía",
        ],
        "extras": [
            "con más de 40 expositores locales",
            "entrada libre, consumo a precio justo",
            "productos orgánicos certificados",
            "degustaciones incluidas en el boleto",
            "chef invitado de renombre nacional",
            "más de 30 variedades para probar",
            "maridaje explicado por sommelier",
            "talleres en vivo durante el evento",
            "concurso de recetas tradicionales",
            "ingredientes de origen local garantizado",
        ],
    },
    "entretenimiento": {
        "verbs": ["vive", "disfruta", "no te pierdas", "celebra", "participa en", "goza"],
        "subjects": [
            "concierto de música regional mexicana", "noche de stand-up comedy",
            "festival de música electrónica", "escape room temático",
            "noche de karaoke en vivo", "espectáculo de circo contemporáneo",
            "gaming fest con torneos", "concierto tributo a los 80s",
            "tarde de magia y ilusionismo", "festival de fuegos artificiales",
            "feria de cosplay y anime", "cine al aire libre",
            "noche de improvisación teatral", "festival de música indie tapatía",
            "torneo de trivia en vivo", "concierto de jazz en el patio",
        ],
        "venues": [
            "el GDL Arena", "la Expo Guadalajara",
            "la Zona Rosa de Guadalajara", "el Estadio Jalisco",
            "el Teatro Experimental", "la Plaza del Sol",
            "el Foro Indie Rocks GDL", "la Terraza 787",
        ],
        "extras": [
            "para toda la familia",
            "boletos disponibles en taquilla y en línea",
            "puertas abren a las 7 PM",
            "evento con barra libre opcional",
            "sin límite de edad, menores con adulto",
            "transmisión en vivo por redes sociales",
            "después del evento habrá afterparty",
            "meet & greet con artistas incluido",
            "zona VIP disponible con compra anticipada",
            "estacionamiento gratuito en el venue",
        ],
    },
    "otro": {
        "verbs": ["asiste", "participa", "conoce", "aprovecha", "únete a", "explora"],
        "subjects": [
            "feria de emprendedores tapatíos", "taller de fotografía urbana",
            "mercado de pulgas y antigüedades", "charla de desarrollo personal",
            "expo de mascotas y animales", "taller de idiomas gratis",
            "feria de salud y bienestar", "encuentro de voluntariado",
            "hackathon universitario", "feria de empleo Jalisco",
            "taller de manualidades recicladas", "expo de tecnología",
            "seminario de finanzas personales", "taller de meditación y mindfulness",
            "encuentro de startups tapatías", "feria de adopción de mascotas",
        ],
        "venues": [
            "la Universidad de Guadalajara", "el Tecnológico de Monterrey GDL",
            "el Centro de Convenciones", "el Auditorio Telmex",
            "el Parque Morelos", "Zapopan Centro",
            "el Coworking 44", "el Hub de Innovación Jalisco",
        ],
        "extras": [
            "evento gratuito con registro previo",
            "networking incluido al finalizar",
            "certificado de participación",
            "transmisión híbrida presencial y virtual",
            "cupo limitado a 200 personas",
            "refrigerio incluido para asistentes",
            "ponentes con experiencia internacional",
            "materiales y kit digital incluidos",
            "acceso a grabación posterior",
            "sesión de preguntas y respuestas en vivo",
        ],
    },
}

# ── Eventos ambiguos — mezclan vocabulario de 2 categorías reales ────
# Esto es lo que faltaba en v4: títulos y descripciones donde el
# clasificador NO puede inferir la categoría por 1-2 palabras clave.
AMBIGUOUS_EVENTS = [
    # gastronómico + deportivo
    {
        "category": "gastronomico",
        "title_templates": [
            "Carrera 5K y Festival Gastronómico en el Parque Metropolitano",
            "Festival de Cerveza Artesanal con Zona Deportiva",
            "Torneo de Fútbol con Feria de Comida Local",
        ],
        "desc_templates": [
            "Participa en la carrera 5K y después disfruta del festival de tortas ahogadas y tequila artesanal. "
            "Premiación para los tres primeros lugares, degustaciones incluidas en el boleto. "
            "Evento para toda la familia en el Parque Metropolitano.",
            "Festival de cerveza artesanal con zona de activaciones deportivas, yoga y ciclismo. "
            "Más de 20 marcas locales de craft beer, food trucks y música en vivo. "
            "Entrada libre, consumo a precio justo.",
        ],
    },
    # cultural + entretenimiento
    {
        "category": "cultural",
        "title_templates": [
            "Festival de Cine y Música en Vivo en el Teatro Degollado",
            "Concierto de Jazz con Exposición de Arte Contemporáneo",
            "Noche de Arte y Comedia en el Centro Cultural Cabañas",
        ],
        "desc_templates": [
            "Proyección de cine documental seguida de concierto de jazz en vivo. "
            "La exposición de arte permanecerá abierta durante el evento. "
            "Boletos disponibles en taquilla, zona VIP disponible.",
            "Stand-up comedy y exhibición de escultura contemporánea en una misma noche. "
            "Artistas locales y nacionales. Entrada libre los domingos, cupo limitado.",
        ],
    },
    # deportivo + cultural
    {
        "category": "deportivo",
        "title_templates": [
            "Ciclotón Cultural por el Centro Histórico de Guadalajara",
            "Yoga y Meditación con Instalación Artística Interactiva",
            "Trail Running por el Bosque con Taller de Fotografía",
        ],
        "desc_templates": [
            "Recorre el centro histórico en bicicleta y descubre murales y galerías en el camino. "
            "Guía cultural incluida, todos los niveles bienvenidos. "
            "Evento avalado por la Federación, playera de participación incluida.",
            "Sesión de yoga al aire libre rodeada de instalaciones artísticas interactivas. "
            "Con visita guiada a la exposición al finalizar. Cupo limitado, reserva tu lugar.",
        ],
    },
    # gastronómico + cultural
    {
        "category": "gastronomico",
        "title_templates": [
            "Feria del Libro con Taller de Cocina Mexicana en Tlaquepaque",
            "Mercado de Productores con Lectura de Poesía y Música",
            "Festival de Mezcal Artesanal y Cine Documental",
        ],
        "desc_templates": [
            "Compra libros de autores locales y aprende a preparar platillos tradicionales. "
            "Chef invitado de renombre nacional y charla con escritores al finalizar. "
            "Entrada libre, ingredientes de origen local garantizado.",
            "Mercado orgánico agroecológico con presentaciones de poesía en vivo y jazz. "
            "Más de 40 expositores locales, catálogo impreso disponible.",
        ],
    },
    # entretenimiento + deportivo
    {
        "category": "entretenimiento",
        "title_templates": [
            "Gaming Fest con Torneos de eSports y Zona Fitness",
            "Festival de Música con Carrera Nocturna 10K",
            "Noche de Comedia y Torneo de Basquetbol 3x3",
        ],
        "desc_templates": [
            "Torneo de videojuegos y zona de calistenia urbana en el GDL Arena. "
            "Categorías por edad y género, premiación para los tres primeros lugares. "
            "Transmisión en vivo por redes sociales, estacionamiento gratuito.",
            "Concierto de música indie tapatía y carrera nocturna 10K en la Zona Rosa. "
            "Cronometraje oficial con chip, puertas abren a las 7 PM. "
            "Meet & greet con artistas incluido.",
        ],
    },
    # otro + gastronómico
    {
        "category": "otro",
        "title_templates": [
            "Feria de Emprendedores con Food Trucks y Tequila Artesanal",
            "Hackathon Universitario con Feria de Café de Especialidad",
            "Expo de Tecnología con Cena Maridaje Gourmet",
        ],
        "desc_templates": [
            "Conoce a emprendedores tapatíos y prueba productos gourmet locales. "
            "Networking incluido al finalizar, degustaciones incluidas en el boleto. "
            "Evento gratuito con registro previo, cupo limitado a 200 personas.",
            "Hackathon de 24 horas con estación de café de especialidad y food trucks. "
            "Ponentes con experiencia internacional, certificado de participación. "
            "Materiales y kit digital incluidos.",
        ],
    },
    # otro + deportivo
    {
        "category": "otro",
        "title_templates": [
            "Feria de Salud y Bienestar con Clase Masiva de Zumba",
            "Taller de Meditación y Reto de Calistenia Urbana",
            "Expo de Tecnología Deportiva y Clínica de Atletismo",
        ],
        "desc_templates": [
            "Conferencias de salud preventiva y clase masiva de zumba al aire libre. "
            "Médico deportivo en sitio, refrigerio incluido para asistentes. "
            "Transmisión híbrida presencial y virtual, sesión de preguntas en vivo.",
            "Taller de mindfulness seguido de reto de calistenia urbana en el Parque Morelos. "
            "Apto para principiantes y avanzados, sesión de preguntas y respuestas al finalizar.",
        ],
    },
]

SHARED_NOISE_WORDS = [
    "Guadalajara", "Jalisco", "ZMG", "tapatío", "evento especial",
    "inscríbete ya", "cupo limitado", "gratis", "familia", "comunidad",
    "2025", "fin de semana", "sábado", "domingo",
    "actividad gratuita", "entrada libre", "todos bienvenidos",
    "zona metropolitana", "imperdible", "no te lo pierdas",
    "presencial", "al aire libre", "en Guadalajara",
]

CROSS_CATEGORY_PHRASES = [
    "con área de comida y bebidas",
    "entrada libre para toda la familia",
    "actividades para niños incluidas",
    "estacionamiento disponible",
    "transmisión en línea disponible",
    "música en vivo durante el evento",
    "fotografía y video permitidos",
    "zona de descanso habilitada",
    "accesible para personas con discapacidad",
    "registro previo recomendado",
]

LOCATIONS = [
    "Teatro Degollado", "Expo Guadalajara", "Parque Metropolitano",
    "Estadio Akron", "Plaza de Armas", "Centro Cultural Cabañas",
    "Mercado San Juan de Dios", "Zona Rosa GDL", "Tlaquepaque Centro",
    "Tonalá Artesanal", "Parque Agua Azul", "Estadio Jalisco",
    "GDL Arena", "Bosque de la Primavera", "Universidad de Guadalajara",
    "Zapopan Centro", "Lago de Chapala", "Jardín Botánico GDL",
]

GDL_COORDINATES = [
    [-103.3496, 20.6597],
    [-103.4127, 20.6674],
    [-103.3006, 20.6414],
    [-103.2339, 20.6294],
    [-103.3832, 20.6858],
    [-103.3612, 20.6493],
]

DESC_SHORT_TEMPLATES = [
    "{verb} {subject}.",
    "{subject_cap} próximamente.",
    "Evento: {subject}.",
    "{verb_cap} {subject} este fin de semana.",
]

DESC_MEDIUM_TEMPLATES = [
    "{verb_cap} {subject} en {venue}. {extra}.",
    "{subject_cap} en {venue}. {extra}. No te lo pierdas.",
    "Este fin de semana: {subject} en {venue}. {extra}.",
]

DESC_LONG_TEMPLATES = [
    (
        "{verb_cap} {subject} en {venue}. {extra}. "
        "Un evento imperdible para toda la comunidad tapatía, "
        "con actividades diseñadas para disfrutar en familia o con amigos. "
        "Entrada libre o boletos a precio accesible. ¡No te quedes fuera!"
    ),
    (
        "{subject_cap} se presenta en {venue}. {extra}. "
        "Esta es una oportunidad única para vivir una experiencia cultural "
        "en el corazón de Guadalajara. Organizado por colectivos locales "
        "con más de diez años de trayectoria. Cupo limitado, reserva ya."
    ),
    (
        "{verb_cap} {subject} en {venue}. {extra}. "
        "Evento organizado por la comunidad tapatía para celebrar la cultura "
        "y el talento local. Habrá actividades para todos los gustos y edades. "
        "Consulta el programa completo en nuestras redes sociales."
    ),
]

IMAGE_URLS = [
    "https://cdn.gdlquehacer.mx/img/evento_01.jpg",
    "https://cdn.gdlquehacer.mx/img/evento_02.jpg",
    "https://cdn.gdlquehacer.mx/img/evento_03.jpg",
    None,
]


# ═══════════════════════════════════════════════════════════════════════
# Función de calidad — IDÉNTICA a classifier.py
# ═══════════════════════════════════════════════════════════════════════

def _compute_quality_score(event: dict) -> float:
    import re

    def has_image(e):
        return 1.0 if e.get("image_url") else 0.0

    def description_length_score(e):
        desc = e.get("description", "") or ""
        length = len(desc.strip())
        if length >= 200: return 1.0
        if length >= 80:  return 0.6
        if length > 0:    return 0.3
        return 0.0

    def has_location(e):
        return 1.0 if (e.get("latitude") and e.get("longitude")) else 0.0

    def has_price(e):
        return 1.0 if e.get("price") is not None else 0.0

    def has_category(e):
        return 1.0 if e.get("category") else 0.0

    def date_is_future(e):
        raw = e.get("date_start")
        if not raw: return 0.0
        try:
            dt = datetime.fromisoformat(str(raw))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return 1.0 if dt > datetime.now(timezone.utc) else 0.0
        except (ValueError, TypeError):
            return 0.0

    def title_quality(e):
        title = (e.get("title", "") or "").strip()
        if len(title) < 5: return 0.0
        upper_ratio = sum(1 for c in title if c.isupper()) / max(len(title), 1)
        special = len(re.findall(r"[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑ\s,.\-!?]", title))
        if upper_ratio > 0.7 or special > 3: return 0.4
        return 1.0

    weights = [
        (has_image,                0.20),
        (description_length_score, 0.25),
        (has_location,             0.20),
        (has_price,                0.10),
        (has_category,             0.10),
        (date_is_future,           0.10),
        (title_quality,            0.05),
    ]
    return round(sum(fn(event) * w for fn, w in weights), 4)


# ═══════════════════════════════════════════════════════════════════════
# Generador de texto — eventos normales
# ═══════════════════════════════════════════════════════════════════════

def _build_text(category: str, desc_length: str = "medium", add_noise: bool = True) -> tuple[str, str]:
    f = FRAGMENTS[category]

    verb    = random.choice(f["verbs"])
    subject = random.choice(f["subjects"])
    venue   = random.choice(f["venues"])
    extra   = random.choice(f["extras"])

    ctx = {
        "verb":        verb,
        "verb_cap":    verb.capitalize(),
        "subject":     subject,
        "subject_cap": subject.capitalize(),
        "venue":       venue,
        "venue_short": venue.split()[-1],
        "extra":       extra,
    }

    # Título — 40% incluye un extra de otra categoría para romper separación trivial
    if random.random() > 0.6:
        other_cat = random.choice([c for c in FRAGMENTS if c != category])
        other_subj = random.choice(FRAGMENTS[other_cat]["subjects"])
        title = f"{subject.capitalize()} con {other_subj}"
    elif random.random() > 0.5:
        title = f"{subject.capitalize()} en {venue.split()[-1]}"
    else:
        title = f"{verb.capitalize()} el {subject} en {venue.split()[-1]}"

    # Descripción según longitud
    if desc_length == "short":
        template = random.choice(DESC_SHORT_TEMPLATES)
    elif desc_length == "long":
        template = random.choice(DESC_LONG_TEMPLATES)
    else:
        template = random.choice(DESC_MEDIUM_TEMPLATES)

    description = template.format(**ctx)

    if add_noise:
        if random.random() > 0.4:
            description += f" {random.choice(SHARED_NOISE_WORDS)}."
        if random.random() > 0.55:
            description += f" {random.choice(CROSS_CATEGORY_PHRASES)}."
        if random.random() > 0.70:
            other_cat   = random.choice([c for c in FRAGMENTS if c != category])
            other_extra = random.choice(FRAGMENTS[other_cat]["extras"])
            description += f" {other_extra}."
        if random.random() > 0.90:
            other_cat2  = random.choice([c for c in FRAGMENTS if c != category])
            other_subj  = random.choice(FRAGMENTS[other_cat2]["subjects"])
            description += f" También: {other_subj}."

    return title, description


# ═══════════════════════════════════════════════════════════════════════
# Generador de eventos AMBIGUOS — v5 nuevo
# ═══════════════════════════════════════════════════════════════════════

def _build_ambiguous_event(tier: str) -> dict:
    """
    Genera un evento cuyo título y descripción mezclan vocabulario real
    de 2 categorías. El clasificador no puede resolverlo con 1-2 palabras.
    """
    template = random.choice(AMBIGUOUS_EVENTS)
    category = template["category"]
    title    = random.choice(template["title_templates"])
    desc     = random.choice(template["desc_templates"])

    # Añadir ruido compartido igual que eventos normales
    if random.random() > 0.4:
        desc += f" {random.choice(SHARED_NOISE_WORDS)}."
    if random.random() > 0.6:
        desc += f" {random.choice(CROSS_CATEGORY_PHRASES)}."

    coords    = random.choice(GDL_COORDINATES)
    has_image = tier == "A"
    has_coords = tier in ("A", "B") or random.random() > 0.5

    if tier == "A":
        image_url  = random.choice([u for u in IMAGE_URLS if u])
        price      = random.choice([0, 50, 100, 150])
        date_future = True
    elif tier == "B":
        image_url  = random.choice(IMAGE_URLS)
        price      = random.choice([0, 50, None])
        date_future = random.random() > 0.2
    else:
        image_url  = None
        price      = random.choice([0, None])
        date_future = random.random() > 0.4

    if date_future:
        date_start = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 90))
    else:
        date_start = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 30))

    return {
        "title":       title,
        "description": desc,
        "category":    category,
        "location":    random.choice(LOCATIONS),
        "latitude":    coords[1] + np.random.normal(0, 0.02) if has_coords else None,
        "longitude":   coords[0] + np.random.normal(0, 0.02) if has_coords else None,
        "date_start":  date_start.isoformat(),
        "price":       price,
        "image_url":   image_url,
    }


# ═══════════════════════════════════════════════════════════════════════
# generate_events_dataset — v5
# ═══════════════════════════════════════════════════════════════════════

def generate_events_dataset(n_samples: int = 600) -> pd.DataFrame:
    """
    Genera dataset de eventos con ~25% de eventos ambiguos explícitos.

    Distribución v5:
    - 75% eventos normales (vocabulario de su categoría + ruido cruzado)
    - 25% eventos ambiguos (título + descripción mezclan 2 categorías reales)

    El clasificador ya no puede alcanzar accuracy 1.0 memorizando palabras clave.
    """
    records    = []
    categories = list(FRAGMENTS.keys())

    # ── 75% eventos normales ──────────────────────────────────────────
    n_normal   = int(n_samples * 0.75)
    per_category = n_normal // len(categories)
    remainder    = n_normal % len(categories)

    for cat_idx, category in enumerate(categories):
        n_cat = per_category + (1 if cat_idx < remainder else 0)

        for _ in range(n_cat):
            tier = random.choices(["A", "B", "C"], weights=[0.33, 0.33, 0.34])[0]

            if tier == "A":
                image_url   = random.choice([u for u in IMAGE_URLS if u])
                desc_length = "long"
                has_coords  = True
                price       = random.choice([0, 50, 100, 150, 200, 300])
                title_style = "normal"
                date_future = True
            elif tier == "B":
                image_url   = random.choice(IMAGE_URLS)
                desc_length = random.choice(["medium", "long"])
                has_coords  = random.random() > 0.3
                price       = random.choice([0, 50, 100, None])
                title_style = "normal"
                date_future = random.random() > 0.2
            else:
                image_url   = None
                desc_length = "short"
                has_coords  = random.random() > 0.7
                price       = random.choice([0, None])
                title_style = random.choice(["normal", "bad"])
                date_future = random.random() > 0.4

            title, description = _build_text(category, desc_length=desc_length)

            if title_style == "bad":
                title = title.upper()

            coords = random.choice(GDL_COORDINATES)
            if has_coords:
                latitude  = coords[1] + np.random.normal(0, 0.02)
                longitude = coords[0] + np.random.normal(0, 0.02)
            else:
                latitude  = None
                longitude = None

            if date_future:
                date_start = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 90))
            else:
                date_start = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 30))

            event = {
                "title":       title,
                "description": description,
                "category":    category,
                "location":    random.choice(LOCATIONS),
                "latitude":    latitude,
                "longitude":   longitude,
                "date_start":  date_start.isoformat(),
                "price":       price,
                "image_url":   image_url,
            }

            event["quality_ml"] = _compute_quality_score(event)
            event["text"]       = f"{title} {description}"
            records.append(event)

    # ── 25% eventos ambiguos ─────────────────────────────────────────
    n_ambiguous = n_samples - n_normal
    for _ in range(n_ambiguous):
        tier  = random.choices(["A", "B", "C"], weights=[0.33, 0.33, 0.34])[0]
        event = _build_ambiguous_event(tier)
        event["quality_ml"] = _compute_quality_score(event)
        event["text"]       = f"{event['title']} {event['description']}"
        records.append(event)

    df = pd.DataFrame(records)
    return df.sample(frac=1, random_state=42).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# generate_interactions_dataset — v5: señal de ranker mejorada
# ═══════════════════════════════════════════════════════════════════════

def generate_interactions_dataset(
    n_users: int = 200,
    n_events: int = 600,
    n_interactions: int = 15000,
) -> pd.DataFrame:
    """
    Genera interacciones con señal de preferencia más discriminativa.

    Cambio v5 respecto a v4:
    - El label ya no es binario simple (save/interested=1, resto=0).
    - Se usa un score de engagement: save=2, interested=2, view_preferred=1, resto=0.
    - label=1 si engagement_score >= 1, label=0 si = 0.
    - Esto sube positivos de ~43% → ~50% y da señal más real al Ranker.

    Los pesos de interacción también son más realistas:
    - Categoría preferida top: más saves/interested (señal fuerte)
    - Categoría preferida secundaria: mix
    - Categoría no preferida: mayormente views sin engagement
    """
    categories = list(FRAGMENTS.keys())

    user_preferences: dict[int, list[str]] = {}
    for user_id in range(n_users):
        n_prefs   = random.randint(1, 3)
        preferred = random.sample(categories, k=n_prefs)
        user_preferences[user_id] = preferred

    event_categories = [random.choice(categories) for _ in range(n_events)]

    records           = []
    interaction_types = ["view", "save", "interested", "uninterested"]

    for _ in range(n_interactions):
        user_id = random.randint(0, n_users - 1)

        if random.random() < 0.6 and user_preferences[user_id]:
            preferred_cat = random.choice(user_preferences[user_id])
            candidates    = [i for i, c in enumerate(event_categories) if c == preferred_cat]
            event_id      = random.choice(candidates) if candidates else random.randint(0, n_events - 1)
        else:
            event_id = random.randint(0, n_events - 1)

        event_cat = event_categories[event_id]
        prefs     = user_preferences[user_id]

        # Pesos más discriminativos por rango de preferencia
        if prefs and event_cat == prefs[0]:
            # Categoría favorita: muchos saves e interested
            weights = [0.20, 0.40, 0.30, 0.10]
        elif event_cat in prefs[1:]:
            # Categoría secundaria: mix equilibrado
            weights = [0.40, 0.25, 0.25, 0.10]
        else:
            # No preferida: mayormente views, pocos saves
            weights = [0.60, 0.08, 0.07, 0.25]

        itype = random.choices(interaction_types, weights=weights)[0]

        # Score de engagement más rico que binario simple
        is_preferred_view = (itype == "view" and event_cat in prefs)
        engagement_score  = (
            2 if itype == "save" else
            2 if itype == "interested" else
            1 if is_preferred_view else
            0
        )
        label = 1 if engagement_score >= 1 else 0

        records.append({
            "user_id":          user_id,
            "event_id":         event_id,
            "event_category":   event_cat,
            "interaction_type": itype,
            "engagement_score": engagement_score,
            "label":            label,
        })

    return pd.DataFrame(records)


# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    output_dir = Path("ml/training/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("📊  Generando dataset de eventos v5...")
    events_df = generate_events_dataset(600)
    events_df.to_csv(output_dir / "events_synthetic.csv", index=False)

    dist = events_df["quality_ml"].describe()
    high = (events_df["quality_ml"] >= 0.5).sum()
    low  = (events_df["quality_ml"] <  0.5).sum()

    print(f"   ✅  {len(events_df)} eventos generados")
    print(f"   Distribución categorías:\n{events_df['category'].value_counts()}")
    print(f"   Calidad — alta (>=0.5): {high} | baja (<0.5): {low}")
    print(f"   quality_ml stats:\n{dist}")

    # Verificar overlap léxico — el accuracy del clasificador en hold-out
    # debería estar entre 0.75 y 0.92 con estos datos (no 1.0)
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score

    X_train, X_test, y_train, y_test = train_test_split(
        events_df["text"], events_df["category"], test_size=0.2, random_state=42
    )
    vec = TfidfVectorizer(ngram_range=(1, 2), max_features=10000)
    clf = LogisticRegression(max_iter=1000)
    clf.fit(vec.fit_transform(X_train), y_train)
    acc = accuracy_score(y_test, clf.predict(vec.transform(X_test)))
    print(f"\n   🔍  Sanity check clasificador (LR hold-out): accuracy = {acc:.4f}")
    if acc < 0.98:
        print("   ✅  Separación no trivial — dataset listo para entrenar sin sobreajuste.")
    else:
        print("   ⚠️  Accuracy aún > 0.98 — considera aumentar n_ambiguous o el overlap.")

    print("\n📊  Generando dataset de interacciones v5...")
    interactions_df = generate_interactions_dataset()
    interactions_df.to_csv(output_dir / "interactions_synthetic.csv", index=False)
    pos = interactions_df["label"].sum()
    neg = len(interactions_df) - pos
    print(f"   ✅  {len(interactions_df)} interacciones")
    print(f"   Labels — positivos: {pos} ({pos/len(interactions_df)*100:.1f}%) | negativos: {neg} ({neg/len(interactions_df)*100:.1f}%)")
    print("\n✅  Datasets v5 listos.")