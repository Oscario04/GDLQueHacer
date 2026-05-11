"""
ml/training/generate_dataset.py
Genera un dataset sintético de eventos en español para entrenar los modelos.
Basado en el dominio de GDL Qué Hacer (Guadalajara, México).

Complementa el dataset real de Kaggle (Event Recommendation Engine Challenge):
  https://www.kaggle.com/c/event-recommendation-engine-challenge

Uso:
    python -m ml.training.generate_dataset
"""
import pandas as pd
import numpy as np
import random
import json
from pathlib import Path
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# ── Plantillas de eventos por categoría ───────────────────────────────

EVENTS_DATA = {
    "cultural": {
        "titles": [
            "Exposición de Arte Contemporáneo en el Museo de las Artes",
            "Concierto de la Orquesta Filarmónica de Jalisco",
            "Festival Internacional de Cine de Guadalajara",
            "Noche de Museos en el Centro Histórico",
            "Obra de Teatro: La Casa de Bernarda Alba",
            "Feria Internacional del Libro de Guadalajara (FIL)",
            "Recital de Poesía en la Biblioteca Pública",
            "Exposición fotográfica: Rostros de la ZMG",
            "Taller de pintura acuarela para adultos",
            "Presentación de libro: Jalisco en el siglo XXI",
            "Festival de Danza Folclórica Tapatía",
            "Conferencia sobre arquitectura colonial mexicana",
            "Concierto de jazz en el Teatro Degollado",
            "Exposición de escultura urbana en Tlaquepaque",
            "Muestra de cine mexicano independiente",
            "Taller de cerámica talavera en Tonalá",
            "Ciclo de cine documental latinoamericano",
            "Presentación de ballet clásico: El Lago de los Cisnes",
            "Curso de historia del arte prehispánico",
            "Evento literario: Escritores de Jalisco",
        ],
        "descriptions": [
            "Una muestra extraordinaria que reúne obras de artistas locales y nacionales en los mejores espacios del centro de Guadalajara.",
            "Disfruta de una velada musical única con los mejores intérpretes de la región en un ambiente incomparable.",
            "El evento cultural más importante de Jalisco presenta una selección cuidadosamente curada para todos los públicos.",
            "Una experiencia inmersiva que combina historia, arte y cultura en el corazón de la ciudad.",
            "No te pierdas esta extraordinaria producción que ha recorrido los mejores escenarios de México y el mundo.",
        ],
    },
    "deportivo": {
        "titles": [
            "Partido de Chivas vs América en el Estadio Akron",
            "Maratón Guadalajara 2025 – Inscripciones abiertas",
            "Torneo de Tenis ATP en el Parque Metropolitano",
            "Carrera 5K por el Bosque de la Primavera",
            "CrossFit Games Regional Guadalajara",
            "Torneo de Fútbol Infantil Copa Jalisco",
            "Campeonato Estatal de Natación Jalisco",
            "Ciclotón Dominical por el Periférico",
            "Clase de yoga al aire libre en el Parque Agua Azul",
            "Torneo de Padel – Club Deportivo Guadalajara",
            "Liga de Voleibol de Playa Tapatío",
            "Entrenamientos de triatlón en el Lago de Chapala",
            "Copa de Boxeo Amateur Jalisco 2025",
            "Torneo de Basquetbol 3x3 en la Plaza de Armas",
            "Caminata de senderismo en la Barranca de Huentitán",
            "Campeonato de Atletismo Universitario CUCEI",
            "Partido de Rugby – Leones Negros vs Borregos",
            "Festival de Artes Marciales en la Expo Guadalajara",
            "Clase masiva de Zumba en el Parque Morelos",
            "Trail Running – Sierra de Quila 2025",
        ],
        "descriptions": [
            "Únete a cientos de deportistas en este evento que combina competencia, diversión y espíritu tapatío.",
            "Un evento deportivo de primera clase que convoca a los mejores atletas de la región y el país.",
            "Ven a vivir la emoción del deporte en vivo en uno de los mejores recintos deportivos de Jalisco.",
            "Evento abierto para todas las edades y niveles. ¡La actividad física es para todos!",
            "Competencia oficial avalada por la Federación con premiación para los tres primeros lugares.",
        ],
    },
    "gastronomico": {
        "titles": [
            "Festival Gastronómico de Tortas Ahogadas en Tlaquepaque",
            "Feria del Tequila y la Gastronomía Jalisciense",
            "Noche de Tapas Mexicanas en el Mercado San Juan de Dios",
            "Curso de Cocina Mexicana: Chiles en Nogada",
            "Beer Fest Guadalajara – Cervezas Artesanales",
            "Mercado Orgánico de Productores Locales",
            "Ruta Gastronómica por el Centro de Guadalajara",
            "Cata de Vinos de la Región Tequilera",
            "Taller de Repostería Tradicional Jalisciense",
            "Festival del Birriegal – Tradición Tapatiá",
            "Cena Maridaje en el Restaurante Alcalde",
            "Feria de Café de Especialidad Jalisco",
            "Cook-off de Pozole – Concurso Regional",
            "Mercado de Productores Agroecológicos Zapopan",
            "Noche de Mezcales Artesanales en Tonalá",
            "Clase magistral con el Chef Paco Ruano",
            "Festival del Aguacate de Jalisco",
            "Brunch de Domingo en el Jardín Botánico",
            "Cena Fusión: México-Japón en el Hotel Hilton",
            "Tour de Street Food por el Mercado Corona",
        ],
        "descriptions": [
            "Disfruta de los sabores más auténticos de Jalisco en un ambiente festivo y familiar.",
            "Una experiencia gastronómica única que celebra la riqueza culinaria de nuestra región.",
            "Los mejores chefs y productores locales se reúnen para deleitar tu paladar.",
            "Aprende técnicas tradicionales de la cocina mexicana de la mano de expertos.",
            "Más de 50 expositores de productos locales y regionales en un ambiente incomparable.",
        ],
    },
    "entretenimiento": {
        "titles": [
            "Concierto de Peso Pluma en el Estadio Jalisco",
            "Obra de Stand-Up Comedy: Los Comediantes de GDL",
            "Festival de Música Electrónica – WDM Guadalajara",
            "Escape Room: El Misterio del Hospicio Cabañas",
            "Noche de Karaoke en la Zona Rosa",
            "Cirque du Soleil – Espectáculo ECHO en Guadalajara",
            "Festival de Halloween en la Expo Guadalajara",
            "Gaming Fest Jalisco – Torneos y Exhibiciones",
            "Concierto Tributo a Los Beatles – GDL Arena",
            "Tarde de Magia con el Gran Mago Mexicano",
            "Festival de Fuegos Artificiales en Chapala",
            "Feria de Cosplay y Manga – Anime Fest Guadalajara",
            "Noche de Cine al Aire Libre en Tlaquepaque",
            "Open Mic Night – Bar Catorce",
            "Festival de Música Regional Mexicana",
            "Noche de Comedia en el Teatro Experimental",
            "Escape the Room: Aventura en el Templo Perdido",
            "DJ Night – Gin Gin Rooftop",
            "Festival Folclórico Nacional en el Teatro Degollado",
            "Concierto de Banda Sinaloense en Plaza del Sol",
        ],
        "descriptions": [
            "Una noche de entretenimiento sin igual en uno de los mejores recintos de Guadalajara.",
            "Diversión garantizada para toda la familia en el corazón de la ciudad.",
            "El evento más esperado del mes llega a Guadalajara con todo su espectáculo.",
            "Vive una experiencia única e irrepetible en compañía de amigos y familia.",
            "El mejor entretenimiento de la ZMG te espera. ¡No faltes!",
        ],
    },
}

LOCATIONS = [
    "Teatro Degollado", "Expo Guadalajara", "Parque Metropolitano",
    "Estadio Akron", "Plaza de Armas", "Centro Cultural Cabañas",
    "Mercado San Juan de Dios", "Zona Rosa GDL", "Tlaquepaque Centro",
    "Tonalá Artesanal", "Parque Agua Azul", "Estadio Jalisco",
    "GDL Arena", "Bosque de la Primavera", "Universidad de Guadalajara",
    "Zapopan Centro", "Lago de Chapala", "Jardín Botánico GDL",
]

GDL_COORDINATES = [
    [-103.3496, 20.6597],  # Centro histórico
    [-103.4127, 20.6674],  # Zapopan
    [-103.3006, 20.6414],  # Tlaquepaque
    [-103.2339, 20.6294],  # Tonalá
    [-103.3832, 20.6858],  # Estadio Akron
    [-103.3612, 20.6493],  # Parque Metropolitano
]


def generate_events_dataset(n_samples: int = 500) -> pd.DataFrame:
    """Genera dataset de eventos para entrenamiento del clasificador de categorías."""
    records = []

    for _ in range(n_samples):
        category = random.choice(list(EVENTS_DATA.keys()))
        cat_data = EVENTS_DATA[category]

        title = random.choice(cat_data["titles"])
        description = random.choice(cat_data["descriptions"])

        # Pequeñas variaciones de texto para aumentar diversidad
        if random.random() > 0.7:
            prefixes = ["¡", "Gran ", "Especial: ", "Próximo: ", ""]
            title = random.choice(prefixes) + title

        location = random.choice(LOCATIONS)
        coords = random.choice(GDL_COORDINATES)
        # Añadir pequeño ruido a coordenadas
        coords = [
            coords[0] + np.random.normal(0, 0.02),
            coords[1] + np.random.normal(0, 0.02),
        ]

        date_start = datetime.now() + timedelta(days=random.randint(1, 90))
        price = random.choice([0, 0, 0, 50, 100, 150, 200, 300, 500])
        quality_ml = np.clip(
            np.random.beta(5, 2),  # Distribución sesgada hacia calidad alta
            0.1, 1.0,
        )

        records.append({
            "title": title,
            "description": description,
            "category": category,
            "location": location,
            "latitude": coords[1],
            "longitude": coords[0],
            "date_start": date_start.isoformat(),
            "price": price,
            "quality_ml": round(quality_ml, 4),
            "text": f"{title} {description}",  # Campo para TF-IDF
        })

    return pd.DataFrame(records)


def generate_interactions_dataset(
    n_users: int = 200,
    n_events: int = 500,
    n_interactions: int = 5000,
) -> pd.DataFrame:
    """
    Genera dataset de interacciones usuario-evento para entrenamiento de KNN/SVM.
    Simula comportamientos realistas: usuarios tienen preferencias por categorías.
    """
    # Asignar preferencias por categoría a cada usuario
    categories = list(EVENTS_DATA.keys())
    user_preferences = {}
    for user_id in range(n_users):
        # Cada usuario prefiere 1-2 categorías
        preferred = random.sample(categories, k=random.randint(1, 2))
        user_preferences[user_id] = preferred

    # Generar eventos con sus categorías
    event_categories = [
        random.choice(categories) for _ in range(n_events)
    ]

    records = []
    interaction_types = ["view", "save", "interested", "uninterested"]
    interaction_weights = [0.5, 0.25, 0.2, 0.05]  # Probabilidades

    for _ in range(n_interactions):
        user_id = random.randint(0, n_users - 1)
        event_id = random.randint(0, n_events - 1)
        event_cat = event_categories[event_id]

        # Más probable interacción positiva si es categoría preferida
        if event_cat in user_preferences[user_id]:
            itype = random.choices(
                interaction_types,
                weights=[0.4, 0.3, 0.25, 0.05]
            )[0]
            label = 1  # Positivo
        else:
            itype = random.choices(
                interaction_types,
                weights=[0.6, 0.15, 0.1, 0.15]
            )[0]
            label = 1 if itype in ["save", "interested"] else 0

        records.append({
            "user_id": user_id,
            "event_id": event_id,
            "event_category": event_cat,
            "interaction_type": itype,
            "label": label,  # 1 = positivo, 0 = negativo/neutro
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    output_dir = Path("ml/training/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("📊  Generando dataset de eventos...")
    events_df = generate_events_dataset(600)
    events_df.to_csv(output_dir / "events_synthetic.csv", index=False)
    print(f"   ✅  {len(events_df)} eventos generados → {output_dir}/events_synthetic.csv")
    print(f"   Distribución:\n{events_df['category'].value_counts()}")

    print("\n📊  Generando dataset de interacciones...")
    interactions_df = generate_interactions_dataset()
    interactions_df.to_csv(output_dir / "interactions_synthetic.csv", index=False)
    print(f"   ✅  {len(interactions_df)} interacciones → {output_dir}/interactions_synthetic.csv")

    print("\n✅  Datasets listos para entrenamiento.")