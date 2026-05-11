# GDLQueHacer
# GDL Qué Hacer 🎉

Plataforma para descubrir eventos y actividades en **Guadalajara, Jalisco, México**.

Recolecta eventos de múltiples fuentes (Eventbrite, etc.), los normaliza, califica su calidad con ML y genera recomendaciones personalizadas para cada usuario.

---

## Arquitectura

```
Usuario Browser
      │
   Vercel CDN
      │
 FastAPI Backend ──► MongoDB Atlas
      ▲
      │
GitHub Actions (Cron)
      │
   Scraper Engine ──► Eventbrite API
      │                    └──► Nominatim (geocodificación)
   ML Pipeline
      │
      └──► MongoDB Atlas
```

### Flujo de un evento

```
Recolectado → Normalizado → (quality_ml >= 0.5) → Publicado
                         → (quality_ml <  0.5) → Pendiente Revisión
                                                       │
                                            Aprobado / Rechazado
```

---

## Stack

| Capa | Tecnología |
|------|-----------|
| Backend API | FastAPI (Python 3.11) |
| Base de datos | MongoDB Atlas |
| Scraping | requests + Eventbrite API |
| Geocodificación | Nominatim (OpenStreetMap) |
| ML | TF-IDF + similitud coseno (sin dependencias pesadas) |
| CI/CD | GitHub Actions |
| Deploy | Vercel Serverless |
| Frontend | React |

---

## Estructura del repositorio

```
GDLQueHacer/
├── .github/
│   └── workflows/
│       └── scraper.yml          # Cron job cada 6h
├── api/
│   ├── main.py                  # FastAPI app
│   ├── models.py                # Esquemas Pydantic
│   ├── routes/
│   │   ├── events.py            # GET /events, GET /events/{id}
│   │   ├── auth.py              # POST /auth/register, POST /auth/login
│   │   └── recommendations.py  # GET /recommendations
│   └── db.py                    # Conexión MongoDB
├── ml/
│   ├── __init__.py
│   ├── classifier.py            # Calcula quality_ml
│   ├── recommender.py           # Recomendaciones por usuario
│   ├── train_classifier.py      # Evalúa el clasificador
│   ├── train_recommender.py     # Evalúa el recomendador
│   └── utils.py
├── scraper/
│   ├── __init__.py
│   ├── base.py                  # Orquestador + normalización + geocoding
│   └── eventbrite.py            # Scraper de Eventbrite
└── README.md
```

---

## Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `MONGODB_URI` | URI de conexión a MongoDB Atlas |
| `EVENTBRITE_TOKEN` | Token de la API de Eventbrite |
| `JWT_SECRET` | Secreto para firmar tokens JWT |

Crea un archivo `.env` en la raíz (nunca lo subas al repo):

```env
MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/gdlquehacer
EVENTBRITE_TOKEN=tu_token_aqui
JWT_SECRET=un_string_muy_largo_y_secreto
```

---

## Instalación local

```bash
# Clonar
git clone https://github.com/Oscario04/GDLQueHacer.git
cd GDLQueHacer

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r api/requirements.txt
pip install -r scraper/requirements.txt

# Variables de entorno
cp .env.example .env
# Edita .env con tus credenciales
```

### Levantar la API

```bash
uvicorn api.main:app --reload
# Documentación interactiva: http://localhost:8000/docs
```

### Correr el scraper manualmente

```bash
python -m scraper.base
```

### Correr el clasificador ML

```bash
python -m ml.classifier
```

### Evaluar modelos

```bash
python -m ml.train_classifier
python -m ml.train_recommender --k 10
```

---

## Endpoints principales

### Eventos

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/events` | Lista eventos publicados (paginado, con filtros) |
| `GET` | `/events/{id}` | Detalle de un evento |
| `GET` | `/events?category=música&date=2025-06-01` | Filtrar por categoría y fecha |

### Autenticación

| Método | Ruta | Descripción |
|--------|------|-------------|
| `POST` | `/auth/register` | Registro de usuario |
| `POST` | `/auth/login` | Login, devuelve JWT |

### Recomendaciones

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/recommendations` | Eventos recomendados para el usuario autenticado |

### Administración

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/admin/events/pending` | Eventos en revisión manual |
| `PATCH` | `/admin/events/{id}/approve` | Aprobar evento |
| `PATCH` | `/admin/events/{id}/reject` | Rechazar evento |

---

## GitHub Actions

El workflow `.github/workflows/scraper.yml` se ejecuta automáticamente cada 6 horas:

1. Corre `scraper.base` → recolecta y normaliza eventos
2. Corre `ml.classifier` → califica y publica/envía a revisión

También se puede lanzar manualmente desde la pestaña **Actions** en GitHub.

Para que funcione, agrega los siguientes **Secrets** en tu repo:
- `MONGODB_URI`
- `EVENTBRITE_TOKEN`

---

## Contribuir

1. Haz fork del repo
2. Crea tu rama: `git checkout -b feature/nueva-fuente`
3. Haz commit: `git commit -m "feat: agrega scraper de Ticketmaster"`
4. Push: `git push origin feature/nueva-fuente`
5. Abre un Pull Request

---

## Licencia

MIT © 2025 GDL Qué Hacer