"""
config/settings.py
Configuración centralizada de la aplicación usando pydantic-settings.
Las variables se leen desde .env o variables de entorno de Vercel.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    # ── Base de datos ────────────────────────────────────────────────
    MONGODB_URI: str = "mongodb://localhost:27017"
    DB_NAME: str = "gdl_que_hacer"

    # ── Seguridad ────────────────────────────────────────────────────
    JWT_SECRET: str = "dev_secret_change_in_production_min_32_chars"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_HOURS: int = 24

    # ── APIs externas ────────────────────────────────────────────────
    EVENTBRITE_API_KEY: str = ""
    SCRAPER_CRON_SECRET: str = "dev_cron_secret"

    # ── Entorno ──────────────────────────────────────────────────────
    ENVIRONMENT: str = "development"
    ALLOWED_ORIGINS: str = "http://localhost:3000"

    # ── ML ───────────────────────────────────────────────────────────
    ML_QUALITY_THRESHOLD: float = 0.5
    ML_MODELS_PATH: str = "ml/saved_models"
    KNN_N_NEIGHBORS: int = 10
    RECOMMENDATIONS_LIMIT: int = 20

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",")]


@lru_cache()
def get_settings() -> Settings:
    """Instancia singleton de Settings (cacheada para evitar re-lecturas)."""
    return Settings()