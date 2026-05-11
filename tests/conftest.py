"""
tests/conftest.py
Configuración global de pytest.
Mockea MongoDB para que los tests no dependan de una conexión real.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport


# ── Mock de la base de datos ──────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_database():
    """
    Mockea get_db para que todos los tests no necesiten MongoDB.
    autouse=True significa que se aplica automáticamente a todos los tests.
    """
    mock_db = MagicMock()

    # Mockea las colecciones más usadas
    mock_db.events = MagicMock()
    mock_db.users = MagicMock()
    mock_db.user_interactions = MagicMock()
    mock_db.user_preferences = MagicMock()
    mock_db.reviews_manual = MagicMock()

    with patch("api.config.database.get_db", return_value=mock_db), \
         patch("api.config.database._db", mock_db):
        yield mock_db


@pytest.fixture(autouse=True)
def mock_connect_db():
    """Mockea connect_db para que no intente conectarse a Atlas."""
    with patch("api.config.database.connect_db", new_callable=AsyncMock), \
         patch("api.config.database.disconnect_db", new_callable=AsyncMock):
        yield


@pytest.fixture(autouse=True)
def mock_ml_models():
    """Mockea la carga de modelos ML."""
    with patch("api.services.ml_service.load_ml_models", return_value=None):
        yield


# ── Cliente HTTP de prueba ────────────────────────────────────────────

@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    from api.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac