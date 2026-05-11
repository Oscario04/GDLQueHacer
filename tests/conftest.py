"""
tests/conftest.py
Configuración global de pytest.
Mockea MongoDB para que los tests no dependan de una conexión real.
"""
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport


def _make_mock_db():
    mock_db = MagicMock()

    for col_name in ("events", "users", "user_interactions", "user_preferences", "reviews_manual"):
        col = MagicMock()
        col.find_one = AsyncMock(return_value=None)
        col.count_documents = AsyncMock(return_value=0)
        col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="fake_id"))
        col.update_one = AsyncMock(return_value=None)
        col.find = MagicMock(return_value=MagicMock(
            sort=MagicMock(return_value=MagicMock(
                skip=MagicMock(return_value=MagicMock(
                    limit=MagicMock(return_value=MagicMock(
                        to_list=AsyncMock(return_value=[])
                    ))
                ))
            ))
        ))
        setattr(mock_db, col_name, col)

    return mock_db


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def client():
    from api.main import app
    from api.config.database import get_db

    mock_db = _make_mock_db()

    # Así es como FastAPI intercepta dependencias correctamente
    app.dependency_overrides[get_db] = lambda: mock_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def mock_connect_db():
    with patch("api.config.database.connect_db", new_callable=AsyncMock), \
         patch("api.config.database.disconnect_db", new_callable=AsyncMock):
        yield


@pytest.fixture(autouse=True)
def mock_ml_models():
    with patch("api.services.ml_service.load_ml_models", return_value=None):
        yield