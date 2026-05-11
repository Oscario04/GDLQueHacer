"""
tests/test_api.py
Tests formales con pytest + httpx para GDL Qué Hacer API.
"""

import pytest
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException

MOCK_EVENT = {
    "_id": "664f1a2b3c4d5e6f7a8b9c0d",
    "title": "Festival de Jazz GDL",
    "description": "El mejor festival de jazz de Guadalajara.",
    "category": "música",
    "tags": ["jazz"],
    "image_url": "https://example.com/jazz.jpg",
    "start_date": "2025-08-15T20:00:00+00:00",
    "price": 150.0,
    "currency": "MXN",
    "url": "https://eventbrite.com/e/festival-jazz-gdl",
    "location": {"address": "Teatro Degollado", "lat": 20.67, "lon": -103.34},
    "status": "publicado",
    "quality_ml": 0.87,
}

class TestHealth:
    @pytest.mark.anyio
    async def test_health_check(self, client):
        response = await client.get("/api/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    @pytest.mark.anyio
    async def test_docs_available(self, client):
        response = await client.get("/api/docs")
        assert response.status_code == 200

class TestAuth:
    @pytest.mark.anyio
    async def test_register_missing_fields(self, client):
        response = await client.post("/api/auth/register", json={"email": "oscar@test.com"})
        assert response.status_code == 422

    @pytest.mark.anyio
    async def test_register_invalid_email(self, client):
        response = await client.post("/api/auth/register", json={
            "email": "no-es-email", "username": "oscar", "password": "Pass123!"
        })
        assert response.status_code == 422

    @pytest.mark.anyio
    async def test_get_me_without_token(self, client):
        response = await client.get("/api/auth/me")
        assert response.status_code in (401, 403)

    @pytest.mark.anyio
    async def test_login_missing_fields(self, client):
        response = await client.post("/api/auth/login", json={})
        assert response.status_code == 422

class TestEvents:
    @pytest.mark.anyio
    async def test_list_events(self, client):
        with patch("api.routes.events.event_service.list_events", new_callable=AsyncMock) as m:
            m.return_value = {"events": [MOCK_EVENT], "total": 1, "page": 1, "pages": 1}
            response = await client.get("/api/events")
            assert response.status_code == 200

    @pytest.mark.anyio
    async def test_list_events_empty(self, client):
        with patch("api.routes.events.event_service.list_events", new_callable=AsyncMock) as m:
            m.return_value = {"events": [], "total": 0, "page": 1, "pages": 0}
            response = await client.get("/api/events")
            assert response.status_code == 200

    @pytest.mark.anyio
    async def test_get_event_not_found(self, client):
        with patch("api.routes.events.event_service.get_event_by_id", new_callable=AsyncMock) as m:
            m.side_effect = HTTPException(status_code=404, detail="Evento no encontrado")
            response = await client.get("/api/events/000000000000000000000000")
            assert response.status_code == 404

class TestInteractions:
    @pytest.mark.anyio
    async def test_create_interaction_without_token(self, client):
        response = await client.post("/api/interactions", json={"event_id": "abc", "type": "like"})
        assert response.status_code in (401, 403)

    @pytest.mark.anyio
    async def test_my_interactions_without_token(self, client):
        response = await client.get("/api/interactions/my")
        assert response.status_code in (401, 403)

class TestAdmin:
    @pytest.mark.anyio
    async def test_admin_reviews_without_token(self, client):
        response = await client.get("/api/admin/reviews")
        assert response.status_code in (401, 403)

    @pytest.mark.anyio
    async def test_admin_stats_without_token(self, client):
        response = await client.get("/api/admin/stats")
        assert response.status_code in (401, 403)

    @pytest.mark.anyio
    async def test_trigger_scraper_without_token(self, client):
        response = await client.post("/api/admin/trigger-scraper")
        assert response.status_code in (401, 403)