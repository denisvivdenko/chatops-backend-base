import time
import pytest

from .helpers import auth_headers


def test_refresh_returns_new_access_token(client):
    client.post("/api/auth/anonymous-session")

    response = client.post("/api/auth/refresh")

    assert response.status_code == 200
    assert "access_token" in response.json()


def test_refreshed_access_token_grants_access_to_own_chats(client):
    old_token = client.post("/api/auth/anonymous-session").json()["access_token"]
    chat_id = client.post("/api/chats", json={"message": "Hello"}, headers=auth_headers(old_token)).json()["id"]

    new_token = client.post("/api/auth/refresh").json()["access_token"]

    chats = client.get("/api/chats", headers=auth_headers(new_token)).json()
    assert any(c["id"] == chat_id for c in chats)


def test_refresh_without_prior_session_is_rejected(client):
    response = client.post("/api/auth/refresh")

    assert response.status_code == 401


def test_reusing_rotated_refresh_token_is_rejected(client):
    client.post("/api/auth/anonymous-session")
    old_refresh_cookie = client.cookies.get("refresh_token")

    client.post("/api/auth/refresh")  # rotates: jar now holds a new refresh_token

    client.cookies.set("refresh_token", old_refresh_cookie)
    reuse_response = client.post("/api/auth/refresh")

    assert reuse_response.status_code == 401


@pytest.mark.parametrize(
    "settings",
    [{"refresh_token_ttl": 0.05}],
    indirect=True,
)
def test_expired_refresh_token_is_rejected(client):
    client.post("/api/auth/anonymous-session")

    time.sleep(0.1)

    response = client.post("/api/auth/refresh")

    assert response.status_code == 401
