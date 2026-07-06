import time
import pytest

from .helpers import auth_headers


def test_list_chats_without_token_is_rejected(client):
    response = client.get("/api/chats")

    assert response.status_code == 401


def test_create_chat_without_token_is_rejected(client):
    response = client.post("/api/chats", json={"message": "Hello"})

    assert response.status_code == 401


def test_request_with_garbage_token_is_rejected(client):
    response = client.get("/api/chats", headers=auth_headers("not-a-real-token"))

    assert response.status_code == 401


def test_request_with_tampered_token_is_rejected(client):
    token = client.post("/api/auth/anonymous-session").json()["access_token"]
    # flip the second-to-last char, not the last: base64's final char can carry
    # unused padding bits, so tampering only it can decode to the same signature bytes
    tampered = token[:-2] + ("a" if token[-2] != "a" else "b") + token[-1]

    response = client.get("/api/chats", headers=auth_headers(tampered))

    assert response.status_code == 401


@pytest.mark.parametrize(
    "settings",
    [{"access_token_ttl": 0.05}],
    indirect=True,
)
def test_expired_access_token_is_rejected(client):
    token = client.post("/api/auth/anonymous-session").json()["access_token"]

    time.sleep(0.1)

    response = client.get("/api/chats", headers=auth_headers(token))

    assert response.status_code == 401
