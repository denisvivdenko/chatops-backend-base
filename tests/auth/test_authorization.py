from .helpers import auth_headers


def test_each_user_sees_only_their_own_chats(client):
    token_a = client.post("/api/auth/anonymous-session").json()["access_token"]
    token_b = client.post("/api/auth/anonymous-session").json()["access_token"]

    chat_a = client.post("/api/chats", json={"message": "From A"}, headers=auth_headers(token_a)).json()["id"]
    chat_b = client.post("/api/chats", json={"message": "From B"}, headers=auth_headers(token_b)).json()["id"]

    chats_seen_by_a = {c["id"] for c in client.get("/api/chats", headers=auth_headers(token_a)).json()}
    chats_seen_by_b = {c["id"] for c in client.get("/api/chats", headers=auth_headers(token_b)).json()}

    assert chats_seen_by_a == {chat_a}
    assert chats_seen_by_b == {chat_b}


def test_user_cannot_read_messages_of_another_users_chat(client):
    token_a = client.post("/api/auth/anonymous-session").json()["access_token"]
    token_b = client.post("/api/auth/anonymous-session").json()["access_token"]
    chat_id = client.post("/api/chats", json={"message": "Hello"}, headers=auth_headers(token_a)).json()["id"]

    response = client.get(f"/api/chats/{chat_id}/messages", headers=auth_headers(token_b))

    assert response.status_code == 403


def test_user_cannot_send_message_to_another_users_chat(client):
    token_a = client.post("/api/auth/anonymous-session").json()["access_token"]
    token_b = client.post("/api/auth/anonymous-session").json()["access_token"]
    chat_id = client.post("/api/chats", json={"message": "Hello"}, headers=auth_headers(token_a)).json()["id"]

    # chat_a's assistant reply is still pending; ownership must still be checked first (403, not 409)
    response = client.post(
        f"/api/chats/{chat_id}/messages", json={"content": "Intruding"}, headers=auth_headers(token_b)
    )

    assert response.status_code == 403


def test_user_cannot_delete_another_users_chat(client):
    token_a = client.post("/api/auth/anonymous-session").json()["access_token"]
    token_b = client.post("/api/auth/anonymous-session").json()["access_token"]
    chat_id = client.post("/api/chats", json={"message": "Hello"}, headers=auth_headers(token_a)).json()["id"]

    response = client.delete(f"/api/chats/{chat_id}", headers=auth_headers(token_b))

    assert response.status_code == 403
    remaining = client.get("/api/chats", headers=auth_headers(token_a)).json()
    assert any(c["id"] == chat_id for c in remaining)


def test_user_cannot_stream_another_users_message(client):
    token_a = client.post("/api/auth/anonymous-session").json()["access_token"]
    token_b = client.post("/api/auth/anonymous-session").json()["access_token"]
    chat_id = client.post("/api/chats", json={"message": "Hello"}, headers=auth_headers(token_a)).json()["id"]
    assistant_id = client.get(f"/api/chats/{chat_id}/messages", headers=auth_headers(token_a)).json()[1]["id"]

    response = client.get(
        f"/api/chats/{chat_id}/messages/{assistant_id}/stream", headers=auth_headers(token_b)
    )

    assert response.status_code == 403
