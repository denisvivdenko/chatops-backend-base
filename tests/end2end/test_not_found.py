from .helpers import create_chat, get_messages, new_user_token


def test_fetch_messages_for_nonexistent_chat_returns_404(authed_client):
    response = authed_client.get("/api/chats/nonexistent-chat/messages")

    assert response.status_code == 404
    assert response.json()["error"] == "chat_not_found"


def test_send_message_to_nonexistent_chat_returns_404(authed_client):
    response = authed_client.post("/api/chats/nonexistent-chat/messages", json={"content": "Hello"})

    assert response.status_code == 404
    assert response.json()["error"] == "chat_not_found"


def test_delete_nonexistent_chat_returns_404(authed_client):
    response = authed_client.delete("/api/chats/nonexistent-chat")

    assert response.status_code == 404
    assert response.json()["error"] == "chat_not_found"


def test_retry_message_in_nonexistent_chat_returns_404(authed_client):
    response = authed_client.post("/api/chats/nonexistent-chat/messages/nonexistent-message/retry")

    assert response.status_code == 404
    assert response.json()["error"] == "chat_not_found"


def test_retry_nonexistent_message_returns_404(authed_client):
    chat_id = create_chat(authed_client, "Hello")

    response = authed_client.post(f"/api/chats/{chat_id}/messages/nonexistent-message/retry")

    assert response.status_code == 404
    assert response.json()["error"] == "message_not_found"


def test_modify_message_in_nonexistent_chat_returns_404(authed_client):
    response = authed_client.post(
        "/api/chats/nonexistent-chat/messages/nonexistent-message/modify", json={"content": "Hello"}
    )

    assert response.status_code == 404
    assert response.json()["error"] == "chat_not_found"


def test_modify_nonexistent_message_returns_404(authed_client):
    chat_id = create_chat(authed_client, "Hello")

    response = authed_client.post(
        f"/api/chats/{chat_id}/messages/nonexistent-message/modify", json={"content": "Hello"}
    )

    assert response.status_code == 404
    assert response.json()["error"] == "message_not_found"


def test_modify_message_with_nonexistent_resource_ref_returns_404(authed_client):
    chat_id = create_chat(authed_client, "Hello")
    user_message_id = get_messages(authed_client, chat_id)[0]["id"]

    response = authed_client.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify",
        json={"content": "[doc.pdf](resource://missing)"},
    )

    assert response.status_code == 404
    assert response.json()["error"] == "resource_not_found"


def test_modify_message_with_another_users_resource_ref_returns_403(client):
    token_a = new_user_token(client)
    token_b = new_user_token(client)

    resource_id = client.post(
        "/api/upload-resource",
        files={"file": ("report.pdf", b"%PDF-1.4\n%mock pdf content", "application/pdf")},
        headers={"Authorization": f"Bearer {token_b}"},
    ).json()["id"]

    client.headers["Authorization"] = f"Bearer {token_a}"
    chat_id = create_chat(client, "Hello")
    user_message_id = get_messages(client, chat_id)[0]["id"]

    response = client.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify",
        json={"content": f"[report.pdf](resource://{resource_id})"},
    )

    assert response.status_code == 403
    assert response.json()["error"] == "forbidden"


def test_stream_for_nonexistent_chat_returns_404(authed_client):
    response = authed_client.get("/api/chats/nonexistent-chat/messages/nonexistent-message/stream")

    assert response.status_code == 404
    assert response.json()["error"] == "chat_not_found"


def test_stream_for_nonexistent_message_returns_404(authed_client):
    chat_id = create_chat(authed_client, "Hello")

    response = authed_client.get(f"/api/chats/{chat_id}/messages/nonexistent-message/stream")

    assert response.status_code == 404
    assert response.json()["error"] == "message_not_found"
