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
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

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
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    response = authed_client.post(
        f"/api/chats/{chat_id}/messages/nonexistent-message/modify", json={"content": "Hello"}
    )

    assert response.status_code == 404
    assert response.json()["error"] == "message_not_found"


def test_stream_for_nonexistent_chat_returns_404(authed_client):
    response = authed_client.get("/api/chats/nonexistent-chat/messages/nonexistent-message/stream")

    assert response.status_code == 404
    assert response.json()["error"] == "chat_not_found"


def test_stream_for_nonexistent_message_returns_404(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    response = authed_client.get(f"/api/chats/{chat_id}/messages/nonexistent-message/stream")

    assert response.status_code == 404
    assert response.json()["error"] == "message_not_found"
