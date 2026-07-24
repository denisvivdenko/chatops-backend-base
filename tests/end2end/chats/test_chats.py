from ..helpers import create_chat, get_messages, stream_to_completion


def test_create_chat(authed_client):
    response = authed_client.post("/api/chats", json={"message": "Hello"})

    assert response.status_code == 201
    body = response.json()
    assert body["title"] == "Hello"
    assert "id" in body
    assert "created_at" in body
    assert "last_activity_at" in body


def test_list_chats_sorted_most_recent_first(authed_client_with_worker):
    chat1_id = create_chat(authed_client_with_worker, "First")
    chat2_id = create_chat(authed_client_with_worker, "Second")

    # drain SSE for both so their assistants are complete and we can send follow-ups
    for chat_id in [chat1_id, chat2_id]:
        assistant_id = get_messages(authed_client_with_worker, chat_id)[1]["id"]
        stream_to_completion(authed_client_with_worker, chat_id, assistant_id)

    # chat2 is currently first (created more recently)
    chats = authed_client_with_worker.get("/api/chats?limit=10").json()
    assert chats[0]["id"] == chat2_id

    # sending a message to chat1 (currently last) should bump it to the top
    authed_client_with_worker.post(f"/api/chats/{chat1_id}/messages", json={"content": "Follow up"})

    chats = authed_client_with_worker.get("/api/chats?limit=10").json()
    assert chats[0]["id"] == chat1_id


def test_list_chats_respects_limit(authed_client):
    for i in range(3):
        create_chat(authed_client, f"Message {i}")

    response = authed_client.get("/api/chats?limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_delete_chat(authed_client):
    chat_id = create_chat(authed_client, "Hello")

    response = authed_client.delete(f"/api/chats/{chat_id}")

    assert response.status_code == 204
    chats = authed_client.get("/api/chats?limit=10").json()
    assert all(c["id"] != chat_id for c in chats)
