import json
import pytest

from chatops.workers.worker import HARDCODED_RESPONSE


# --- Chats ---

def test_create_chat(client):
    response = client.post("/api/chats", json={"message": "Hello"})

    assert response.status_code == 201
    body = response.json()
    assert body["title"] == "Hello"
    assert "id" in body
    assert "created_at" in body
    assert "last_activity_at" in body


def test_list_chats_sorted_most_recent_first(client_with_worker):
    chat1_id = client_with_worker.post("/api/chats", json={"message": "First"}).json()["id"]
    chat2_id = client_with_worker.post("/api/chats", json={"message": "Second"}).json()["id"]

    # drain SSE for both so their assistants are complete and we can send follow-ups
    for chat_id in [chat1_id, chat2_id]:
        assistant_id = client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
        with client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
            list(resp.iter_lines())

    # chat2 is currently first (created more recently)
    chats = client_with_worker.get("/api/chats?limit=10").json()
    assert chats[0]["id"] == chat2_id

    # sending a message to chat1 (currently last) should bump it to the top
    client_with_worker.post(f"/api/chats/{chat1_id}/messages", json={"content": "Follow up"})

    chats = client_with_worker.get("/api/chats?limit=10").json()
    assert chats[0]["id"] == chat1_id


def test_list_chats_respects_limit(client):
    for i in range(3):
        client.post("/api/chats", json={"message": f"Message {i}"})

    response = client.get("/api/chats?limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_delete_chat(client):
    chat_id = client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    response = client.delete(f"/api/chats/{chat_id}")

    assert response.status_code == 204
    chats = client.get("/api/chats?limit=10").json()
    assert all(c["id"] != chat_id for c in chats)


# --- Messages ---

def test_fetch_messages_after_create_chat(client):
    chat_id = client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    response = client.get(f"/api/chats/{chat_id}/messages")

    assert response.status_code == 200
    messages = response.json()
    assert len(messages) == 2

    assert messages[0]["role"] == "user"
    assert messages[0]["status"] == "complete"
    assert messages[0]["content"] == "Hello"

    assert messages[1]["role"] == "assistant"
    assert messages[1]["status"] == "pending"
    assert "id" in messages[1]


def test_send_message_lifecycle(client_with_worker):
    chat_id = client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    first_assistant_id = client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    pending_response = client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": "Follow up"})
    assert pending_response.status_code == 409
    assert pending_response.json()["error"] == "last_assistant_message_not_finished"

    with client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{first_assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == HARDCODED_RESPONSE

    follow_up = client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": "What is the weather?"})
    assert follow_up.status_code == 201
    assert follow_up.json()["role"] == "assistant"
    assert follow_up.json()["status"] == "pending"


# --- SSE streaming ---


def test_stream_assistant_response(client_with_worker):

    def _collect_events(lines, limit=None):
        events = []
        for line in lines:
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
                if limit and len(events) >= limit:
                    break
        return events

    chat_id = client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    with client_with_worker.stream("GET", url) as first_resp:
        assert first_resp.status_code == 200
        assert first_resp.headers["content-type"] == "text/event-stream; charset=utf-8"
        first_iter = first_resp.iter_lines()

        first_events = _collect_events(first_iter, limit=3)

        with client_with_worker.stream("GET", url) as second_resp:
            second_events = _collect_events(second_resp.iter_lines())

        first_events += _collect_events(first_iter)

    assert [e["seq_id"] for e in first_events] == list(range(len(first_events)))
    assert "".join(e["token"] for e in first_events) == HARDCODED_RESPONSE
    assert "".join(e["token"] for e in second_events) == HARDCODED_RESPONSE
