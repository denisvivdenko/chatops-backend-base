import json
import pytest
from fastapi.testclient import TestClient

from chatops.api.main import create_app
from chatops.repositories.chat_repository import InMemoryChatRepository
from chatops.jobs.job_stream import InMemoryJobStream
from chatops.observers.in_memory_event_stream import InMemoryEventStream
from chatops.workers.worker import Worker, HARDCODED_RESPONSE


@pytest.fixture
def infra():
    return dict(
        chat_repository=InMemoryChatRepository(),
        job_stream=InMemoryJobStream(),
        event_stream=InMemoryEventStream(),
    )


@pytest.fixture
def client(infra):
    return TestClient(create_app(**infra))


@pytest.fixture
def client_with_worker(infra):
    Worker(
        chat_repository=infra["chat_repository"],
        jobs_stream=infra["job_stream"],
        event_stream=infra["event_stream"],
    ).start()
    return TestClient(create_app(**infra))


# --- Chats ---

def test_create_chat(client):
    response = client.post("/chats", json={"message": "Hello"})

    assert response.status_code == 201
    body = response.json()
    assert body["title"] == "Hello"
    assert "id" in body
    assert "created_at" in body
    assert "last_activity_at" in body


def test_list_chats_sorted_most_recent_first(client_with_worker):
    chat1_id = client_with_worker.post("/chats", json={"message": "First"}).json()["id"]
    chat2_id = client_with_worker.post("/chats", json={"message": "Second"}).json()["id"]

    # drain SSE for both so their assistants are complete and we can send follow-ups
    # for chat_id in [chat1_id, chat2_id]:
    #     assistant_id = client_with_worker.get(f"/chats/{chat_id}/messages").json()[1]["id"]
    #     with client_with_worker.stream("GET", f"/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
    #         list(resp.iter_lines())

    # chat2 is currently first (created more recently)
    chats = client_with_worker.get("/chats?limit=10").json()
    assert chats[0]["id"] == chat2_id

    # sending a message to chat1 (currently last) should bump it to the top
    # client_with_worker.post(f"/chats/{chat1_id}/messages", json={"content": "Follow up"})

    # chats = client_with_worker.get("/chats?limit=10").json()
    # assert chats[0]["id"] == chat1_id


def test_list_chats_respects_limit(client):
    for i in range(3):
        client.post("/chats", json={"message": f"Message {i}"})

    response = client.get("/chats?limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


# def test_delete_chat(client):
#     chat_id = client.post("/chats", json={"message": "Hello"}).json()["id"]

#     response = client.delete(f"/chats/{chat_id}")

#     assert response.status_code == 204
#     chats = client.get("/chats?limit=10").json()
#     assert all(c["id"] != chat_id for c in chats)


# # --- Messages ---

# def test_fetch_messages_after_create_chat(client):
#     chat_id = client.post("/chats", json={"message": "Hello"}).json()["id"]

#     response = client.get(f"/chats/{chat_id}/messages")

#     assert response.status_code == 200
#     messages = response.json()
#     assert len(messages) == 2

#     assert messages[0]["role"] == "user"
#     assert messages[0]["status"] == "complete"
#     assert messages[0]["content"] == "Hello"

#     assert messages[1]["role"] == "assistant"
#     assert messages[1]["status"] == "pending"
#     assert "id" in messages[1]


# def test_send_message_returns_409_when_assistant_is_pending(client):
#     chat_id = client.post("/chats", json={"message": "Hello"}).json()["id"]

#     response = client.post(f"/chats/{chat_id}/messages", json={"content": "Follow up"})

#     assert response.status_code == 409
#     assert response.json()["error"] == "last_assistant_message_not_finished"


# def test_send_message_returns_pending_assistant_after_completion(client_with_worker):
#     chat_id = client_with_worker.post("/chats", json={"message": "Hello"}).json()["id"]
#     first_assistant_id = client_with_worker.get(f"/chats/{chat_id}/messages").json()[1]["id"]

#     # consuming the SSE stream blocks until the worker finishes the first response
#     with client_with_worker.stream("GET", f"/chats/{chat_id}/messages/{first_assistant_id}/stream") as resp:
#         list(resp.iter_lines())

#     response = client_with_worker.post(f"/chats/{chat_id}/messages", json={"content": "What is the weather?"})

#     assert response.status_code == 201
#     assistant = response.json()
#     assert assistant["role"] == "assistant"
#     assert assistant["status"] == "pending"


# # --- SSE streaming ---

# def test_stream_assistant_response(client_with_worker):
#     chat_id = client_with_worker.post("/chats", json={"message": "Hello"}).json()["id"]
#     pending_assistant_id = client_with_worker.get(f"/chats/{chat_id}/messages").json()[1]["id"]

#     events = []
#     with client_with_worker.stream("GET", f"/chats/{chat_id}/messages/{pending_assistant_id}/stream") as response:
#         assert response.status_code == 200
#         assert response.headers["content-type"] == "text/event-stream; charset=utf-8"
#         for line in response.iter_lines():
#             if line.startswith("data: "):
#                 events.append(json.loads(line[6:]))

#     assert len(events) > 0
#     assert [e["seq_id"] for e in events] == list(range(len(events)))
#     assert " ".join(e["token"] for e in events) == HARDCODED_RESPONSE
