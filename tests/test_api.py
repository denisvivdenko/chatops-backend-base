import json
import time
import pytest

from chatops.workers.worker import TEST_RESPONSE


# --- Chats ---

def test_create_chat(authed_client):
    response = authed_client.post("/api/chats", json={"message": "Hello"})

    assert response.status_code == 201
    body = response.json()
    assert body["title"] == "Hello"
    assert "id" in body
    assert "created_at" in body
    assert "last_activity_at" in body


def test_list_chats_sorted_most_recent_first(authed_client_with_worker):
    chat1_id = authed_client_with_worker.post("/api/chats", json={"message": "First"}).json()["id"]
    chat2_id = authed_client_with_worker.post("/api/chats", json={"message": "Second"}).json()["id"]

    # drain SSE for both so their assistants are complete and we can send follow-ups
    for chat_id in [chat1_id, chat2_id]:
        assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
        with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
            list(resp.iter_lines())

    # chat2 is currently first (created more recently)
    chats = authed_client_with_worker.get("/api/chats?limit=10").json()
    assert chats[0]["id"] == chat2_id

    # sending a message to chat1 (currently last) should bump it to the top
    authed_client_with_worker.post(f"/api/chats/{chat1_id}/messages", json={"content": "Follow up"})

    chats = authed_client_with_worker.get("/api/chats?limit=10").json()
    assert chats[0]["id"] == chat1_id


def test_list_chats_respects_limit(authed_client):
    for i in range(3):
        authed_client.post("/api/chats", json={"message": f"Message {i}"})

    response = authed_client.get("/api/chats?limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_delete_chat(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    response = authed_client.delete(f"/api/chats/{chat_id}")

    assert response.status_code == 204
    chats = authed_client.get("/api/chats?limit=10").json()
    assert all(c["id"] != chat_id for c in chats)


# --- Messages ---

def test_fetch_messages_after_create_chat(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    response = authed_client.get(f"/api/chats/{chat_id}/messages")

    assert response.status_code == 200
    messages = response.json()
    assert len(messages) == 2

    assert messages[0]["role"] == "user"
    assert messages[0]["status"] == "complete"
    assert messages[0]["content"] == "Hello"

    assert messages[1]["role"] == "assistant"
    assert messages[1]["status"] == "pending"
    assert "id" in messages[1]


def test_send_message_lifecycle(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    first_assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    pending_response = authed_client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": "Follow up"})
    assert pending_response.status_code == 409
    assert pending_response.json()["error"] == "last_assistant_message_not_finished"

    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{first_assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == TEST_RESPONSE

    follow_up = authed_client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": "What is the weather?"})
    assert follow_up.status_code == 201
    assert follow_up.json()["role"] == "assistant"
    assert follow_up.json()["status"] == "pending"


@pytest.mark.parametrize(
    "settings",
    [{"message_generation_timeout": 0.05}],
    indirect=True,
)
def test_assistant_message_marked_failed_when_not_picked_up_by_worker(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "pending"

    time.sleep(0.1)

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "failed"


@pytest.mark.parametrize(
    "settings",
    [{"message_generation_timeout": 0.5}],
    indirect=True,
)
def test_worker_discards_stale_job_for_message_already_failed_by_timeout(authed_client, request):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "pending"

    time.sleep(0.6)  # no worker running yet, so this message's job sits stale in the queue
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "failed"

    other_chat_id = authed_client.post("/api/chats", json={"message": "Other"}).json()["id"]
    other_assistant_id = authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["id"]

    request.getfixturevalue("worker")  # starts consuming the queue: stale job first, then the fresh one

    with authed_client.stream("GET", f"/api/chats/{other_chat_id}/messages/{other_assistant_id}/stream") as resp:
        list(resp.iter_lines())
    assert authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["status"] == "complete"

    stale_message = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]
    assert stale_message["status"] == "failed"
    assert stale_message["content"] == ""


@pytest.mark.parametrize(
    "settings",
    [{"message_generation_timeout": 0.05}],
    indirect=True,
)
def test_retry_failed_message_marks_it_pending(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    time.sleep(0.1)
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "failed"

    response = authed_client.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == assistant_id
    assert body["status"] == "pending"


def test_retry_pending_message_is_rejected(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    response = authed_client.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")

    assert response.status_code == 409
    assert response.json()["error"] == "message_not_failed"


def test_retry_complete_message_is_rejected(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
        list(resp.iter_lines())
    assert authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "complete"

    response = authed_client_with_worker.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")

    assert response.status_code == 409
    assert response.json()["error"] == "message_not_failed"


# --- SSE streaming ---


def test_stream_assistant_response(authed_client_with_worker):

    def _collect_events(lines, limit=None):
        events = []
        for line in lines:
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
                if limit and len(events) >= limit:
                    break
        return events

    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    with authed_client_with_worker.stream("GET", url) as first_resp:
        assert first_resp.status_code == 200
        assert first_resp.headers["content-type"] == "text/event-stream; charset=utf-8"
        first_iter = first_resp.iter_lines()

        first_events = _collect_events(first_iter, limit=3)

        with authed_client_with_worker.stream("GET", url) as second_resp:
            second_events = _collect_events(second_resp.iter_lines())

        first_events += _collect_events(first_iter)

    assert [e["seq_id"] for e in first_events] == list(range(len(first_events)))
    assert "".join(e["token"] for e in first_events) == TEST_RESPONSE
    assert "".join(e["token"] for e in second_events) == TEST_RESPONSE


@pytest.mark.parametrize(
    "settings",
    [{"event_stream_timeout": 0.05, "message_generation_timeout": 0.2}],
    indirect=True,
)
def test_stream_emits_error_event_when_generation_times_out(authed_client):
    url = "/api/chats/nonexistent-chat/messages/nonexistent-message/stream"

    with authed_client.stream("GET", url) as response:
        assert response.status_code == 200
        lines = list(response.iter_lines())

    assert "event: error" in lines
    data_line = lines[lines.index("event: error") + 1]
    assert json.loads(data_line.removeprefix("data: ")) == {"error": "message_generation_timeout"}


@pytest.mark.parametrize(
    "settings",
    [{"event_stream_timeout": 0.05, "message_generation_timeout": 0.3}],
    indirect=True,
)
def test_reopening_stream_after_generation_timeout_receives_error(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    with authed_client_with_worker.stream("GET", url) as first_resp:
        first_line = next(line for line in first_resp.iter_lines() if line.startswith("data: "))
    assert json.loads(first_line.removeprefix("data: "))["token"]

    time.sleep(0.4)

    with authed_client_with_worker.stream("GET", url) as second_resp:
        lines = list(second_resp.iter_lines())

    assert "event: error" in lines
    lines_before_error = lines[:lines.index("event: error")]
    assert not any(line.startswith("data: ") for line in lines_before_error)
    data_line = lines[lines.index("event: error") + 1]
    assert json.loads(data_line.removeprefix("data: ")) == {"error": "message_generation_timeout"}
