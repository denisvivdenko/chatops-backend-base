import pytest

from chatops.workers.response_generator import TEST_RESPONSE
from conftest import sleep_until_message_timed_out


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
    [{"message_timeout": {"message_generation_timeout": 0.05}}],
    indirect=True,
)
def test_assistant_message_marked_failed_when_not_picked_up_by_worker(authed_client, settings):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "pending"

    sleep_until_message_timed_out(messages[1], settings.message_timeout)

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "failed"


@pytest.mark.parametrize(
    "settings",
    [{"message_timeout": {"message_generation_timeout": 0.5}}],
    indirect=True,
)
def test_worker_discards_stale_job_for_message_already_failed_by_timeout(authed_client, settings, request):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_message = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]
    assert assistant_message["status"] == "pending"

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)  # no worker running yet, so this message's job sits stale in the queue
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
