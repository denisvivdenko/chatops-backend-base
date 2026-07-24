import pytest

from chatops.workers.response_generator import TEST_RESPONSE
from conftest import sleep_until_message_timed_out

from ..helpers import create_chat, get_messages, stream_to_completion


def test_fetch_messages_after_create_chat(authed_client):
    chat_id = create_chat(authed_client, "Hello")

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
    chat_id = create_chat(authed_client_with_worker, "Hello")
    first_assistant_id = get_messages(authed_client_with_worker, chat_id)[1]["id"]

    pending_response = authed_client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": "Follow up"})
    assert pending_response.status_code == 409
    assert pending_response.json()["error"] == "last_assistant_message_not_finished"

    stream_to_completion(authed_client_with_worker, chat_id, first_assistant_id)

    messages = get_messages(authed_client_with_worker, chat_id)
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
    chat_id = create_chat(authed_client, "Hello")

    messages = get_messages(authed_client, chat_id)
    assert messages[1]["status"] == "pending"

    sleep_until_message_timed_out(messages[1], settings.message_timeout)

    messages = get_messages(authed_client, chat_id)
    assert messages[1]["status"] == "failed"


@pytest.mark.parametrize(
    "settings",
    [{"message_timeout": {"message_generation_timeout": 0.5}}],
    indirect=True,
)
def test_worker_discards_stale_job_for_message_already_failed_by_timeout(authed_client, settings, request):
    chat_id = create_chat(authed_client, "Hello")
    assistant_message = get_messages(authed_client, chat_id)[1]
    assert assistant_message["status"] == "pending"

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)  # no worker running yet, so this message's job sits stale in the queue
    assert get_messages(authed_client, chat_id)[1]["status"] == "failed"

    other_chat_id = create_chat(authed_client, "Other")
    other_assistant_id = get_messages(authed_client, other_chat_id)[1]["id"]

    request.getfixturevalue("worker")  # starts consuming the queue: stale job first, then the fresh one

    stream_to_completion(authed_client, other_chat_id, other_assistant_id)
    assert get_messages(authed_client, other_chat_id)[1]["status"] == "complete"

    stale_message = get_messages(authed_client, chat_id)[1]
    assert stale_message["status"] == "failed"
    assert stale_message["content"] == ""
