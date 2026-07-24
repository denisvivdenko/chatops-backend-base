import pytest

from conftest import sleep_until_message_timed_out

from ..helpers import create_chat, get_messages, stream_to_completion


@pytest.mark.parametrize(
    "settings",
    [{"message_timeout": {"message_generation_timeout": 0.05}}],
    indirect=True,
)
def test_retry_failed_message_marks_it_pending(authed_client, settings):
    chat_id = create_chat(authed_client, "Hello")
    assistant_message = get_messages(authed_client, chat_id)[1]
    assistant_id = assistant_message["id"]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)
    assert get_messages(authed_client, chat_id)[1]["status"] == "failed"

    response = authed_client.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == assistant_id
    assert body["status"] == "pending"


def test_retry_pending_message_is_rejected(authed_client):
    chat_id = create_chat(authed_client, "Hello")
    assistant_id = get_messages(authed_client, chat_id)[1]["id"]

    response = authed_client.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")

    assert response.status_code == 409
    assert response.json()["error"] == "message_not_failed"


def test_retry_complete_message_is_rejected(authed_client_with_worker):
    chat_id = create_chat(authed_client_with_worker, "Hello")
    assistant_id = get_messages(authed_client_with_worker, chat_id)[1]["id"]

    stream_to_completion(authed_client_with_worker, chat_id, assistant_id)
    assert get_messages(authed_client_with_worker, chat_id)[1]["status"] == "complete"

    response = authed_client_with_worker.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")

    assert response.status_code == 409
    assert response.json()["error"] == "message_not_failed"
