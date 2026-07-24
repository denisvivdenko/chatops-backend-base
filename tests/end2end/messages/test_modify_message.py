import pytest

from chatops.workers.response_generator import TEST_RESPONSE
from conftest import sleep_until_message_timed_out

from ..helpers import create_chat, get_messages, stream_to_completion


def test_modify_message_lifecycle(authed_client_with_worker):
    chat_id = create_chat(authed_client_with_worker, "Hello")
    user_message_id = get_messages(authed_client_with_worker, chat_id)[0]["id"]
    assistant_message_id = get_messages(authed_client_with_worker, chat_id)[1]["id"]
    stream_to_completion(authed_client_with_worker, chat_id, assistant_message_id)

    rejected = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{assistant_message_id}/modify", json={"content": "Hijacked"}
    )
    assert rejected.status_code == 409
    assert rejected.json()["error"] == "cannot_modify_assistant_message"
    messages = get_messages(authed_client_with_worker, chat_id)
    assert len(messages) == 2
    assert messages[1]["content"] == TEST_RESPONSE

    response = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify", json={"content": "Hello, edited"}
    )

    assert response.status_code == 200
    body = response.json()
    assert body["role"] == "assistant"
    assert body["status"] == "pending"
    assert body["content"] == ""
    assert body["id"] != assistant_message_id

    messages = get_messages(authed_client_with_worker, chat_id)
    assert len(messages) == 2

    assert messages[0]["id"] == user_message_id
    assert messages[0]["role"] == "user"
    assert messages[0]["status"] == "complete"
    assert messages[0]["content"] == "Hello, edited"

    assert messages[1]["id"] == body["id"]
    assert messages[1]["status"] == "pending"

    new_assistant_message_id = body["id"]
    stream_to_completion(authed_client_with_worker, chat_id, new_assistant_message_id)

    messages = get_messages(authed_client_with_worker, chat_id)
    assert messages[1]["id"] == new_assistant_message_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == TEST_RESPONSE


def test_modify_first_message_deletes_all_subsequent_turns_but_modify_second_preserves_earlier_turn(authed_client_with_worker):

    def _build_two_turn_chat(message: str, follow_up: str) -> tuple[str, list[dict]]:
        chat_id = create_chat(authed_client_with_worker, message)
        first_assistant_id = get_messages(authed_client_with_worker, chat_id)[1]["id"]
        stream_to_completion(authed_client_with_worker, chat_id, first_assistant_id)

        authed_client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": follow_up})
        second_assistant_id = get_messages(authed_client_with_worker, chat_id)[3]["id"]
        stream_to_completion(authed_client_with_worker, chat_id, second_assistant_id)

        return chat_id, get_messages(authed_client_with_worker, chat_id)

    # modifying the first message discards every later turn
    chat_id, messages = _build_two_turn_chat("Hello", "Second")
    assert len(messages) == 4
    first_user_id, first_assistant_id, second_assistant_id = messages[0]["id"], messages[1]["id"], messages[3]["id"]

    response = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{first_user_id}/modify", json={"content": "Hello, edited"}
    )
    assert response.status_code == 200

    messages = get_messages(authed_client_with_worker, chat_id)
    assert len(messages) == 2
    assert messages[0]["id"] == first_user_id
    assert messages[0]["content"] == "Hello, edited"
    assert messages[1]["status"] == "pending"
    assert messages[1]["id"] not in (first_assistant_id, second_assistant_id)

    # modifying the second message preserves the earlier turn untouched
    chat_id, messages = _build_two_turn_chat("Hello", "Second")
    first_user, first_assistant, second_user = messages[0], messages[1], messages[2]

    response = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{second_user['id']}/modify", json={"content": "Second, edited"}
    )
    assert response.status_code == 200

    messages = get_messages(authed_client_with_worker, chat_id)
    assert len(messages) == 4
    assert messages[0] == first_user
    assert messages[1] == first_assistant
    assert messages[2]["id"] == second_user["id"]
    assert messages[2]["content"] == "Second, edited"
    assert messages[2]["status"] == "complete"
    assert messages[3]["status"] == "pending"


@pytest.mark.parametrize(
    "settings",
    [{"message_timeout": {"message_generation_timeout": 0.05}}],
    indirect=True,
)
def test_modify_message_allowed_after_assistant_failed(authed_client, settings):
    chat_id = create_chat(authed_client, "Hello")
    messages = get_messages(authed_client, chat_id)
    user_message_id = messages[0]["id"]
    assistant_message = messages[1]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)
    assert get_messages(authed_client, chat_id)[1]["status"] == "failed"

    response = authed_client.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify", json={"content": "Hello, edited"}
    )

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    messages = get_messages(authed_client, chat_id)
    assert len(messages) == 2
    assert messages[0]["content"] == "Hello, edited"
    assert messages[1]["status"] == "pending"


def test_modify_message_while_assistant_pending_is_rejected(authed_client):
    chat_id = create_chat(authed_client, "Hello")
    user_message_id = get_messages(authed_client, chat_id)[0]["id"]

    response = authed_client.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify", json={"content": "Hello, edited"}
    )

    assert response.status_code == 409
    assert response.json()["error"] == "last_assistant_message_not_finished"

    messages = get_messages(authed_client, chat_id)
    assert len(messages) == 2
    assert messages[0]["content"] == "Hello"
