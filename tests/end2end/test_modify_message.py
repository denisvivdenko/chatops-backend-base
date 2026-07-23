import pytest

from chatops.workers.response_generator import TEST_RESPONSE
from conftest import sleep_until_message_timed_out


def test_modify_message_updates_content_and_returns_new_pending_assistant(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    user_message_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[0]["id"]
    assistant_message_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_message_id}/stream") as resp:
        list(resp.iter_lines())

    response = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify", json={"content": "Hello, edited"}
    )

    assert response.status_code == 200
    body = response.json()
    assert body["role"] == "assistant"
    assert body["status"] == "pending"
    assert body["content"] == ""
    assert body["id"] != assistant_message_id

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert len(messages) == 2

    assert messages[0]["id"] == user_message_id
    assert messages[0]["role"] == "user"
    assert messages[0]["status"] == "complete"
    assert messages[0]["content"] == "Hello, edited"

    assert messages[1]["id"] == body["id"]
    assert messages[1]["status"] == "pending"


def test_modify_first_message_deletes_all_subsequent_turns(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    first_assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{first_assistant_id}/stream") as resp:
        list(resp.iter_lines())

    authed_client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": "Second"})
    second_assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[3]["id"]
    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{second_assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert len(messages) == 4
    first_user_id = messages[0]["id"]

    response = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{first_user_id}/modify", json={"content": "Hello, edited"}
    )
    assert response.status_code == 200

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert len(messages) == 2

    assert messages[0]["id"] == first_user_id
    assert messages[0]["content"] == "Hello, edited"

    assert messages[1]["status"] == "pending"
    assert messages[1]["id"] not in (first_assistant_id, second_assistant_id)


def test_modify_second_message_preserves_earlier_turn(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    first_assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{first_assistant_id}/stream") as resp:
        list(resp.iter_lines())

    authed_client_with_worker.post(f"/api/chats/{chat_id}/messages", json={"content": "Second"})
    second_assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[3]["id"]
    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{second_assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    first_user, first_assistant, second_user = messages[0], messages[1], messages[2]

    response = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{second_user['id']}/modify", json={"content": "Second, edited"}
    )
    assert response.status_code == 200

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert len(messages) == 4

    assert messages[0] == first_user

    assert messages[1] == first_assistant

    assert messages[2]["id"] == second_user["id"]
    assert messages[2]["content"] == "Second, edited"
    assert messages[2]["status"] == "complete"

    assert messages[3]["status"] == "pending"


def test_modified_message_can_be_completed_by_worker(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    user_message_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[0]["id"]
    assistant_message_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_message_id}/stream") as resp:
        list(resp.iter_lines())

    new_assistant_message_id = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify", json={"content": "Hello, edited"}
    ).json()["id"]

    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{new_assistant_message_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["id"] == new_assistant_message_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == TEST_RESPONSE


@pytest.mark.parametrize(
    "settings",
    [{"message_timeout": {"message_generation_timeout": 0.05}}],
    indirect=True,
)
def test_modify_message_allowed_after_assistant_failed(authed_client, settings):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]
    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    user_message_id = messages[0]["id"]
    assistant_message = messages[1]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "failed"

    response = authed_client.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify", json={"content": "Hello, edited"}
    )

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert len(messages) == 2
    assert messages[0]["content"] == "Hello, edited"
    assert messages[1]["status"] == "pending"


def test_modify_message_while_assistant_pending_is_rejected(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]
    user_message_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[0]["id"]

    response = authed_client.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify", json={"content": "Hello, edited"}
    )

    assert response.status_code == 409
    assert response.json()["error"] == "last_assistant_message_not_finished"

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert len(messages) == 2
    assert messages[0]["content"] == "Hello"


def test_modify_assistant_message_is_rejected(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_message_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_worker.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_message_id}/stream") as resp:
        list(resp.iter_lines())

    response = authed_client_with_worker.post(
        f"/api/chats/{chat_id}/messages/{assistant_message_id}/modify", json={"content": "Hijacked"}
    )

    assert response.status_code == 409
    assert response.json()["error"] == "cannot_modify_assistant_message"

    messages = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert len(messages) == 2
    assert messages[1]["content"] == TEST_RESPONSE
