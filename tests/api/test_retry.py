import time
import pytest


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
