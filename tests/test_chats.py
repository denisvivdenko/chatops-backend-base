from fastapi.testclient import TestClient


def test_create_chat_returns_chat(client: TestClient) -> None:
    response = client.post("/chats", json={"first_message": "Hello"})

    assert response.status_code == 201
    chat = response.json()["chat"]
    assert isinstance(chat["id"], str)
    assert isinstance(chat["title"], str)
    assert isinstance(chat["last_activity_at"], int)
    assert isinstance(chat["created_at"], int)
