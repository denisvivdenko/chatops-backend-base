from fastapi.testclient import TestClient


def test_create_chat_returns_chat_id_and_assistant_message_id(client: TestClient) -> None:
    response = client.post("/chats", json={"first_message": "Hello"})

    assert response.status_code == 201
    body = response.json()
    assert "chat_id" in body
