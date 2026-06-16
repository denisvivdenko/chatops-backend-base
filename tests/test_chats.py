from fastapi.testclient import TestClient


def test_fetch_chats_returns_list_of_chats(client: TestClient) -> None:
    response = client.get("/chats", params={"limit": 3})

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["chats"], list)
    for chat in body["chats"]:
        assert isinstance(chat["id"], str)
        assert isinstance(chat["title"], str)
        assert isinstance(chat["last_activity_at"], int)
        assert isinstance(chat["created_at"], int)


def test_create_chat_returns_chat(client: TestClient) -> None:
    response = client.post("/chats", json={"first_message": "Hello"})

    assert response.status_code == 201
    chat = response.json()["chat"]
    assert isinstance(chat["id"], str)
    assert isinstance(chat["title"], str) and len(chat["title"]) > 0
    assert isinstance(chat["last_activity_at"], int) and chat["last_activity_at"] >= chat["created_at"]
    assert isinstance(chat["created_at"], int)
