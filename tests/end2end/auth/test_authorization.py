from .helpers import auth_headers
from ..helpers import create_chat, get_messages, new_user_token


def test_each_user_sees_only_their_own_chats(client):
    token_a = new_user_token(client)
    token_b = new_user_token(client)

    chat_a = create_chat(client, "From A", headers=auth_headers(token_a))
    chat_b = create_chat(client, "From B", headers=auth_headers(token_b))

    chats_seen_by_a = {c["id"] for c in client.get("/api/chats", headers=auth_headers(token_a)).json()}
    chats_seen_by_b = {c["id"] for c in client.get("/api/chats", headers=auth_headers(token_b)).json()}

    assert chats_seen_by_a == {chat_a}
    assert chats_seen_by_b == {chat_b}


def test_user_cannot_read_messages_of_another_users_chat(client):
    token_a = new_user_token(client)
    token_b = new_user_token(client)
    chat_id = create_chat(client, "Hello", headers=auth_headers(token_a))

    response = client.get(f"/api/chats/{chat_id}/messages", headers=auth_headers(token_b))

    assert response.status_code == 403


def test_user_cannot_send_message_to_another_users_chat(client):
    token_a = new_user_token(client)
    token_b = new_user_token(client)
    chat_id = create_chat(client, "Hello", headers=auth_headers(token_a))

    # chat_a's assistant reply is still pending; ownership must still be checked first (403, not 409)
    response = client.post(
        f"/api/chats/{chat_id}/messages", json={"content": "Intruding"}, headers=auth_headers(token_b)
    )

    assert response.status_code == 403


def test_user_cannot_modify_message_in_another_users_chat(client):
    token_a = new_user_token(client)
    token_b = new_user_token(client)
    chat_id = create_chat(client, "Hello", headers=auth_headers(token_a))
    user_message_id = get_messages(client, chat_id, headers=auth_headers(token_a))[0]["id"]

    # chat_a's assistant reply is still pending; ownership must still be checked first (403, not 409)
    response = client.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify",
        json={"content": "Intruding"},
        headers=auth_headers(token_b),
    )

    assert response.status_code == 403


def test_user_cannot_delete_another_users_chat(client):
    token_a = new_user_token(client)
    token_b = new_user_token(client)
    chat_id = create_chat(client, "Hello", headers=auth_headers(token_a))

    response = client.delete(f"/api/chats/{chat_id}", headers=auth_headers(token_b))

    assert response.status_code == 403
    remaining = client.get("/api/chats", headers=auth_headers(token_a)).json()
    assert any(c["id"] == chat_id for c in remaining)


def test_user_cannot_stream_another_users_message(client):
    token_a = new_user_token(client)
    token_b = new_user_token(client)
    chat_id = create_chat(client, "Hello", headers=auth_headers(token_a))
    assistant_id = get_messages(client, chat_id, headers=auth_headers(token_a))[1]["id"]

    response = client.get(
        f"/api/chats/{chat_id}/messages/{assistant_id}/stream", headers=auth_headers(token_b)
    )

    assert response.status_code == 403
