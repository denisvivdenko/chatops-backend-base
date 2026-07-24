def create_chat(client, message: str, **kwargs) -> str:
    return client.post("/api/chats", json={"message": message}, **kwargs).json()["id"]


def get_messages(client, chat_id: str, **kwargs) -> list[dict]:
    return client.get(f"/api/chats/{chat_id}/messages", **kwargs).json()


def stream_to_completion(client, chat_id: str, message_id: str, **kwargs) -> list[str]:
    with client.stream("GET", f"/api/chats/{chat_id}/messages/{message_id}/stream", **kwargs) as resp:
        return list(resp.iter_lines())


def new_user_token(client) -> str:
    return client.post("/api/auth/anonymous-session").json()["access_token"]
