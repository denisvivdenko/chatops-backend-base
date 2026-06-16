import uuid
import time

from chatops.domain.chat import Chat


class ChatService:
    def __init__(self) -> None:
        self._chats: list[Chat] = []

    def create_chat(self, first_message: str) -> Chat:
        now = int(time.time() * 1000)
        chat = Chat(
            id=str(uuid.uuid4()),
            title=first_message[:50],
            last_activity_at=now,
            created_at=now,
        )
        self._chats.append(chat)
        return chat

    def fetch_chats(self, limit: int) -> list[Chat]:
        sorted_chats = sorted(self._chats, key=lambda c: c.last_activity_at, reverse=True)
        return sorted_chats[:limit]
