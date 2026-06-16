import uuid
import time

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus


class ChatService:
    def __init__(self) -> None:
        self._chats: list[Chat] = []
        self._messages: dict[str, list[Message]] = {}

    def create_chat(self, first_message: str) -> Chat:
        now = int(time.time() * 1000)
        chat = Chat(
            id=str(uuid.uuid4()),
            title=first_message[:50],
            last_activity_at=now,
            created_at=now,
        )
        self._chats.append(chat)
        self._messages[chat.id] = [
            Message(
                id=str(uuid.uuid4()),
                role=MessageRole.USER,
                status=MessageStatus.COMPLETE,
                content=first_message,
                created_at=now,
            ),
            Message(
                id=str(uuid.uuid4()),
                role=MessageRole.ASSISTANT,
                status=MessageStatus.PENDING,
                content="",
                created_at=now,
            ),
        ]
        return chat

    def fetch_chats(self, limit: int) -> list[Chat]:
        sorted_chats = sorted(self._chats, key=lambda c: c.last_activity_at, reverse=True)
        return sorted_chats[:limit]

    def fetch_messages(self, chat_id: str) -> list[Message]:
        return self._messages.get(chat_id, [])
