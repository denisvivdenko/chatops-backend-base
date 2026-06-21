import threading
from abc import ABC, abstractmethod

from chatops.domain.chat import Chat, Message


class ChatRepository(ABC):
    @abstractmethod
    def save_chat(self, chat: Chat) -> None: ...

    @abstractmethod
    def fetch_chats(self, limit: int | None = None) -> list[Chat]: ...

    @abstractmethod
    def save_message(self, chat_id: str, message: Message) -> None: ...

    @abstractmethod
    def fetch_messages(self, chat_id: str) -> list[Message]: ...

    @abstractmethod
    def delete_chat(self, chat_id: str) -> None: ...


class InMemoryChatRepository(ChatRepository):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._chats: list[Chat] = []
        self._messages: dict[str, list[Message]] = {}

    def save_chat(self, chat: Chat) -> None:
        with self._lock:
            self._chats.append(chat)

    def fetch_chats(self, limit: int) -> list[Chat]:
        with self._lock:
            sorted_chats = sorted(self._chats, key=lambda c: c.last_activity_at, reverse=True)
            return sorted_chats[:limit]

    def delete_chat(self, chat_id: str) -> None:
        with self._lock:
            self._chats = [c for c in self._chats if c.id != chat_id]
            self._messages.pop(chat_id, None)

    def save_message(self, chat_id: str, message: Message) -> None:
        with self._lock:
            messages = self._messages.setdefault(chat_id, [])
            for i, m in enumerate(messages):
                if m.id == message.id:
                    messages[i] = message
                    return
            messages.append(message)

    def fetch_messages(self, chat_id: str) -> list[Message]:
        with self._lock:
            return list(self._messages.get(chat_id, []))
