import uuid
import time

from chatops.domain.chat import Chat


class ChatService:
    def create_chat(self, first_message: str) -> Chat:
        now = int(time.time() * 1000)
        return Chat(
            id=str(uuid.uuid4()),
            title=first_message[:50],
            last_activity_at=now,
            created_at=now,
        )

    def fetch_chats(self, limit: int) -> list[Chat]:
        return []
