import uuid
import time

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus
from chatops.repositories.chat_repository import ChatRepository, InMemoryChatRepository
from chatops.jobs.job_stream import JobStream, InMemoryJobStream, AssistantJob


class LastAssistantMessageIsNotFinished(Exception):
    pass


class ChatService:
    def __init__(self, chat_repository: ChatRepository | None = None, jobs_stream: JobStream | None = None) -> None:
        self._repo = chat_repository or InMemoryChatRepository()
        self._jobs = jobs_stream or InMemoryJobStream()

    def create_chat(self, first_message: str) -> Chat:
        now = int(time.time() * 1000)
        chat = Chat(
            id=str(uuid.uuid4()),
            title=first_message[:50],
            last_activity_at=now,
            created_at=now,
        )
        user_message = Message(
            id=str(uuid.uuid4()),
            role=MessageRole.USER,
            status=MessageStatus.COMPLETE,
            content=first_message,
            created_at=now,
        )
        assistant_message = Message(
            id=str(uuid.uuid4()),
            role=MessageRole.ASSISTANT,
            status=MessageStatus.PENDING,
            content="",
            created_at=now,
        )
        self._repo.save_chat(chat)
        self._repo.save_message(chat.id, user_message)
        self._repo.save_message(chat.id, assistant_message)
        self._jobs.publish(AssistantJob(chat_id=chat.id, message_id=assistant_message.id))
        return chat

    def send_message(self, chat_id: str, content: str) -> Message:
        messages = self._repo.fetch_messages(chat_id)
        if messages and messages[-1].role == MessageRole.ASSISTANT and messages[-1].status == MessageStatus.PENDING:
            raise LastAssistantMessageIsNotFinished()

        now = int(time.time() * 1000)
        user_message = Message(
            id=str(uuid.uuid4()),
            role=MessageRole.USER,
            status=MessageStatus.COMPLETE,
            content=content,
            created_at=now,
        )
        assistant_message = Message(
            id=str(uuid.uuid4()),
            role=MessageRole.ASSISTANT,
            status=MessageStatus.PENDING,
            content="",
            created_at=now,
        )
        self._repo.save_message(chat_id, user_message)
        self._repo.save_message(chat_id, assistant_message)
        self._jobs.publish(AssistantJob(chat_id=chat_id, message_id=assistant_message.id))
        return assistant_message

    def fetch_chats(self, limit: int) -> list[Chat]:
        return self._repo.fetch_chats(limit)

    def fetch_messages(self, chat_id: str) -> list[Message]:
        return self._repo.fetch_messages(chat_id)
