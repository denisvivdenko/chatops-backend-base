import uuid
import time

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus
from chatops.repositories.chat_repository import ChatRepository, InMemoryChatRepository
from chatops.jobs.job_stream import JobStream, InMemoryJobStream, AssistantJob


class LastAssistantMessageIsNotFinished(Exception):
    pass


class ChatService:
    def __init__(self, chat_repository: ChatRepository, jobs_stream: JobStream) -> None:
        self._repo = chat_repository
        self._jobs = jobs_stream

    def create_chat(self, first_message: str) -> Chat:
        now = int(time.time() * 1000)
        chat = Chat(
            id=str(uuid.uuid4()),
            title=first_message[:50],
            last_activity_at=now,
            created_at=now,
        )
        self._repo.save_chat(chat)
        self.send_message(chat.id, first_message)
        return chat

    def send_message(self, chat_id: str, content: str) -> Message:
        messages = self._repo.fetch_messages(chat_id)
        if messages and messages[-1].role == MessageRole.ASSISTANT and messages[-1].status == MessageStatus.PENDING:
            raise LastAssistantMessageIsNotFinished()

        now = int(time.time() * 1000)
        chat = self._repo.fetch_chat(chat_id)
        self._repo.save_chat(chat.model_copy(update={"last_activity_at": now}))

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

    def complete_message(self, chat_id: str, message_id: str, content: str) -> None:
        for message in self._repo.fetch_messages(chat_id):
            if message.id == message_id:
                self._repo.save_message(
                    chat_id,
                    message.model_copy(update={"status": MessageStatus.COMPLETE, "content": content}),
                )
                return

    def fetch_chats(self, limit: int) -> list[Chat]:
        return self._repo.fetch_chats(limit)

    def delete_chat(self, chat_id: str) -> None:
        self._repo.delete_chat(chat_id)

    def fetch_messages(self, chat_id: str) -> list[Message]:
        return self._repo.fetch_messages(chat_id)
