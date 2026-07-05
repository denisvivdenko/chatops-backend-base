import uuid
import time

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus
from chatops.repositories.chat_repository import ChatRepository, InMemoryChatRepository
from chatops.stream.job_stream import JobStream, InMemoryJobStream, AssistantJob


class AssistantMessagePendingError(Exception):
    pass


class MessageNotFailedError(Exception):
    pass


class MessageNotFoundError(Exception):
    pass


class MessageStatusTransitionError(Exception):
    pass


ALLOWED_MESSAGE_STATUS_TRANSITIONS: dict[MessageStatus, set[MessageStatus]] = {
    MessageStatus.PENDING: {MessageStatus.COMPLETE, MessageStatus.FAILED},
    MessageStatus.COMPLETE: set(),
    MessageStatus.FAILED: set(),
}


class ChatService:
    def __init__(self, chat_repository: ChatRepository) -> None:
        self._repo = chat_repository

    def create_chat(self, first_message: str, jobs_stream: JobStream) -> Chat:
        now = int(time.time() * 1000)
        chat = Chat(
            id=str(uuid.uuid4()),
            title=first_message[:50],
            last_activity_at=now,
            created_at=now,
        )
        self._repo.save_chat(chat)
        self.send_message(chat.id, first_message, jobs_stream)
        return chat

    def send_message(self, chat_id: str, content: str, jobs_stream: JobStream) -> Message:
        messages = self._repo.fetch_messages(chat_id)
        if messages and messages[-1].role == MessageRole.ASSISTANT and messages[-1].status == MessageStatus.PENDING:
            raise AssistantMessagePendingError()

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
        jobs_stream.publish(AssistantJob(chat_id=chat_id, message_id=assistant_message.id))
        return assistant_message

    def complete_message(self, chat_id: str, message_id: str, content: str) -> None:
        self._transition_message_status(chat_id, message_id, MessageStatus.COMPLETE, content=content)

    def fail_message(self, chat_id: str, message_id: str) -> None:
        self._transition_message_status(chat_id, message_id, MessageStatus.FAILED)

    def _transition_message_status(
        self, chat_id: str, message_id: str, status: MessageStatus, content: str | None = None,
    ) -> None:
        message = self.get_message(chat_id, message_id)
        if status not in ALLOWED_MESSAGE_STATUS_TRANSITIONS[message.status]:
            raise MessageStatusTransitionError(
                f"Failed to change message status from {message.status} to {status}"
            )
        update = {"status": status}
        if content is not None:
            update["content"] = content
        self._repo.save_message(chat_id, message.model_copy(update=update))

    def fetch_chats(self, limit: int) -> list[Chat]:
        return self._repo.fetch_chats(limit)

    def delete_chat(self, chat_id: str) -> None:
        self._repo.delete_chat(chat_id)

    def fail_stale_pending_messages(self, chat_id: str, fail_message_after_timeout: float) -> None:
        now = int(time.time() * 1000)
        for message in self._repo.fetch_messages(chat_id):
            if (
                message.role == MessageRole.ASSISTANT
                and message.status == MessageStatus.PENDING
                and now - message.created_at > fail_message_after_timeout * 1000
            ):
                self.fail_message(chat_id, message.id)

    def fetch_messages(self, chat_id: str) -> list[Message]:
        return self._repo.fetch_messages(chat_id)

    def get_message(self, chat_id: str, message_id: str) -> Message:
        for message in self._repo.fetch_messages(chat_id):
            if message.id == message_id:
                return message
        raise MessageNotFoundError()

    def retry_message(self, chat_id: str, message_id: str, jobs_stream: JobStream) -> Message:
        for message in self._repo.fetch_messages(chat_id):
            if message.id != message_id:
                continue
            if message.status != MessageStatus.FAILED:
                raise MessageNotFailedError()
            retried = message.model_copy(update={
                "status": MessageStatus.PENDING,
                "content": "",
                "created_at": int(time.time() * 1000),
            })
            self._repo.save_message(chat_id, retried)
            jobs_stream.publish(AssistantJob(chat_id=chat_id, message_id=message_id))
            return retried
