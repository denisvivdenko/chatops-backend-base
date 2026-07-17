import uuid
import time

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus
from chatops.repositories.chat_repository import ChatRepository
from chatops.services.resource_service import ResourceService
from chatops.stream.ingestion_job_stream import IngestionJob, IngestionJobStream
from chatops.stream.job_stream import JobStream, AssistantJob


class AssistantMessagePendingError(Exception):
    pass


class MessageNotFailedError(Exception):
    pass


class MessageNotFoundError(Exception):
    pass


class CannotModifyAssistantMessageError(Exception):
    pass


class MessageStatusTransitionError(Exception):
    pass


class ChatAccessDeniedError(Exception):
    pass


class ChatNotFoundError(Exception):
    pass


ALLOWED_MESSAGE_STATUS_TRANSITIONS: dict[MessageStatus, set[MessageStatus]] = {
    MessageStatus.PENDING: {MessageStatus.COMPLETE, MessageStatus.FAILED},
    MessageStatus.COMPLETE: set(),
    MessageStatus.FAILED: set(),
}


class ChatService:
    def __init__(self, chat_repository: ChatRepository, resource_service: ResourceService) -> None:
        self._repo = chat_repository
        self._resource_service = resource_service

    def create_chat(
        self, first_message: str, user_id: str, jobs_stream: JobStream, ingestion_jobs: IngestionJobStream,
    ) -> Chat:
        now = int(time.time() * 1000)
        chat = Chat(
            id=str(uuid.uuid4()),
            user_id=user_id,
            title=first_message[:50],
            last_activity_at=now,
            created_at=now,
        )
        self._repo.save_chat(chat)
        self.send_message(chat.id, user_id, first_message, jobs_stream, ingestion_jobs)
        return chat

    def send_message(
        self, chat_id: str, user_id: str, content: str, jobs_stream: JobStream, ingestion_jobs: IngestionJobStream,
    ) -> Message:
        self._assert_owns_chat(chat_id, user_id)
        resource_ids = self._resource_service.parse_resource_refs(content)
        for resource_id in resource_ids:
            self._resource_service.assert_owns_resource(resource_id, user_id)

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
            resource_ids_to_process=resource_ids,
        )
        self._repo.save_message(chat_id, user_message)
        self._repo.save_message(chat_id, assistant_message)
        self._dispatch_assistant_job(chat_id, user_id, assistant_message.id, resource_ids, jobs_stream, ingestion_jobs)
        return assistant_message

    def complete_message(self, chat_id: str, user_id: str, message_id: str, content: str) -> None:
        self._transition_message_status(chat_id, user_id, message_id, MessageStatus.COMPLETE, content=content)

    def fail_message(self, chat_id: str, user_id: str, message_id: str) -> None:
        self._transition_message_status(chat_id, user_id, message_id, MessageStatus.FAILED)

    def fetch_chats(self, user_id: str, limit: int) -> list[Chat]:
        return self._repo.fetch_chats(user_id, limit)

    def delete_chat(self, chat_id: str, user_id: str) -> None:
        self._assert_owns_chat(chat_id, user_id)
        self._repo.delete_chat(chat_id)

    def fail_stale_pending_messages(self, chat_id: str, user_id: str, fail_message_after_timeout: float) -> None:
        now = int(time.time() * 1000)
        for message in self.fetch_messages(chat_id, user_id):
            if (
                message.role == MessageRole.ASSISTANT
                and message.status == MessageStatus.PENDING
                and now - message.created_at > fail_message_after_timeout * 1000
            ):
                self.fail_message(chat_id, user_id, message.id)

    def fetch_messages(self, chat_id: str, user_id: str) -> list[Message]:
        self._assert_owns_chat(chat_id, user_id)
        return self._repo.fetch_messages(chat_id)

    def get_message(self, chat_id: str, user_id: str, message_id: str) -> Message:
        self._assert_owns_chat(chat_id, user_id)
        for message in self._repo.fetch_messages(chat_id):
            if message.id == message_id:
                return message
        raise MessageNotFoundError()

    def retry_message(
        self, chat_id: str, user_id: str, message_id: str, jobs_stream: JobStream, ingestion_jobs: IngestionJobStream,
    ) -> Message:
        message = self.get_message(chat_id, user_id, message_id)
        if message.status != MessageStatus.FAILED:
            raise MessageNotFailedError()
        for resource_id in message.resource_ids_to_process:
            self._resource_service.assert_owns_resource(resource_id, user_id)
        retried = message.model_copy(update={
            "status": MessageStatus.PENDING,
            "content": "",
            "created_at": int(time.time() * 1000),
        })
        self._repo.save_message(chat_id, retried)
        self._dispatch_assistant_job(
            chat_id, user_id, retried.id, retried.resource_ids_to_process, jobs_stream, ingestion_jobs,
        )
        return retried

    def modify_message(
        self,
        chat_id: str,
        user_id: str,
        message_id: str,
        content: str,
        jobs_stream: JobStream,
        ingestion_jobs: IngestionJobStream,
    ) -> Message:
        self._assert_owns_chat(chat_id, user_id)
        resource_ids = self._resource_service.parse_resource_refs(content)
        for resource_id in resource_ids:
            self._resource_service.assert_owns_resource(resource_id, user_id)

        messages = self._repo.fetch_messages(chat_id)

        target_index = next((i for i, m in enumerate(messages) if m.id == message_id), None)
        if target_index is None:
            raise MessageNotFoundError()
        target = messages[target_index]
        if target.role != MessageRole.USER:
            raise CannotModifyAssistantMessageError()
        if messages[-1].role == MessageRole.ASSISTANT and messages[-1].status == MessageStatus.PENDING:
            raise AssistantMessagePendingError()

        for stale in messages[target_index + 1:]:
            self._repo.delete_message(chat_id, stale.id)
        self._repo.save_message(chat_id, target.model_copy(update={"content": content}))

        now = int(time.time() * 1000)
        chat = self._repo.fetch_chat(chat_id)
        self._repo.save_chat(chat.model_copy(update={"last_activity_at": now}))

        assistant_message = Message(
            id=str(uuid.uuid4()),
            role=MessageRole.ASSISTANT,
            status=MessageStatus.PENDING,
            content="",
            created_at=now,
            resource_ids_to_process=resource_ids,
        )
        self._repo.save_message(chat_id, assistant_message)
        self._dispatch_assistant_job(chat_id, user_id, assistant_message.id, resource_ids, jobs_stream, ingestion_jobs)
        return assistant_message

    def _assert_owns_chat(self, chat_id: str, user_id: str) -> None:
        try:
            chat = self._repo.fetch_chat(chat_id)
        except KeyError:
            raise ChatNotFoundError()
        if chat.user_id != user_id:
            raise ChatAccessDeniedError()

    def _dispatch_assistant_job(
        self,
        chat_id: str,
        user_id: str,
        message_id: str,
        resource_ids: list[str],
        jobs_stream: JobStream,
        ingestion_jobs: IngestionJobStream,
    ) -> None:
        if resource_ids:
            ingestion_jobs.publish(IngestionJob(
                chat_id=chat_id, user_id=user_id, message_id=message_id, resource_ids=tuple(resource_ids),
            ))
        else:
            jobs_stream.publish(AssistantJob(chat_id=chat_id, user_id=user_id, message_id=message_id))

    def _transition_message_status(
        self, chat_id: str, user_id: str, message_id: str, status: MessageStatus, content: str | None = None,
    ) -> None:
        message = self.get_message(chat_id, user_id, message_id)
        if status not in ALLOWED_MESSAGE_STATUS_TRANSITIONS[message.status]:
            raise MessageStatusTransitionError(
                f"Failed to change message status from {message.status} to {status}"
            )
        update = {"status": status}
        if content is not None:
            update["content"] = content
        self._repo.save_message(chat_id, message.model_copy(update=update))
