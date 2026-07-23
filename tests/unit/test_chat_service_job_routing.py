from unittest.mock import MagicMock

import pytest

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus
from chatops.domain.resource import Resource
from chatops.repositories.chat_repository import ChatRepository
from chatops.repositories.resource_repository import ResourceRepository
from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceAccessDeniedError, ResourceNotFoundError, ResourceService
from chatops.storage.resource_storage import ResourceStorage
from chatops.stream.job_stream import Job, JobStream

USER_ID = "test-user"
OTHER_USER_ID = "other-user"
CHAT_ID = "chat-1"


def _make_service() -> tuple[ChatService, MagicMock, MagicMock]:
    chat_repo = MagicMock(spec=ChatRepository)
    chat_repo.fetch_chat.return_value = Chat(
        id=CHAT_ID, user_id=USER_ID, title="Hello", last_activity_at=1, created_at=1,
    )
    chat_repo.fetch_messages.return_value = []
    resource_repo = MagicMock(spec=ResourceRepository)
    resource_service = ResourceService(resource_repository=resource_repo, resource_storage=MagicMock(spec=ResourceStorage))
    service = ChatService(chat_repository=chat_repo, resource_service=resource_service)
    return service, chat_repo, resource_repo


def _owned_resource(resource_id: str, user_id: str = USER_ID) -> Resource:
    return Resource(id=resource_id, user_id=user_id, filename="doc.pdf", file_path=f"/data/resources/{resource_id}", created_at=1)


def test_message_without_resource_refs_publishes_assistant_job_only() -> None:
    service, _, _ = _make_service()
    jobs_stream = MagicMock(spec=JobStream)
    ingestion_jobs = MagicMock(spec=JobStream)

    assistant = service.send_message(CHAT_ID, USER_ID, "plain text, no refs", jobs_stream, ingestion_jobs)

    jobs_stream.publish.assert_called_once_with(Job(chat_id=CHAT_ID, user_id=USER_ID, message_id=assistant.id))
    ingestion_jobs.publish.assert_not_called()


@pytest.mark.parametrize(
    "content, resource_ids",
    [
        ("[doc.pdf](resource://res-1)", ("res-1",)),
        ("[a.pdf](resource://res-1) [b.pdf](resource://res-2)", ("res-1", "res-2")),
    ],
    ids=["single_ref", "multiple_refs"],
)
def test_message_with_resource_refs_publishes_single_ingestion_job_with_all_ids(content, resource_ids) -> None:
    service, _, resource_repo = _make_service()
    resources = {"res-1": _owned_resource("res-1"), "res-2": _owned_resource("res-2")}
    resource_repo.fetch_resource.side_effect = lambda resource_id: resources[resource_id]
    jobs_stream = MagicMock(spec=JobStream)
    ingestion_jobs = MagicMock(spec=JobStream)

    assistant = service.send_message(CHAT_ID, USER_ID, content, jobs_stream, ingestion_jobs)

    ingestion_jobs.publish.assert_called_once_with(
        Job(chat_id=CHAT_ID, user_id=USER_ID, message_id=assistant.id, resource_ids=resource_ids)
    )
    jobs_stream.publish.assert_not_called()


def test_message_with_nonexistent_resource_ref_raises_and_creates_nothing() -> None:
    service, chat_repo, resource_repo = _make_service()
    resource_repo.fetch_resource.side_effect = KeyError()
    jobs_stream = MagicMock(spec=JobStream)
    ingestion_jobs = MagicMock(spec=JobStream)

    with pytest.raises(ResourceNotFoundError):
        service.send_message(CHAT_ID, USER_ID, "[doc.pdf](resource://missing)", jobs_stream, ingestion_jobs)

    chat_repo.save_message.assert_not_called()
    jobs_stream.publish.assert_not_called()
    ingestion_jobs.publish.assert_not_called()


def test_message_with_another_users_resource_ref_raises_and_creates_nothing() -> None:
    service, chat_repo, resource_repo = _make_service()
    resource_repo.fetch_resource.return_value = _owned_resource("res-1", user_id=OTHER_USER_ID)
    jobs_stream = MagicMock(spec=JobStream)
    ingestion_jobs = MagicMock(spec=JobStream)

    with pytest.raises(ResourceAccessDeniedError):
        service.send_message(CHAT_ID, USER_ID, "[doc.pdf](resource://res-1)", jobs_stream, ingestion_jobs)

    chat_repo.save_message.assert_not_called()
    jobs_stream.publish.assert_not_called()
    ingestion_jobs.publish.assert_not_called()


def test_retry_message_with_resource_ref_routes_to_ingestion() -> None:
    service, chat_repo, resource_repo = _make_service()
    resource_repo.fetch_resource.return_value = _owned_resource("res-1")
    failed_message = Message(
        id="msg-1", role=MessageRole.ASSISTANT, status=MessageStatus.FAILED, content="",
        created_at=1, resource_ids_to_process=["res-1"],
    )
    chat_repo.fetch_messages.return_value = [failed_message]
    jobs_stream = MagicMock(spec=JobStream)
    ingestion_jobs = MagicMock(spec=JobStream)

    retried = service.retry_message(CHAT_ID, USER_ID, failed_message.id, jobs_stream, ingestion_jobs)

    ingestion_jobs.publish.assert_called_once_with(
        Job(chat_id=CHAT_ID, user_id=USER_ID, message_id=retried.id, resource_ids=("res-1",))
    )
    jobs_stream.publish.assert_not_called()


def test_modify_message_with_new_resource_ref_routes_to_ingestion() -> None:
    service, chat_repo, resource_repo = _make_service()
    resource_repo.fetch_resource.return_value = _owned_resource("res-1")
    user_message = Message(id="msg-1", role=MessageRole.USER, status=MessageStatus.COMPLETE, content="Hello", created_at=1)
    chat_repo.fetch_messages.return_value = [user_message]
    jobs_stream = MagicMock(spec=JobStream)
    ingestion_jobs = MagicMock(spec=JobStream)

    assistant = service.modify_message(
        CHAT_ID, USER_ID, user_message.id, "[doc.pdf](resource://res-1)", jobs_stream, ingestion_jobs,
    )

    ingestion_jobs.publish.assert_called_once_with(
        Job(chat_id=CHAT_ID, user_id=USER_ID, message_id=assistant.id, resource_ids=("res-1",))
    )
    jobs_stream.publish.assert_not_called()
