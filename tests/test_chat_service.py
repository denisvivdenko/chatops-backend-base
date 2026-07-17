import time
import pytest

from chatops.services.chat_service import (
    ChatService,
    AssistantMessagePendingError,
    MessageStatusTransitionError,
    ResourceAccessDeniedError,
    ResourceNotFoundError,
)
from chatops.domain.chat import MessageRole, MessageStatus
from chatops.domain.resource import Resource
from chatops.settings import Settings
from chatops.workers.worker import TEST_RESPONSE
from chatops.stream.message_observer import MessageObserver

FAIL_MESSAGE_AFTER_TIMEOUT = Settings().message_generation_timeout
USER_ID = "test-user"
OTHER_USER_ID = "other-user"


def _make_service(infra) -> ChatService:
    return ChatService(chat_repository=infra["repo"], resource_repository=infra["resource_repo"])


def test_fetch_chats_sorted_by_most_recent_first_and_respects_limit(infra) -> None:
    service = _make_service(infra)
    jobs_stream = infra["job_stream"]
    ingestion_jobs = infra["ingestion_job_stream"]
    first_chat = service.create_chat("First message", USER_ID, jobs_stream, ingestion_jobs)
    time.sleep(0.01)
    second_chat = service.create_chat("Second message", USER_ID, jobs_stream, ingestion_jobs)
    time.sleep(0.01)
    third_chat = service.create_chat("Third message", USER_ID, jobs_stream, ingestion_jobs)

    chats = service.fetch_chats(USER_ID, limit=2)
    assert len(chats) == 2
    assert chats[0].id == third_chat.id
    assert chats[1].id == second_chat.id


def test_delete_chat(infra) -> None:
    service = _make_service(infra)
    chat = service.create_chat("First message", USER_ID, infra["job_stream"], infra["ingestion_job_stream"])
    assert len(service.fetch_chats(USER_ID, limit=10)) == 1
    service.delete_chat(chat.id, USER_ID)
    assert len(service.fetch_chats(USER_ID, limit=10)) == 0


def test_delete_resource_removes_it_for_owner(infra) -> None:
    service = _make_service(infra)
    resource = Resource(id="r1", user_id=USER_ID, filename="a.pdf", file_path="/data/resources/r1", created_at=1)
    infra["resource_repo"].save_resource(resource)

    service.delete_resource(resource.id, USER_ID)

    with pytest.raises(KeyError):
        infra["resource_repo"].fetch_resource(resource.id)


def test_delete_resource_raises_when_not_owned_by_caller(infra) -> None:
    service = _make_service(infra)
    resource = Resource(id="r1", user_id=OTHER_USER_ID, filename="a.pdf", file_path="/data/resources/r1", created_at=1)
    infra["resource_repo"].save_resource(resource)

    with pytest.raises(ResourceAccessDeniedError):
        service.delete_resource(resource.id, USER_ID)

    assert infra["resource_repo"].fetch_resource(resource.id) == resource


def test_delete_resource_raises_when_missing(infra) -> None:
    service = _make_service(infra)

    with pytest.raises(ResourceNotFoundError):
        service.delete_resource("nonexistent", USER_ID)


def test_create_chat_produces_user_and_pending_assistant_and_blocks_follow_up(infra) -> None:
    service = _make_service(infra)
    jobs_stream = infra["job_stream"]
    ingestion_jobs = infra["ingestion_job_stream"]
    chat = service.create_chat("Hello", USER_ID, jobs_stream, ingestion_jobs)
    messages = service.fetch_messages(chat.id, USER_ID)

    assert len(messages) == 2

    user_message = messages[0]
    assert user_message.role == MessageRole.USER
    assert user_message.status == MessageStatus.COMPLETE
    assert user_message.content == "Hello"

    assistant_message = messages[1]
    assert assistant_message.role == MessageRole.ASSISTANT
    assert assistant_message.status == MessageStatus.PENDING

    with pytest.raises(AssistantMessagePendingError):
        service.send_message(chat.id, USER_ID, "What is the weather today?", jobs_stream, ingestion_jobs)


def test_fail_message_marks_assistant_message_as_failed(infra) -> None:
    service = _make_service(infra)
    chat = service.create_chat("Hello", USER_ID, infra["job_stream"], infra["ingestion_job_stream"])
    assistant = service.fetch_messages(chat.id, USER_ID)[1]

    service.fail_message(chat.id, USER_ID, assistant.id)

    failed = service.fetch_messages(chat.id, USER_ID)[1]
    assert failed.id == assistant.id
    assert failed.status == MessageStatus.FAILED


def test_complete_message_raises_when_message_not_pending(infra) -> None:
    service = _make_service(infra)
    chat = service.create_chat("Hello", USER_ID, infra["job_stream"], infra["ingestion_job_stream"])
    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    service.fail_message(chat.id, USER_ID, assistant.id)

    with pytest.raises(MessageStatusTransitionError):
        service.complete_message(chat.id, USER_ID, assistant.id, "some content")

    unchanged = service.fetch_messages(chat.id, USER_ID)[1]
    assert unchanged.status == MessageStatus.FAILED
    assert unchanged.content == ""


def test_fail_stale_pending_messages_fails_assistant_message_past_timeout(infra) -> None:
    service = _make_service(infra)
    chat = service.create_chat("Hello", USER_ID, infra["job_stream"], infra["ingestion_job_stream"])

    service.fail_stale_pending_messages(chat.id, USER_ID, fail_message_after_timeout=0)

    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    assert assistant.status == MessageStatus.FAILED


def test_fail_stale_pending_messages_leaves_fresh_pending_message_untouched(infra) -> None:
    service = _make_service(infra)
    chat = service.create_chat("Hello", USER_ID, infra["job_stream"], infra["ingestion_job_stream"])

    service.fail_stale_pending_messages(chat.id, USER_ID, fail_message_after_timeout=FAIL_MESSAGE_AFTER_TIMEOUT)

    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    assert assistant.status == MessageStatus.PENDING


@pytest.mark.asyncio
async def test_worker_streams_hardcoded_response(infra, worker) -> None:
    event_stream = infra["event_stream"]
    service = _make_service(infra)
    chat = service.create_chat("Hello", USER_ID, infra["job_stream"], infra["ingestion_job_stream"])
    pending_assistant = service.fetch_messages(chat.id, USER_ID)[1]

    events = [e async for e in MessageObserver(chat.id, pending_assistant.id, event_stream)]
    assert "".join(e.token for e in events) == TEST_RESPONSE


def test_can_send_next_message_after_assistant_completes(infra) -> None:
    chat_repository = infra["repo"]
    service = _make_service(infra)
    jobs_stream = infra["job_stream"]
    ingestion_jobs = infra["ingestion_job_stream"]
    chat = service.create_chat("Hello", USER_ID, jobs_stream, ingestion_jobs)

    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    chat_repository.save_message(chat.id, assistant.model_copy(update={"status": MessageStatus.COMPLETE, "content": "Done"}))

    response = service.send_message(chat.id, USER_ID, "What is the weather today?", jobs_stream, ingestion_jobs)
    assert response.role == MessageRole.ASSISTANT
    assert response.status == MessageStatus.PENDING
