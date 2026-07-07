import time
import pytest

from chatops.services.chat_service import ChatService, AssistantMessagePendingError, MessageStatusTransitionError
from chatops.domain.chat import MessageRole, MessageStatus
from chatops.settings import Settings
from chatops.workers.worker import TEST_RESPONSE
from chatops.stream.message_observer import MessageObserver

FAIL_MESSAGE_AFTER_TIMEOUT = Settings().message_generation_timeout
USER_ID = "test-user"


def test_fetch_chats_sorted_by_most_recent_first_and_respects_limit(infra) -> None:
    service = ChatService(chat_repository=infra["repo"])
    jobs_stream = infra["job_stream"]
    first_chat = service.create_chat("First message", jobs_stream, USER_ID)
    time.sleep(0.01)
    second_chat = service.create_chat("Second message", jobs_stream, USER_ID)
    time.sleep(0.01)
    third_chat = service.create_chat("Third message", jobs_stream, USER_ID)

    chats = service.fetch_chats(USER_ID, limit=2)
    assert len(chats) == 2
    assert chats[0].id == third_chat.id
    assert chats[1].id == second_chat.id


def test_delete_chat(infra) -> None:
    service = ChatService(chat_repository=infra["repo"])
    chat = service.create_chat("First message", infra["job_stream"], USER_ID)
    assert len(service.fetch_chats(USER_ID, limit=10)) == 1
    service.delete_chat(chat.id, USER_ID)
    assert len(service.fetch_chats(USER_ID, limit=10)) == 0


def test_create_chat_produces_user_and_pending_assistant_and_blocks_follow_up(infra) -> None:
    service = ChatService(chat_repository=infra["repo"])
    jobs_stream = infra["job_stream"]
    chat = service.create_chat("Hello", jobs_stream, USER_ID)
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
        service.send_message(chat.id, USER_ID, "What is the weather today?", jobs_stream)


def test_fail_message_marks_assistant_message_as_failed(infra) -> None:
    service = ChatService(chat_repository=infra["repo"])
    chat = service.create_chat("Hello", infra["job_stream"], USER_ID)
    assistant = service.fetch_messages(chat.id, USER_ID)[1]

    service.fail_message(chat.id, USER_ID, assistant.id)

    failed = service.fetch_messages(chat.id, USER_ID)[1]
    assert failed.id == assistant.id
    assert failed.status == MessageStatus.FAILED


def test_complete_message_raises_when_message_not_pending(infra) -> None:
    service = ChatService(chat_repository=infra["repo"])
    chat = service.create_chat("Hello", infra["job_stream"], USER_ID)
    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    service.fail_message(chat.id, USER_ID, assistant.id)

    with pytest.raises(MessageStatusTransitionError):
        service.complete_message(chat.id, USER_ID, assistant.id, "some content")

    unchanged = service.fetch_messages(chat.id, USER_ID)[1]
    assert unchanged.status == MessageStatus.FAILED
    assert unchanged.content == ""


def test_fail_stale_pending_messages_fails_assistant_message_past_timeout(infra) -> None:
    service = ChatService(chat_repository=infra["repo"])
    chat = service.create_chat("Hello", infra["job_stream"], USER_ID)

    service.fail_stale_pending_messages(chat.id, USER_ID, fail_message_after_timeout=0)

    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    assert assistant.status == MessageStatus.FAILED


def test_fail_stale_pending_messages_leaves_fresh_pending_message_untouched(infra) -> None:
    service = ChatService(chat_repository=infra["repo"])
    chat = service.create_chat("Hello", infra["job_stream"], USER_ID)

    service.fail_stale_pending_messages(chat.id, USER_ID, fail_message_after_timeout=FAIL_MESSAGE_AFTER_TIMEOUT)

    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    assert assistant.status == MessageStatus.PENDING


@pytest.mark.asyncio
async def test_worker_streams_hardcoded_response(infra, worker) -> None:
    event_stream = infra["event_stream"]
    service = ChatService(chat_repository=infra["repo"])
    chat = service.create_chat("Hello", infra["job_stream"], USER_ID)
    pending_assistant = service.fetch_messages(chat.id, USER_ID)[1]

    events = [e async for e in MessageObserver(chat.id, pending_assistant.id, event_stream)]
    assert "".join(e.token for e in events) == TEST_RESPONSE


def test_can_send_next_message_after_assistant_completes(infra) -> None:
    chat_repository = infra["repo"]
    service = ChatService(chat_repository=chat_repository)
    jobs_stream = infra["job_stream"]
    chat = service.create_chat("Hello", jobs_stream, USER_ID)

    assistant = service.fetch_messages(chat.id, USER_ID)[1]
    chat_repository.save_message(chat.id, assistant.model_copy(update={"status": MessageStatus.COMPLETE, "content": "Done"}))

    response = service.send_message(chat.id, USER_ID, "What is the weather today?", jobs_stream)
    assert response.role == MessageRole.ASSISTANT
    assert response.status == MessageStatus.PENDING
