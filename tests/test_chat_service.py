import time
import pytest

from chatops.services.chat_service import ChatService, AssistantMessagePendingError
from chatops.domain.chat import MessageRole, MessageStatus
from chatops.repositories.chat_repository import InMemoryChatRepository
from chatops.stream.job_stream import InMemoryJobStream
from chatops.workers.worker import Worker, TEST_RESPONSE
from chatops.stream.event_stream import InMemoryEventStream
from chatops.stream.message_observer import MessageObserver


def test_fetch_chats_sorted_by_most_recent_first_and_respects_limit() -> None:
    service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())
    first_chat = service.create_chat("First message")
    time.sleep(0.01)
    second_chat = service.create_chat("Second message")
    time.sleep(0.01)
    third_chat = service.create_chat("Third message")

    chats = service.fetch_chats(limit=2)
    assert len(chats) == 2
    assert chats[0].id == third_chat.id
    assert chats[1].id == second_chat.id


def test_delete_chat() -> None:
    service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())
    chat = service.create_chat("First message")
    assert len(service.fetch_chats(limit=10)) == 1
    service.delete_chat(chat.id)
    assert len(service.fetch_chats(limit=10)) == 0


def test_create_chat_produces_user_and_pending_assistant_and_blocks_follow_up() -> None:
    service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())
    chat = service.create_chat("Hello")
    messages = service.fetch_messages(chat.id)

    assert len(messages) == 2

    user_message = messages[0]
    assert user_message.role == MessageRole.USER
    assert user_message.status == MessageStatus.COMPLETE
    assert user_message.content == "Hello"

    assistant_message = messages[1]
    assert assistant_message.role == MessageRole.ASSISTANT
    assert assistant_message.status == MessageStatus.PENDING

    with pytest.raises(AssistantMessagePendingError):
        service.send_message(chat.id, "What is the weather today?")


@pytest.mark.asyncio
async def test_worker_streams_hardcoded_response() -> None:
    job_stream = InMemoryJobStream()
    event_stream = InMemoryEventStream()
    chat_repo = InMemoryChatRepository()

    service = ChatService(chat_repository=chat_repo, jobs_stream=job_stream)
    chat = service.create_chat("Hello")
    pending_assistant = service.fetch_messages(chat.id)[1]

    Worker(jobs_stream=job_stream, chat_service=service, event_stream=event_stream, response=TEST_RESPONSE).start()

    events = [e async for e in MessageObserver(chat.id, pending_assistant.id, event_stream)]
    assert "".join(e.token for e in events) == TEST_RESPONSE


def test_can_send_next_message_after_assistant_completes() -> None:
    chat_repository = InMemoryChatRepository()
    service = ChatService(chat_repository=chat_repository, jobs_stream=InMemoryJobStream())
    chat = service.create_chat("Hello")

    assistant = service.fetch_messages(chat.id)[1]
    chat_repository.save_message(chat.id, assistant.model_copy(update={"status": MessageStatus.COMPLETE, "content": "Done"}))

    response = service.send_message(chat.id, "What is the weather today?")
    assert response.role == MessageRole.ASSISTANT
    assert response.status == MessageStatus.PENDING
