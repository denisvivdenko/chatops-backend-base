import time
import pytest

from chatops.services.chat_service import ChatService, LastAssistantMessageIsNotFinished
from chatops.domain.chat import MessageRole, MessageStatus
from chatops.repositories.chat_repository import InMemoryChatRepository
from chatops.jobs.job_stream import InMemoryJobStream
from chatops.workers.worker import Worker, HARDCODED_RESPONSE
from chatops.observers.in_memory_event_stream import InMemoryEventStream
from chatops.observers.message_observer import MessageObserver


def test_fetch_chats_sorted_by_most_recent_first() -> None:
    service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())

    first_chat = service.create_chat("First message")
    time.sleep(1)
    second_chat = service.create_chat("Second message")

    chats = service.fetch_chats(limit=10)
    assert len(chats) == 2
    assert chats[0].id == second_chat.id
    assert chats[1].id == first_chat.id


def test_fetch_chats_respects_limit() -> None:
    service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())
    service.create_chat("First message")
    service.create_chat("Second message")
    service.create_chat("Third message")

    chats = service.fetch_chats(limit=2)
    assert len(chats) == 2


def test_delete_chat() -> None:
    service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())
    chat = service.create_chat("First message")
    assert len(service.fetch_chats(limit=10)) == 1
    service.delete_chat(chat.id)
    assert len(service.fetch_chats(limit=10)) == 0


def test_create_chat_produces_user_and_pending_assistant_messages() -> None:
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


def test_send_message_raises_when_last_assistant_message_is_pending() -> None:
    service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())
    chat = service.create_chat("Hello")

    with pytest.raises(LastAssistantMessageIsNotFinished):
        service.send_message(chat.id, "What is the weather today?")


@pytest.mark.asyncio
async def test_worker_completes_pending_assistant_message() -> None:
    job_stream = InMemoryJobStream()
    event_stream = InMemoryEventStream()
    chat_repository = InMemoryChatRepository()

    service = ChatService(chat_repository=chat_repository, jobs_stream=job_stream)
    chat = service.create_chat("Hello")
    pending_assistant = service.fetch_messages(chat.id)[1]

    Worker(chat_repository=chat_repository, jobs_stream=job_stream, event_stream=event_stream).start()

    events = [e async for e in MessageObserver(chat.id, pending_assistant.id, event_stream)]
    assert " ".join(e.token for e in events) == HARDCODED_RESPONSE

    messages = service.fetch_messages(chat.id)
    assert messages[1].status == MessageStatus.COMPLETE
    assert messages[1].content == HARDCODED_RESPONSE


@pytest.mark.asyncio
async def test_can_send_next_message_after_assistant_completes() -> None:
    job_stream = InMemoryJobStream()
    event_stream = InMemoryEventStream()
    chat_repository = InMemoryChatRepository()

    service = ChatService(chat_repository=chat_repository, jobs_stream=job_stream)
    chat = service.create_chat("Hello")
    first_assistant = service.fetch_messages(chat.id)[1]

    Worker(chat_repository=chat_repository, jobs_stream=job_stream, event_stream=event_stream).start()

    _ = [e async for e in MessageObserver(chat.id, first_assistant.id, event_stream)]

    second_assistant = service.send_message(chat.id, "What is the weather today?")
    assert second_assistant.role == MessageRole.ASSISTANT
    assert second_assistant.status == MessageStatus.PENDING

    messages = service.fetch_messages(chat.id)
    assert len(messages) == 4
    assert messages[-1].role == MessageRole.ASSISTANT
    assert messages[-1].status == MessageStatus.PENDING

    events = [e async for e in MessageObserver(chat.id, second_assistant.id, event_stream)]
    assert " ".join(e.token for e in events) == HARDCODED_RESPONSE

    messages = service.fetch_messages(chat.id)
    assert len(messages) == 4
    assert messages[-1].status == MessageStatus.COMPLETE
