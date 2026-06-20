import time
import pytest

from chatops.services.chat_service import ChatService
from chatops.domain.chat import MessageRole, MessageStatus
from chatops.repositories.chat_repository import ChatRepository
from chatops.jobs.job_stream import JobStream
from chatops.workers.worker import Worker, HARDCODED_RESPONSE
from chatops.observers.in_memory_event_stream import InMemoryEventStream
from chatops.observers.message_observer import MessageObserver


def test_created_chats_appear_on_top_sorted_by_last_activity() -> None:
    service = ChatService()

    first_chat = service.create_chat("First message")
    time.sleep(1)
    second_chat = service.create_chat("Second message")

    chats = service.fetch_chats(limit=10)
    assert len(chats) == 2
    assert chats[0].id == second_chat.id
    assert chats[1].id == first_chat.id

    chats_limited = service.fetch_chats(limit=1)
    assert len(chats_limited) == 1
    assert chats_limited[0].id == second_chat.id


@pytest.mark.asyncio
async def test_create_chat_produces_user_and_pending_assistant_messages() -> None:
    job_stream = JobStream()
    event_stream = InMemoryEventStream()
    chat_repository = ChatRepository()

    service = ChatService(chat_repository=chat_repository, jobs_stream=job_stream)

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

    worker = Worker(
        chat_repository=chat_repository, 
        jobs_stream=job_stream,
        event_stream=event_stream
    )
    worker.start()

    message_observer = MessageObserver(chat.id, assistant_message.id, event_stream)
    events = [e async for e in message_observer]
    assert "".join(e.token for e in events) == HARDCODED_RESPONSE

    messages = service.fetch_messages(chat.id)

    assert len(messages) == 2

    assistant_message = messages[1]
    assert assistant_message.role == MessageRole.ASSISTANT
    assert assistant_message.status == MessageStatus.COMPLETE
    assert assistant_message.content == HARDCODED_RESPONSE