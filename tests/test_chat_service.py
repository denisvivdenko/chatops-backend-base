import time
import pytest
from typing import AsyncIterator

from chatops.services.chat_service import ChatService
from chatops.observers.message_observer import MessageObserver, MessageNotObservableError
from chatops.domain.chat import MessageRole, MessageStatus, MessageStreamEvent


class FakeMessageObserver(MessageObserver):
    def __init__(self, tokens: list[str]) -> None:
        self._tokens = tokens

    @classmethod
    async def create(cls, chat_id: str, message_id: str) -> "FakeMessageObserver":
        if chat_id == "unknown-chat-id" or message_id == "unknown-message-id":
            raise MessageNotObservableError
        return cls(tokens=["Hi", " there"])

    def __aiter__(self) -> AsyncIterator[MessageStreamEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[MessageStreamEvent]:
        for token in self._tokens:
            yield MessageStreamEvent(token=token, status=MessageStatus.PENDING)
        yield MessageStreamEvent(token="", status=MessageStatus.COMPLETE)


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
async def test_create_chat_produces_messages_and_streams_assistant_response() -> None:
    service = ChatService()

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

    observer = await FakeMessageObserver.create(chat.id, assistant_message.id)

    stream = [e async for e in observer]
    assert stream[-1].status == MessageStatus.COMPLETE
    assert "".join(e.token for e in stream) == "Hi there"


@pytest.mark.asyncio
async def test_observe_unknown_message_raises() -> None:
    with pytest.raises(MessageNotObservableError):
        await FakeMessageObserver.create("unknown-chat-id", "unknown-message-id")
