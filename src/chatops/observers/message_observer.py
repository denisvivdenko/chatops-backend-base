from typing import AsyncIterator

from chatops.domain.chat import MessageStatus, MessageStreamEvent
from chatops.observers.event_stream import EventStream


class MessageNotObservableError(Exception):
    pass


class MessageObserver:
    def __init__(self, chat_id: str, message_id: str, stream: EventStream) -> None:
        self._chat_id = chat_id
        self._message_id = message_id
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[MessageStreamEvent]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MessageStreamEvent]:
        if not await self._stream.exists(self._chat_id, self._message_id):
            raise MessageNotObservableError(f"Message {self._message_id} is not observable")
        async for token in self._stream.read(self._chat_id, self._message_id):
            yield MessageStreamEvent(token=token, status=MessageStatus.PENDING)
        yield MessageStreamEvent(token="", status=MessageStatus.COMPLETE)
