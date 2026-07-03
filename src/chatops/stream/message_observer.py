from typing import AsyncIterator

from chatops.domain.chat import EOM, MessageStreamEvent
from chatops.stream.event_stream import EventStream, StreamNotFoundError


class MessageNotObservableError(Exception):
    pass


class MessageIsAlreadyConsumed(Exception):
    pass


class MessageObserver:
    def __init__(self, chat_id: str, message_id: str, stream: EventStream) -> None:
        self._chat_id = chat_id
        self._message_id = message_id
        self._stream = stream
        self._consumed = False

    def __aiter__(self) -> AsyncIterator[MessageStreamEvent]:
        if self._consumed:
            raise MessageIsAlreadyConsumed()
        self._consumed = True
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MessageStreamEvent]:
        stream_key = self._stream.stream_key(self._chat_id, self._message_id)
        last_id = "0"
        seq_id = 0
        try:
            while True:
                entries = await self._stream.read(stream_key, last_id=last_id)
                for entry in entries:
                    token = entry.data["token"]
                    if token == EOM:
                        return
                    yield MessageStreamEvent(seq_id=seq_id, token=token)
                    last_id = entry.id
                    seq_id += 1
        except StreamNotFoundError:
            raise MessageNotObservableError(f"Message {self._message_id} is not found.")
