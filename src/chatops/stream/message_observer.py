import time
from typing import AsyncIterator

from chatops.domain.chat import EOM, MessageStreamEvent
from chatops.stream.event_stream import EventStream


class MessageAlreadyConsumedError(Exception):
    pass


class MessageGenerationTimeoutError(Exception):
    pass


class MessageObserver:
    def __init__(
        self,
        chat_id: str,
        message_id: str,
        stream: EventStream,
        timeout: float | None = None,
    ) -> None:
        self._chat_id = chat_id
        self._message_id = message_id
        self._stream = stream
        self._timeout = timeout
        self._consumed = False

    def __aiter__(self) -> AsyncIterator[MessageStreamEvent]:
        if self._consumed:
            raise MessageAlreadyConsumedError()
        self._consumed = True
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MessageStreamEvent]:
        stream_key = self._stream.stream_key(self._chat_id, self._message_id)
        deadline = time.monotonic() + self._timeout if self._timeout is not None else None
        last_id = "0"
        seq_id = 0
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                raise MessageGenerationTimeoutError(
                    f"Message {self._message_id} generation exceeded {self._timeout}s."
                )
            try:
                entries = await self._stream.read(stream_key, last_id=last_id)
            except TimeoutError:
                continue
            for entry in entries:
                token = entry.data["token"]
                if token == EOM:
                    return
                yield MessageStreamEvent(seq_id=seq_id, token=token)
                last_id = entry.id
                seq_id += 1
