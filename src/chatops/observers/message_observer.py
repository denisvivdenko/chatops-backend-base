from typing import AsyncIterator

from chatops.domain.chat import EOM, MessageStreamEvent
from chatops.observers.event_stream import EventStream


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
        if not await self._stream.exists(self._chat_id, self._message_id):
            raise MessageNotObservableError(f"Message {self._message_id} is not observable")

        seq_id = 0
        while True:
            entries = await self._stream.listen_for_message_tokens(self._chat_id, self._message_id, from_seq_id=seq_id)
            for seq_id, token in sorted(entries, key=lambda e: e.seq_id):
                if token == EOM:
                    return
                yield MessageStreamEvent(seq_id=seq_id, token=token)
            if entries:
                seq_id += 1
