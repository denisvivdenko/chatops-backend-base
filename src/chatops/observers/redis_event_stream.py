from typing import AsyncIterator

from chatops.observers.event_stream import EventStream


class RedisEventStream(EventStream):
    async def exists(self, chat_id: str, message_id: str) -> bool:
        raise NotImplementedError

    def read(self, chat_id: str, message_id: str) -> AsyncIterator[str]:
        raise NotImplementedError
