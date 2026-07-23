import asyncio
from abc import ABC, abstractmethod
from typing import NamedTuple

import redis


class StreamEntry(NamedTuple):
    id: str
    data: dict[str, str]


class EventStream(ABC):
    @staticmethod
    def stream_key(chat_id: str, message_id: str) -> str:
        return f"{chat_id}:{message_id}"

    @abstractmethod
    def write(self, stream_key: str, data: dict[str, str]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def read(self, stream_key: str, last_id: str = "0") -> list[StreamEntry]:
        """Raises TimeoutError if no new entries arrive before the read timeout elapses,
        or once ttl has elapsed since the first entry was written to this stream_key."""
        raise NotImplementedError


class RedisEventStream(EventStream):
    def __init__(self, client: redis.Redis, timeout: float = 3.0, ttl: float = 10.0) -> None:
        self._client = client
        self._timeout = timeout
        self._ttl = ttl

    def write(self, stream_key: str, data: dict[str, str]) -> None:
        self._client.xadd(stream_key, data)
        self._client.pexpire(stream_key, int(self._ttl * 1000), nx=True)

    async def read(self, stream_key: str, last_id: str = "0") -> list[StreamEntry]:
        timeout_ms = int(self._timeout * 1000)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self._client.xread({stream_key: last_id}, block=timeout_ms),
        )
        if not result:
            raise TimeoutError()
        _, entries = result[0]
        return [
            StreamEntry(
                id=entry_id.decode(),
                data={k.decode(): v.decode() for k, v in fields.items()},
            )
            for entry_id, fields in entries
        ]
