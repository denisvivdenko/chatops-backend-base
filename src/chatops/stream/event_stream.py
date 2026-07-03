import asyncio
import threading
import time
from abc import ABC, abstractmethod
from typing import NamedTuple

import redis


class StreamEntry(NamedTuple):
    id: str
    data: dict[str, str]


class StreamTimeoutError(TimeoutError):
    pass


class EventStream(ABC):
    @staticmethod
    def stream_key(chat_id: str, message_id: str) -> str:
        return f"{chat_id}:{message_id}"

    @abstractmethod
    def write(self, stream_key: str, data: dict[str, str]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def read(self, stream_key: str, last_id: str = "0") -> list[StreamEntry]:
        """Raises StreamTimeoutError if no new entries arrive before the timeout elapses."""
        raise NotImplementedError


class InMemoryEventStream(EventStream):
    def __init__(self, timeout: float = 3.0) -> None:
        self._entries: dict[str, list[StreamEntry]] = {}
        self._lock = threading.Lock()
        self._timeout = timeout

    def write(self, stream_key: str, data: dict[str, str]) -> None:
        with self._lock:
            entries = self._entries.setdefault(stream_key, [])
            entries.append(StreamEntry(id=str(len(entries) + 1), data=data))

    async def read(self, stream_key: str, last_id: str = "0") -> list[StreamEntry]:
        deadline = time.monotonic() + self._timeout
        while True:
            with self._lock:
                new_entries = self._entries_since(self._entries.get(stream_key, []), last_id)
                if new_entries:
                    return new_entries

            if time.monotonic() >= deadline:
                raise StreamTimeoutError(stream_key)

            await asyncio.sleep(0.05)

    def _entries_since(self, entries: list[StreamEntry], last_id: str) -> list[StreamEntry]:
        if last_id == "0":
            return list(entries)
        for i, entry in enumerate(entries):
            if entry.id == last_id:
                return list(entries[i + 1:])
        return []


class RedisEventStream(EventStream):
    def __init__(self, client: redis.Redis, timeout: float = 3.0) -> None:
        self._client = client
        self._timeout = timeout

    def write(self, stream_key: str, data: dict[str, str]) -> None:
        self._client.xadd(stream_key, data)

    async def read(self, stream_key: str, last_id: str = "0") -> list[StreamEntry]:
        timeout_ms = int(self._timeout * 1000)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self._client.xread({stream_key: last_id}, block=timeout_ms),
        )
        if not result:
            raise StreamTimeoutError(stream_key)
        _, entries = result[0]
        return [
            StreamEntry(
                id=entry_id.decode(),
                data={k.decode(): v.decode() for k, v in fields.items()},
            )
            for entry_id, fields in entries
        ]
