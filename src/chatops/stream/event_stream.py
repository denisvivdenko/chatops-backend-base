import asyncio
import threading
import time
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


class InMemoryEventStream(EventStream):
    def __init__(self, timeout: float = 3.0, ttl: float = 10.0) -> None:
        self._entries: dict[str, list[StreamEntry]] = {}
        self._started_at: dict[str, float] = {}
        self._lock = threading.Lock()
        self._timeout = timeout
        self._ttl = ttl

    def write(self, stream_key: str, data: dict[str, str]) -> None:
        with self._lock:
            self._started_at.setdefault(stream_key, time.time())
            entries = self._entries.setdefault(stream_key, [])
            entries.append(StreamEntry(id=str(len(entries) + 1), data=data))

    async def read(self, stream_key: str, last_id: str = "0") -> list[StreamEntry]:
        with self._lock:
            started_at = self._started_at.setdefault(stream_key, time.time())

        self._expire_if_due(stream_key, started_at)

        deadline = time.monotonic() + self._timeout
        while True:
            with self._lock:
                new_entries = self._entries_since(self._entries.get(stream_key, []), last_id)
                if new_entries:
                    return new_entries

            self._expire_if_due(stream_key, started_at)
            if time.monotonic() >= deadline:
                raise TimeoutError()

            await asyncio.sleep(0.05)

    def _expire_if_due(self, stream_key: str, started_at: float) -> None:
        if time.time() - started_at < self._ttl:
            return
        with self._lock:
            self._entries.pop(stream_key, None)
            self._started_at.pop(stream_key, None)
        raise TimeoutError()

    def _entries_since(self, entries: list[StreamEntry], last_id: str) -> list[StreamEntry]:
        if last_id == "0":
            return list(entries)
        for i, entry in enumerate(entries):
            if entry.id == last_id:
                return list(entries[i + 1:])
        return []


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
