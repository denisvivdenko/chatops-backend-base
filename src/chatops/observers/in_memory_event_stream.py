import asyncio
import threading
import time

from chatops.observers.event_stream import EventStream, StreamEntry, StreamNotFoundError


class InMemoryEventStream(EventStream):
    def __init__(self, timeout: float = 3.0) -> None:
        self._entries: dict[str, list[StreamEntry]] = {}
        self._lock = threading.Lock()
        self._timeout = timeout

    def write(self, stream_key: str, data: dict[str, str]) -> None:
        with self._lock:
            entries = self._entries.setdefault(stream_key, [])
            entries.append(StreamEntry(id=str(len(entries)), data=data))

    async def read(self, stream_key: str, last_id: str | None = None) -> list[StreamEntry]:
        deadline = time.monotonic() + self._timeout
        while True:
            with self._lock:
                new_entries = self._entries_since(self._entries.get(stream_key, []), last_id)
                if new_entries:
                    return new_entries

            if time.monotonic() >= deadline:
                raise StreamNotFoundError(stream_key)

            await asyncio.sleep(0.05)

    def _entries_since(self, entries: list[StreamEntry], last_id: str | None) -> list[StreamEntry]:
        if last_id is None:
            return list(entries)
        for i, entry in enumerate(entries):
            if entry.id == last_id:
                return list(entries[i + 1:])
        return []
