import asyncio

import redis

from chatops.observers.event_stream import EventStream, StreamEntry, StreamNotFoundError


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
            raise StreamNotFoundError(stream_key)
        _, entries = result[0]
        return [
            StreamEntry(
                id=entry_id.decode(),
                data={k.decode(): v.decode() for k, v in fields.items()},
            )
            for entry_id, fields in entries
        ]
