from chatops.observers.event_stream import EventStream, StreamEntry, StreamNotFoundError


class RedisEventStream(EventStream):
    async def read(self, stream_key: str, last_id: str | None = None) -> list[StreamEntry]:
        raise NotImplementedError
