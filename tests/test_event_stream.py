import os
import time
from functools import partial
from typing import Callable, Iterator

import pytest
import redis as redis_lib

from chatops.stream.event_stream import EventStream, InMemoryEventStream, RedisEventStream

StreamFactory = Callable[..., EventStream]


@pytest.fixture(params=[
    "in_memory",
    pytest.param("redis", marks=pytest.mark.integration),
])
def make_stream(request: pytest.FixtureRequest) -> Iterator[StreamFactory]:
    if request.param == "in_memory":
        yield InMemoryEventStream
        return

    redis_host = os.environ.get("REDIS_HOST", "localhost")
    client = redis_lib.Redis(host=redis_host, port=6379, db=1)
    client.flushdb()

    yield partial(RedisEventStream, client)

    client.flushdb()
    client.close()


@pytest.mark.asyncio
async def test_stream_expires_after_generation_timeout(make_stream: StreamFactory) -> None:
    stream = make_stream(timeout=0.05, ttl=0.15)
    key = stream.stream_key("chat-1", "msg-1")

    stream.write(key, {"token": "Hi"})
    entries = await stream.read(key, last_id="0")
    assert [e.data["token"] for e in entries] == ["Hi"]

    time.sleep(0.2)

    with pytest.raises(TimeoutError):
        await stream.read(key, last_id="0")
