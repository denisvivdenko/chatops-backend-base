import time
from functools import partial
from typing import Callable

import pytest
import redis as redis_lib

from chatops.stream.event_stream import EventStream, RedisEventStream

StreamFactory = Callable[..., EventStream]


@pytest.fixture
def make_stream(redis_client: redis_lib.Redis) -> StreamFactory:
    return partial(RedisEventStream, redis_client)


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
