import os
from typing import Iterator

import pytest
import redis as redis_lib

from chatops.stream.job_stream import REDIS_INGESTION_JOBS_KEY, Job, RedisJobStream


@pytest.fixture(params=[
    pytest.param("redis", marks=pytest.mark.integration),
])
def job_stream(request: pytest.FixtureRequest) -> Iterator[RedisJobStream]:
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    client = redis_lib.Redis(host=redis_host, port=6379, db=1)
    client.flushdb()

    yield RedisJobStream(client, redis_key=REDIS_INGESTION_JOBS_KEY, timeout=0.1)

    client.flushdb()
    client.close()


def test_publish_then_consume_round_trips_fields(job_stream: RedisJobStream) -> None:
    job = Job(chat_id="chat-1", user_id="user-1", message_id="msg-1", resource_ids=("r1", "r2"))
    job_stream.publish(job)

    consumed = job_stream.consume()

    assert consumed == job


def test_publish_then_consume_defaults_resource_ids_to_empty_tuple(job_stream: RedisJobStream) -> None:
    job = Job(chat_id="chat-1", user_id="user-1", message_id="msg-1")
    job_stream.publish(job)

    consumed = job_stream.consume()

    assert consumed == job
    assert consumed.resource_ids == ()


def test_consume_raises_timeout_error_when_empty(job_stream: RedisJobStream) -> None:
    with pytest.raises(TimeoutError):
        job_stream.consume()
