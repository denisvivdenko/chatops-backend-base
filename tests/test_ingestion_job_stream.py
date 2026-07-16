import os
from typing import Iterator

import pytest
import redis as redis_lib

from chatops.stream.ingestion_job_stream import IngestionJob, RedisIngestionJobStream


@pytest.fixture(params=[
    pytest.param("redis", marks=pytest.mark.integration),
])
def ingestion_job_stream(request: pytest.FixtureRequest) -> Iterator[RedisIngestionJobStream]:
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    client = redis_lib.Redis(host=redis_host, port=6379, db=1)
    client.flushdb()

    yield RedisIngestionJobStream(client, timeout=0.1)

    client.flushdb()
    client.close()


def test_publish_then_consume_round_trips_fields(ingestion_job_stream: RedisIngestionJobStream) -> None:
    job = IngestionJob(chat_id="chat-1", user_id="user-1", message_id="msg-1", resource_ids=("r1", "r2"))
    ingestion_job_stream.publish(job)

    consumed = ingestion_job_stream.consume()

    assert consumed == job


def test_consume_raises_timeout_error_when_empty(ingestion_job_stream: RedisIngestionJobStream) -> None:
    with pytest.raises(TimeoutError):
        ingestion_job_stream.consume()
