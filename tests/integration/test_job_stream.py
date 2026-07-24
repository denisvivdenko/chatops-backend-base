import pytest
import redis as redis_lib

from chatops.stream.job_stream import REDIS_INGESTION_JOBS_KEY, Job, RedisJobStream


@pytest.fixture
def job_stream(redis_client: redis_lib.Redis) -> RedisJobStream:
    return RedisJobStream(redis_client, redis_key=REDIS_INGESTION_JOBS_KEY, timeout=0.1)


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
