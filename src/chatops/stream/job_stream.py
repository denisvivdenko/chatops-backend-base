import json
from abc import ABC, abstractmethod
from typing import NamedTuple

import redis


class Job(NamedTuple):
    chat_id: str
    user_id: str
    message_id: str
    resource_ids: tuple[str, ...] = ()


class JobStream(ABC):
    @abstractmethod
    def publish(self, job: Job) -> None: ...

    @abstractmethod
    def consume(self) -> Job:
        """Raises TimeoutError if no job is available before the timeout elapses."""
        ...


REDIS_JOBS_KEY = "jobs"
REDIS_INGESTION_JOBS_KEY = "ingestion_jobs"


class RedisJobStream(JobStream):
    def __init__(self, client: redis.Redis, redis_key: str = REDIS_JOBS_KEY, timeout: float = 1.0) -> None:
        self._client = client
        self._redis_key = redis_key
        self._timeout = timeout

    def publish(self, job: Job) -> None:
        self._client.lpush(self._redis_key, json.dumps(job._asdict()))

    def consume(self) -> Job:
        result = self._client.brpop(self._redis_key, timeout=self._timeout)
        if result is None:
            raise TimeoutError()
        _, value = result
        data = json.loads(value)
        data["resource_ids"] = tuple(data["resource_ids"])
        return Job(**data)
