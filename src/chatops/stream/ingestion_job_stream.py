import json
from abc import ABC, abstractmethod
from typing import NamedTuple

import redis


class IngestionJob(NamedTuple):
    chat_id: str
    user_id: str
    message_id: str
    resource_ids: tuple[str, ...]


class IngestionJobStream(ABC):
    @abstractmethod
    def publish(self, job: IngestionJob) -> None: ...

    @abstractmethod
    def consume(self) -> IngestionJob:
        """Raises TimeoutError if no job is available before the timeout elapses."""
        ...


REDIS_INGESTION_JOBS_KEY = "ingestion_jobs"


class RedisIngestionJobStream(IngestionJobStream):
    def __init__(self, client: redis.Redis, timeout: float = 1.0) -> None:
        self._client = client
        self._timeout = timeout

    def publish(self, job: IngestionJob) -> None:
        self._client.lpush(REDIS_INGESTION_JOBS_KEY, json.dumps(job._asdict()))

    def consume(self) -> IngestionJob:
        result = self._client.brpop(REDIS_INGESTION_JOBS_KEY, timeout=self._timeout)
        if result is None:
            raise TimeoutError()
        _, value = result
        data = json.loads(value)
        data["resource_ids"] = tuple(data["resource_ids"])
        return IngestionJob(**data)
