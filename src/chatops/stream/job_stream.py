import json
import queue
from abc import ABC, abstractmethod
from typing import NamedTuple

import redis


class AssistantJob(NamedTuple):
    chat_id: str
    message_id: str


class JobStream(ABC):
    @abstractmethod
    def publish(self, job: AssistantJob) -> None: ...

    @abstractmethod
    def consume(self) -> AssistantJob:
        """Raises TimeoutError if no job is available before the timeout elapses."""
        ...


class InMemoryJobStream(JobStream):
    def __init__(self, timeout: float = 1.0) -> None:
        self._queue: queue.Queue[AssistantJob] = queue.Queue()
        self._timeout = timeout

    def publish(self, job: AssistantJob) -> None:
        self._queue.put(job)

    def consume(self) -> AssistantJob:
        try:
            return self._queue.get(timeout=self._timeout)
        except queue.Empty:
            raise TimeoutError()


REDIS_JOBS_KEY = "jobs"


class RedisJobStream(JobStream):
    def __init__(self, client: redis.Redis, timeout: float = 1.0) -> None:
        self._client = client
        self._timeout = timeout

    def publish(self, job: AssistantJob) -> None:
        self._client.lpush(REDIS_JOBS_KEY, json.dumps(job._asdict()))

    def consume(self) -> AssistantJob:
        result = self._client.brpop(REDIS_JOBS_KEY, timeout=self._timeout)
        if result is None:
            raise TimeoutError()
        _, value = result
        return AssistantJob(**json.loads(value))
