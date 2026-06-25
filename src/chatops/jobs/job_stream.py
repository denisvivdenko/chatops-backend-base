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
    def consume(self) -> AssistantJob: ...


class InMemoryJobStream(JobStream):
    def __init__(self) -> None:
        self._queue: queue.Queue[AssistantJob] = queue.Queue()

    def publish(self, job: AssistantJob) -> None:
        self._queue.put(job)

    def consume(self) -> AssistantJob:
        return self._queue.get(block=True)


REDIS_JOBS_KEY = "jobs"


class RedisJobStream(JobStream):
    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    def publish(self, job: AssistantJob) -> None:
        self._client.lpush(REDIS_JOBS_KEY, json.dumps(job._asdict()))

    def consume(self) -> AssistantJob:
        _, value = self._client.brpop(REDIS_JOBS_KEY, timeout=0)
        return AssistantJob(**json.loads(value))
