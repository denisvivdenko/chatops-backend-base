import json
import queue
from abc import ABC, abstractmethod
from typing import NamedTuple

import redis

from chatops.jobs.job_stream import ConsumeTimeout


class JobResult(NamedTuple):
    chat_id: str
    message_id: str
    content: str


class ResultStream(ABC):
    @abstractmethod
    def publish(self, result: JobResult) -> None: ...

    @abstractmethod
    def consume(self) -> JobResult: ...


class InMemoryResultStream(ResultStream):
    def __init__(self) -> None:
        self._queue: queue.Queue[JobResult] = queue.Queue()

    def publish(self, result: JobResult) -> None:
        self._queue.put(result)

    def consume(self) -> JobResult:
        try:
            return self._queue.get(timeout=1)
        except queue.Empty:
            raise ConsumeTimeout()


REDIS_RESULTS_KEY = "results"


class RedisResultStream(ResultStream):
    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    def publish(self, result: JobResult) -> None:
        self._client.lpush(REDIS_RESULTS_KEY, json.dumps(result._asdict()))

    def consume(self) -> JobResult:
        result = self._client.brpop(REDIS_RESULTS_KEY, timeout=1)
        if result is None:
            raise ConsumeTimeout()
        _, value = result
        return JobResult(**json.loads(value))
