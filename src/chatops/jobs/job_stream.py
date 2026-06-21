import queue
from abc import ABC, abstractmethod
from typing import NamedTuple


class AssistantJob(NamedTuple):
    chat_id: str
    message_id: str


class JobStream(ABC):
    @abstractmethod
    def publish(self, job: AssistantJob) -> None: ...

    @abstractmethod
    def consume(self) -> AssistantJob | None: ...


class InMemoryJobStream(JobStream):
    def __init__(self) -> None:
        self._queue: queue.Queue[AssistantJob] = queue.Queue()

    def publish(self, job: AssistantJob) -> None:
        self._queue.put(job)

    def consume(self) -> AssistantJob | None:
        try:
            return self._queue.get(block=True, timeout=0.1)
        except queue.Empty:
            return None
