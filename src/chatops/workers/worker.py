import logging
import threading
import time

from chatops.domain.chat import EOM
from chatops.stream.job_stream import JobStream, AssistantJob
from chatops.stream.event_stream import EventStream
from chatops.services.chat_service import ChatService

HARDCODED_RESPONSE = """
## Markdown support

This response demonstrates **bold**, *italic*, and `inline code`.

### Lists

- Unordered items work fine
- As do nested concepts

1. Ordered lists too
2. With multiple entries

### Code blocks

```ts
function greet(name: string): string {
  return `Hello, ${name}!`;
}
```

> Blockquotes are also supported for callouts or citations.

---

Let me know what you'd like to explore next.`;
"""


TEST_RESPONSE = HARDCODED_RESPONSE[:12]

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        jobs_stream: JobStream,
        chat_service: ChatService,
        event_stream: EventStream,
        response: str = HARDCODED_RESPONSE,
    ) -> None:
        self._jobs = jobs_stream
        self._service = chat_service
        self._event_stream = event_stream
        self._response = response
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> "Worker":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join()

    def join(self) -> None:
        if self._thread:
            self._thread.join()

    def _run(self) -> None:
        logger.info("Worker started, waiting for jobs")
        while not self._stop.is_set():
            try:
                self._process(self._jobs.consume())
            except TimeoutError:
                pass

    def _process(self, job: AssistantJob) -> None:
        logger.info("Received job chat_id=%s message_id=%s", job.chat_id, job.message_id)
        try:
            stream_key = self._event_stream.stream_key(job.chat_id, job.message_id)
            chunk_size = 6
            for i in range(0, len(self._response), chunk_size):
                self._event_stream.write(stream_key, {"token": self._response[i:i + chunk_size]})
                time.sleep(0.1)
            self._service.complete_message(job.chat_id, job.message_id, self._response)
            self._event_stream.write(stream_key, {"token": EOM})
            logger.info("Finished job chat_id=%s message_id=%s", job.chat_id, job.message_id)
        except Exception:
            logger.exception("Failed job chat_id=%s message_id=%s", job.chat_id, job.message_id)
            self._service.fail_message(job.chat_id, job.message_id)


if __name__ == "__main__":
    import os
    import pymongo
    import redis

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from chatops.stream.job_stream import RedisJobStream
    from chatops.stream.event_stream import RedisEventStream
    from chatops.repositories.chat_repository import MongoChatRepository

    redis_client = redis.Redis(host=os.environ["REDIS_HOST"], port=6379, socket_timeout=None)
    mongo_client = pymongo.MongoClient(os.environ["MONGO_HOST"], 27017)
    job_stream = RedisJobStream(redis_client)
    chat_service = ChatService(chat_repository=MongoChatRepository(mongo_client), jobs_stream=job_stream)

    Worker(
        jobs_stream=job_stream,
        chat_service=chat_service,
        event_stream=RedisEventStream(redis_client),
    ).start().join()
