import logging
import threading
import time

from chatops.domain.chat import EOM
from chatops.jobs.job_stream import JobStream, AssistantJob, ConsumeTimeout
from chatops.jobs.result_stream import ResultStream, JobResult
from chatops.observers.in_memory_event_stream import InMemoryEventStream

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
"""[:10]


logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, jobs_stream: JobStream, result_stream: ResultStream, event_stream: InMemoryEventStream) -> None:
        self._jobs = jobs_stream
        self._results = result_stream
        self._event_stream = event_stream
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

    def _run(self) -> None:
        logger.info("Worker started, waiting for jobs")
        while not self._stop.is_set():
            try:
                self._process(self._jobs.consume())
            except ConsumeTimeout:
                pass

    def _process(self, job: AssistantJob) -> None:
        logger.info("Received job chat_id=%s message_id=%s", job.chat_id, job.message_id)
        stream_key = self._event_stream.stream_key(job.chat_id, job.message_id)
        chunk_size = 6
        for i in range(0, len(HARDCODED_RESPONSE), chunk_size):
            self._event_stream.write(stream_key, {"token": HARDCODED_RESPONSE[i:i + chunk_size]})
            time.sleep(0.1)
        self._event_stream.write(stream_key, {"token": EOM})
        self._results.publish(JobResult(chat_id=job.chat_id, message_id=job.message_id, content=HARDCODED_RESPONSE))
        logger.info("Finished job chat_id=%s message_id=%s", job.chat_id, job.message_id)


if __name__ == "__main__":
    import os
    import redis
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from chatops.jobs.job_stream import RedisJobStream
    from chatops.jobs.result_stream import RedisResultStream

    redis_client = redis.Redis(host=os.environ["REDIS_HOST"], port=6379, socket_timeout=None)

    thread = Worker(
        jobs_stream=RedisJobStream(redis_client),
        result_stream=RedisResultStream(redis_client),
        event_stream=InMemoryEventStream(),
    ).start()
    thread.join()
