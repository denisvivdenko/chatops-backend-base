import threading
import time

from chatops.domain.chat import EOM, MessageStatus
from chatops.repositories.chat_repository import ChatRepository
from chatops.jobs.job_stream import JobStream, AssistantJob
from chatops.observers.in_memory_event_stream import InMemoryEventStream

HARDCODED_RESPONSE = """
## Markdown support

This response demonstrates **bold**, *italic*, and \`inline code\`.

### Lists

- Unordered items work fine
- As do nested concepts

1. Ordered lists too
2. With multiple entries

### Code blocks

\`\`\`ts
function greet(name: string): string {
  return \`Hello, \${name}!\`;
}
\`\`\`

> Blockquotes are also supported for callouts or citations.

---

Let me know what you'd like to explore next.`;
"""


class Worker:
    def __init__(self, chat_repository: ChatRepository, jobs_stream: JobStream, event_stream: InMemoryEventStream) -> None:
        self._repo = chat_repository
        self._jobs = jobs_stream
        self._event_stream = event_stream

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()
        return thread

    def _run(self) -> None:
        while True:
            self._process(self._jobs.consume())

    def _process(self, job: AssistantJob) -> None:
        stream_key = self._event_stream.stream_key(job.chat_id, job.message_id)
        chunk_size = 6
        for i in range(0, len(HARDCODED_RESPONSE), chunk_size):
            self._event_stream.write(stream_key, {"token": HARDCODED_RESPONSE[i:i + chunk_size]})
            time.sleep(0.1)
        self._event_stream.write(stream_key, {"token": EOM})

        for message in self._repo.fetch_messages(job.chat_id):
            if message.id == job.message_id:
                updated = message.model_copy(update={"status": MessageStatus.COMPLETE, "content": HARDCODED_RESPONSE})
                self._repo.save_message(job.chat_id, updated)
                return


if __name__ == "__main__":
    from chatops.repositories.chat_repository import InMemoryChatRepository
    from chatops.jobs.job_stream import InMemoryJobStream
    from chatops.observers.in_memory_event_stream import InMemoryEventStream

    thread = Worker(
        chat_repository=InMemoryChatRepository(),
        jobs_stream=InMemoryJobStream(),
        event_stream=InMemoryEventStream(),
    ).start()
    thread.join()
