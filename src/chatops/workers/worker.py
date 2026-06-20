import threading

from chatops.domain.chat import EOM, MessageStatus
from chatops.repositories.chat_repository import ChatRepository
from chatops.jobs.job_stream import JobStream, AssistantJob
from chatops.observers.in_memory_event_stream import InMemoryEventStream

HARDCODED_RESPONSE = "Hello! I'm an AI assistant. How can I help you?"


class Worker:
    def __init__(self, chat_repository: ChatRepository, jobs_stream: JobStream, event_stream: InMemoryEventStream) -> None:
        self._repo = chat_repository
        self._jobs = jobs_stream
        self._event_stream = event_stream

    def start(self) -> None:
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    def _run(self) -> None:
        while True:
            job = self._jobs.consume()
            if job is not None:
                self._process(job)

    def _process(self, job: AssistantJob) -> None:
        stream_key = f"{job.chat_id}:{job.message_id}"
        tokens = HARDCODED_RESPONSE.split()
        for t in tokens:
            self._event_stream.write(stream_key, {"token": t})
        self._event_stream.write(stream_key, {"token": EOM})

        for message in self._repo.fetch_messages(job.chat_id):
            if message.id == job.message_id:
                updated = message.model_copy(update={"status": MessageStatus.COMPLETE, "content": HARDCODED_RESPONSE})
                self._repo.save_message(job.chat_id, updated)
                return
