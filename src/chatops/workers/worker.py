import threading

from chatops.domain.chat import MessageStatus
from chatops.repositories.chat_repository import ChatRepository
from chatops.jobs.job_stream import JobStream, AssistantJob

HARDCODED_RESPONSE = "Hello! I'm an AI assistant. How can I help you?"


class Worker:
    def __init__(self, chat_repository: ChatRepository, jobs_stream: JobStream) -> None:
        self._repo = chat_repository
        self._jobs = jobs_stream

    def start(self) -> None:
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    def _run(self) -> None:
        while True:
            job = self._jobs.consume()
            if job is not None:
                self._process(job)

    def _process(self, job: AssistantJob) -> None:
        for message in self._repo.fetch_messages(job.chat_id):
            if message.id == job.message_id:
                updated = message.model_copy(update={"status": MessageStatus.COMPLETE, "content": HARDCODED_RESPONSE})
                self._repo.save_message(job.chat_id, updated)
                return
