import logging
import threading

from chatops.domain.chat import EOM, MessageStatus
from chatops.services.chat_service import ChatAccessDeniedError, ChatNotFoundError, ChatService, MessageNotFoundError
from chatops.stream.event_stream import EventStream
from chatops.stream.job_stream import Job, JobStream
from chatops.workers.response_generator import ResponseGenerator

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        jobs_stream: JobStream,
        chat_service: ChatService,
        event_stream: EventStream,
        response_generator: ResponseGenerator,
    ) -> None:
        self._jobs = jobs_stream
        self._service = chat_service
        self._event_stream = event_stream
        self._response_generator = response_generator
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

    def _process(self, job: Job) -> None:
        logger.info("Received job chat_id=%s message_id=%s", job.chat_id, job.message_id)
        try:
            message = self._service.get_message(job.chat_id, job.user_id, job.message_id)
        except (MessageNotFoundError, ChatAccessDeniedError, ChatNotFoundError):
            logger.warning("Discarding job chat_id=%s message_id=%s: message not found", job.chat_id, job.message_id)
            return
        if message.status != MessageStatus.PENDING:
            logger.warning(
                "Discarding job chat_id=%s message_id=%s: status=%s", job.chat_id, job.message_id, message.status,
            )
            return
        try:
            stream_key = self._event_stream.stream_key(job.chat_id, job.message_id)
            chunks: list[str] = []
            for chunk in self._response_generator.generate(job):
                chunks.append(chunk)
                self._event_stream.write(stream_key, {"token": chunk})
            self._service.complete_message(job.chat_id, job.user_id, job.message_id, "".join(chunks))
            self._event_stream.write(stream_key, {"token": EOM})
            logger.info("Finished job chat_id=%s message_id=%s", job.chat_id, job.message_id)
        except Exception:
            logger.exception("Failed job chat_id=%s message_id=%s", job.chat_id, job.message_id)
            self._service.fail_message(job.chat_id, job.user_id, job.message_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from chatops.api.dependencies import (
        get_chat_repository,
        get_event_stream,
        get_job_stream,
        get_openai_client,
        get_resource_repository,
        get_resource_service,
        get_resource_storage,
        get_settings,
    )
    from chatops.workers.llm_message_generator import LLMMessageGenerator

    chat_service = ChatService(
        chat_repository=get_chat_repository(),
        resource_service=get_resource_service(repo=get_resource_repository(), storage=get_resource_storage()),
    )

    Worker(
        jobs_stream=get_job_stream(),
        chat_service=chat_service,
        event_stream=get_event_stream(),
        response_generator=LLMMessageGenerator(
            chat_service=chat_service, client=get_openai_client(), model=get_settings().openai_model,
        ),
    ).start().join()
