import logging
import threading

from chatops.domain.chat import EOM, MessageStatus
from chatops.services.chat_service import (
    ChatAccessDeniedError,
    ChatNotFoundError,
    ChatService,
    MessageNotFoundError,
)
from chatops.stream.event_stream import EventStream
from chatops.stream.ingestion_job_stream import IngestionJob, IngestionJobStream

logger = logging.getLogger(__name__)

DOCUMENT_PROCESSED_RESPONSE = "Document processed"


class IngestionWorker:
    def __init__(
        self,
        ingestion_jobs: IngestionJobStream,
        chat_service: ChatService,
        event_stream: EventStream,
    ) -> None:
        self._jobs = ingestion_jobs
        self._service = chat_service
        self._event_stream = event_stream
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> "IngestionWorker":
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
        logger.info("Ingestion worker started, waiting for jobs")
        while not self._stop.is_set():
            try:
                self._process(self._jobs.consume())
            except TimeoutError:
                pass

    def _process(self, job: IngestionJob) -> None:
        logger.info("Received ingestion job chat_id=%s message_id=%s", job.chat_id, job.message_id)
        try:
            message = self._service.get_message(job.chat_id, job.user_id, job.message_id)
        except (MessageNotFoundError, ChatAccessDeniedError, ChatNotFoundError):
            logger.warning(
                "Discarding ingestion job chat_id=%s message_id=%s: message not found", job.chat_id, job.message_id,
            )
            return
        if message.status != MessageStatus.PENDING:
            logger.warning(
                "Discarding ingestion job chat_id=%s message_id=%s: status=%s",
                job.chat_id, job.message_id, message.status,
            )
            return
        try:
            # import time
            # time.sleep(15)
            stream_key = self._event_stream.stream_key(job.chat_id, job.message_id)
            response = DOCUMENT_PROCESSED_RESPONSE
            self._event_stream.write(stream_key, {"token": response})
            self._service.complete_message(job.chat_id, job.user_id, job.message_id, response)
            self._event_stream.write(stream_key, {"token": EOM})
            logger.info("Finished ingestion job chat_id=%s message_id=%s", job.chat_id, job.message_id)
        except Exception:
            logger.exception("Failed ingestion job chat_id=%s message_id=%s", job.chat_id, job.message_id)
            self._service.fail_message(job.chat_id, job.user_id, job.message_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from chatops.api.dependencies import (
        get_chat_repository,
        get_event_stream,
        get_ingestion_job_stream,
        get_resource_repository,
        get_resource_service,
        get_resource_storage,
    )

    chat_service = ChatService(
        chat_repository=get_chat_repository(),
        resource_service=get_resource_service(repo=get_resource_repository(), storage=get_resource_storage()),
    )

    IngestionWorker(
        ingestion_jobs=get_ingestion_job_stream(),
        chat_service=chat_service,
        event_stream=get_event_stream(),
    ).start().join()
