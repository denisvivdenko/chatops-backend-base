import logging
import threading

from chatops.jobs.job_stream import ConsumeTimeout
from chatops.jobs.result_stream import ResultStream, JobResult
from chatops.services.chat_service import ChatService

logger = logging.getLogger(__name__)


class ResultConsumer:
    def __init__(self, result_stream: ResultStream, chat_service: ChatService) -> None:
        self._results = result_stream
        self._service = chat_service
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> "ResultConsumer":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join()

    def _run(self) -> None:
        logger.info("ResultConsumer started, waiting for results")
        while not self._stop.is_set():
            try:
                self._process(self._results.consume())
            except ConsumeTimeout:
                pass

    def _process(self, result: JobResult) -> None:
        logger.info("Received result chat_id=%s message_id=%s", result.chat_id, result.message_id)
        self._service.complete_message(result.chat_id, result.message_id, result.content)
