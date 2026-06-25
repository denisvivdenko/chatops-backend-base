import logging
import threading

from chatops.jobs.result_stream import ResultStream, JobResult
from chatops.services.chat_service import ChatService

logger = logging.getLogger(__name__)


class ResultConsumer:
    def __init__(self, result_stream: ResultStream, chat_service: ChatService) -> None:
        self._results = result_stream
        self._service = chat_service

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()
        return thread

    def _run(self) -> None:
        logger.info("ResultConsumer started, waiting for results")
        while True:
            self._process(self._results.consume())

    def _process(self, result: JobResult) -> None:
        logger.info("Received result chat_id=%s message_id=%s", result.chat_id, result.message_id)
        self._service.complete_message(result.chat_id, result.message_id, result.content)
