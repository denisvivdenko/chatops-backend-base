import logging
import threading

from chatops.domain.chat import MessageStatus
from chatops.jobs.result_stream import ResultStream, JobResult
from chatops.repositories.chat_repository import ChatRepository

logger = logging.getLogger(__name__)


class ResultConsumer:
    def __init__(self, result_stream: ResultStream, chat_repository: ChatRepository) -> None:
        self._results = result_stream
        self._repo = chat_repository

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
        for message in self._repo.fetch_messages(result.chat_id):
            if message.id == result.message_id:
                updated = message.model_copy(update={"status": MessageStatus.COMPLETE, "content": result.content})
                self._repo.save_message(result.chat_id, updated)
                logger.info("Saved result chat_id=%s message_id=%s", result.chat_id, result.message_id)
                return
        logger.warning("Message not found chat_id=%s message_id=%s", result.chat_id, result.message_id)
