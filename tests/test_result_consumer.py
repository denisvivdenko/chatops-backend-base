import threading
from unittest.mock import MagicMock

from chatops.consumers.result_consumer import ResultConsumer
from chatops.jobs.result_stream import InMemoryResultStream, JobResult
from chatops.services.chat_service import ChatService


def test_result_consumer_calls_complete_message() -> None:
    service = MagicMock(spec=ChatService)
    done = threading.Event()
    service.complete_message.side_effect = lambda *_: done.set()

    result_stream = InMemoryResultStream()
    ResultConsumer(result_stream=result_stream, chat_service=service).start()
    result_stream.publish(JobResult(chat_id="chat-1", message_id="msg-1", content="Done"))

    assert done.wait(timeout=1)
    service.complete_message.assert_called_once_with("chat-1", "msg-1", "Done")
