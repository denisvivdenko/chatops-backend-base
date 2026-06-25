import threading
from unittest.mock import MagicMock

from chatops.consumers.result_consumer import ResultConsumer
from chatops.domain.chat import Message, MessageRole, MessageStatus
from chatops.jobs.result_stream import InMemoryResultStream, JobResult
from chatops.repositories.chat_repository import ChatRepository


def _make_message(message_id: str) -> Message:
    return Message(id=message_id, role=MessageRole.ASSISTANT, status=MessageStatus.PENDING, content="", created_at=0)


def test_result_consumer_marks_message_complete() -> None:
    repo = MagicMock(spec=ChatRepository)
    repo.fetch_messages.return_value = [_make_message("msg-1")]
    done = threading.Event()
    repo.save_message.side_effect = lambda *_: done.set()

    result_stream = InMemoryResultStream()
    ResultConsumer(result_stream=result_stream, chat_repository=repo).start()
    result_stream.publish(JobResult(chat_id="chat-1", message_id="msg-1", content="Done"))

    assert done.wait(timeout=1)
    repo.save_message.assert_called_once()
    _, saved = repo.save_message.call_args.args
    assert saved.status == MessageStatus.COMPLETE
    assert saved.content == "Done"


def test_result_consumer_ignores_unknown_message() -> None:
    repo = MagicMock(spec=ChatRepository)
    repo.fetch_messages.return_value = [_make_message("msg-1")]
    processed = threading.Event()
    repo.fetch_messages.side_effect = lambda *_: (processed.set(), [_make_message("msg-1")])[1]

    result_stream = InMemoryResultStream()
    ResultConsumer(result_stream=result_stream, chat_repository=repo).start()
    result_stream.publish(JobResult(chat_id="chat-1", message_id="nonexistent", content="Done"))

    assert processed.wait(timeout=1)
    repo.save_message.assert_not_called()
