from unittest.mock import MagicMock

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus
from chatops.repositories.chat_repository import ChatRepository
from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceService
from chatops.stream.event_stream import EventStream
from chatops.stream.job_stream import Job, JobStream
from chatops.workers.response_generator import ResponseGenerator
from chatops.workers.worker import Worker

USER_ID = "test-user"
CHAT_ID = "chat-1"
MESSAGE_ID = "msg-1"


def _consume_once_then_stall(job: Job):
    yield job
    while True:
        yield TimeoutError()


def test_worker_survives_message_marked_failed_mid_generation() -> None:
    chat = Chat(id=CHAT_ID, user_id=USER_ID, title="Hello", last_activity_at=1, created_at=1)
    state = {
        "message": Message(
            id=MESSAGE_ID, role=MessageRole.ASSISTANT, status=MessageStatus.PENDING, content="", created_at=1,
        ),
    }

    repo = MagicMock(spec=ChatRepository)
    repo.fetch_chat.return_value = chat
    repo.fetch_messages.side_effect = lambda _chat_id: [state["message"]]
    repo.save_message.side_effect = lambda _chat_id, message: state.update(message=message)

    chat_service = ChatService(chat_repository=repo, resource_service=MagicMock(spec=ResourceService))
    chat_service.fail_message = MagicMock(wraps=chat_service.fail_message)

    def _fail_concurrently_then_yield(_job):
        # simulates fail_stale_pending_messages marking the message FAILED via
        # another request's timeout check, while this worker is still generating
        repo.save_message(CHAT_ID, state["message"].model_copy(update={"status": MessageStatus.FAILED}))
        return iter(["partial response"])

    response_generator = MagicMock(spec=ResponseGenerator)
    response_generator.generate.side_effect = _fail_concurrently_then_yield

    job = Job(chat_id=CHAT_ID, user_id=USER_ID, message_id=MESSAGE_ID)
    jobs_stream = MagicMock(spec=JobStream)
    jobs_stream.consume.side_effect = _consume_once_then_stall(job)

    worker = Worker(
        jobs_stream=jobs_stream,
        chat_service=chat_service,
        event_stream=MagicMock(spec=EventStream),
        response_generator=response_generator,
    ).start()

    try:
        worker._thread.join(timeout=0.2)
        chat_service.fail_message.assert_called_once_with(CHAT_ID, USER_ID, MESSAGE_ID)
        assert worker._thread.is_alive()
        assert state["message"].status == MessageStatus.FAILED
    finally:
        worker.stop()
