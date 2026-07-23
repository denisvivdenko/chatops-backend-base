import time
from unittest.mock import MagicMock

from chatops.domain.chat import Chat, Message, MessageRole, MessageStatus
from chatops.repositories.chat_repository import ChatRepository
from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceService

USER_ID = "test-user"
CHAT_ID = "chat-1"


def test_fail_message_marks_assistant_message_as_failed() -> None:
    repo = MagicMock(spec=ChatRepository)
    repo.fetch_chat.return_value = Chat(
        id=CHAT_ID, user_id=USER_ID, title="Hello", last_activity_at=1, created_at=1,
    )
    assistant_message = Message(
        id="msg-1",
        role=MessageRole.ASSISTANT,
        status=MessageStatus.PENDING,
        content="",
        created_at=int(time.time() * 1000),
    )
    repo.fetch_messages.return_value = [assistant_message]
    service = ChatService(chat_repository=repo, resource_service=MagicMock(spec=ResourceService))

    service.fail_message(CHAT_ID, USER_ID, assistant_message.id)

    repo.save_message.assert_called_once_with(
        CHAT_ID, assistant_message.model_copy(update={"status": MessageStatus.FAILED}),
    )
