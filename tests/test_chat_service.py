import time

from chatops.services.chat_service import ChatService
from chatops.domain.chat import MessageRole, MessageStatus


def test_created_chats_appear_on_top_sorted_by_last_activity() -> None:
    service = ChatService()

    first_chat = service.create_chat("First message")
    time.sleep(1)
    second_chat = service.create_chat("Second message")

    chats = service.fetch_chats(limit=10)
    assert len(chats) == 2
    assert chats[0].id == second_chat.id
    assert chats[1].id == first_chat.id

    chats_limited = service.fetch_chats(limit=1)
    assert len(chats_limited) == 1
    assert chats_limited[0].id == second_chat.id


def test_create_chat_produces_user_message_followed_by_pending_assistant_message() -> None:
    service = ChatService()

    chat = service.create_chat("Hello")
    messages = service.fetch_messages(chat.id)

    assert len(messages) == 2

    user_message = messages[0]
    assert user_message.role == MessageRole.USER
    assert user_message.status == MessageStatus.COMPLETE
    assert user_message.content == "Hello"

    assistant_message = messages[1]
    assert assistant_message.role == MessageRole.ASSISTANT
    assert assistant_message.status == MessageStatus.PENDING
    assert assistant_message.content == ""
