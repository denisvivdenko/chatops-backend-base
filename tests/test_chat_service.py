from chatops.services.chat_service import ChatService


def test_create_chat_returns_chat() -> None:
    service = ChatService()

    chat = service.create_chat("Hello")

    assert isinstance(chat.id, str)
    assert isinstance(chat.title, str)
    assert isinstance(chat.last_activity_at, int)
    assert isinstance(chat.created_at, int)


def test_fetch_chats_returns_list() -> None:
    service = ChatService()

    chats = service.fetch_chats(limit=10)

    assert isinstance(chats, list)
