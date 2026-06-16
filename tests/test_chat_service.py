import time

from chatops.services.chat_service import ChatService


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
