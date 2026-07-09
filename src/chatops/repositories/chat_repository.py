from abc import ABC, abstractmethod

import pymongo

from chatops.domain.chat import Chat, Message


class ChatRepository(ABC):
    @abstractmethod
    def save_chat(self, chat: Chat) -> None: ...

    @abstractmethod
    def fetch_chat(self, chat_id: str) -> Chat: ...

    @abstractmethod
    def fetch_chats(self, user_id: str, limit: int | None = None) -> list[Chat]: ...

    @abstractmethod
    def save_message(self, chat_id: str, message: Message) -> None: ...

    @abstractmethod
    def fetch_messages(self, chat_id: str) -> list[Message]: ...

    @abstractmethod
    def delete_message(self, chat_id: str, message_id: str) -> None: ...

    @abstractmethod
    def delete_chat(self, chat_id: str) -> None: ...


class MongoChatRepository(ChatRepository):
    def __init__(self, client: pymongo.MongoClient, db_name: str = "chatops") -> None:
        db = client[db_name]
        self._chats = db["chats"]
        self._messages = db["messages"]
        self._messages.create_index("message_id", unique=True)
        self._messages.create_index([("chat_id", pymongo.ASCENDING), ("_id", pymongo.ASCENDING)])

    def save_chat(self, chat: Chat) -> None:
        doc = chat.model_dump(exclude={"id"})
        self._chats.replace_one({"_id": chat.id}, doc, upsert=True)

    def fetch_chat(self, chat_id: str) -> Chat:
        doc = self._chats.find_one({"_id": chat_id})
        if doc is None:
            raise KeyError(chat_id)
        return self._chat_from_doc(doc)

    def fetch_chats(self, user_id: str, limit: int | None = None) -> list[Chat]:
        cursor = self._chats.find({"user_id": user_id}).sort("last_activity_at", pymongo.DESCENDING)
        if limit is not None:
            cursor = cursor.limit(limit)
        return [self._chat_from_doc(doc) for doc in cursor]

    @staticmethod
    def _chat_from_doc(doc: dict) -> Chat:
        return Chat(
            id=doc["_id"],
            user_id=doc["user_id"],
            title=doc["title"],
            last_activity_at=doc["last_activity_at"],
            created_at=doc["created_at"],
        )

    def delete_chat(self, chat_id: str) -> None:
        self._chats.delete_one({"_id": chat_id})
        self._messages.delete_many({"chat_id": chat_id})

    def delete_message(self, chat_id: str, message_id: str) -> None:
        self._messages.delete_one({"chat_id": chat_id, "message_id": message_id})

    def save_message(self, chat_id: str, message: Message) -> None:
        doc = {**message.model_dump(exclude={"id"}), "chat_id": chat_id, "message_id": message.id}
        self._messages.replace_one({"message_id": message.id}, doc, upsert=True)

    def fetch_messages(self, chat_id: str) -> list[Message]:
        cursor = self._messages.find({"chat_id": chat_id}).sort("_id", pymongo.ASCENDING)
        return [
            Message(id=doc["message_id"], role=doc["role"], status=doc["status"], content=doc["content"], created_at=doc["created_at"])
            for doc in cursor
        ]
