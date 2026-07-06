from abc import ABC, abstractmethod

import pymongo

from chatops.domain.user import User


class UserRepository(ABC):
    @abstractmethod
    def save_user(self, user: User) -> None: ...

    @abstractmethod
    def fetch_user(self, user_id: str) -> User: ...


class MongoUserRepository(UserRepository):
    def __init__(self, client: pymongo.MongoClient, db_name: str = "chatops") -> None:
        db = client[db_name]
        self._users = db["users"]

    def save_user(self, user: User) -> None:
        doc = user.model_dump(exclude={"id"})
        self._users.replace_one({"_id": user.id}, doc, upsert=True)

    def fetch_user(self, user_id: str) -> User:
        doc = self._users.find_one({"_id": user_id})
        if doc is None:
            raise KeyError(user_id)
        return User(
            id=doc["_id"],
            email=doc["email"],
            password_hash=doc["password_hash"],
            is_anonymous=doc["is_anonymous"],
            created_at=doc["created_at"],
        )
