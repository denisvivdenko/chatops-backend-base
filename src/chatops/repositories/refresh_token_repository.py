from abc import ABC, abstractmethod

import pymongo

from chatops.domain.user import RefreshToken


class RefreshTokenRepository(ABC):
    @abstractmethod
    def save_token(self, token: RefreshToken) -> None: ...

    @abstractmethod
    def fetch_token_by_hash(self, token_hash: str) -> RefreshToken: ...


class MongoRefreshTokenRepository(RefreshTokenRepository):
    def __init__(self, client: pymongo.MongoClient, db_name: str = "chatops") -> None:
        db = client[db_name]
        self._tokens = db["refresh_tokens"]
        self._tokens.create_index("token_hash", unique=True)

    def save_token(self, token: RefreshToken) -> None:
        doc = token.model_dump(exclude={"id"})
        self._tokens.replace_one({"_id": token.id}, doc, upsert=True)

    def fetch_token_by_hash(self, token_hash: str) -> RefreshToken:
        doc = self._tokens.find_one({"token_hash": token_hash})
        if doc is None:
            raise KeyError(token_hash)
        return RefreshToken(
            id=doc["_id"],
            user_id=doc["user_id"],
            token_hash=doc["token_hash"],
            expires_at=doc["expires_at"],
            revoked=doc["revoked"],
            created_at=doc["created_at"],
            last_used_at=doc["last_used_at"],
        )
