from abc import ABC, abstractmethod
from typing import NamedTuple


class MessageToken(NamedTuple):
    seq_id: int
    token: str


class EventStream(ABC):
    @abstractmethod
    async def exists(self, chat_id: str, message_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def listen_for_message_tokens(self, chat_id: str, message_id: str, from_seq_id: int) -> list[MessageToken]:
        raise NotImplementedError
