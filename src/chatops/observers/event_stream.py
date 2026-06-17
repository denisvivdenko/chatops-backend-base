from abc import ABC, abstractmethod
from typing import AsyncIterator


class EventStream(ABC):
    @abstractmethod
    async def exists(self, chat_id: str, message_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def read(self, chat_id: str, message_id: str) -> AsyncIterator[str]:
        raise NotImplementedError
