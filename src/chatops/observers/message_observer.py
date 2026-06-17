from abc import ABC, abstractmethod
from typing import AsyncIterator

from chatops.domain.chat import MessageStreamEvent


class MessageNotObservableError(Exception):
    pass


class MessageObserver(ABC):
    @classmethod
    @abstractmethod
    async def create(cls, chat_id: str, message_id: str) -> "MessageObserver":
        raise NotImplementedError

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[MessageStreamEvent]:
        raise NotImplementedError
