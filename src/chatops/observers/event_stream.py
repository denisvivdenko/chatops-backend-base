from abc import ABC, abstractmethod
from typing import NamedTuple


class StreamEntry(NamedTuple):
    id: str
    data: dict[str, str]


class StreamNotFoundError(Exception):
    pass


class EventStream(ABC):
    @staticmethod
    def stream_key(chat_id: str, message_id: str) -> str:
        return f"{chat_id}:{message_id}"

    @abstractmethod
    async def read(self, stream_key: str, last_id: str | None = None) -> list[StreamEntry]:
        """Raises StreamNotFoundError if the stream does not exist."""
        raise NotImplementedError
