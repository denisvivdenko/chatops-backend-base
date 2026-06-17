from enum import StrEnum
from pydantic import BaseModel


class Chat(BaseModel):
    id: str
    title: str
    last_activity_at: int
    created_at: int


class MessageRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"


class MessageStatus(StrEnum):
    PENDING = "pending"
    COMPLETE = "complete"
    FAILED = "failed"


class Message(BaseModel):
    id: str
    role: MessageRole
    status: MessageStatus
    content: str
    created_at: int


class MessageStreamEvent(BaseModel):
    token: str
    status: MessageStatus
