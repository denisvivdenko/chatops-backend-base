from enum import StrEnum
from pydantic import BaseModel


class Chat(BaseModel):
    id: str
    user_id: str
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
    resource_ids_to_process: list[str] = []


EOM = "<EOM>"


class MessageStreamEvent(BaseModel):
    seq_id: int
    token: str
