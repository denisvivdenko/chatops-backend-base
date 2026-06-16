from pydantic import BaseModel


class Chat(BaseModel):
    id: str
    title: str
    last_activity_at: int
    created_at: int
