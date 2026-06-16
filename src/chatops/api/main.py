import uuid
import time
from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI()


class CreateChatRequest(BaseModel):
    first_message: str


class Chat(BaseModel):
    id: str
    title: str
    last_activity_at: int
    created_at: int


class CreateChatResponse(BaseModel):
    chat: Chat


class FetchChatsResponse(BaseModel):
    chats: list[Chat]


@app.get("/chats", response_model=FetchChatsResponse)
def fetch_chats(limit: int = Query(default=10, ge=1)) -> FetchChatsResponse:
    return FetchChatsResponse(chats=[])


@app.post("/chats", status_code=201, response_model=CreateChatResponse)
def create_chat(body: CreateChatRequest) -> CreateChatResponse:
    now = int(time.time() * 1000)
    return CreateChatResponse(
        chat=Chat(
            id=str(uuid.uuid4()),
            title=body.first_message[:50],
            last_activity_at=now,
            created_at=now,
        )
    )
