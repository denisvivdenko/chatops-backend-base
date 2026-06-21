from fastapi import FastAPI, Query
from pydantic import BaseModel

from chatops.services.chat_service import ChatService
from chatops.repositories.chat_repository import InMemoryChatRepository
from chatops.jobs.job_stream import InMemoryJobStream

app = FastAPI()
chat_service = ChatService(chat_repository=InMemoryChatRepository(), jobs_stream=InMemoryJobStream())


class ChatResponse(BaseModel):
    id: str
    title: str
    last_activity_at: int
    created_at: int


class CreateChatRequest(BaseModel):
    first_message: str


class CreateChatResponse(BaseModel):
    chat: ChatResponse


class FetchChatsResponse(BaseModel):
    chats: list[ChatResponse]


@app.get("/chats", response_model=FetchChatsResponse)
def fetch_chats(limit: int = Query(default=10, ge=1)) -> FetchChatsResponse:
    chats = chat_service.fetch_chats(limit=limit)
    return FetchChatsResponse(chats=[ChatResponse(**c.model_dump()) for c in chats])


@app.post("/chats", status_code=201, response_model=CreateChatResponse)
def create_chat(body: CreateChatRequest) -> CreateChatResponse:
    chat = chat_service.create_chat(body.first_message)
    return CreateChatResponse(chat=ChatResponse(**chat.model_dump()))
