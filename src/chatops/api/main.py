from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from chatops.domain.chat import Chat, Message
from chatops.services.chat_service import ChatService, LastAssistantMessageIsNotFinished
from chatops.repositories.chat_repository import ChatRepository, InMemoryChatRepository
from chatops.jobs.job_stream import JobStream, InMemoryJobStream
from chatops.observers.in_memory_event_stream import InMemoryEventStream


class CreateChatRequest(BaseModel):
    message: str


class SendMessageRequest(BaseModel):
    content: str


def create_app(
    chat_repository: ChatRepository,
    job_stream: JobStream,
    event_stream: InMemoryEventStream,
) -> FastAPI:
    app = FastAPI()
    service = ChatService(chat_repository=chat_repository, jobs_stream=job_stream)

    @app.get("/chats", response_model=list[Chat])
    def fetch_chats(limit: int = Query(default=10, ge=1)) -> list[Chat]:
        return service.fetch_chats(limit=limit)

    @app.post("/chats", status_code=201, response_model=Chat)
    def create_chat(body: CreateChatRequest) -> Chat:
        return service.create_chat(body.message)

    @app.delete("/chats/{chat_id}", status_code=204)
    def delete_chat(chat_id: str) -> None:
        service.delete_chat(chat_id)

    @app.get("/chats/{chat_id}/messages", response_model=list[Message])
    def fetch_messages(chat_id: str) -> list[Message]:
        return service.fetch_messages(chat_id)

    @app.post("/chats/{chat_id}/messages", status_code=201, response_model=Message)
    def send_message(chat_id: str, body: SendMessageRequest):
        try:
            return service.send_message(chat_id, body.content)
        except LastAssistantMessageIsNotFinished:
            return JSONResponse(status_code=409, content={"error": "last_assistant_message_not_finished"})

    return app


app = create_app(
    chat_repository=InMemoryChatRepository(),
    job_stream=InMemoryJobStream(),
    event_stream=InMemoryEventStream(),
)
