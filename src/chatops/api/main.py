from fastapi import FastAPI, Query
from pydantic import BaseModel

from chatops.domain.chat import Chat
from chatops.services.chat_service import ChatService
from chatops.repositories.chat_repository import ChatRepository, InMemoryChatRepository
from chatops.jobs.job_stream import JobStream, InMemoryJobStream
from chatops.observers.in_memory_event_stream import InMemoryEventStream


class CreateChatRequest(BaseModel):
    message: str


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

    return app


app = create_app(
    chat_repository=InMemoryChatRepository(),
    job_stream=InMemoryJobStream(),
    event_stream=InMemoryEventStream(),
)
