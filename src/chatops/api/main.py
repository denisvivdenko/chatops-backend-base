import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from fastapi import APIRouter, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from chatops.api.dependencies import (
    ChatServiceDep,
    EventStreamDep,
    get_chat_repository,
    get_job_stream,
    get_result_stream,
)
from chatops.consumers.result_consumer import ResultConsumer
from chatops.domain.chat import Chat, Message
from chatops.observers.message_observer import MessageObserver, MessageNotObservableError
from chatops.services.chat_service import ChatService, LastAssistantMessageIsNotFinished


class CreateChatRequest(BaseModel):
    message: str


class SendMessageRequest(BaseModel):
    content: str


router = APIRouter(prefix="/api")


@router.get("/chats", response_model=list[Chat])
def fetch_chats(
    service: ChatServiceDep,
    limit: int = Query(default=10, ge=1),
) -> list[Chat]:
    return service.fetch_chats(limit=limit)


@router.post("/chats", status_code=201, response_model=Chat)
def create_chat(
    body: CreateChatRequest,
    service: ChatServiceDep,
) -> Chat:
    return service.create_chat(body.message)


@router.delete("/chats/{chat_id}", status_code=204)
def delete_chat(
    chat_id: str,
    service: ChatServiceDep,
) -> None:
    service.delete_chat(chat_id)


@router.get("/chats/{chat_id}/messages", response_model=list[Message])
def fetch_messages(
    chat_id: str,
    service: ChatServiceDep,
) -> list[Message]:
    return service.fetch_messages(chat_id)


@router.post("/chats/{chat_id}/messages", status_code=201, response_model=Message)
def send_message(
    chat_id: str,
    body: SendMessageRequest,
    service: ChatServiceDep,
):
    try:
        return service.send_message(chat_id, body.content)
    except LastAssistantMessageIsNotFinished:
        return JSONResponse(status_code=409, content={"error": "last_assistant_message_not_finished"})


@router.get("/chats/{chat_id}/messages/{message_id}/stream")
def stream_message(
    chat_id: str,
    message_id: str,
    service: ChatServiceDep,
    event_stream: EventStreamDep,
) -> StreamingResponse:
    async def event_generator() -> AsyncIterator[str]:
        tokens = []
        try:
            async for event in MessageObserver(chat_id, message_id, event_stream):
                tokens.append(event.token)
                yield f"data: {json.dumps(event.model_dump())}\n\n"
        except MessageNotObservableError:
            return
        service.complete_message(chat_id, message_id, "".join(tokens))

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@asynccontextmanager
async def lifespan(app: FastAPI):
    result_stream = getattr(app.state, 'result_stream', None) or get_result_stream()
    repo = getattr(app.state, 'chat_repository', None) or get_chat_repository()
    jobs = getattr(app.state, 'job_stream', None) or get_job_stream()
    service = ChatService(chat_repository=repo, jobs_stream=jobs)
    consumer = ResultConsumer(result_stream=result_stream, chat_service=service).start()
    yield
    consumer.stop()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
