import json
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

import redis
from fastapi import APIRouter, FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from chatops.domain.chat import Chat, Message
from chatops.services.chat_service import ChatService, LastAssistantMessageIsNotFinished
from chatops.repositories.chat_repository import ChatRepository, InMemoryChatRepository
from chatops.jobs.job_stream import JobStream, RedisJobStream
from chatops.jobs.result_stream import ResultStream, RedisResultStream
from chatops.observers.event_stream import EventStream
from chatops.observers.redis_event_stream import RedisEventStream
from chatops.observers.message_observer import MessageObserver, MessageNotObservableError
from chatops.consumers.result_consumer import ResultConsumer


class CreateChatRequest(BaseModel):
    message: str


class SendMessageRequest(BaseModel):
    content: str


def create_app(
    chat_repository: ChatRepository,
    job_stream: JobStream,
    result_stream: ResultStream,
    event_stream: EventStream,
) -> FastAPI:
    service = ChatService(chat_repository=chat_repository, jobs_stream=job_stream)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        consumer = ResultConsumer(result_stream=result_stream, chat_service=service).start()
        yield
        consumer.stop()

    app = FastAPI(lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    router = APIRouter(prefix="/api")

    @router.get("/chats", response_model=list[Chat])
    def fetch_chats(limit: int = Query(default=10, ge=1)) -> list[Chat]:
        return service.fetch_chats(limit=limit)

    @router.post("/chats", status_code=201, response_model=Chat)
    def create_chat(body: CreateChatRequest) -> Chat:
        return service.create_chat(body.message)

    @router.delete("/chats/{chat_id}", status_code=204)
    def delete_chat(chat_id: str) -> None:
        service.delete_chat(chat_id)

    @router.get("/chats/{chat_id}/messages", response_model=list[Message])
    def fetch_messages(chat_id: str) -> list[Message]:
        return service.fetch_messages(chat_id)

    @router.post("/chats/{chat_id}/messages", status_code=201, response_model=Message)
    def send_message(chat_id: str, body: SendMessageRequest):
        try:
            return service.send_message(chat_id, body.content)
        except LastAssistantMessageIsNotFinished:
            return JSONResponse(status_code=409, content={"error": "last_assistant_message_not_finished"})

    @router.get("/chats/{chat_id}/messages/{message_id}/stream")
    def stream_message(chat_id: str, message_id: str) -> StreamingResponse:
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

    app.include_router(router)

    return app


if __name__ == "__main__":
    import uvicorn

    redis_client = redis.Redis(host=os.environ["REDIS_HOST"], port=6379, socket_timeout=None)
    app = create_app(
        chat_repository=InMemoryChatRepository(),
        job_stream=RedisJobStream(redis_client),
        result_stream=RedisResultStream(redis_client),
        event_stream=RedisEventStream(redis_client),
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)
