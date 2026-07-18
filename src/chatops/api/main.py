import json
import logging
from typing import AsyncIterator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from fastapi import APIRouter, FastAPI, File, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from chatops.api.auth import router as auth_router
from chatops.api.dependencies import (
    ChatServiceDep,
    CurrentUserIdDep,
    EventStreamDep,
    IngestionJobStreamDep,
    JobStreamDep,
    ResourceServiceDep,
    SettingsDep,
)
from chatops.domain.chat import Chat, Message
from chatops.stream.message_observer import MessageGenerationTimeoutError, MessageObserver
from chatops.services.chat_service import (
    AssistantMessagePendingError,
    CannotModifyAssistantMessageError,
    ChatAccessDeniedError,
    ChatNotFoundError,
    MessageNotFailedError,
    MessageNotFoundError,
)
from chatops.services.resource_service import (
    FileTooLargeError,
    InvalidFileTypeError,
    ResourceAccessDeniedError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)


class CreateChatRequest(BaseModel):
    message: str


class SendMessageRequest(BaseModel):
    content: str


router = APIRouter(prefix="/api")


@router.get("/chats", response_model=list[Chat])
def fetch_chats(
    service: ChatServiceDep,
    user_id: CurrentUserIdDep,
    limit: int = Query(default=10, ge=1),
) -> list[Chat]:
    return service.fetch_chats(user_id, limit=limit)


@router.post("/chats", status_code=201, response_model=Chat)
def create_chat(
    body: CreateChatRequest,
    service: ChatServiceDep,
    jobs: JobStreamDep,
    ingestion_jobs: IngestionJobStreamDep,
    user_id: CurrentUserIdDep,
) -> Chat:
    return service.create_chat(body.message, user_id, jobs, ingestion_jobs)


@router.delete("/chats/{chat_id}", status_code=204)
def delete_chat(
    chat_id: str,
    service: ChatServiceDep,
    user_id: CurrentUserIdDep,
):
    try:
        service.delete_chat(chat_id, user_id)
    except ChatAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    except ChatNotFoundError:
        return JSONResponse(status_code=404, content={"error": "chat_not_found"})


@router.get("/chats/{chat_id}/messages", response_model=list[Message])
def fetch_messages(
    chat_id: str,
    service: ChatServiceDep,
    settings: SettingsDep,
    user_id: CurrentUserIdDep,
):
    try:
        service.fail_stale_pending_messages(chat_id, user_id, fail_message_after_timeout=settings.message_generation_timeout)
        return service.fetch_messages(chat_id, user_id)
    except ChatAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    except ChatNotFoundError:
        return JSONResponse(status_code=404, content={"error": "chat_not_found"})


@router.post("/chats/{chat_id}/messages", status_code=201, response_model=Message)
def send_message(
    chat_id: str,
    body: SendMessageRequest,
    service: ChatServiceDep,
    jobs: JobStreamDep,
    ingestion_jobs: IngestionJobStreamDep,
    user_id: CurrentUserIdDep,
):
    try:
        return service.send_message(chat_id, user_id, body.content, jobs, ingestion_jobs)
    except AssistantMessagePendingError:
        return JSONResponse(status_code=409, content={"error": "last_assistant_message_not_finished"})
    except ChatAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    except ChatNotFoundError:
        return JSONResponse(status_code=404, content={"error": "chat_not_found"})
    except ResourceNotFoundError:
        return JSONResponse(status_code=404, content={"error": "resource_not_found"})
    except ResourceAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})


@router.post("/chats/{chat_id}/messages/{message_id}/retry", response_model=Message)
def retry_message(
    chat_id: str,
    message_id: str,
    service: ChatServiceDep,
    jobs: JobStreamDep,
    ingestion_jobs: IngestionJobStreamDep,
    user_id: CurrentUserIdDep,
):
    try:
        return service.retry_message(chat_id, user_id, message_id, jobs, ingestion_jobs)
    except MessageNotFailedError:
        return JSONResponse(status_code=409, content={"error": "message_not_failed"})
    except ChatAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    except ChatNotFoundError:
        return JSONResponse(status_code=404, content={"error": "chat_not_found"})
    except MessageNotFoundError:
        return JSONResponse(status_code=404, content={"error": "message_not_found"})
    except ResourceNotFoundError:
        return JSONResponse(status_code=404, content={"error": "resource_not_found"})
    except ResourceAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})


@router.post("/chats/{chat_id}/messages/{message_id}/modify", response_model=Message)
def modify_message(
    chat_id: str,
    message_id: str,
    body: SendMessageRequest,
    service: ChatServiceDep,
    jobs: JobStreamDep,
    ingestion_jobs: IngestionJobStreamDep,
    user_id: CurrentUserIdDep,
):
    try:
        return service.modify_message(chat_id, user_id, message_id, body.content, jobs, ingestion_jobs)
    except AssistantMessagePendingError:
        return JSONResponse(status_code=409, content={"error": "last_assistant_message_not_finished"})
    except CannotModifyAssistantMessageError:
        return JSONResponse(status_code=409, content={"error": "cannot_modify_assistant_message"})
    except ChatAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    except ChatNotFoundError:
        return JSONResponse(status_code=404, content={"error": "chat_not_found"})
    except MessageNotFoundError:
        return JSONResponse(status_code=404, content={"error": "message_not_found"})
    except ResourceNotFoundError:
        return JSONResponse(status_code=404, content={"error": "resource_not_found"})
    except ResourceAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})


@router.get("/chats/{chat_id}/messages/{message_id}/stream")
def stream_message(
    chat_id: str,
    message_id: str,
    service: ChatServiceDep,
    event_stream: EventStreamDep,
    settings: SettingsDep,
    user_id: CurrentUserIdDep,
):
    try:
        service.get_message(chat_id, user_id, message_id)
    except ChatAccessDeniedError:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    except ChatNotFoundError:
        return JSONResponse(status_code=404, content={"error": "chat_not_found"})
    except MessageNotFoundError:
        return JSONResponse(status_code=404, content={"error": "message_not_found"})

    observer = MessageObserver(
        chat_id, message_id, event_stream, timeout=settings.message_generation_timeout
    )

    async def event_generator() -> AsyncIterator[str]:
        try:
            async for event in observer:
                yield f"data: {json.dumps(event.model_dump())}\n\n"
        except MessageGenerationTimeoutError:
            yield f"event: error\ndata: {json.dumps({'error': 'message_generation_timeout'})}\n\n"
            return
        yield f"event: done\ndata: {json.dumps({'status': 'complete'})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/upload-resource", status_code=201)
async def upload_resource(
    service: ResourceServiceDep,
    user_id: CurrentUserIdDep,
    file: UploadFile = File(...),
):
    content = await file.read()
    try:
        try:
            resource = service.upload_resource(user_id, file.filename, content)
        except ResourceAlreadyExistsError:
            existing = next(r for r in service.fetch_resources(user_id) if r.filename == file.filename)
            service.delete_resource(existing.id, user_id)
            resource = service.upload_resource(user_id, file.filename, content)
    except InvalidFileTypeError:
        return JSONResponse(status_code=400, content={"error": "invalid_file_type"})
    except FileTooLargeError:
        return JSONResponse(status_code=400, content={"error": "file_too_large"})
    return {"id": resource.id, "filename": resource.filename}


@router.get("/resources")
def list_resources(
    service: ResourceServiceDep,
    user_id: CurrentUserIdDep,
):
    return [{"id": r.id, "filename": r.filename} for r in service.fetch_resources(user_id)]


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
app.include_router(auth_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
