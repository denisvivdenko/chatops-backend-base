import uuid
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class CreateChatRequest(BaseModel):
    first_message: str


class CreateChatResponse(BaseModel):
    chat_id: str


@app.post("/chats", status_code=201, response_model=CreateChatResponse)
def create_chat(body: CreateChatRequest) -> CreateChatResponse:
    return CreateChatResponse(
        chat_id=str(uuid.uuid4()),
    )
