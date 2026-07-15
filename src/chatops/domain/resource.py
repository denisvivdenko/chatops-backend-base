from pydantic import BaseModel


class Resource(BaseModel):
    id: str
    user_id: str
    filename: str
    file_path: str
    created_at: int
