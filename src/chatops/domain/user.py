from pydantic import BaseModel


class User(BaseModel):
    id: str
    email: str | None = None
    password_hash: str | None = None
    is_anonymous: bool
    created_at: int


class RefreshToken(BaseModel):
    id: str
    user_id: str
    token_hash: str
    expires_at: int
    revoked: bool
    created_at: int
    last_used_at: int | None = None
