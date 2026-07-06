from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    mongo_host: str = "localhost"
    mongo_port: int = 27017
    job_stream_timeout: float = 1.0
    event_stream_timeout: float = 3.0
    message_generation_timeout: float = 10
    jwt_secret: str = "insecure-dev-secret-change-me-in-production"
    jwt_algorithm: str = "HS256"
    access_token_ttl: float = 900
    refresh_token_ttl: float = 60 * 60 * 24 * 14