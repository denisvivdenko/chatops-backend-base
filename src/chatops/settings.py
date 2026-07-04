from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    mongo_host: str = "localhost"
    mongo_port: int = 27017
    job_stream_timeout: float = 1.0
    event_stream_timeout: float = 3.0
