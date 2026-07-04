from functools import lru_cache
from typing import Annotated

import pymongo
import redis as redis_lib
from fastapi import Depends

from chatops.repositories.chat_repository import ChatRepository, MongoChatRepository
from chatops.settings import Settings
from chatops.stream.job_stream import JobStream, RedisJobStream
from chatops.stream.event_stream import EventStream, RedisEventStream
from chatops.services.chat_service import ChatService


@lru_cache
def get_settings() -> Settings:
    return Settings()


@lru_cache
def get_redis_client() -> redis_lib.Redis:
    settings = get_settings()
    return redis_lib.Redis(host=settings.redis_host, port=settings.redis_port, socket_timeout=None)


@lru_cache
def get_mongo_client() -> pymongo.MongoClient:
    settings = get_settings()
    return pymongo.MongoClient(settings.mongo_host, settings.mongo_port)


@lru_cache
def get_chat_repository() -> ChatRepository:
    return MongoChatRepository(get_mongo_client())


def get_job_stream() -> JobStream:
    return RedisJobStream(get_redis_client(), timeout=get_settings().job_stream_timeout)


def get_event_stream() -> EventStream:
    settings = get_settings()
    return RedisEventStream(
        get_redis_client(), timeout=settings.event_stream_timeout, ttl=settings.message_generation_timeout,
    )


def get_chat_service(
    repo: Annotated[ChatRepository, Depends(get_chat_repository)],
) -> ChatService:
    return ChatService(chat_repository=repo)


ChatServiceDep = Annotated[ChatService, Depends(get_chat_service)]
JobStreamDep = Annotated[JobStream, Depends(get_job_stream)]
EventStreamDep = Annotated[EventStream, Depends(get_event_stream)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
