import os
from functools import lru_cache
from typing import Annotated

import pymongo
import redis as redis_lib
from fastapi import Depends

from chatops.repositories.chat_repository import ChatRepository, MongoChatRepository
from chatops.stream.job_stream import JobStream, RedisJobStream
from chatops.stream.event_stream import EventStream, RedisEventStream
from chatops.services.chat_service import ChatService


@lru_cache
def get_redis_client() -> redis_lib.Redis:
    return redis_lib.Redis(host=os.environ["REDIS_HOST"], port=6379, socket_timeout=None)


@lru_cache
def get_mongo_client() -> pymongo.MongoClient:
    return pymongo.MongoClient(os.environ["MONGO_HOST"], 27017)


@lru_cache
def get_chat_repository() -> ChatRepository:
    return MongoChatRepository(get_mongo_client())


def get_job_stream() -> JobStream:
    return RedisJobStream(get_redis_client())


def get_event_stream() -> EventStream:
    return RedisEventStream(get_redis_client())


def get_chat_service(
    repo: Annotated[ChatRepository, Depends(get_chat_repository)],
    jobs: Annotated[JobStream, Depends(get_job_stream)],
) -> ChatService:
    return ChatService(chat_repository=repo, jobs_stream=jobs)


ChatServiceDep = Annotated[ChatService, Depends(get_chat_service)]
EventStreamDep = Annotated[EventStream, Depends(get_event_stream)]
