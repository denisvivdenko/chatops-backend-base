import os
from functools import lru_cache

import redis as redis_lib
from fastapi import Depends

from chatops.repositories.chat_repository import ChatRepository, InMemoryChatRepository
from chatops.jobs.job_stream import JobStream, RedisJobStream
from chatops.jobs.result_stream import ResultStream, RedisResultStream
from chatops.observers.event_stream import EventStream
from chatops.observers.redis_event_stream import RedisEventStream
from chatops.services.chat_service import ChatService


@lru_cache
def get_redis_client() -> redis_lib.Redis:
    return redis_lib.Redis(host=os.environ["REDIS_HOST"], port=6379, socket_timeout=None)


@lru_cache
def get_chat_repository() -> ChatRepository:
    return InMemoryChatRepository()


def get_job_stream() -> JobStream:
    return RedisJobStream(get_redis_client())


def get_result_stream() -> ResultStream:
    return RedisResultStream(get_redis_client())


def get_event_stream() -> EventStream:
    return RedisEventStream(get_redis_client())


def get_chat_service(
    repo: ChatRepository = Depends(get_chat_repository),
    jobs: JobStream = Depends(get_job_stream),
) -> ChatService:
    return ChatService(chat_repository=repo, jobs_stream=jobs)
