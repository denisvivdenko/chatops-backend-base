from functools import lru_cache
from typing import Annotated

import pymongo
import redis as redis_lib
from fastapi import Depends, Header, HTTPException

from chatops.repositories.chat_repository import ChatRepository, MongoChatRepository
from chatops.repositories.refresh_token_repository import RefreshTokenRepository, MongoRefreshTokenRepository
from chatops.repositories.user_repository import UserRepository, MongoUserRepository
from chatops.settings import Settings
from chatops.stream.job_stream import JobStream, RedisJobStream
from chatops.stream.event_stream import EventStream, RedisEventStream
from chatops.services.auth_service import AuthService, InvalidAccessTokenError
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


@lru_cache
def get_user_repository() -> UserRepository:
    return MongoUserRepository(get_mongo_client())


@lru_cache
def get_refresh_token_repository() -> RefreshTokenRepository:
    return MongoRefreshTokenRepository(get_mongo_client())


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


def get_auth_service(
    users: Annotated[UserRepository, Depends(get_user_repository)],
    refresh_tokens: Annotated[RefreshTokenRepository, Depends(get_refresh_token_repository)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> AuthService:
    return AuthService(user_repository=users, refresh_token_repository=refresh_tokens, settings=settings)


ChatServiceDep = Annotated[ChatService, Depends(get_chat_service)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
JobStreamDep = Annotated[JobStream, Depends(get_job_stream)]
EventStreamDep = Annotated[EventStream, Depends(get_event_stream)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def get_current_user_id(
    auth_service: AuthServiceDep,
    authorization: Annotated[str | None, Header()] = None,
) -> str:
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.removeprefix("Bearer ")
    try:
        return auth_service.verify_access_token(token)
    except InvalidAccessTokenError:
        raise HTTPException(status_code=401, detail="unauthorized")


CurrentUserIdDep = Annotated[str, Depends(get_current_user_id)]
