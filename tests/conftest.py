import os
import shutil
import tempfile
import time

import pymongo
import pytest
import redis as redis_lib
from fastapi.testclient import TestClient

from chatops.api.main import app
from chatops.api.dependencies import (
    get_chat_repository,
    get_event_stream,
    get_ingestion_job_stream,
    get_job_stream,
    get_refresh_token_repository,
    get_resource_repository,
    get_resource_storage,
    get_settings,
    get_user_repository,
)
from chatops.domain.chat import Message
from chatops.settings import MessageTimeoutSettings, Settings
from chatops.storage.resource_storage import ResourceStorage
from chatops.stream.ingestion_job_stream import RedisIngestionJobStream
from chatops.stream.job_stream import RedisJobStream
from chatops.stream.event_stream import RedisEventStream
from chatops.repositories.chat_repository import MongoChatRepository
from chatops.repositories.refresh_token_repository import MongoRefreshTokenRepository
from chatops.repositories.resource_repository import MongoResourceRepository
from chatops.repositories.user_repository import MongoUserRepository
from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceService
from chatops.workers.worker import Worker, TEST_RESPONSE
from chatops.workers.ingestion_worker import IngestionWorker

MONGO_TEST_DB = "chatops_test"


def pytest_addoption(parser):
    parser.addoption("--integration", action="store_true", default=False, help="run integration tests against real services")


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires real external services (e.g. Redis)")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--integration"):
        skip = pytest.mark.skip(reason="pass --integration to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(params=[
    pytest.param("redis+mongo", marks=pytest.mark.integration),
])
def infra(request):
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_client = redis_lib.Redis(host=redis_host, port=6379, db=1)
    redis_client.flushdb()

    mongo_host = os.environ.get("MONGO_HOST", "localhost")
    mongo_client = pymongo.MongoClient(mongo_host, 27017)
    mongo_client.drop_database(MONGO_TEST_DB)

    resource_storage_dir = tempfile.mkdtemp()

    yield dict(
        repo=MongoChatRepository(mongo_client, db_name=MONGO_TEST_DB),
        user_repo=MongoUserRepository(mongo_client, db_name=MONGO_TEST_DB),
        refresh_token_repo=MongoRefreshTokenRepository(mongo_client, db_name=MONGO_TEST_DB),
        resource_repo=MongoResourceRepository(mongo_client, db_name=MONGO_TEST_DB),
        resource_storage=ResourceStorage(resource_storage_dir),
        redis_client=redis_client,
        job_stream=RedisJobStream(redis_client),
        ingestion_job_stream=RedisIngestionJobStream(redis_client),
        event_stream=RedisEventStream(redis_client),
    )

    redis_client.flushdb()
    redis_client.close()
    mongo_client.drop_database(MONGO_TEST_DB)
    mongo_client.close()
    shutil.rmtree(resource_storage_dir, ignore_errors=True)


@pytest.fixture
def settings(request) -> Settings:
    overrides = getattr(request, "param", {})
    return Settings(**overrides)


def sleep_until_message_timed_out(message: dict, timeout_settings: MessageTimeoutSettings, buffer: float = 0.05) -> None:
    remaining = ChatService.estimate_message_timeout(Message(**message), timeout_settings)
    time.sleep(remaining + buffer)


def _make_event_stream(infra, settings):
    return RedisEventStream(
        infra["redis_client"], timeout=settings.event_stream_timeout, ttl=settings.message_timeout.message_generation_timeout,
    )


def _setup_app(infra, settings):
    app.dependency_overrides[get_chat_repository] = lambda: infra["repo"]
    app.dependency_overrides[get_user_repository] = lambda: infra["user_repo"]
    app.dependency_overrides[get_refresh_token_repository] = lambda: infra["refresh_token_repo"]
    app.dependency_overrides[get_resource_repository] = lambda: infra["resource_repo"]
    app.dependency_overrides[get_resource_storage] = lambda: infra["resource_storage"]
    app.dependency_overrides[get_job_stream] = lambda: infra["job_stream"]
    app.dependency_overrides[get_ingestion_job_stream] = lambda: infra["ingestion_job_stream"]
    app.dependency_overrides[get_event_stream] = lambda: _make_event_stream(infra, settings)
    app.dependency_overrides[get_settings] = lambda: settings


def _teardown_app():
    app.dependency_overrides.clear()


@pytest.fixture
def client(infra, settings):
    _setup_app(infra, settings)
    with TestClient(app) as c:
        yield c
    _teardown_app()


@pytest.fixture
def authed_client(client):
    token = client.post("/api/auth/anonymous-session").json()["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"
    yield client


@pytest.fixture
def worker(infra, settings):
    resource_service = ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])
    chat_service = ChatService(chat_repository=infra["repo"], resource_service=resource_service)
    w = Worker(
        jobs_stream=infra["job_stream"],
        chat_service=chat_service,
        event_stream=_make_event_stream(infra, settings),
        response=TEST_RESPONSE,
    ).start()
    yield w
    w.stop()


@pytest.fixture
def client_with_worker(infra, settings, worker):
    _setup_app(infra, settings)
    with TestClient(app) as c:
        yield c
    _teardown_app()


@pytest.fixture
def authed_client_with_worker(client_with_worker):
    token = client_with_worker.post("/api/auth/anonymous-session").json()["access_token"]
    client_with_worker.headers["Authorization"] = f"Bearer {token}"
    yield client_with_worker


@pytest.fixture
def ingestion_worker(infra, settings):
    resource_service = ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])
    chat_service = ChatService(chat_repository=infra["repo"], resource_service=resource_service)
    w = IngestionWorker(
        ingestion_jobs=infra["ingestion_job_stream"],
        chat_service=chat_service,
        event_stream=_make_event_stream(infra, settings),
    ).start()
    yield w
    w.stop()


@pytest.fixture
def client_with_ingestion_worker(infra, settings, ingestion_worker):
    _setup_app(infra, settings)
    with TestClient(app) as c:
        yield c
    _teardown_app()


@pytest.fixture
def authed_client_with_ingestion_worker(client_with_ingestion_worker):
    token = client_with_ingestion_worker.post("/api/auth/anonymous-session").json()["access_token"]
    client_with_ingestion_worker.headers["Authorization"] = f"Bearer {token}"
    yield client_with_ingestion_worker
