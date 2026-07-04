import os

import pymongo
import pytest
import redis as redis_lib
from fastapi.testclient import TestClient

from chatops.api.main import app
from chatops.api.dependencies import (
    get_chat_repository,
    get_event_stream,
    get_job_stream,
    get_settings,
)
from chatops.settings import Settings
from chatops.stream.job_stream import RedisJobStream
from chatops.stream.event_stream import RedisEventStream
from chatops.repositories.chat_repository import MongoChatRepository
from chatops.services.chat_service import ChatService
from chatops.workers.worker import Worker, TEST_RESPONSE

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

    yield dict(
        repo=MongoChatRepository(mongo_client, db_name=MONGO_TEST_DB),
        redis_client=redis_client,
        job_stream=RedisJobStream(redis_client),
    )

    redis_client.flushdb()
    redis_client.close()
    mongo_client.drop_database(MONGO_TEST_DB)
    mongo_client.close()


@pytest.fixture
def settings(request) -> Settings:
    overrides = getattr(request, "param", {})
    return Settings(**overrides)


def _make_event_stream(infra, settings):
    return RedisEventStream(infra["redis_client"], timeout=settings.event_stream_timeout)


def _setup_app(infra, settings):
    app.dependency_overrides[get_chat_repository] = lambda: infra["repo"]
    app.dependency_overrides[get_job_stream] = lambda: infra["job_stream"]
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
def client_with_worker(infra, settings):
    _setup_app(infra, settings)
    chat_service = ChatService(chat_repository=infra["repo"])
    worker = Worker(
        jobs_stream=infra["job_stream"],
        chat_service=chat_service,
        event_stream=_make_event_stream(infra, settings),
        response=TEST_RESPONSE,
    ).start()
    with TestClient(app) as c:
        yield c
    worker.stop()
    _teardown_app()
