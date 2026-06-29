import os

import pytest
import redis as redis_lib
from fastapi.testclient import TestClient

from chatops.api.main import app
from chatops.api.dependencies import (
    get_chat_repository,
    get_event_stream,
    get_job_stream,
    get_result_stream,
)
from chatops.jobs.job_stream import InMemoryJobStream, RedisJobStream
from chatops.jobs.result_stream import InMemoryResultStream, RedisResultStream
from chatops.observers.in_memory_event_stream import InMemoryEventStream
from chatops.observers.redis_event_stream import RedisEventStream
from chatops.repositories.chat_repository import InMemoryChatRepository
from chatops.workers.worker import Worker


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
    "memory",
    pytest.param("redis", marks=pytest.mark.integration),
])
def infra(request):
    if request.param == "memory":
        yield dict(
            repo=InMemoryChatRepository(),
            job_stream=InMemoryJobStream(),
            result_stream=InMemoryResultStream(),
            event_stream=InMemoryEventStream(),
        )
        return

    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_client = redis_lib.Redis(host=redis_host, port=6379, db=1)
    redis_client.flushdb()
    yield dict(
        repo=InMemoryChatRepository(),
        job_stream=RedisJobStream(redis_client),
        result_stream=RedisResultStream(redis_client),
        event_stream=RedisEventStream(redis_client),
    )
    redis_client.flushdb()
    redis_client.close()


@pytest.fixture
def client(infra):
    app.dependency_overrides[get_chat_repository] = lambda: infra["repo"]
    app.dependency_overrides[get_job_stream] = lambda: infra["job_stream"]
    app.dependency_overrides[get_result_stream] = lambda: infra["result_stream"]
    app.dependency_overrides[get_event_stream] = lambda: infra["event_stream"]
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def client_with_worker(infra):
    app.dependency_overrides[get_chat_repository] = lambda: infra["repo"]
    app.dependency_overrides[get_job_stream] = lambda: infra["job_stream"]
    app.dependency_overrides[get_result_stream] = lambda: infra["result_stream"]
    app.dependency_overrides[get_event_stream] = lambda: infra["event_stream"]
    worker = Worker(
        jobs_stream=infra["job_stream"],
        result_stream=infra["result_stream"],
        event_stream=infra["event_stream"],
    ).start()
    with TestClient(app) as c:
        yield c
    worker.stop()
    app.dependency_overrides.clear()
