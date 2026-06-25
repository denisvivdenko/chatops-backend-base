import os

import pytest
import redis as redis_lib
from fastapi.testclient import TestClient

from chatops.api.main import create_app
from chatops.jobs.job_stream import InMemoryJobStream, RedisJobStream, REDIS_JOBS_KEY
from chatops.jobs.result_stream import InMemoryResultStream, RedisResultStream, REDIS_RESULTS_KEY
from chatops.observers.in_memory_event_stream import InMemoryEventStream
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
            chat_repository=InMemoryChatRepository(),
            job_stream=InMemoryJobStream(),
            result_stream=InMemoryResultStream(),
            event_stream=InMemoryEventStream(),
        )
        return

    redis_host = os.environ.get("REDIS_HOST", "localhost")
    client = redis_lib.Redis(host=redis_host, port=6379, db=1)
    client.delete(REDIS_JOBS_KEY, REDIS_RESULTS_KEY)
    yield dict(
        chat_repository=InMemoryChatRepository(),
        job_stream=RedisJobStream(client),
        result_stream=RedisResultStream(client),
        event_stream=InMemoryEventStream(),
    )
    client.delete(REDIS_JOBS_KEY, REDIS_RESULTS_KEY)
    client.close()


@pytest.fixture
def client(infra):
    with TestClient(create_app(**infra)) as c:
        yield c


@pytest.fixture
def client_with_worker(infra):
    worker = Worker(
        jobs_stream=infra["job_stream"],
        result_stream=infra["result_stream"],
        event_stream=infra["event_stream"],
    ).start()
    with TestClient(create_app(**infra)) as c:
        yield c
    worker.stop()
