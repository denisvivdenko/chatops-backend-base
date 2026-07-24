import os
from typing import Iterator

import pymongo
import pytest
import redis as redis_lib

MONGO_TEST_DB = "chatops_test"


@pytest.fixture
def redis_client() -> Iterator[redis_lib.Redis]:
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    client = redis_lib.Redis(host=redis_host, port=6379, db=1)
    client.flushdb()

    yield client

    client.flushdb()
    client.close()


@pytest.fixture
def mongo_client() -> Iterator[pymongo.MongoClient]:
    mongo_host = os.environ.get("MONGO_HOST", "localhost")
    client = pymongo.MongoClient(mongo_host, 27017)
    client.drop_database(MONGO_TEST_DB)

    yield client

    client.drop_database(MONGO_TEST_DB)
    client.close()
