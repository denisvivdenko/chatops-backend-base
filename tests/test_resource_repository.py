import os
from typing import Iterator

import pymongo
import pytest

from chatops.domain.resource import Resource
from chatops.repositories.resource_repository import MongoResourceRepository

MONGO_TEST_DB = "chatops_test"


@pytest.fixture(params=[
    pytest.param("mongo", marks=pytest.mark.integration),
])
def resource_repo() -> Iterator[MongoResourceRepository]:
    mongo_host = os.environ.get("MONGO_HOST", "localhost")
    mongo_client = pymongo.MongoClient(mongo_host, 27017)
    mongo_client.drop_database(MONGO_TEST_DB)

    yield MongoResourceRepository(mongo_client, db_name=MONGO_TEST_DB)

    mongo_client.drop_database(MONGO_TEST_DB)
    mongo_client.close()


def test_save_and_fetch_resource_round_trip(resource_repo: MongoResourceRepository) -> None:
    resource = Resource(id="r1", user_id="user-1", filename="a.pdf", file_path="/data/resources/r1", created_at=1)
    resource_repo.save_resource(resource)

    assert resource_repo.fetch_resource("r1") == resource


def test_fetch_resource_raises_key_error_when_missing(resource_repo: MongoResourceRepository) -> None:
    with pytest.raises(KeyError):
        resource_repo.fetch_resource("unknown")


def test_fetch_resources_returns_only_calling_users_resources_newest_first(
    resource_repo: MongoResourceRepository,
) -> None:
    resource_repo.save_resource(Resource(id="r1", user_id="user-1", filename="a.pdf", file_path="/data/resources/r1", created_at=1))
    resource_repo.save_resource(Resource(id="r2", user_id="user-2", filename="b.pdf", file_path="/data/resources/r2", created_at=2))
    resource_repo.save_resource(Resource(id="r3", user_id="user-1", filename="c.pdf", file_path="/data/resources/r3", created_at=3))

    resources = resource_repo.fetch_resources("user-1")

    assert [r.id for r in resources] == ["r3", "r1"]


def test_delete_resource_removes_it(resource_repo: MongoResourceRepository) -> None:
    resource = Resource(id="r1", user_id="user-1", filename="a.pdf", file_path="/data/resources/r1", created_at=1)
    resource_repo.save_resource(resource)

    resource_repo.delete_resource("r1")

    with pytest.raises(KeyError):
        resource_repo.fetch_resource("r1")


def test_delete_resource_leaves_other_users_resource_with_same_filename_untouched(
    resource_repo: MongoResourceRepository,
) -> None:
    resource_a = Resource(id="r1", user_id="user-1", filename="a.pdf", file_path="/data/resources/r1", created_at=1)
    resource_b = Resource(id="r2", user_id="user-2", filename="a.pdf", file_path="/data/resources/r2", created_at=2)
    resource_repo.save_resource(resource_a)
    resource_repo.save_resource(resource_b)

    resource_repo.delete_resource(resource_a.id)

    assert resource_repo.fetch_resources("user-1") == []
    assert resource_repo.fetch_resource(resource_b.id) == resource_b
