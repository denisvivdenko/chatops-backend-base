from abc import ABC, abstractmethod

import pymongo

from chatops.domain.resource import Resource


class ResourceRepository(ABC):
    @abstractmethod
    def save_resource(self, resource: Resource) -> None: ...

    @abstractmethod
    def fetch_resource(self, resource_id: str) -> Resource:
        """Raises KeyError if no resource with this id exists."""
        ...

    @abstractmethod
    def fetch_resources(self, user_id: str) -> list[Resource]: ...


class MongoResourceRepository(ResourceRepository):
    def __init__(self, client: pymongo.MongoClient, db_name: str = "chatops") -> None:
        db = client[db_name]
        self._resources = db["resources"]
        self._resources.create_index("user_id")

    def save_resource(self, resource: Resource) -> None:
        doc = resource.model_dump(exclude={"id"})
        self._resources.replace_one({"_id": resource.id}, doc, upsert=True)

    def fetch_resource(self, resource_id: str) -> Resource:
        doc = self._resources.find_one({"_id": resource_id})
        if doc is None:
            raise KeyError(resource_id)
        return self._resource_from_doc(doc)

    def fetch_resources(self, user_id: str) -> list[Resource]:
        cursor = self._resources.find({"user_id": user_id}).sort("created_at", pymongo.DESCENDING)
        return [self._resource_from_doc(doc) for doc in cursor]

    @staticmethod
    def _resource_from_doc(doc: dict) -> Resource:
        return Resource(
            id=doc["_id"],
            user_id=doc["user_id"],
            filename=doc["filename"],
            file_path=doc["file_path"],
            created_at=doc["created_at"],
        )
