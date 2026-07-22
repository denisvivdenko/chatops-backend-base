import re
import time
import uuid

from chatops.domain.resource import Resource
from chatops.repositories.resource_repository import ResourceRepository
from chatops.storage.resource_storage import ResourceStorage

PDF_MAGIC_BYTES = b"%PDF-"
MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024
RESOURCE_REF_PATTERN = re.compile(r"\[[^\]]+\]\(resource://([^)]+)\)")


class InvalidFileTypeError(Exception):
    pass


class FileTooLargeError(Exception):
    pass


class ResourceNotFoundError(Exception):
    pass


class ResourceAccessDeniedError(Exception):
    pass


class ResourceAlreadyExistsError(Exception):
    pass


class ResourceService:
    def __init__(self, resource_repository: ResourceRepository, resource_storage: ResourceStorage) -> None:
        self._repo = resource_repository
        self._storage = resource_storage

    def upload_resource(self, user_id: str, filename: str, content: bytes) -> Resource:
        if not content.startswith(PDF_MAGIC_BYTES):
            raise InvalidFileTypeError()
        if len(content) > MAX_FILE_SIZE_BYTES:
            raise FileTooLargeError()
        if any(resource.filename == filename for resource in self._repo.fetch_resources(user_id)):
            raise ResourceAlreadyExistsError()

        resource_id = str(uuid.uuid4())
        file_path = self._storage.save(resource_id, content)
        resource = Resource(
            id=resource_id,
            user_id=user_id,
            filename=filename,
            file_path=file_path,
            created_at=int(time.time() * 1000),
        )
        self._repo.save_resource(resource)
        return resource

    def fetch_resources(self, user_id: str) -> list[Resource]:
        return self._repo.fetch_resources(user_id)

    @staticmethod
    def parse_resource_refs(content: str) -> list[str]:
        return RESOURCE_REF_PATTERN.findall(content)

    def assert_owns_resource(self, resource_id: str, user_id: str) -> None:
        self._fetch_owned_resource(resource_id, user_id)

    def get_resource(self, resource_id: str, user_id: str) -> Resource:
        return self._fetch_owned_resource(resource_id, user_id)

    def read_resource_content(self, resource: Resource) -> bytes:
        return self._storage.read(resource.file_path)

    def delete_resource(self, resource_id: str, user_id: str) -> None:
        resource = self._fetch_owned_resource(resource_id, user_id)
        self._repo.delete_resource(resource_id)
        self._storage.delete(resource.file_path)

    def delete_resource_by_name(self, user_id: str, filename: str) -> None:
        resource = next((r for r in self._repo.fetch_resources(user_id) if r.filename == filename), None)
        if resource is None:
            raise ResourceNotFoundError()
        self._repo.delete_resource(resource.id)
        self._storage.delete(resource.file_path)

    def _fetch_owned_resource(self, resource_id: str, user_id: str) -> Resource:
        try:
            resource = self._repo.fetch_resource(resource_id)
        except KeyError:
            raise ResourceNotFoundError()
        if resource.user_id != user_id:
            raise ResourceAccessDeniedError()
        return resource
