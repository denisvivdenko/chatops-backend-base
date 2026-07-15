import time
import uuid

from chatops.domain.resource import Resource
from chatops.repositories.resource_repository import ResourceRepository
from chatops.storage.resource_storage import ResourceStorage

PDF_MAGIC_BYTES = b"%PDF-"
MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024


class InvalidFileTypeError(Exception):
    pass


class FileTooLargeError(Exception):
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
