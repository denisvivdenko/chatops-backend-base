from unittest.mock import MagicMock

import pytest

from chatops.repositories.resource_repository import ResourceRepository
from chatops.services.resource_service import ResourceNotFoundError, ResourceService
from chatops.storage.resource_storage import ResourceStorage

USER_ID = "test-user"
PDF_CONTENT = b"%PDF-1.4\n%mock pdf content"


def test_delete_resource() -> None:
    repo = MagicMock(spec=ResourceRepository)
    repo.fetch_resources.return_value = []
    storage = MagicMock(spec=ResourceStorage)
    storage.save.return_value = "/data/resources/r1"
    service = ResourceService(
        resource_repository=repo,
        resource_storage=storage
    )

    resource = service.upload_resource(USER_ID, "a.pdf", PDF_CONTENT)
    repo.fetch_resources.return_value = [resource]

    service.delete_resource_by_name(USER_ID, "a.pdf")

    repo.delete_resource.assert_called_once_with(resource.id)
    storage.delete.assert_called_once_with(resource.file_path)


def test_delete_resource_raises_when_missing() -> None:
    repo = MagicMock(spec=ResourceRepository)
    repo.fetch_resources.return_value = []
    storage = MagicMock(spec=ResourceStorage)
    service = ResourceService(
        resource_repository=repo,
        resource_storage=storage
    )

    with pytest.raises(ResourceNotFoundError):
        service.delete_resource_by_name(USER_ID, "nonexistent.pdf")

    repo.delete_resource.assert_not_called()
    storage.delete.assert_not_called()
