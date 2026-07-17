from pathlib import Path

import pytest

from chatops.services.resource_service import ResourceAccessDeniedError, ResourceNotFoundError, ResourceService

USER_ID = "test-user"
OTHER_USER_ID = "other-user"
PDF_CONTENT = b"%PDF-1.4\n%mock pdf content"


def _make_service(infra) -> ResourceService:
    return ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])


def test_delete_resource_removes_record_and_file_for_owner(infra) -> None:
    service = _make_service(infra)
    resource = service.upload_resource(USER_ID, "a.pdf", PDF_CONTENT)

    service.delete_resource(resource.id, USER_ID)

    with pytest.raises(KeyError):
        infra["resource_repo"].fetch_resource(resource.id)
    assert not Path(resource.file_path).exists()


def test_delete_resource_raises_when_not_owned_by_caller(infra) -> None:
    service = _make_service(infra)
    resource = service.upload_resource(OTHER_USER_ID, "a.pdf", PDF_CONTENT)

    with pytest.raises(ResourceAccessDeniedError):
        service.delete_resource(resource.id, USER_ID)

    assert infra["resource_repo"].fetch_resource(resource.id) == resource
    assert Path(resource.file_path).exists()


def test_delete_resource_raises_when_missing(infra) -> None:
    service = _make_service(infra)

    with pytest.raises(ResourceNotFoundError):
        service.delete_resource("nonexistent", USER_ID)
