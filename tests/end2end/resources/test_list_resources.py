from .helpers import PDF_CONTENT, upload_resource
from ..helpers import new_user_token


def test_list_resources_empty_for_user_with_no_uploads(authed_client) -> None:
    response = authed_client.get("/api/resources")

    assert response.status_code == 200
    assert response.json() == []


def test_list_resources_returns_only_calling_users_resources(client) -> None:
    token_a = new_user_token(client)
    token_b = new_user_token(client)

    client.post(
        "/api/upload-resource",
        files={"file": ("a.pdf", PDF_CONTENT, "application/pdf")},
        headers={"Authorization": f"Bearer {token_a}"},
    )
    client.post(
        "/api/upload-resource",
        files={"file": ("b.pdf", PDF_CONTENT, "application/pdf")},
        headers={"Authorization": f"Bearer {token_b}"},
    )

    resources_a = client.get("/api/resources", headers={"Authorization": f"Bearer {token_a}"}).json()

    assert [r["filename"] for r in resources_a] == ["a.pdf"]


def test_list_resources_ordered_most_recently_uploaded_first(authed_client) -> None:
    upload_resource(authed_client, "first.pdf")
    upload_resource(authed_client, "second.pdf")

    resources = authed_client.get("/api/resources").json()

    assert [r["filename"] for r in resources] == ["second.pdf", "first.pdf"]


def test_list_resources_without_token_is_rejected(client) -> None:
    response = client.get("/api/resources")

    assert response.status_code == 401
