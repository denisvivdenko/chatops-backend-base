from .helpers import PDF_CONTENT


def test_upload_valid_pdf_returns_id_and_filename(authed_client) -> None:
    response = authed_client.post(
        "/api/upload-resource",
        files={"file": ("report.pdf", PDF_CONTENT, "application/pdf")},
    )

    assert response.status_code == 201
    body = response.json()
    assert "id" in body
    assert body["filename"] == "report.pdf"


def test_upload_non_pdf_content_is_rejected(authed_client) -> None:
    response = authed_client.post(
        "/api/upload-resource",
        files={"file": ("fake.pdf", b"not actually a pdf", "application/pdf")},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_file_type"


def test_upload_oversized_file_is_rejected(authed_client) -> None:
    oversized_content = b"%PDF-1.4\n" + b"a" * (20 * 1024 * 1024 + 1)

    response = authed_client.post(
        "/api/upload-resource",
        files={"file": ("big.pdf", oversized_content, "application/pdf")},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "file_too_large"


def test_upload_without_token_is_rejected(client) -> None:
    response = client.post(
        "/api/upload-resource",
        files={"file": ("report.pdf", PDF_CONTENT, "application/pdf")},
    )

    assert response.status_code == 401


def test_upload_same_filename_twice_replaces_existing_resource(authed_client) -> None:
    first = authed_client.post(
        "/api/upload-resource",
        files={"file": ("report.pdf", PDF_CONTENT, "application/pdf")},
    )
    second = authed_client.post(
        "/api/upload-resource",
        files={"file": ("report.pdf", PDF_CONTENT, "application/pdf")},
    )

    assert second.status_code == 201
    assert second.json()["id"] != first.json()["id"]

    resources = authed_client.get("/api/resources").json()
    resource_ids = [r["id"] for r in resources]
    assert first.json()["id"] not in resource_ids
    assert second.json()["id"] in resource_ids
    assert len(resources) == 1


def test_two_users_uploading_same_filename_get_different_ids(client) -> None:
    token_a = client.post("/api/auth/anonymous-session").json()["access_token"]
    token_b = client.post("/api/auth/anonymous-session").json()["access_token"]

    response_a = client.post(
        "/api/upload-resource",
        files={"file": ("report.pdf", PDF_CONTENT, "application/pdf")},
        headers={"Authorization": f"Bearer {token_a}"},
    )
    response_b = client.post(
        "/api/upload-resource",
        files={"file": ("report.pdf", PDF_CONTENT, "application/pdf")},
        headers={"Authorization": f"Bearer {token_b}"},
    )

    assert response_a.json()["id"] != response_b.json()["id"]
