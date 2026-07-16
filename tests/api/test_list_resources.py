PDF_CONTENT = b"%PDF-1.4\n%mock pdf content"


def test_list_resources_empty_for_user_with_no_uploads(authed_client) -> None:
    response = authed_client.get("/api/resources")

    assert response.status_code == 200
    assert response.json() == []


def test_list_resources_returns_only_calling_users_resources(client) -> None:
    token_a = client.post("/api/auth/anonymous-session").json()["access_token"]
    token_b = client.post("/api/auth/anonymous-session").json()["access_token"]

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
    authed_client.post("/api/upload-resource", files={"file": ("first.pdf", PDF_CONTENT, "application/pdf")})
    authed_client.post("/api/upload-resource", files={"file": ("second.pdf", PDF_CONTENT, "application/pdf")})

    resources = authed_client.get("/api/resources").json()

    assert [r["filename"] for r in resources] == ["second.pdf", "first.pdf"]


def test_list_resources_without_token_is_rejected(client) -> None:
    response = client.get("/api/resources")

    assert response.status_code == 401
