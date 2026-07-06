def test_anonymous_session_returns_access_token(client):
    response = client.post("/api/auth/anonymous-session")

    assert response.status_code == 201
    body = response.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"


def test_anonymous_session_sets_refresh_cookie(client):
    response = client.post("/api/auth/anonymous-session")

    assert response.cookies.get("refresh_token") is not None
    assert "HttpOnly" in response.headers["set-cookie"]


def test_two_anonymous_sessions_get_different_access_tokens(client):
    token1 = client.post("/api/auth/anonymous-session").json()["access_token"]
    token2 = client.post("/api/auth/anonymous-session").json()["access_token"]

    assert token1 != token2
