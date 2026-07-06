from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from chatops.api.dependencies import AuthServiceDep, SettingsDep
from chatops.services.auth_service import InvalidRefreshTokenError


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


router = APIRouter(prefix="/api/auth")


def _set_refresh_cookie(response: Response, request: Request, settings, refresh_token: str) -> None:
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="lax",
        max_age=int(settings.refresh_token_ttl),
        path="/api/auth",
    )


@router.post("/anonymous-session", status_code=201, response_model=TokenResponse)
def create_anonymous_session(
    request: Request,
    response: Response,
    auth_service: AuthServiceDep,
    settings: SettingsDep,
) -> TokenResponse:
    access_token, refresh_token = auth_service.create_anonymous_session()
    _set_refresh_cookie(response, request, settings, refresh_token)
    return TokenResponse(access_token=access_token)


@router.post("/refresh", response_model=TokenResponse)
def refresh(
    request: Request,
    response: Response,
    auth_service: AuthServiceDep,
    settings: SettingsDep,
) -> TokenResponse:
    refresh_token = request.cookies.get("refresh_token")
    if refresh_token is None:
        raise HTTPException(status_code=401, detail="unauthorized")

    try:
        access_token, new_refresh_token = auth_service.refresh(refresh_token)
    except InvalidRefreshTokenError:
        raise HTTPException(status_code=401, detail="unauthorized")

    _set_refresh_cookie(response, request, settings, new_refresh_token)
    return TokenResponse(access_token=access_token)
