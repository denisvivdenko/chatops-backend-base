import hashlib
import secrets
import time
import uuid

import jwt

from chatops.domain.user import RefreshToken, User
from chatops.repositories.refresh_token_repository import RefreshTokenRepository
from chatops.repositories.user_repository import UserRepository
from chatops.settings import Settings


class InvalidAccessTokenError(Exception):
    pass


class InvalidRefreshTokenError(Exception):
    pass


class AuthService:
    def __init__(
        self,
        user_repository: UserRepository,
        refresh_token_repository: RefreshTokenRepository,
        settings: Settings,
    ) -> None:
        self._users = user_repository
        self._refresh_tokens = refresh_token_repository
        self._settings = settings

    def create_anonymous_session(self) -> tuple[str, str]:
        now = int(time.time() * 1000)
        user = User(id=str(uuid.uuid4()), is_anonymous=True, created_at=now)
        self._users.save_user(user)

        access_token = self._issue_access_token(user)
        refresh_token = self._issue_refresh_token(user)
        return access_token, refresh_token

    def refresh(self, refresh_token: str) -> tuple[str, str]:
        try:
            stored = self._refresh_tokens.fetch_token_by_hash(self._hash_token(refresh_token))
        except KeyError:
            raise InvalidRefreshTokenError()

        now = int(time.time() * 1000)
        if stored.revoked or stored.expires_at < now:
            raise InvalidRefreshTokenError()

        self._refresh_tokens.save_token(stored.model_copy(update={"revoked": True, "last_used_at": now}))

        user = self._users.fetch_user(stored.user_id)
        access_token = self._issue_access_token(user)
        new_refresh_token = self._issue_refresh_token(user)
        return access_token, new_refresh_token

    def verify_access_token(self, token: str) -> str:
        try:
            payload = jwt.decode(token, self._settings.jwt_secret, algorithms=[self._settings.jwt_algorithm])
        except jwt.InvalidTokenError as exc:
            raise InvalidAccessTokenError() from exc
        return payload["sub"]

    def _issue_access_token(self, user: User) -> str:
        now = int(time.time())
        payload = {
            "sub": user.id,
            "type": "anonymous" if user.is_anonymous else "registered",
            "iat": now,
            "exp": now + int(self._settings.access_token_ttl),
        }
        return jwt.encode(payload, self._settings.jwt_secret, algorithm=self._settings.jwt_algorithm)

    def _issue_refresh_token(self, user: User) -> str:
        raw_token = secrets.token_urlsafe(32)
        now = int(time.time() * 1000)
        token = RefreshToken(
            id=str(uuid.uuid4()),
            user_id=user.id,
            token_hash=self._hash_token(raw_token),
            expires_at=now + int(self._settings.refresh_token_ttl * 1000),
            revoked=False,
            created_at=now,
        )
        self._refresh_tokens.save_token(token)
        return raw_token

    @staticmethod
    def _hash_token(raw_token: str) -> str:
        return hashlib.sha256(raw_token.encode()).hexdigest()
