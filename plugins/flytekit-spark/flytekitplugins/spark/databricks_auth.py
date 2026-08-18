"""Authentication strategies for the Databricks connector.

PAT remains the default for backward compatibility. OAuth machine-to-machine
(M2M) authentication is opt-in through ``FLYTE_DATABRICKS_AUTH_TYPE`` or the
equivalent per-task setting.
"""

import asyncio
import json
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from flytekit import lazy_module
from flytekit.models.task import TaskTemplate

aiohttp = lazy_module("aiohttp")

logger = logging.getLogger(__name__)

FLYTE_DATABRICKS_AUTH_TYPE_ENV = "FLYTE_DATABRICKS_AUTH_TYPE"
FLYTE_DATABRICKS_OAUTH_SECRET_NAME_ENV = "FLYTE_DATABRICKS_OAUTH_SECRET_NAME"
FLYTE_DATABRICKS_OIDC_TOKEN_FILE_ENV = "FLYTE_DATABRICKS_OIDC_TOKEN_FILE"
FLYTE_DATABRICKS_OIDC_AUDIENCE_ENV = "FLYTE_DATABRICKS_OIDC_AUDIENCE"
DATABRICKS_CLIENT_ID_ENV = "DATABRICKS_CLIENT_ID"
DATABRICKS_CLIENT_SECRET_ENV = "DATABRICKS_CLIENT_SECRET"
AWS_WEB_IDENTITY_TOKEN_FILE_ENV = "AWS_WEB_IDENTITY_TOKEN_FILE"

DEFAULT_OAUTH_SECRET_NAME = "databricks-oauth"
DEFAULT_OIDC_AUDIENCE = "databricks"
DEFAULT_PROJECTED_SA_TOKEN_PATH = "/var/run/secrets/databricks/token"
TOKEN_REFRESH_BUFFER_SECONDS = 60
TOKEN_ENDPOINT_MAX_RETRIES = 3
TOKEN_ENDPOINT_BACKOFF_BASE_SECONDS = 0.2
VALID_AUTH_TYPES = {"pat", "oauth_m2m", "oidc_federation"}


class DatabricksAuthError(Exception):
    """Raised when Databricks authentication cannot be obtained."""


@dataclass
class _Settings:
    """Authentication settings resolved for one Databricks task."""

    task_template: Optional[TaskTemplate]
    auth_type: Optional[str]
    client_id: Optional[str]
    oauth_secret_name: str
    token_secret_name: Optional[str]
    oidc_token_file: Optional[str]
    oidc_audience: str
    namespace: Optional[str]

    @staticmethod
    def from_task(task_template: Optional[TaskTemplate], namespace: Optional[str]) -> "_Settings":
        custom: Dict[str, Any] = task_template.custom if task_template is not None else {}

        def _pick(task_key: str, env_key: Optional[str], default: Optional[str] = None) -> Optional[str]:
            task_value = custom.get(task_key)
            if task_value:
                return task_value
            if env_key:
                env_value = os.getenv(env_key)
                if env_value:
                    return env_value
            return default

        return _Settings(
            task_template=task_template,
            auth_type=_pick("databricksAuthType", FLYTE_DATABRICKS_AUTH_TYPE_ENV),
            client_id=_pick("databricksClientId", DATABRICKS_CLIENT_ID_ENV),
            oauth_secret_name=_pick(
                "databricksOauthSecret",
                FLYTE_DATABRICKS_OAUTH_SECRET_NAME_ENV,
                DEFAULT_OAUTH_SECRET_NAME,
            )
            or DEFAULT_OAUTH_SECRET_NAME,
            token_secret_name=custom.get("databricksTokenSecret"),
            oidc_token_file=_pick(
                "databricksOidcTokenFile",
                FLYTE_DATABRICKS_OIDC_TOKEN_FILE_ENV,
            ),
            oidc_audience=_pick(
                "databricksOidcAudience",
                FLYTE_DATABRICKS_OIDC_AUDIENCE_ENV,
                DEFAULT_OIDC_AUDIENCE,
            )
            or DEFAULT_OIDC_AUDIENCE,
            namespace=namespace,
        )


def _resolve_oidc_token_file(settings: _Settings) -> Optional[str]:
    """Resolve the first configured projected JWT file that exists."""
    candidates = (
        settings.oidc_token_file,
        os.getenv(AWS_WEB_IDENTITY_TOKEN_FILE_ENV),
        DEFAULT_PROJECTED_SA_TOKEN_PATH,
    )
    return next((path for path in candidates if path and os.path.exists(path)), None)


@dataclass
class _CachedToken:
    access_token: str
    refresh_at: float


class _TokenCache:
    """Async-safe in-memory cache for short-lived OAuth tokens."""

    def __init__(self) -> None:
        self._store: Dict[Tuple[str, str, str], _CachedToken] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: Tuple[str, str, str]) -> Optional[str]:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.refresh_at <= time.time():
                self._store.pop(key, None)
                return None
            return entry.access_token

    async def put(self, key: Tuple[str, str, str], access_token: str, expires_in: int) -> None:
        async with self._lock:
            refresh_after = max(int(expires_in) - TOKEN_REFRESH_BUFFER_SECONDS, 0)
            self._store[key] = _CachedToken(
                access_token=access_token,
                refresh_at=time.time() + refresh_after,
            )

    async def invalidate(self, key: Tuple[str, str, str]) -> None:
        async with self._lock:
            self._store.pop(key, None)


_TOKEN_CACHE = _TokenCache()


async def _post_token(
    session: "aiohttp.ClientSession",  # type: ignore[name-defined]
    workspace_url: str,
    form: Dict[str, str],
) -> Dict[str, Any]:
    """Request a Databricks OAuth token with bounded transient retries."""
    url = f"https://{workspace_url.rstrip('/')}/oidc/v1/token"
    last_error: Optional[str] = None

    for attempt in range(TOKEN_ENDPOINT_MAX_RETRIES):
        try:
            async with session.post(
                url,
                data=form,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as response:
                body = await response.text()
                if response.status == 200:
                    try:
                        return json.loads(body)
                    except json.JSONDecodeError as error:
                        raise DatabricksAuthError(
                            f"Databricks token endpoint returned invalid JSON: {error}"
                        ) from error
                if response.status in (429, 500, 502, 503, 504):
                    last_error = f"HTTP {response.status}: {body[:500]}"
                else:
                    raise DatabricksAuthError(
                        f"Databricks token endpoint returned HTTP {response.status}: {body[:500]}"
                    )
        except aiohttp.ClientError as error:  # type: ignore[attr-defined]
            last_error = f"network error: {error}"
        except asyncio.TimeoutError:
            last_error = "timeout"

        if attempt < TOKEN_ENDPOINT_MAX_RETRIES - 1:
            delay = TOKEN_ENDPOINT_BACKOFF_BASE_SECONDS * (2**attempt) + random.uniform(0, 0.1)
            await asyncio.sleep(delay)

    raise DatabricksAuthError(
        f"Databricks token endpoint failed after {TOKEN_ENDPOINT_MAX_RETRIES} attempts: {last_error}"
    )


class DatabricksAuth(ABC):
    """Interface for obtaining a bearer token for Databricks API calls."""

    auth_type = "unknown"
    strategy_name = "DatabricksAuth"

    def __init__(self, workspace_url: str, settings: _Settings):
        self.workspace_url = workspace_url
        self.settings = settings

    @abstractmethod
    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        """Return a bearer token for a Databricks API request."""

    async def invalidate_cache(self) -> None:
        """Invalidate cached authentication state, if any."""

    def describe(self) -> str:
        """Return a description that is safe to write to connector logs."""
        return (
            f"strategy={self.strategy_name} auth_type={self.auth_type} " f"namespace={self.settings.namespace or 'N/A'}"
        )


class PATAuth(DatabricksAuth):
    """Delegate to the connector's existing multi-tenant PAT lookup."""

    auth_type = "pat"
    strategy_name = "PATAuth"

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        from .connector import get_databricks_token

        return get_databricks_token(
            namespace=self.settings.namespace,
            task_template=self.settings.task_template,
            secret_name=self.settings.token_secret_name,
        )


class OAuthM2MAuth(DatabricksAuth):
    """Authenticate a Databricks service principal with client credentials."""

    auth_type = "oauth_m2m"
    strategy_name = "OAuthM2MAuth"

    @property
    def cache_key(self) -> Tuple[str, str, str]:
        return (
            self.workspace_url,
            self.settings.client_id or "",
            self.settings.namespace or "_",
        )

    def _resolve_credentials(self) -> Tuple[str, str]:
        from .connector import get_secret_from_k8s

        client_id = self.settings.client_id
        client_secret: Optional[str] = None

        if self.settings.namespace:
            secret_client_id = get_secret_from_k8s(
                secret_name=self.settings.oauth_secret_name,
                secret_key="client_id",
                namespace=self.settings.namespace,
            )
            secret_client_secret = get_secret_from_k8s(
                secret_name=self.settings.oauth_secret_name,
                secret_key="client_secret",
                namespace=self.settings.namespace,
            )
            if secret_client_id:
                client_id = secret_client_id
            if secret_client_secret:
                client_secret = secret_client_secret

        client_id = client_id or os.getenv(DATABRICKS_CLIENT_ID_ENV)
        client_secret = client_secret or os.getenv(DATABRICKS_CLIENT_SECRET_ENV)

        if not client_id:
            raise DatabricksAuthError(
                "OAuth M2M requires a client ID. Configure databricks_client_id, "
                "DATABRICKS_CLIENT_ID, or the namespace OAuth secret."
            )
        if not client_secret:
            raise DatabricksAuthError(
                "OAuth M2M requires a client secret. Configure DATABRICKS_CLIENT_SECRET "
                "or the namespace OAuth secret."
            )
        return client_id, client_secret

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        client_id, client_secret = self._resolve_credentials()
        key = (self.workspace_url, client_id, self.settings.namespace or "_")
        cached = await _TOKEN_CACHE.get(key)
        if cached:
            return cached

        payload = await _post_token(
            session,
            self.workspace_url,
            {
                "grant_type": "client_credentials",
                "scope": "all-apis",
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )
        access_token = payload.get("access_token")
        if not access_token:
            raise DatabricksAuthError("Databricks OAuth response did not contain access_token")
        await _TOKEN_CACHE.put(key, access_token, int(payload.get("expires_in", 3600)))
        return access_token

    async def invalidate_cache(self) -> None:
        client_id, _ = self._resolve_credentials()
        await _TOKEN_CACHE.invalidate((self.workspace_url, client_id, self.settings.namespace or "_"))


class OIDCConnectorAuth(DatabricksAuth):
    """Exchange the connector workload's projected JWT for a Databricks token."""

    auth_type = "oidc_federation"
    strategy_name = "OIDCConnectorAuth"

    def _client_id(self) -> str:
        client_id = self.settings.client_id or os.getenv(DATABRICKS_CLIENT_ID_ENV)
        if not client_id:
            raise DatabricksAuthError(
                "OIDC federation requires a client ID. Configure databricks_client_id " "or DATABRICKS_CLIENT_ID."
            )
        return client_id

    def _subject_token_file(self) -> str:
        token_file = _resolve_oidc_token_file(self.settings)
        if not token_file:
            raise DatabricksAuthError(
                "OIDC federation requires a projected JWT file. Configure "
                "databricks_oidc_token_file, FLYTE_DATABRICKS_OIDC_TOKEN_FILE, "
                "or AWS_WEB_IDENTITY_TOKEN_FILE."
            )
        return token_file

    def _cache_key(self, client_id: str) -> Tuple[str, str, str]:
        return (
            self.workspace_url,
            client_id,
            f"oidc:{self.settings.oidc_audience}",
        )

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        client_id = self._client_id()
        key = self._cache_key(client_id)
        cached = await _TOKEN_CACHE.get(key)
        if cached:
            return cached

        token_file = self._subject_token_file()
        try:
            with open(token_file) as token_stream:
                subject_token = token_stream.read().strip()
        except OSError as error:
            raise DatabricksAuthError(f"Unable to read projected OIDC token file '{token_file}': {error}") from error
        if not subject_token:
            raise DatabricksAuthError(f"Projected OIDC token file '{token_file}' is empty")

        payload = await _post_token(
            session,
            self.workspace_url,
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "scope": "all-apis",
                "client_id": client_id,
                "subject_token": subject_token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            },
        )
        access_token = payload.get("access_token")
        if not access_token:
            raise DatabricksAuthError("Databricks OIDC response did not contain access_token")
        await _TOKEN_CACHE.put(key, access_token, int(payload.get("expires_in", 3600)))
        return access_token

    async def invalidate_cache(self) -> None:
        await _TOKEN_CACHE.invalidate(self._cache_key(self._client_id()))


async def select_auth(
    task_template: Optional[TaskTemplate],
    workspace_url: str,
    namespace: Optional[str],
) -> DatabricksAuth:
    """Select an explicitly configured strategy, defaulting to PAT."""
    settings = _Settings.from_task(task_template, namespace)
    auth_type = settings.auth_type or "pat"
    if auth_type not in VALID_AUTH_TYPES:
        raise DatabricksAuthError(
            f"Invalid Databricks auth type '{auth_type}'. Expected one of {sorted(VALID_AUTH_TYPES)}."
        )
    if auth_type == "oauth_m2m":
        return OAuthM2MAuth(workspace_url, settings)
    if auth_type == "oidc_federation":
        return OIDCConnectorAuth(workspace_url, settings)
    return PATAuth(workspace_url, settings)


def build_auth(
    workspace_url: str,
    auth_type: str,
    namespace: Optional[str] = None,
    client_id: Optional[str] = None,
    oauth_secret_name: Optional[str] = None,
    oidc_token_file: Optional[str] = None,
    oidc_audience: Optional[str] = None,
) -> DatabricksAuth:
    """Rebuild an auth strategy from persisted connector metadata."""
    settings = _Settings(
        task_template=None,
        auth_type=auth_type,
        client_id=client_id,
        oauth_secret_name=oauth_secret_name or DEFAULT_OAUTH_SECRET_NAME,
        token_secret_name=None,
        oidc_token_file=oidc_token_file,
        oidc_audience=oidc_audience or DEFAULT_OIDC_AUDIENCE,
        namespace=namespace,
    )
    if auth_type == "oauth_m2m":
        return OAuthM2MAuth(workspace_url, settings)
    if auth_type == "pat":
        return PATAuth(workspace_url, settings)
    if auth_type == "oidc_federation":
        return OIDCConnectorAuth(workspace_url, settings)
    raise DatabricksAuthError(
        f"Invalid Databricks auth type '{auth_type}'. Expected one of {sorted(VALID_AUTH_TYPES)}."
    )


def validate_connector_config() -> None:
    """Validate an explicitly configured connector-wide auth type."""
    auth_type = os.getenv(FLYTE_DATABRICKS_AUTH_TYPE_ENV)
    if auth_type and auth_type not in VALID_AUTH_TYPES:
        raise DatabricksAuthError(
            f"Invalid {FLYTE_DATABRICKS_AUTH_TYPE_ENV}='{auth_type}'. " f"Expected one of {sorted(VALID_AUTH_TYPES)}."
        )
