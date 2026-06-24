"""Databricks authentication strategies for the flytekit-spark connector.

This module centralises Databricks auth behind a small strategy abstraction. Three
user-visible auth types are supported, all of which end up producing an
``Authorization: Bearer <token>`` header for the Databricks Jobs API:

- ``pat`` - Personal Access Token (legacy, long-lived). Unchanged from the
  upstream multi-tenant PAT flow: the token is read from a cross-namespace k8s
  secret and/or the connector's ``FLYTE_DATABRICKS_ACCESS_TOKEN`` env var.
- ``oauth_m2m`` - OAuth Service Principal, ``client_credentials`` grant. Reads
  ``client_id`` / ``client_secret`` from a k8s secret in the workflow namespace
  (default name ``databricks-oauth``) or from connector env vars.
- ``oidc_federation`` - OAuth Workload Identity Federation, ``token-exchange``
  grant. Two flavours, dispatched automatically:

  - **Model 2** (per-workflow-namespace identity): if the workflow namespace
    contains a ``ServiceAccount`` labelled ``flyte.org/databricks-enabled=true``
    and annotated with ``flyte.org/databricks-client-id``, the connector mints
    a fresh JWT for that SA via Kubernetes ``TokenRequest`` and exchanges it
    for a token issued to the SP named in the annotation. Each namespace can
    federate to a different Databricks Service Principal.
  - **Model 1** (single connector identity): fallback when no annotated SA is
    discovered. Uses the connector pod's own projected OIDC JWT
    (``AWS_WEB_IDENTITY_TOKEN_FILE`` from EKS IRSA, or an equivalent path)
    and the connector-level ``DATABRICKS_CLIENT_ID``.

Every setting follows a consistent resolution order::

    task config field  ->  FLYTE_DATABRICKS_* env var  ->  well-known default
"""

import asyncio
import json as _json
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


# --------------------------------------------------------------------------- #
# Env-var names (public so operators can grep for them)                       #
# --------------------------------------------------------------------------- #
FLYTE_DATABRICKS_AUTH_TYPE_ENV = "FLYTE_DATABRICKS_AUTH_TYPE"
FLYTE_DATABRICKS_OAUTH_SECRET_NAME_ENV = "FLYTE_DATABRICKS_OAUTH_SECRET_NAME"
FLYTE_DATABRICKS_OIDC_TOKEN_FILE_ENV = "FLYTE_DATABRICKS_OIDC_TOKEN_FILE"
FLYTE_DATABRICKS_OIDC_AUDIENCE_ENV = "FLYTE_DATABRICKS_OIDC_AUDIENCE"
FLYTE_DATABRICKS_TOKEN_SECRET_NAME_ENV = "FLYTE_DATABRICKS_TOKEN_SECRET_NAME"
DATABRICKS_CLIENT_ID_ENV = "DATABRICKS_CLIENT_ID"
DATABRICKS_CLIENT_SECRET_ENV = "DATABRICKS_CLIENT_SECRET"
AWS_WEB_IDENTITY_TOKEN_FILE_ENV = "AWS_WEB_IDENTITY_TOKEN_FILE"

# --------------------------------------------------------------------------- #
# OIDC Model 2 discovery: SA annotation/label keys (community convention)     #
# --------------------------------------------------------------------------- #
# A ServiceAccount in the workflow namespace becomes a Model 2 federation
# identity by carrying:
#   labels:      flyte.org/databricks-enabled: "true"
#   annotations: flyte.org/databricks-client-id: "<sp-application-id-uuid>"
#                flyte.org/databricks-audience: "databricks"   # optional
LABEL_DATABRICKS_ENABLED = "flyte.org/databricks-enabled"
ANNOTATION_DATABRICKS_CLIENT_ID = "flyte.org/databricks-client-id"
ANNOTATION_DATABRICKS_AUDIENCE = "flyte.org/databricks-audience"
DATABRICKS_ENABLED_LABEL_SELECTOR = f"{LABEL_DATABRICKS_ENABLED}=true"

# --------------------------------------------------------------------------- #
# Defaults                                                                    #
# --------------------------------------------------------------------------- #
DEFAULT_OAUTH_SECRET_NAME = "databricks-oauth"
DEFAULT_TOKEN_SECRET_NAME = "databricks-token"
DEFAULT_OIDC_AUDIENCE = "databricks"
DEFAULT_PROJECTED_SA_TOKEN_PATH = "/var/run/secrets/databricks/token"

TOKEN_REFRESH_BUFFER_SECONDS = 60
TOKEN_ENDPOINT_MAX_RETRIES = 3
TOKEN_ENDPOINT_BACKOFF_BASE_SECONDS = 0.2

# Per-namespace SA discovery cache: avoid listing SAs on every create() call.
NS_DISCOVERY_CACHE_TTL_SECONDS = 300

VALID_AUTH_TYPES = {"pat", "oauth_m2m", "oidc_federation"}


class DatabricksAuthError(Exception):
    """Raised when Databricks authentication cannot be obtained."""


# =========================================================================== #
# Settings                                                                    #
# =========================================================================== #


@dataclass
class _Settings:
    """Resolved auth settings for a single task.

    Resolution for each field is ``task cfg`` -> ``connector env`` -> ``default``.

    For OIDC Model 2 the workflow-namespace SA name and per-namespace ``client_id``
    do not appear here - they are auto-discovered at submit time from labelled SA
    annotations in the workflow namespace (see :class:`_DiscoveredOIDCConfig`).
    """

    auth_type: Optional[str]
    client_id: Optional[str]
    oauth_secret_name: str
    token_secret_name: str
    oidc_token_file: Optional[str]
    oidc_audience: str
    namespace: Optional[str]

    @staticmethod
    def from_task(task_template: Optional[TaskTemplate], namespace: Optional[str]) -> "_Settings":
        custom: Dict[str, Any] = task_template.custom if task_template is not None else {}

        def _pick(task_key: str, env_key: Optional[str], default: Optional[str] = None) -> Optional[str]:
            v = custom.get(task_key)
            if v:
                return v
            if env_key:
                env_v = os.getenv(env_key)
                if env_v:
                    return env_v
            return default

        return _Settings(
            auth_type=_pick("databricksAuthType", FLYTE_DATABRICKS_AUTH_TYPE_ENV),
            client_id=_pick("databricksClientId", DATABRICKS_CLIENT_ID_ENV),
            oauth_secret_name=_pick(
                "databricksOauthSecret", FLYTE_DATABRICKS_OAUTH_SECRET_NAME_ENV, DEFAULT_OAUTH_SECRET_NAME
            )
            or DEFAULT_OAUTH_SECRET_NAME,
            token_secret_name=_pick(
                "databricksTokenSecret", FLYTE_DATABRICKS_TOKEN_SECRET_NAME_ENV, DEFAULT_TOKEN_SECRET_NAME
            )
            or DEFAULT_TOKEN_SECRET_NAME,
            oidc_token_file=_pick("databricksOidcTokenFile", FLYTE_DATABRICKS_OIDC_TOKEN_FILE_ENV),
            oidc_audience=_pick("databricksOidcAudience", FLYTE_DATABRICKS_OIDC_AUDIENCE_ENV, DEFAULT_OIDC_AUDIENCE)
            or DEFAULT_OIDC_AUDIENCE,
            namespace=namespace,
        )


@dataclass
class _DiscoveredOIDCConfig:
    """A federation identity discovered from an annotated workflow-namespace SA."""

    service_account: str
    client_id: str
    audience: str


def _resolve_oidc_token_file(settings: _Settings) -> Optional[str]:
    """Return the first subject-JWT file path that exists, or ``None``.

    Order: explicit cfg/env override -> IRSA-injected path -> well-known projected SA path.
    """
    if settings.oidc_token_file and os.path.exists(settings.oidc_token_file):
        return settings.oidc_token_file
    irsa = os.getenv(AWS_WEB_IDENTITY_TOKEN_FILE_ENV)
    if irsa and os.path.exists(irsa):
        return irsa
    if os.path.exists(DEFAULT_PROJECTED_SA_TOKEN_PATH):
        return DEFAULT_PROJECTED_SA_TOKEN_PATH
    return None


# =========================================================================== #
# Token cache                                                                 #
# =========================================================================== #


@dataclass
class _CachedToken:
    access_token: str
    expires_at_unix: float


class _TokenCache:
    """Async-safe in-memory cache of Databricks bearer tokens.

    Keys are ``(workspace_url, client_id, subject_identity)``. Entries expire
    ``TOKEN_REFRESH_BUFFER_SECONDS`` early so callers never receive a token
    that is about to expire.
    """

    def __init__(self) -> None:
        self._store: Dict[Tuple[str, str, str], _CachedToken] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _now() -> float:
        return time.time()

    async def get(self, key: Tuple[str, str, str]) -> Optional[str]:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.expires_at_unix - self._now() < TOKEN_REFRESH_BUFFER_SECONDS:
                self._store.pop(key, None)
                return None
            return entry.access_token

    async def put(self, key: Tuple[str, str, str], access_token: str, expires_in: int) -> None:
        async with self._lock:
            self._store[key] = _CachedToken(
                access_token=access_token,
                expires_at_unix=self._now() + max(int(expires_in) - TOKEN_REFRESH_BUFFER_SECONDS, 30),
            )

    async def invalidate(self, key: Tuple[str, str, str]) -> None:
        async with self._lock:
            self._store.pop(key, None)


# One cache per connector pod.
_TOKEN_CACHE = _TokenCache()


# =========================================================================== #
# Per-namespace OIDC discovery cache                                          #
# =========================================================================== #


@dataclass
class _CachedDiscovery:
    config: Optional[_DiscoveredOIDCConfig]  # ``None`` is a valid cached "nothing here" answer
    expires_at_unix: float


class _NSDiscoveryCache:
    """TTL-bounded cache of per-namespace OIDC SA discovery results.

    Caches both hits and misses (a namespace with no annotated SA stays a miss for the
    TTL duration). Operators who change SA annotations and want immediate effect can
    restart the connector pod.
    """

    def __init__(self) -> None:
        self._store: Dict[str, _CachedDiscovery] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _now() -> float:
        return time.time()

    async def get(self, namespace: str) -> Tuple[bool, Optional[_DiscoveredOIDCConfig]]:
        """Return ``(is_cached, config_or_None)``."""
        async with self._lock:
            entry = self._store.get(namespace)
            if entry is None or entry.expires_at_unix <= self._now():
                self._store.pop(namespace, None)
                return False, None
            return True, entry.config

    async def put(self, namespace: str, config: Optional[_DiscoveredOIDCConfig]) -> None:
        async with self._lock:
            self._store[namespace] = _CachedDiscovery(
                config=config,
                expires_at_unix=self._now() + NS_DISCOVERY_CACHE_TTL_SECONDS,
            )

    async def invalidate(self, namespace: str) -> None:
        async with self._lock:
            self._store.pop(namespace, None)


_NS_DISCOVERY_CACHE = _NSDiscoveryCache()


# =========================================================================== #
# Low-level helpers: Databricks token endpoint + Kubernetes TokenRequest      #
# =========================================================================== #


async def _post_oidc_token(
    session: "aiohttp.ClientSession",  # type: ignore[name-defined]
    workspace_url: str,
    form: Dict[str, str],
) -> Dict[str, Any]:
    """POST to ``https://<workspace>/oidc/v1/token`` with retries.

    Retries 429/500/502/503/504 and transient network errors with exponential
    backoff plus jitter. Fails fast on 400/401/403/404 and surfaces the
    server-provided error body (minus any secrets, which the body never
    contains for this endpoint).
    """
    url = f"https://{workspace_url.rstrip('/')}/oidc/v1/token"
    last_err: Optional[str] = None
    for attempt in range(TOKEN_ENDPOINT_MAX_RETRIES):
        try:
            async with session.post(
                url,
                data=form,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as resp:
                body_text = await resp.text()
                if resp.status == 200:
                    try:
                        return _json.loads(body_text)
                    except Exception as e:
                        raise DatabricksAuthError(f"Databricks token endpoint returned 200 but body was not JSON: {e}")
                if resp.status in (429, 500, 502, 503, 504):
                    last_err = f"HTTP {resp.status}: {body_text[:500]}"
                else:
                    raise DatabricksAuthError(
                        f"Databricks token endpoint returned HTTP {resp.status}: {body_text[:500]}"
                    )
        except aiohttp.ClientError as e:  # type: ignore[attr-defined]
            last_err = f"network error: {e}"
        except asyncio.TimeoutError:
            last_err = "timeout"
        if attempt < TOKEN_ENDPOINT_MAX_RETRIES - 1:
            await asyncio.sleep(TOKEN_ENDPOINT_BACKOFF_BASE_SECONDS * (2**attempt) + random.uniform(0, 0.1))
    raise DatabricksAuthError(
        f"Databricks token endpoint failed after {TOKEN_ENDPOINT_MAX_RETRIES} attempts: {last_err}"
    )


def _discover_namespace_oidc_sa_sync(namespace: str, default_audience: str) -> Optional[_DiscoveredOIDCConfig]:
    """Look for a labelled+annotated ServiceAccount in ``namespace`` (synchronous).

    Returns the discovered config when exactly one SA in the namespace carries
    ``flyte.org/databricks-enabled=true`` and a non-empty
    ``flyte.org/databricks-client-id`` annotation. Returns ``None`` when zero
    matches are found (caller decides whether to fall back to Model 1). Raises
    :class:`DatabricksAuthError` when the namespace contains more than one
    candidate SA - silent "first wins" would mask configuration mistakes.
    """
    from .connector import list_serviceaccounts_in_k8s  # lazy: avoid import cycle

    sas = list_serviceaccounts_in_k8s(namespace=namespace, label_selector=DATABRICKS_ENABLED_LABEL_SELECTOR)

    matches: list = []
    for sa in sas:
        annotations = (sa.metadata.annotations or {}) if sa.metadata is not None else {}
        client_id = (annotations.get(ANNOTATION_DATABRICKS_CLIENT_ID) or "").strip()
        if not client_id:
            continue
        audience = (annotations.get(ANNOTATION_DATABRICKS_AUDIENCE) or "").strip() or default_audience
        matches.append(
            _DiscoveredOIDCConfig(
                service_account=sa.metadata.name,
                client_id=client_id,
                audience=audience,
            )
        )

    if not matches:
        return None
    if len(matches) > 1:
        names = ", ".join(sorted(m.service_account for m in matches))
        raise DatabricksAuthError(
            f"Ambiguous OIDC federation configuration in namespace '{namespace}': "
            f"multiple ServiceAccounts carry both the '{LABEL_DATABRICKS_ENABLED}=true' label "
            f"and a '{ANNOTATION_DATABRICKS_CLIENT_ID}' annotation: [{names}]. "
            "Annotate exactly one SA per namespace."
        )
    return matches[0]


async def _discover_namespace_oidc_sa(namespace: str, default_audience: str) -> Optional[_DiscoveredOIDCConfig]:
    """TTL-cached async wrapper around :func:`_discover_namespace_oidc_sa_sync`."""
    cached, value = await _NS_DISCOVERY_CACHE.get(namespace)
    if cached:
        return value
    loop = asyncio.get_event_loop()
    discovered = await loop.run_in_executor(None, _discover_namespace_oidc_sa_sync, namespace, default_audience)
    await _NS_DISCOVERY_CACHE.put(namespace, discovered)
    return discovered


def _request_sa_token(namespace: str, service_account: str, audience: str, expiration_seconds: int = 3600) -> str:
    """Mint a JWT for ``namespace/service_account`` via the Kubernetes TokenRequest API.

    Required RBAC on the connector's ServiceAccount (sample in the plugin README)::

        apiGroups: [""]
        resources: ["serviceaccounts/token"]
        verbs: ["create"]
    """
    try:
        from kubernetes import client, config
        from kubernetes.client.exceptions import ApiException
    except ImportError as e:
        raise DatabricksAuthError(
            f"kubernetes python client required for OIDC Model 2 (annotated workflow-namespace SA) but not installed: {e}"
        )
    try:
        config.load_incluster_config()
    except Exception:
        try:
            config.load_kube_config()
        except Exception as e:
            raise DatabricksAuthError(f"unable to load Kubernetes config for TokenRequest: {e}")
    v1 = client.CoreV1Api()
    body = client.AuthenticationV1TokenRequest(
        spec=client.V1TokenRequestSpec(audiences=[audience], expiration_seconds=expiration_seconds)
    )
    try:
        resp = v1.create_namespaced_service_account_token(name=service_account, namespace=namespace, body=body)
        return resp.status.token
    except ApiException as e:
        if e.status == 404:
            raise DatabricksAuthError(
                f"ServiceAccount '{namespace}/{service_account}' not found. "
                "The discovery cache is stale - the SA was annotated but has since been deleted. "
                "Re-create the SA or remove the 'flyte.org/databricks-enabled' label."
            )
        if e.status == 403:
            raise DatabricksAuthError(
                f"Forbidden creating a token for '{namespace}/{service_account}'. "
                "The connector's ServiceAccount needs RBAC: verbs=['create'] on resources=['serviceaccounts/token']. "
                "See the 'Authentication' section of the flytekit-spark README for a sample ClusterRole."
            )
        raise DatabricksAuthError(f"Kubernetes TokenRequest for '{namespace}/{service_account}' failed: {e}")


# =========================================================================== #
# Strategy interface + concrete strategies                                    #
# =========================================================================== #


class DatabricksAuth(ABC):
    """Abstract base class for Databricks auth strategies."""

    auth_type: str = "unknown"
    strategy_name: str = "DatabricksAuth"

    def __init__(self, workspace_url: str, settings: _Settings):
        self.workspace_url = workspace_url
        self.settings = settings

    @abstractmethod
    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        ...

    @property
    @abstractmethod
    def cache_key(self) -> Tuple[str, str, str]: ...

    async def invalidate_cache(self) -> None:
        await _TOKEN_CACHE.invalidate(self.cache_key)

    def describe(self) -> str:
        """One-line human-readable description, safe to log (no secrets)."""
        cid = self.settings.client_id or "N/A"
        masked = f"{cid[:4]}...{cid[-4:]}" if len(cid) > 12 else cid
        return (
            f"strategy={self.strategy_name} auth_type={self.auth_type} "
            f"client_id={masked} audience={self.settings.oidc_audience} "
            f"namespace={self.settings.namespace or 'N/A'}"
        )


class PATAuth(DatabricksAuth):
    """Personal Access Token: delegates to the existing multi-tenant PAT flow."""

    auth_type = "pat"
    strategy_name = "PATAuth"

    @property
    def cache_key(self) -> Tuple[str, str, str]:
        return (self.workspace_url, "pat", self.settings.namespace or "_")

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        # Lazy import to avoid a circular dependency with connector.py.
        from .connector import get_databricks_token

        return get_databricks_token(
            namespace=self.settings.namespace,
            secret_name=self.settings.token_secret_name,
        )

    async def invalidate_cache(self) -> None:
        # PATs are long-lived; a 401 is a misconfig, not a stale-cache issue.
        return


class OAuthM2MAuth(DatabricksAuth):
    """OAuth M2M (``grant_type=client_credentials``)."""

    auth_type = "oauth_m2m"
    strategy_name = "OAuthM2MAuth"

    @property
    def cache_key(self) -> Tuple[str, str, str]:
        return (
            self.workspace_url,
            self.settings.client_id or "",
            f"m2m:{self.settings.namespace or '_'}",
        )

    def _resolve_creds(self) -> Tuple[str, str]:
        from .connector import get_secret_from_k8s  # lazy import for the same reason as above

        client_id = self.settings.client_id
        client_secret: Optional[str] = None

        if self.settings.namespace:
            cid_from_secret = get_secret_from_k8s(
                secret_name=self.settings.oauth_secret_name,
                secret_key="client_id",
                namespace=self.settings.namespace,
            )
            csecret_from_secret = get_secret_from_k8s(
                secret_name=self.settings.oauth_secret_name,
                secret_key="client_secret",
                namespace=self.settings.namespace,
            )
            if cid_from_secret:
                client_id = client_id or cid_from_secret
            if csecret_from_secret:
                client_secret = csecret_from_secret

        if client_id is None:
            client_id = os.getenv(DATABRICKS_CLIENT_ID_ENV)
        if client_secret is None:
            client_secret = os.getenv(DATABRICKS_CLIENT_SECRET_ENV)

        if not client_id:
            raise DatabricksAuthError(
                "OAuth M2M selected but no client_id found. Set databricks_client_id on the task, "
                f"{DATABRICKS_CLIENT_ID_ENV} on the connector, or add 'client_id' to the "
                f"'{self.settings.oauth_secret_name}' k8s secret in the workflow namespace."
            )
        if not client_secret:
            raise DatabricksAuthError(
                "OAuth M2M selected but no client_secret found. Set "
                f"{DATABRICKS_CLIENT_SECRET_ENV} on the connector, or add 'client_secret' to the "
                f"'{self.settings.oauth_secret_name}' k8s secret in the workflow namespace "
                f"('{self.settings.namespace or 'N/A'}')."
            )
        return client_id, client_secret

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        cached = await _TOKEN_CACHE.get(self.cache_key)
        if cached is not None:
            return cached
        client_id, client_secret = self._resolve_creds()
        payload = await _post_oidc_token(
            session,
            self.workspace_url,
            form={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "all-apis",
            },
        )
        access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        if not access_token:
            raise DatabricksAuthError(f"Databricks M2M token response missing 'access_token': {payload}")
        await _TOKEN_CACHE.put(self.cache_key, access_token, expires_in)
        return access_token


class OIDCConnectorIRSAAuth(DatabricksAuth):
    """OIDC Federation Model 1: exchanges the connector pod's own projected JWT."""

    auth_type = "oidc_federation"
    strategy_name = "OIDCConnectorIRSAAuth"

    @property
    def cache_key(self) -> Tuple[str, str, str]:
        return (self.workspace_url, self.settings.client_id or "", "irsa:connector")

    def _read_subject_jwt(self) -> str:
        path = _resolve_oidc_token_file(self.settings)
        if not path:
            raise DatabricksAuthError(
                "OIDC federation selected but no subject token file found. Looked in "
                "databricks_oidc_token_file task cfg, FLYTE_DATABRICKS_OIDC_TOKEN_FILE env, "
                "AWS_WEB_IDENTITY_TOKEN_FILE env, and "
                f"{DEFAULT_PROJECTED_SA_TOKEN_PATH}. Configure IRSA on the connector pod or mount a "
                "projected ServiceAccount token."
            )
        try:
            with open(path, "r") as f:
                return f.read().strip()
        except OSError as e:
            raise DatabricksAuthError(f"Failed to read OIDC subject token from {path}: {e}")

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        cached = await _TOKEN_CACHE.get(self.cache_key)
        if cached is not None:
            return cached
        if not self.settings.client_id:
            raise DatabricksAuthError(
                "OIDC federation selected but no client_id. Set databricks_client_id on the task "
                f"or {DATABRICKS_CLIENT_ID_ENV} on the connector."
            )
        # Re-read on every refresh: projected/IRSA tokens rotate.
        subject_jwt = self._read_subject_jwt()
        payload = await _post_oidc_token(
            session,
            self.workspace_url,
            form={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "subject_token": subject_jwt,
                "client_id": self.settings.client_id,
                "scope": "all-apis",
            },
        )
        access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        if not access_token:
            raise DatabricksAuthError(f"Databricks OIDC token response missing 'access_token': {payload}")
        await _TOKEN_CACHE.put(self.cache_key, access_token, expires_in)
        return access_token


class OIDCNamespaceSAAuth(DatabricksAuth):
    """OIDC Federation Model 2: mints a JWT for a workflow-namespace SA via TokenRequest.

    The (service_account, client_id, audience) tuple is supplied via :class:`_DiscoveredOIDCConfig`
    rather than read from :class:`_Settings` - it is auto-discovered from the workflow
    namespace's annotated SA in :func:`select_auth`, or rebuilt from persisted metadata in
    :func:`build_auth`.
    """

    auth_type = "oidc_federation"
    strategy_name = "OIDCNamespaceSAAuth"

    def __init__(self, workspace_url: str, settings: _Settings, discovered: _DiscoveredOIDCConfig):
        super().__init__(workspace_url, settings)
        self.discovered = discovered

    @property
    def cache_key(self) -> Tuple[str, str, str]:
        ns = self.settings.namespace or "_"
        return (
            self.workspace_url,
            self.discovered.client_id,
            f"sa:{ns}/{self.discovered.service_account}",
        )

    def describe(self) -> str:
        cid = self.discovered.client_id
        masked = f"{cid[:4]}...{cid[-4:]}" if len(cid) > 12 else cid
        return (
            f"strategy={self.strategy_name} auth_type={self.auth_type} "
            f"client_id={masked} audience={self.discovered.audience} "
            f"namespace={self.settings.namespace or 'N/A'} "
            f"service_account={self.discovered.service_account}"
        )

    async def get_bearer_token(self, session: "aiohttp.ClientSession") -> str:  # type: ignore[name-defined]
        cached = await _TOKEN_CACHE.get(self.cache_key)
        if cached is not None:
            return cached
        if not self.settings.namespace:
            raise DatabricksAuthError(
                "OIDC Model 2 requires a workflow namespace, but task_execution_metadata.namespace is None."
            )
        loop = asyncio.get_event_loop()
        subject_jwt: str = await loop.run_in_executor(
            None,
            _request_sa_token,
            self.settings.namespace,
            self.discovered.service_account,
            self.discovered.audience,
        )
        payload = await _post_oidc_token(
            session,
            self.workspace_url,
            form={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "subject_token": subject_jwt,
                "client_id": self.discovered.client_id,
                "scope": "all-apis",
            },
        )
        access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        if not access_token:
            raise DatabricksAuthError(f"Databricks OIDC token response missing 'access_token': {payload}")
        await _TOKEN_CACHE.put(self.cache_key, access_token, expires_in)
        return access_token

    async def invalidate_cache(self) -> None:
        # Wipe both the bearer cache and the SA-discovery cache so the next call re-reads
        # the SA annotation in case the operator just rotated the federation policy.
        await _TOKEN_CACHE.invalidate(self.cache_key)
        if self.settings.namespace:
            await _NS_DISCOVERY_CACHE.invalidate(self.settings.namespace)


# =========================================================================== #
# Selection                                                                   #
# =========================================================================== #


async def select_auth(
    task_template: Optional[TaskTemplate],
    workspace_url: str,
    namespace: Optional[str] = None,
) -> DatabricksAuth:
    """Build the :class:`DatabricksAuth` strategy for a single task.

    Resolution order:

    1. Explicit ``databricks_auth_type`` from task config, or
       ``FLYTE_DATABRICKS_AUTH_TYPE`` env var on the connector.
    2. If unset, auto-detect in the order OIDC federation -> OAuth M2M -> PAT,
       based on which credentials / tokens are reachable. PAT is the last-resort
       default so existing deployments keep working with zero config.

    When the resolved type is ``oidc_federation``:

    - Look in the workflow namespace for a ServiceAccount labelled
      ``flyte.org/databricks-enabled=true`` and annotated with
      ``flyte.org/databricks-client-id``. If exactly one is found, return
      Model 2 (:class:`OIDCNamespaceSAAuth`) federated as that SA.
    - If none are found, fall back to Model 1 (:class:`OIDCConnectorIRSAAuth`)
      using the connector pod's own IRSA JWT - only when the connector has both
      a reachable subject token file and a ``DATABRICKS_CLIENT_ID``. Otherwise
      fail with :class:`DatabricksAuthError` so the misconfiguration is
      surfaced rather than silently downgraded.
    - If multiple annotated SAs exist in the namespace, fail loudly.
    """
    settings = _Settings.from_task(task_template, namespace)

    auth_type = settings.auth_type or _auto_detect(settings)

    if auth_type not in VALID_AUTH_TYPES:
        raise DatabricksAuthError(
            f"Invalid databricks auth_type '{auth_type}'. Expected one of: {sorted(VALID_AUTH_TYPES)}."
        )

    if auth_type == "pat":
        return PATAuth(workspace_url, settings)
    if auth_type == "oauth_m2m":
        return OAuthM2MAuth(workspace_url, settings)

    # auth_type == "oidc_federation": annotation-driven discovery picks Model 1 vs Model 2.
    discovered: Optional[_DiscoveredOIDCConfig] = None
    if namespace:
        discovered = await _discover_namespace_oidc_sa(namespace, settings.oidc_audience)
    if discovered is not None:
        return OIDCNamespaceSAAuth(workspace_url, settings, discovered)

    # Fall back to Model 1 only when its config is complete; otherwise fail loudly.
    if not settings.client_id:
        raise DatabricksAuthError(
            "OIDC federation selected but no Model 2 SA was discovered in namespace "
            f"'{namespace or 'N/A'}' and Model 1 cannot run without a client_id. "
            f"Either annotate a ServiceAccount with '{LABEL_DATABRICKS_ENABLED}=true' + "
            f"'{ANNOTATION_DATABRICKS_CLIENT_ID}=<sp-uuid>' in the workflow namespace, "
            f"or set {DATABRICKS_CLIENT_ID_ENV} on the connector for Model 1 fallback."
        )
    if _resolve_oidc_token_file(settings) is None:
        raise DatabricksAuthError(
            "OIDC federation selected but no Model 2 SA was discovered in namespace "
            f"'{namespace or 'N/A'}' and Model 1 cannot run without a subject token file. "
            "Either annotate a workflow-namespace ServiceAccount, or configure IRSA on the "
            "connector pod (AWS_WEB_IDENTITY_TOKEN_FILE) / mount a projected SA token at "
            f"{DEFAULT_PROJECTED_SA_TOKEN_PATH}."
        )
    return OIDCConnectorIRSAAuth(workspace_url, settings)


def build_auth(
    workspace_url: str,
    auth_type: str,
    namespace: Optional[str] = None,
    client_id: Optional[str] = None,
    oauth_secret_name: Optional[str] = None,
    token_secret_name: Optional[str] = None,
    oidc_token_file: Optional[str] = None,
    oidc_audience: Optional[str] = None,
    oidc_service_account: Optional[str] = None,
) -> DatabricksAuth:
    """Build a :class:`DatabricksAuth` directly from an explicit context.

    Used by the connector's ``get`` / ``delete`` paths to reconstruct the auth
    strategy for long-running jobs from persisted metadata, without re-reading
    the task template, environment, or running namespace discovery.

    For OIDC Model 2 the caller passes the previously-discovered SA name via
    ``oidc_service_account``. When that is set, this function builds Model 2
    directly with the supplied (sa, client_id, audience) tuple. Without it,
    Model 1 is built when ``auth_type=oidc_federation``.
    """
    if auth_type not in VALID_AUTH_TYPES:
        raise DatabricksAuthError(
            f"Invalid databricks auth_type '{auth_type}'. Expected one of: {sorted(VALID_AUTH_TYPES)}."
        )
    settings = _Settings(
        auth_type=auth_type,
        client_id=client_id,
        oauth_secret_name=oauth_secret_name or DEFAULT_OAUTH_SECRET_NAME,
        token_secret_name=token_secret_name or DEFAULT_TOKEN_SECRET_NAME,
        oidc_token_file=oidc_token_file,
        oidc_audience=oidc_audience or DEFAULT_OIDC_AUDIENCE,
        namespace=namespace,
    )
    if auth_type == "pat":
        return PATAuth(workspace_url, settings)
    if auth_type == "oauth_m2m":
        return OAuthM2MAuth(workspace_url, settings)
    if oidc_service_account:
        if not client_id:
            raise DatabricksAuthError(
                "Cannot rebuild OIDC Model 2 strategy without client_id "
                "(persisted metadata is missing the discovered Databricks SP)."
            )
        discovered = _DiscoveredOIDCConfig(
            service_account=oidc_service_account,
            client_id=client_id,
            audience=settings.oidc_audience,
        )
        return OIDCNamespaceSAAuth(workspace_url, settings, discovered)
    return OIDCConnectorIRSAAuth(workspace_url, settings)


def _auto_detect(settings: _Settings) -> str:
    """Return ``oidc_federation`` / ``oauth_m2m`` / ``pat`` based on reachable resources."""
    subject_file = _resolve_oidc_token_file(settings)
    has_client_id = bool(settings.client_id or os.getenv(DATABRICKS_CLIENT_ID_ENV))
    if subject_file and has_client_id:
        return "oidc_federation"
    has_client_secret = bool(os.getenv(DATABRICKS_CLIENT_SECRET_ENV))
    if has_client_id and has_client_secret:
        return "oauth_m2m"
    return "pat"


def validate_connector_config() -> None:
    """Fail-fast connector startup check.

    Emits a single structured log line describing the default auth mode. Raises
    :class:`DatabricksAuthError` on clearly-invalid configurations (unknown
    auth_type); emits warnings for configurations that are incomplete but may
    still work via per-namespace discovery (OIDC) or per-namespace secrets (M2M).
    """
    env_auth = os.getenv(FLYTE_DATABRICKS_AUTH_TYPE_ENV)
    if env_auth is None:
        logger.info(
            "Databricks connector auth: no %s set; per-task auth type will be auto-detected.",
            FLYTE_DATABRICKS_AUTH_TYPE_ENV,
        )
        return
    if env_auth not in VALID_AUTH_TYPES:
        raise DatabricksAuthError(
            f"{FLYTE_DATABRICKS_AUTH_TYPE_ENV}='{env_auth}' is not valid. "
            f"Expected one of: {sorted(VALID_AUTH_TYPES)}."
        )
    cid = os.getenv(DATABRICKS_CLIENT_ID_ENV)
    if env_auth == "oauth_m2m" and not cid:
        logger.warning(
            "Databricks connector auth: %s=oauth_m2m but %s is not set. "
            "Each workflow namespace must supply 'client_id' in the OAuth secret.",
            FLYTE_DATABRICKS_AUTH_TYPE_ENV,
            DATABRICKS_CLIENT_ID_ENV,
        )
    if env_auth == "oidc_federation" and not cid:
        logger.info(
            "Databricks connector auth: %s=oidc_federation with no %s. "
            "Model 2 will be used when a workflow namespace has an annotated "
            "ServiceAccount; Model 1 fallback is disabled (no connector-level client_id).",
            FLYTE_DATABRICKS_AUTH_TYPE_ENV,
            DATABRICKS_CLIENT_ID_ENV,
        )
    logger.info(
        "Databricks connector auth: default auth_type=%s (per-task overrides still apply).",
        env_auth,
    )
