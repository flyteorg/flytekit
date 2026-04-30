"""Tests for the flytekit-spark Databricks auth strategy module.

Covers:

- ``_Settings.from_task``: task-cfg + env-var + default resolution
- ``_resolve_oidc_token_file``: subject JWT discovery order
- ``_TokenCache``: async set/get/expire/invalidate
- ``_post_oidc_token``: retries on 429/5xx, fail-fast on 4xx
- ``_auto_detect``: auto-detection precedence
- ``select_auth`` and ``build_auth``: strategy dispatch
- Each strategy's ``get_bearer_token`` happy-path + error paths
- ``OIDCNamespaceSAAuth``: Kubernetes ``TokenRequest`` success / 404 / 403
- Backward compatibility: legacy metadata (``auth_type=None``) continues to work
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import ClientSession
from aioresponses import aioresponses
from flytekitplugins.spark.databricks_auth import (
    ANNOTATION_DATABRICKS_AUDIENCE,
    ANNOTATION_DATABRICKS_CLIENT_ID,
    DATABRICKS_ENABLED_LABEL_SELECTOR,
    DEFAULT_OAUTH_SECRET_NAME,
    DEFAULT_OIDC_AUDIENCE,
    DEFAULT_TOKEN_SECRET_NAME,
    LABEL_DATABRICKS_ENABLED,
    VALID_AUTH_TYPES,
    DatabricksAuthError,
    OAuthM2MAuth,
    OIDCConnectorIRSAAuth,
    OIDCNamespaceSAAuth,
    PATAuth,
    _auto_detect,
    _DiscoveredOIDCConfig,
    _discover_namespace_oidc_sa_sync,
    _NS_DISCOVERY_CACHE,
    _post_oidc_token,
    _resolve_oidc_token_file,
    _Settings,
    _TokenCache,
    build_auth,
    select_auth,
    validate_connector_config,
)


# --------------------------------------------------------------------------- #
# Minimal TaskTemplate-compatible stub                                        #
# --------------------------------------------------------------------------- #


@dataclass
class _FakeTaskTemplate:
    custom: Dict[str, Any]


def _tt(**custom) -> _FakeTaskTemplate:
    return _FakeTaskTemplate(custom=dict(custom))


def _fake_sa(name: str, client_id: str = None, audience: str = None) -> MagicMock:
    """Build a minimal stand-in for ``kubernetes.client.V1ServiceAccount``.

    Discovery only reads ``.metadata.name`` and ``.metadata.annotations`` -
    construct just those two attributes.
    """
    annotations = {}
    if client_id is not None:
        annotations[ANNOTATION_DATABRICKS_CLIENT_ID] = client_id
    if audience is not None:
        annotations[ANNOTATION_DATABRICKS_AUDIENCE] = audience
    sa = MagicMock()
    sa.metadata = MagicMock()
    sa.metadata.name = name
    sa.metadata.annotations = annotations or None
    return sa


@pytest.fixture(autouse=True)
def _clear_ns_discovery_cache():
    """Reset the discovery cache between tests so cached results don't leak."""
    _NS_DISCOVERY_CACHE._store.clear()
    yield
    _NS_DISCOVERY_CACHE._store.clear()


# =========================================================================== #
# _Settings                                                                   #
# =========================================================================== #


class TestSettings:
    def test_task_cfg_wins_over_env_and_default(self, monkeypatch):
        monkeypatch.setenv("FLYTE_DATABRICKS_AUTH_TYPE", "oauth_m2m")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "from-env")
        s = _Settings.from_task(
            _tt(databricksAuthType="pat", databricksClientId="from-cfg"),
            namespace="ns-a",
        )
        assert s.auth_type == "pat"
        assert s.client_id == "from-cfg"
        assert s.namespace == "ns-a"

    def test_env_used_when_cfg_absent(self, monkeypatch):
        monkeypatch.setenv("FLYTE_DATABRICKS_AUTH_TYPE", "oidc_federation")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "env-cid")
        s = _Settings.from_task(_tt(), namespace=None)
        assert s.auth_type == "oidc_federation"
        assert s.client_id == "env-cid"

    def test_defaults_populated(self, monkeypatch):
        for v in [
            "FLYTE_DATABRICKS_AUTH_TYPE",
            "FLYTE_DATABRICKS_OAUTH_SECRET_NAME",
            "FLYTE_DATABRICKS_TOKEN_SECRET_NAME",
            "FLYTE_DATABRICKS_OIDC_AUDIENCE",
            "DATABRICKS_CLIENT_ID",
        ]:
            monkeypatch.delenv(v, raising=False)
        s = _Settings.from_task(_tt(), namespace=None)
        assert s.auth_type is None
        assert s.client_id is None
        assert s.oauth_secret_name == DEFAULT_OAUTH_SECRET_NAME
        assert s.token_secret_name == DEFAULT_TOKEN_SECRET_NAME
        assert s.oidc_audience == DEFAULT_OIDC_AUDIENCE


# =========================================================================== #
# _resolve_oidc_token_file                                                    #
# =========================================================================== #


class TestResolveOidcTokenFile:
    def test_explicit_cfg_wins(self, tmp_path, monkeypatch):
        f = tmp_path / "explicit.jwt"
        f.write_text("jwt")
        monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
        s = _Settings.from_task(_tt(databricksOidcTokenFile=str(f)), namespace=None)
        assert _resolve_oidc_token_file(s) == str(f)

    def test_falls_back_to_irsa(self, tmp_path, monkeypatch):
        f = tmp_path / "irsa.jwt"
        f.write_text("irsa-jwt")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(f))
        s = _Settings.from_task(_tt(), namespace=None)
        assert _resolve_oidc_token_file(s) == str(f)

    def test_returns_none_when_nothing_found(self, monkeypatch):
        monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
        monkeypatch.delenv("FLYTE_DATABRICKS_OIDC_TOKEN_FILE", raising=False)
        s = _Settings.from_task(_tt(), namespace=None)
        with patch("os.path.exists", return_value=False):
            assert _resolve_oidc_token_file(s) is None


# =========================================================================== #
# _TokenCache                                                                 #
# =========================================================================== #


class TestTokenCache:
    @pytest.mark.asyncio
    async def test_put_and_get(self):
        cache = _TokenCache()
        await cache.put(("w", "c", "s"), "tok", expires_in=3600)
        assert await cache.get(("w", "c", "s")) == "tok"

    @pytest.mark.asyncio
    async def test_miss(self):
        cache = _TokenCache()
        assert await cache.get(("none", "none", "none")) is None

    @pytest.mark.asyncio
    async def test_expired_eviction(self):
        cache = _TokenCache()
        await cache.put(("w", "c", "s"), "tok", expires_in=1)  # clamped to >=30s buffer
        # Force the stored expiry into the past.
        cache._store[("w", "c", "s")].expires_at_unix = time.time() - 1
        assert await cache.get(("w", "c", "s")) is None

    @pytest.mark.asyncio
    async def test_invalidate(self):
        cache = _TokenCache()
        await cache.put(("w", "c", "s"), "tok", expires_in=3600)
        await cache.invalidate(("w", "c", "s"))
        assert await cache.get(("w", "c", "s")) is None


# =========================================================================== #
# _post_oidc_token                                                            #
# =========================================================================== #


class TestPostOidcToken:
    @pytest.mark.asyncio
    async def test_success(self):
        async with ClientSession() as session:
            with aioresponses() as m:
                m.post(
                    "https://ws.cloud.databricks.com/oidc/v1/token",
                    status=200,
                    payload={"access_token": "A", "expires_in": 3600},
                )
                result = await _post_oidc_token(session, "ws.cloud.databricks.com", {"grant_type": "x"})
                assert result == {"access_token": "A", "expires_in": 3600}

    @pytest.mark.asyncio
    async def test_fast_fail_on_401(self):
        async with ClientSession() as session:
            with aioresponses() as m:
                m.post(
                    "https://ws.cloud.databricks.com/oidc/v1/token",
                    status=401,
                    body="bad-client",
                )
                with pytest.raises(DatabricksAuthError, match="401"):
                    await _post_oidc_token(session, "ws.cloud.databricks.com", {"grant_type": "x"})

    @pytest.mark.asyncio
    async def test_retries_on_5xx_then_succeeds(self, monkeypatch):
        async with ClientSession() as session:
            with aioresponses() as m:
                url = "https://ws.cloud.databricks.com/oidc/v1/token"
                m.post(url, status=503, body="busy")
                m.post(url, status=200, payload={"access_token": "B", "expires_in": 60})
                result = await _post_oidc_token(session, "ws.cloud.databricks.com", {"grant_type": "x"})
                assert result["access_token"] == "B"

    @pytest.mark.asyncio
    async def test_exhausts_retries(self):
        async with ClientSession() as session:
            with aioresponses() as m:
                url = "https://ws.cloud.databricks.com/oidc/v1/token"
                for _ in range(3):
                    m.post(url, status=500, body="boom")
                with pytest.raises(DatabricksAuthError, match="failed after"):
                    await _post_oidc_token(session, "ws.cloud.databricks.com", {"grant_type": "x"})


# =========================================================================== #
# _auto_detect                                                                #
# =========================================================================== #


class TestAutoDetect:
    def test_oidc_when_subject_file_and_client_id_present(self, tmp_path, monkeypatch):
        f = tmp_path / "tok.jwt"
        f.write_text("x")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(f))
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        s = _Settings.from_task(_tt(), namespace=None)
        assert _auto_detect(s) == "oidc_federation"

    def test_m2m_when_client_id_and_secret_present(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
        monkeypatch.delenv("FLYTE_DATABRICKS_OIDC_TOKEN_FILE", raising=False)
        with patch("os.path.exists", return_value=False):
            s = _Settings.from_task(_tt(), namespace=None)
            assert _auto_detect(s) == "oauth_m2m"

    def test_pat_when_nothing_else_configured(self, monkeypatch):
        for v in [
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "FLYTE_DATABRICKS_OIDC_TOKEN_FILE",
        ]:
            monkeypatch.delenv(v, raising=False)
        with patch("os.path.exists", return_value=False):
            s = _Settings.from_task(_tt(), namespace=None)
            assert _auto_detect(s) == "pat"


# =========================================================================== #
# select_auth / build_auth                                                    #
# =========================================================================== #


class TestSelectAuth:
    @pytest.mark.asyncio
    async def test_explicit_pat(self, monkeypatch):
        monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
        auth = await select_auth(_tt(databricksAuthType="pat"), "ws", namespace="ns")
        assert isinstance(auth, PATAuth)

    @pytest.mark.asyncio
    async def test_explicit_m2m(self):
        auth = await select_auth(_tt(databricksAuthType="oauth_m2m"), "ws", namespace="ns")
        assert isinstance(auth, OAuthM2MAuth)

    @pytest.mark.asyncio
    async def test_oidc_model_1_when_no_sa_discovered(self, tmp_path, monkeypatch):
        # Provide a reachable IRSA + client_id so Model 1 is a valid fallback.
        jwt_path = tmp_path / "irsa.jwt"
        jwt_path.write_text("j")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(jwt_path))
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "fallback-cid")
        with patch(
            "flytekitplugins.spark.databricks_auth._discover_namespace_oidc_sa_sync",
            return_value=None,
        ):
            auth = await select_auth(_tt(databricksAuthType="oidc_federation"), "ws", namespace="ns")
        assert isinstance(auth, OIDCConnectorIRSAAuth)

    @pytest.mark.asyncio
    async def test_oidc_model_2_when_sa_discovered(self, monkeypatch):
        discovered = _DiscoveredOIDCConfig(
            service_account="dbx-runner", client_id="ns-cid", audience="databricks"
        )
        with patch(
            "flytekitplugins.spark.databricks_auth._discover_namespace_oidc_sa_sync",
            return_value=discovered,
        ):
            auth = await select_auth(
                _tt(databricksAuthType="oidc_federation"), "ws", namespace="team-a"
            )
        assert isinstance(auth, OIDCNamespaceSAAuth)
        assert auth.discovered.service_account == "dbx-runner"
        assert auth.discovered.client_id == "ns-cid"

    @pytest.mark.asyncio
    async def test_oidc_no_sa_and_no_fallback_errors(self, monkeypatch):
        # Both discovery and Model 1 fallback are unavailable.
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
        with patch(
            "flytekitplugins.spark.databricks_auth._discover_namespace_oidc_sa_sync",
            return_value=None,
        ), patch("os.path.exists", return_value=False):
            with pytest.raises(DatabricksAuthError, match="no Model 2 SA was discovered"):
                await select_auth(_tt(databricksAuthType="oidc_federation"), "ws", namespace="ns")

    @pytest.mark.asyncio
    async def test_oidc_no_sa_and_no_subject_file_errors(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
        with patch(
            "flytekitplugins.spark.databricks_auth._discover_namespace_oidc_sa_sync",
            return_value=None,
        ), patch("os.path.exists", return_value=False):
            with pytest.raises(DatabricksAuthError, match="without a subject token file"):
                await select_auth(_tt(databricksAuthType="oidc_federation"), "ws", namespace="ns")

    @pytest.mark.asyncio
    async def test_invalid_auth_type_raises(self):
        with pytest.raises(DatabricksAuthError, match="Invalid databricks auth_type"):
            await select_auth(_tt(databricksAuthType="not-real"), "ws")

    def test_build_auth_dispatch(self):
        assert isinstance(build_auth("ws", "pat"), PATAuth)
        assert isinstance(build_auth("ws", "oauth_m2m"), OAuthM2MAuth)
        assert isinstance(build_auth("ws", "oidc_federation"), OIDCConnectorIRSAAuth)
        assert isinstance(
            build_auth(
                "ws",
                "oidc_federation",
                oidc_service_account="sa",
                client_id="cid",
                namespace="ns",
            ),
            OIDCNamespaceSAAuth,
        )

    def test_build_auth_model_2_without_client_id_errors(self):
        with pytest.raises(DatabricksAuthError, match="without client_id"):
            build_auth("ws", "oidc_federation", oidc_service_account="sa", namespace="ns")


# =========================================================================== #
# PATAuth                                                                     #
# =========================================================================== #


class TestPATAuth:
    @pytest.mark.asyncio
    async def test_delegates_to_get_databricks_token(self):
        auth = build_auth("ws", "pat", namespace="team-a", token_secret_name="custom-sec")
        with patch("flytekitplugins.spark.connector.get_databricks_token") as m:
            m.return_value = "dapi_xyz"
            async with ClientSession() as session:
                tok = await auth.get_bearer_token(session)
            assert tok == "dapi_xyz"
            m.assert_called_once_with(namespace="team-a", secret_name="custom-sec")


# =========================================================================== #
# OAuthM2MAuth                                                                #
# =========================================================================== #


class TestOAuthM2M:
    @pytest.mark.asyncio
    async def test_happy_path_with_env_creds(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        auth = build_auth("ws.cloud.databricks.com", "oauth_m2m", client_id="cid", namespace="ns-a")
        with patch("flytekitplugins.spark.connector.get_secret_from_k8s", return_value=None):
            async with ClientSession() as session:
                with aioresponses() as m:
                    m.post(
                        "https://ws.cloud.databricks.com/oidc/v1/token",
                        status=200,
                        payload={"access_token": "M2M-TOK", "expires_in": 3600},
                    )
                    tok = await auth.get_bearer_token(session)
                    assert tok == "M2M-TOK"

    @pytest.mark.asyncio
    async def test_namespace_secret_overrides_env(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "env-secret")
        auth = build_auth("ws.cloud.databricks.com", "oauth_m2m", namespace="ns-a")

        def fake_secret(secret_name, secret_key, namespace):
            return {"client_id": "ns-cid", "client_secret": "ns-secret"}[secret_key]

        with patch("flytekitplugins.spark.connector.get_secret_from_k8s", side_effect=fake_secret):
            posted = {}

            async def _capture(session, url, form):
                posted["form"] = form
                return {"access_token": "X", "expires_in": 60}

            with patch(
                "flytekitplugins.spark.databricks_auth._post_oidc_token", side_effect=_capture
            ):
                async with ClientSession() as session:
                    await auth.get_bearer_token(session)
        assert posted["form"]["client_id"] == "ns-cid"
        assert posted["form"]["client_secret"] == "ns-secret"

    @pytest.mark.asyncio
    async def test_missing_client_id_errors(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        auth = build_auth("ws", "oauth_m2m")
        with patch("flytekitplugins.spark.connector.get_secret_from_k8s", return_value=None):
            async with ClientSession() as session:
                with pytest.raises(DatabricksAuthError, match="no client_id"):
                    await auth.get_bearer_token(session)

    @pytest.mark.asyncio
    async def test_caches_token(self, monkeypatch):
        # Unique cache key per invocation to avoid cross-test contamination.
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        auth = build_auth(
            "cache-test.cloud.databricks.com", "oauth_m2m", client_id="unique-cid", namespace="ns-cache"
        )
        calls = {"n": 0}

        async def _fake_post(session, url, form):
            calls["n"] += 1
            return {"access_token": "T", "expires_in": 3600}

        with patch(
            "flytekitplugins.spark.databricks_auth._post_oidc_token", side_effect=_fake_post
        ), patch("flytekitplugins.spark.connector.get_secret_from_k8s", return_value=None):
            async with ClientSession() as session:
                await auth.get_bearer_token(session)
                await auth.get_bearer_token(session)
        assert calls["n"] == 1
        await auth.invalidate_cache()


# =========================================================================== #
# OIDCConnectorIRSAAuth (Model 1)                                             #
# =========================================================================== #


class TestOIDCConnectorIRSAAuth:
    @pytest.mark.asyncio
    async def test_token_exchange_happy_path(self, tmp_path, monkeypatch):
        jwt_path = tmp_path / "irsa.jwt"
        jwt_path.write_text("irsa-jwt-contents")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(jwt_path))
        auth = build_auth(
            "ws1.cloud.databricks.com",
            "oidc_federation",
            client_id="irsa-cid",
        )
        posted = {}

        async def _capture(session, url, form):
            posted["form"] = form
            return {"access_token": "OIDC-TOK", "expires_in": 3600}

        with patch(
            "flytekitplugins.spark.databricks_auth._post_oidc_token", side_effect=_capture
        ):
            async with ClientSession() as session:
                tok = await auth.get_bearer_token(session)
        assert tok == "OIDC-TOK"
        assert posted["form"]["subject_token"] == "irsa-jwt-contents"
        assert posted["form"]["grant_type"] == "urn:ietf:params:oauth:grant-type:token-exchange"
        assert posted["form"]["client_id"] == "irsa-cid"

    @pytest.mark.asyncio
    async def test_jwt_reread_each_refresh(self, tmp_path, monkeypatch):
        jwt_path = tmp_path / "rotating.jwt"
        jwt_path.write_text("v1")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(jwt_path))
        auth = build_auth("ws-rotate.cloud.databricks.com", "oidc_federation", client_id="rot-cid")

        captured = []

        async def _capture(session, url, form):
            captured.append(form["subject_token"])
            return {"access_token": f"tok-{len(captured)}", "expires_in": 3600}

        with patch(
            "flytekitplugins.spark.databricks_auth._post_oidc_token", side_effect=_capture
        ):
            async with ClientSession() as session:
                await auth.get_bearer_token(session)
                await auth.invalidate_cache()
                jwt_path.write_text("v2")
                await auth.get_bearer_token(session)

        assert captured == ["v1", "v2"]

    @pytest.mark.asyncio
    async def test_missing_subject_file_errors(self, monkeypatch):
        monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
        auth = build_auth("ws", "oidc_federation", client_id="cid")
        with patch("os.path.exists", return_value=False):
            async with ClientSession() as session:
                with pytest.raises(DatabricksAuthError, match="no subject token file"):
                    await auth.get_bearer_token(session)

    @pytest.mark.asyncio
    async def test_missing_client_id_errors(self, tmp_path, monkeypatch):
        jwt_path = tmp_path / "irsa.jwt"
        jwt_path.write_text("j")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(jwt_path))
        auth = build_auth("ws-noid.cloud.databricks.com", "oidc_federation")
        async with ClientSession() as session:
            with pytest.raises(DatabricksAuthError, match="no client_id"):
                await auth.get_bearer_token(session)


# =========================================================================== #
# OIDCNamespaceSAAuth (Model 2)                                               #
# =========================================================================== #


class TestOIDCNamespaceSAAuth:
    @pytest.mark.asyncio
    async def test_happy_path(self):
        auth = build_auth(
            "ws-sa.cloud.databricks.com",
            "oidc_federation",
            client_id="sa-cid",
            namespace="team-a",
            oidc_service_account="team-sa",
            oidc_audience="databricks",
        )

        async def _capture(session, url, form):
            assert form["subject_token"] == "minted-jwt"
            assert form["client_id"] == "sa-cid"
            return {"access_token": "SA-OIDC", "expires_in": 3600}

        with patch(
            "flytekitplugins.spark.databricks_auth._request_sa_token", return_value="minted-jwt"
        ), patch(
            "flytekitplugins.spark.databricks_auth._post_oidc_token", side_effect=_capture
        ):
            async with ClientSession() as session:
                tok = await auth.get_bearer_token(session)
        assert tok == "SA-OIDC"

    @pytest.mark.asyncio
    async def test_missing_namespace_errors(self):
        auth = build_auth(
            "ws", "oidc_federation", client_id="cid", oidc_service_account="sa", namespace=None
        )
        async with ClientSession() as session:
            with pytest.raises(DatabricksAuthError, match="requires a workflow namespace"):
                await auth.get_bearer_token(session)

    def test_sa_not_found_translates_404(self):
        from flytekitplugins.spark.databricks_auth import _request_sa_token
        from kubernetes.client.exceptions import ApiException

        with (
            patch("kubernetes.config.load_incluster_config"),
            patch("kubernetes.client.CoreV1Api") as api_cls,
        ):
            api_cls.return_value.create_namespaced_service_account_token.side_effect = ApiException(
                status=404
            )
            with pytest.raises(DatabricksAuthError, match="not found"):
                _request_sa_token("ns-a", "missing-sa", "databricks")

    def test_forbidden_translates_403(self):
        from flytekitplugins.spark.databricks_auth import _request_sa_token
        from kubernetes.client.exceptions import ApiException

        with (
            patch("kubernetes.config.load_incluster_config"),
            patch("kubernetes.client.CoreV1Api") as api_cls,
        ):
            api_cls.return_value.create_namespaced_service_account_token.side_effect = ApiException(
                status=403
            )
            with pytest.raises(DatabricksAuthError, match="Forbidden"):
                _request_sa_token("ns-a", "sa", "databricks")


# =========================================================================== #
# Namespace SA discovery (annotation-driven)                                  #
# =========================================================================== #


class TestNamespaceDiscovery:
    """Unit tests for the synchronous discovery helper (no caching)."""

    def test_zero_matches_returns_none(self):
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s",
            return_value=[],
        ):
            assert _discover_namespace_oidc_sa_sync("team-a", "databricks") is None

    def test_label_filter_passed_through(self):
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s", return_value=[]
        ) as mock_list:
            _discover_namespace_oidc_sa_sync("team-a", "databricks")
        mock_list.assert_called_once_with(
            namespace="team-a", label_selector=DATABRICKS_ENABLED_LABEL_SELECTOR
        )

    def test_single_match_returns_config(self):
        sa = _fake_sa("dbx-runner", client_id="cid-A")
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s",
            return_value=[sa],
        ):
            cfg = _discover_namespace_oidc_sa_sync("team-a", "databricks")
        assert cfg is not None
        assert cfg.service_account == "dbx-runner"
        assert cfg.client_id == "cid-A"
        assert cfg.audience == "databricks"  # default applied

    def test_per_sa_audience_annotation_overrides_default(self):
        sa = _fake_sa("dbx-runner", client_id="cid-A", audience="custom-aud")
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s",
            return_value=[sa],
        ):
            cfg = _discover_namespace_oidc_sa_sync("team-a", "databricks")
        assert cfg.audience == "custom-aud"

    def test_sa_without_client_id_annotation_skipped(self):
        # Labelled but no client-id annotation -> not a candidate.
        sa = _fake_sa("misconfigured-sa", client_id=None)
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s",
            return_value=[sa],
        ):
            assert _discover_namespace_oidc_sa_sync("team-a", "databricks") is None

    def test_empty_client_id_annotation_skipped(self):
        sa = _fake_sa("misconfigured-sa", client_id="   ")
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s",
            return_value=[sa],
        ):
            assert _discover_namespace_oidc_sa_sync("team-a", "databricks") is None

    def test_multiple_matches_raises_with_names(self):
        sa1 = _fake_sa("alpha", client_id="cid-1")
        sa2 = _fake_sa("beta", client_id="cid-2")
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s",
            return_value=[sa1, sa2],
        ):
            with pytest.raises(DatabricksAuthError, match=r"Ambiguous.*alpha.*beta"):
                _discover_namespace_oidc_sa_sync("team-a", "databricks")


class TestNamespaceDiscoveryCache:
    """Caching wrapper around the synchronous helper."""

    @pytest.mark.asyncio
    async def test_cache_hit_skips_listing(self):
        from flytekitplugins.spark.databricks_auth import _discover_namespace_oidc_sa

        sa = _fake_sa("dbx-runner", client_id="cid")
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s", return_value=[sa]
        ) as mock_list:
            r1 = await _discover_namespace_oidc_sa("ns-cache-a", "databricks")
            r2 = await _discover_namespace_oidc_sa("ns-cache-a", "databricks")
        assert r1 is not None and r2 is not None
        assert r1.service_account == r2.service_account
        assert mock_list.call_count == 1  # second call served from cache

    @pytest.mark.asyncio
    async def test_cache_miss_is_also_cached(self):
        from flytekitplugins.spark.databricks_auth import _discover_namespace_oidc_sa

        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s", return_value=[]
        ) as mock_list:
            r1 = await _discover_namespace_oidc_sa("ns-cache-b", "databricks")
            r2 = await _discover_namespace_oidc_sa("ns-cache-b", "databricks")
        assert r1 is None and r2 is None
        assert mock_list.call_count == 1

    @pytest.mark.asyncio
    async def test_invalidate_forces_relookup(self):
        from flytekitplugins.spark.databricks_auth import (
            _NS_DISCOVERY_CACHE,
            _discover_namespace_oidc_sa,
        )

        sa = _fake_sa("dbx-runner", client_id="cid")
        with patch(
            "flytekitplugins.spark.connector.list_serviceaccounts_in_k8s", return_value=[sa]
        ) as mock_list:
            await _discover_namespace_oidc_sa("ns-cache-c", "databricks")
            await _NS_DISCOVERY_CACHE.invalidate("ns-cache-c")
            await _discover_namespace_oidc_sa("ns-cache-c", "databricks")
        assert mock_list.call_count == 2


class TestOIDCNamespaceSAAuthInvalidatesDiscovery:
    """Model 2's invalidate_cache should evict the discovery cache too."""

    @pytest.mark.asyncio
    async def test_invalidate_clears_discovery(self):
        from flytekitplugins.spark.databricks_auth import _NS_DISCOVERY_CACHE

        auth = build_auth(
            "ws.cloud.databricks.com",
            "oidc_federation",
            client_id="cid",
            namespace="team-a",
            oidc_service_account="sa",
            oidc_audience="databricks",
        )
        # Pre-seed the discovery cache for that namespace.
        await _NS_DISCOVERY_CACHE.put(
            "team-a",
            _DiscoveredOIDCConfig(service_account="sa", client_id="cid", audience="databricks"),
        )
        await auth.invalidate_cache()
        cached, _ = await _NS_DISCOVERY_CACHE.get("team-a")
        assert cached is False


# =========================================================================== #
# validate_connector_config                                                   #
# =========================================================================== #


class TestValidateConnectorConfig:
    def test_no_default_logs_and_returns(self, monkeypatch, caplog):
        monkeypatch.delenv("FLYTE_DATABRICKS_AUTH_TYPE", raising=False)
        validate_connector_config()  # must not raise

    def test_invalid_env_raises(self, monkeypatch):
        monkeypatch.setenv("FLYTE_DATABRICKS_AUTH_TYPE", "bogus")
        with pytest.raises(DatabricksAuthError):
            validate_connector_config()

    def test_valid_env_returns(self, monkeypatch):
        monkeypatch.setenv("FLYTE_DATABRICKS_AUTH_TYPE", "pat")
        validate_connector_config()  # no raise


# =========================================================================== #
# Connector integration: backward compat + 401 refresh                       #
# =========================================================================== #


class TestConnectorAuthBackwardCompat:
    """Metadata written by older connectors (auth_type=None) must still work."""

    @pytest.mark.asyncio
    async def test_legacy_metadata_get_uses_stored_token_no_refresh(self):
        import http as _http

        from flytekit.extend.backend.base_agent import AgentRegistry
        from flytekitplugins.spark.connector import DATABRICKS_API_ENDPOINT, DatabricksJobMetadata

        meta = DatabricksJobMetadata(
            databricks_instance="legacy.cloud.databricks.com",
            run_id="111",
            auth_token="legacy-pat",
            # auth_type intentionally left None (simulating old connector)
        )
        agent = AgentRegistry.get_agent("spark")
        url = f"https://legacy.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/get?run_id=111"
        with aioresponses() as m:
            m.get(
                url,
                status=_http.HTTPStatus.OK,
                payload={"job_id": "1", "run_id": "111", "state": {"life_cycle_state": "RUNNING"}},
            )
            await agent.get(meta)
            call = list(m.requests.values())[0][0]
            assert call.kwargs["headers"]["Authorization"] == "Bearer legacy-pat"


class TestConnectorAuthRefresh:
    """OAuth/OIDC metadata should transparently refresh on 401."""

    @pytest.mark.asyncio
    async def test_get_refreshes_token_on_401(self, monkeypatch):
        import http as _http

        from flytekit.extend.backend.base_agent import AgentRegistry
        from flytekitplugins.spark.connector import DATABRICKS_API_ENDPOINT, DatabricksJobMetadata

        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        meta = DatabricksJobMetadata(
            databricks_instance="refresh-ws.cloud.databricks.com",
            run_id="222",
            auth_type="oauth_m2m",
            client_id="refresh-cid",
            namespace="ns-a",
            oauth_secret_name="databricks-oauth",
        )
        agent = AgentRegistry.get_agent("spark")
        url = f"https://refresh-ws.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/get?run_id=222"

        tokens = iter(["stale-token", "fresh-token"])

        async def _fake_post(session, workspace_url, form):
            return {"access_token": next(tokens), "expires_in": 3600}

        with patch(
            "flytekitplugins.spark.databricks_auth._post_oidc_token", side_effect=_fake_post
        ), patch("flytekitplugins.spark.connector.get_secret_from_k8s", return_value=None):
            with aioresponses() as m:
                # First call: 401 with the stale token.
                m.get(url, status=_http.HTTPStatus.UNAUTHORIZED, body="expired")
                # Second call: 200 with the refreshed token.
                m.get(
                    url,
                    status=_http.HTTPStatus.OK,
                    payload={"job_id": "1", "run_id": "222", "state": {"life_cycle_state": "RUNNING"}},
                )
                await agent.get(meta)

            requests = [r for rs in m.requests.values() for r in rs]
            assert len(requests) == 2
            assert requests[0].kwargs["headers"]["Authorization"] == "Bearer stale-token"
            assert requests[1].kwargs["headers"]["Authorization"] == "Bearer fresh-token"
