"""Tests for Databricks PAT and OAuth M2M authentication."""

import http
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientSession
from aioresponses import aioresponses

from flytekitplugins.spark.databricks_auth import (
    DEFAULT_OIDC_AUDIENCE,
    DEFAULT_OAUTH_SECRET_NAME,
    DatabricksAuthError,
    OIDCConnectorAuth,
    OAuthM2MAuth,
    PATAuth,
    _TokenCache,
    _Settings,
    _post_token,
    _resolve_oidc_token_file,
    build_auth,
    select_auth,
)


def _task_template(**custom):
    task_template = MagicMock()
    task_template.custom = custom
    return task_template


@pytest.fixture(autouse=True)
def _clear_auth_environment(monkeypatch):
    for name in (
        "FLYTE_DATABRICKS_AUTH_TYPE",
        "FLYTE_DATABRICKS_OAUTH_SECRET_NAME",
        "FLYTE_DATABRICKS_OIDC_TOKEN_FILE",
        "FLYTE_DATABRICKS_OIDC_AUDIENCE",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
    ):
        monkeypatch.delenv(name, raising=False)


def test_settings_use_default_secret_name():
    settings = _Settings.from_task(task_template=None, namespace="project-a")

    assert settings.token_secret_name is None
    assert settings.oauth_secret_name == DEFAULT_OAUTH_SECRET_NAME
    assert settings.namespace == "project-a"


def test_settings_use_task_secret_name():
    task_template = _task_template()
    task_template.custom["databricksTokenSecret"] = "custom-token"

    settings = _Settings.from_task(task_template=task_template, namespace="project-a")

    assert settings.token_secret_name == "custom-token"


@pytest.mark.asyncio
async def test_select_auth_returns_pat_by_default():
    task_template = _task_template()
    auth = await select_auth(
        task_template=task_template,
        workspace_url="example.cloud.databricks.com",
        namespace="project-a",
    )

    assert isinstance(auth, PATAuth)
    assert auth.auth_type == "pat"


@pytest.mark.asyncio
async def test_pat_auth_delegates_to_existing_token_lookup():
    task_template = _task_template()
    settings = _Settings(
        task_template=task_template,
        auth_type="pat",
        client_id=None,
        oauth_secret_name=DEFAULT_OAUTH_SECRET_NAME,
        token_secret_name="custom-token",
        oidc_token_file=None,
        oidc_audience=DEFAULT_OIDC_AUDIENCE,
        namespace="project-a",
    )
    auth = PATAuth("example.cloud.databricks.com", settings)

    with patch(
        "flytekitplugins.spark.connector.get_databricks_token",
        return_value="example-token",
    ) as get_token:
        token = await auth.get_bearer_token(AsyncMock())

    assert token == "example-token"
    get_token.assert_called_once_with(
        namespace="project-a",
        task_template=task_template,
        secret_name="custom-token",
    )


@pytest.mark.asyncio
async def test_select_auth_uses_explicit_m2m():
    auth = await select_auth(
        task_template=_task_template(databricksAuthType="oauth_m2m"),
        workspace_url="example.cloud.databricks.com",
        namespace="project-a",
    )

    assert isinstance(auth, OAuthM2MAuth)


@pytest.mark.asyncio
async def test_select_auth_rejects_unknown_type():
    with pytest.raises(DatabricksAuthError, match="Invalid Databricks auth type"):
        await select_auth(
            task_template=_task_template(databricksAuthType="unknown"),
            workspace_url="example.cloud.databricks.com",
            namespace="project-a",
        )


@pytest.mark.asyncio
async def test_m2m_uses_environment_credentials(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "example-client")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "example-secret")
    auth = build_auth("example.cloud.databricks.com", "oauth_m2m")

    with aioresponses() as mocked:
        mocked.post(
            "https://example.cloud.databricks.com/oidc/v1/token",
            status=200,
            payload={"access_token": "example-access-token", "expires_in": 3600},
        )
        async with ClientSession() as session:
            token = await auth.get_bearer_token(session)

    assert token == "example-access-token"


@pytest.mark.asyncio
async def test_namespace_secret_overrides_environment(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "environment-client")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "environment-secret")
    auth = build_auth(
        "namespace.cloud.databricks.com",
        "oauth_m2m",
        namespace="project-a",
    )

    def _secret(secret_name, secret_key, namespace):
        assert secret_name == DEFAULT_OAUTH_SECRET_NAME
        assert namespace == "project-a"
        return {
            "client_id": "namespace-client",
            "client_secret": "namespace-secret",
        }[secret_key]

    posted = {}

    async def _capture(session, workspace_url, form):
        posted.update(form)
        return {"access_token": "namespace-access-token", "expires_in": 3600}

    with patch(
        "flytekitplugins.spark.connector.get_secret_from_k8s",
        side_effect=_secret,
    ), patch(
        "flytekitplugins.spark.databricks_auth._post_token",
        side_effect=_capture,
    ):
        async with ClientSession() as session:
            await auth.get_bearer_token(session)

    assert posted["client_id"] == "namespace-client"
    assert posted["client_secret"] == "namespace-secret"


@pytest.mark.asyncio
async def test_m2m_requires_client_id(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "example-secret")
    auth = build_auth("missing-id.cloud.databricks.com", "oauth_m2m")

    async with ClientSession() as session:
        with pytest.raises(DatabricksAuthError, match="requires a client ID"):
            await auth.get_bearer_token(session)


@pytest.mark.asyncio
async def test_m2m_requires_client_secret(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "example-client")
    auth = build_auth("missing-secret.cloud.databricks.com", "oauth_m2m")

    async with ClientSession() as session:
        with pytest.raises(DatabricksAuthError, match="requires a client secret"):
            await auth.get_bearer_token(session)


@pytest.mark.asyncio
async def test_token_cache_expires(monkeypatch):
    cache = _TokenCache()
    now = {"value": 1000.0}
    monkeypatch.setattr(
        "flytekitplugins.spark.databricks_auth.time.time",
        lambda: now["value"],
    )
    key = ("example.cloud.databricks.com", "client", "project-a")

    await cache.put(key, "cached-token", expires_in=120)
    assert await cache.get(key) == "cached-token"
    now["value"] = 1061.0
    assert await cache.get(key) is None


@pytest.mark.asyncio
async def test_token_endpoint_fails_fast_on_401():
    async with ClientSession() as session:
        with aioresponses() as mocked:
            mocked.post(
                "https://example.cloud.databricks.com/oidc/v1/token",
                status=401,
                body="unauthorized",
            )
            with pytest.raises(DatabricksAuthError, match="HTTP 401"):
                await _post_token(
                    session,
                    "example.cloud.databricks.com",
                    {"grant_type": "client_credentials"},
                )


@pytest.mark.asyncio
async def test_token_endpoint_retries_transient_failure(monkeypatch):
    monkeypatch.setattr(
        "flytekitplugins.spark.databricks_auth.asyncio.sleep",
        AsyncMock(),
    )
    async with ClientSession() as session:
        with aioresponses() as mocked:
            url = "https://retry.cloud.databricks.com/oidc/v1/token"
            mocked.post(url, status=503, body="busy")
            mocked.post(
                url,
                status=200,
                payload={"access_token": "retried-token", "expires_in": 3600},
            )
            result = await _post_token(
                session,
                "retry.cloud.databricks.com",
                {"grant_type": "client_credentials"},
            )

    assert result["access_token"] == "retried-token"


@pytest.mark.asyncio
async def test_connector_create_persists_m2m_metadata(monkeypatch):
    from flytekit.extend.backend.base_agent import AgentRegistry
    from flytekitplugins.spark.connector import DATABRICKS_API_ENDPOINT

    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "create-client")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "create-secret")
    task_template = _task_template(
        databricksInstance="create.cloud.databricks.com",
        databricksAuthType="oauth_m2m",
    )
    execution_metadata = MagicMock(namespace="project-a")
    connector = AgentRegistry.get_agent("spark")

    with patch(
        "flytekitplugins.spark.connector._get_databricks_job_spec",
        return_value={"run_name": "example"},
    ), patch(
        "flytekitplugins.spark.connector.get_secret_from_k8s",
        return_value=None,
    ):
        with aioresponses() as mocked:
            mocked.post(
                "https://create.cloud.databricks.com/oidc/v1/token",
                status=200,
                payload={"access_token": "create-access-token", "expires_in": 3600},
            )
            mocked.post(
                f"https://create.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit",
                status=200,
                payload={"run_id": 42},
            )
            result = await connector.create(
                task_template,
                task_execution_metadata=execution_metadata,
            )

    assert result.auth_type == "oauth_m2m"
    assert result.auth_token is None
    assert result.client_id == "create-client"
    assert result.oauth_secret_name == DEFAULT_OAUTH_SECRET_NAME
    assert result.namespace == "project-a"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["get", "delete"])
async def test_connector_refreshes_m2m_once_after_401(monkeypatch, operation):
    from flytekit.extend.backend.base_agent import AgentRegistry
    from flytekitplugins.spark.connector import DATABRICKS_API_ENDPOINT, DatabricksJobMetadata

    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "refresh-secret")
    metadata = DatabricksJobMetadata(
        databricks_instance=f"{operation}-refresh.cloud.databricks.com",
        run_id="42",
        auth_type="oauth_m2m",
        client_id="refresh-client",
        oauth_secret_name=DEFAULT_OAUTH_SECRET_NAME,
        namespace="project-a",
    )
    connector = AgentRegistry.get_agent("spark")
    issued_tokens = iter(("stale-token", "fresh-token"))

    async def _token_response(session, workspace_url, form):
        return {"access_token": next(issued_tokens), "expires_in": 3600}

    if operation == "get":
        url = (
            f"https://{metadata.databricks_instance}"
            f"{DATABRICKS_API_ENDPOINT}/runs/get?run_id={metadata.run_id}"
        )
    else:
        url = f"https://{metadata.databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/cancel"

    with patch(
        "flytekitplugins.spark.databricks_auth._post_token",
        side_effect=_token_response,
    ), patch(
        "flytekitplugins.spark.connector.get_secret_from_k8s",
        return_value=None,
    ):
        with aioresponses() as mocked:
            request = mocked.get if operation == "get" else mocked.post
            request(url, status=http.HTTPStatus.UNAUTHORIZED)
            request(
                url,
                status=http.HTTPStatus.OK,
                payload={
                    "job_id": "1",
                    "state": {"life_cycle_state": "RUNNING"},
                },
            )
            if operation == "get":
                await connector.get(metadata)
            else:
                await connector.delete(metadata)

        requests = [call for calls in mocked.requests.values() for call in calls]

    assert len(requests) == 2
    assert requests[0].kwargs["headers"]["Authorization"] == "Bearer stale-token"
    assert requests[1].kwargs["headers"]["Authorization"] == "Bearer fresh-token"


@pytest.mark.asyncio
async def test_connector_does_not_retry_non_401(monkeypatch):
    from flytekit.extend.backend.base_agent import AgentRegistry
    from flytekitplugins.spark.connector import DATABRICKS_API_ENDPOINT, DatabricksJobMetadata

    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "failure-secret")
    metadata = DatabricksJobMetadata(
        databricks_instance="failure.cloud.databricks.com",
        run_id="42",
        auth_type="oauth_m2m",
        client_id="failure-client",
    )
    connector = AgentRegistry.get_agent("spark")
    url = (
        f"https://{metadata.databricks_instance}"
        f"{DATABRICKS_API_ENDPOINT}/runs/get?run_id={metadata.run_id}"
    )

    with patch(
        "flytekitplugins.spark.databricks_auth._post_token",
        return_value={"access_token": "failure-token", "expires_in": 3600},
    ):
        with aioresponses() as mocked:
            mocked.get(url, status=http.HTTPStatus.FORBIDDEN)
            with pytest.raises(RuntimeError, match="Failed to get"):
                await connector.get(metadata)

        requests = [call for calls in mocked.requests.values() for call in calls]

    assert len(requests) == 1


def test_oidc_token_file_precedence(tmp_path, monkeypatch):
    explicit = tmp_path / "explicit.jwt"
    fallback = tmp_path / "fallback.jwt"
    explicit.write_text("explicit")
    fallback.write_text("fallback")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(fallback))
    settings = _Settings.from_task(
        _task_template(databricksOidcTokenFile=str(explicit)),
        namespace=None,
    )

    assert _resolve_oidc_token_file(settings) == str(explicit)


@pytest.mark.asyncio
async def test_select_auth_uses_explicit_oidc():
    auth = await select_auth(
        task_template=_task_template(databricksAuthType="oidc_federation"),
        workspace_url="oidc-select.cloud.databricks.com",
        namespace=None,
    )

    assert isinstance(auth, OIDCConnectorAuth)


@pytest.mark.asyncio
async def test_oidc_exchanges_projected_jwt(tmp_path):
    token_file = tmp_path / "workload.jwt"
    token_file.write_text("example-subject-token")
    auth = build_auth(
        "oidc-exchange.cloud.databricks.com",
        "oidc_federation",
        client_id="oidc-client",
        oidc_token_file=str(token_file),
    )
    posted = {}

    async def _capture(session, workspace_url, form):
        posted.update(form)
        return {"access_token": "oidc-access-token", "expires_in": 3600}

    with patch(
        "flytekitplugins.spark.databricks_auth._post_token",
        side_effect=_capture,
    ):
        async with ClientSession() as session:
            token = await auth.get_bearer_token(session)

    assert token == "oidc-access-token"
    assert posted["client_id"] == "oidc-client"
    assert posted["subject_token"] == "example-subject-token"
    assert posted["grant_type"] == "urn:ietf:params:oauth:grant-type:token-exchange"


@pytest.mark.asyncio
async def test_oidc_rereads_projected_jwt_after_refresh(tmp_path):
    token_file = tmp_path / "rotating.jwt"
    token_file.write_text("version-one")
    auth = build_auth(
        "oidc-refresh.cloud.databricks.com",
        "oidc_federation",
        client_id="refresh-client",
        oidc_token_file=str(token_file),
    )
    observed = []

    async def _capture(session, workspace_url, form):
        observed.append(form["subject_token"])
        return {"access_token": f"token-{len(observed)}", "expires_in": 3600}

    with patch(
        "flytekitplugins.spark.databricks_auth._post_token",
        side_effect=_capture,
    ):
        async with ClientSession() as session:
            await auth.get_bearer_token(session)
            await auth.invalidate_cache()
            token_file.write_text("version-two")
            await auth.get_bearer_token(session)

    assert observed == ["version-one", "version-two"]


@pytest.mark.asyncio
async def test_oidc_requires_projected_token_file():
    auth = build_auth(
        "oidc-missing-file.cloud.databricks.com",
        "oidc_federation",
        client_id="oidc-client",
        oidc_token_file="/does/not/exist",
    )

    with patch("os.path.exists", return_value=False):
        async with ClientSession() as session:
            with pytest.raises(DatabricksAuthError, match="requires a projected JWT"):
                await auth.get_bearer_token(session)


@pytest.mark.asyncio
async def test_oidc_requires_client_id(tmp_path):
    token_file = tmp_path / "workload.jwt"
    token_file.write_text("example-subject-token")
    auth = build_auth(
        "oidc-missing-client.cloud.databricks.com",
        "oidc_federation",
        oidc_token_file=str(token_file),
    )

    async with ClientSession() as session:
        with pytest.raises(DatabricksAuthError, match="requires a client ID"):
            await auth.get_bearer_token(session)


@pytest.mark.asyncio
async def test_oidc_rejects_missing_access_token(tmp_path):
    token_file = tmp_path / "workload.jwt"
    token_file.write_text("example-subject-token")
    auth = build_auth(
        "oidc-missing-response.cloud.databricks.com",
        "oidc_federation",
        client_id="oidc-client",
        oidc_token_file=str(token_file),
    )

    with patch(
        "flytekitplugins.spark.databricks_auth._post_token",
        return_value={"expires_in": 3600},
    ):
        async with ClientSession() as session:
            with pytest.raises(DatabricksAuthError, match="did not contain access_token"):
                await auth.get_bearer_token(session)


@pytest.mark.asyncio
async def test_token_endpoint_rejects_invalid_json():
    async with ClientSession() as session:
        with aioresponses() as mocked:
            mocked.post(
                "https://invalid-json.cloud.databricks.com/oidc/v1/token",
                status=200,
                body="not-json",
            )
            with pytest.raises(DatabricksAuthError, match="invalid JSON"):
                await _post_token(
                    session,
                    "invalid-json.cloud.databricks.com",
                    {"grant_type": "example"},
                )


@pytest.mark.asyncio
async def test_connector_create_persists_oidc_metadata(tmp_path):
    from flytekit.extend.backend.base_agent import AgentRegistry
    from flytekitplugins.spark.connector import DATABRICKS_API_ENDPOINT

    token_file = tmp_path / "connector.jwt"
    token_file.write_text("connector-subject-token")
    task_template = _task_template(
        databricksInstance="oidc-create.cloud.databricks.com",
        databricksAuthType="oidc_federation",
        databricksClientId="connector-client",
        databricksOidcTokenFile=str(token_file),
        databricksOidcAudience="example-audience",
    )
    connector = AgentRegistry.get_agent("spark")

    with patch(
        "flytekitplugins.spark.connector._get_databricks_job_spec",
        return_value={"run_name": "example"},
    ):
        with aioresponses() as mocked:
            mocked.post(
                "https://oidc-create.cloud.databricks.com/oidc/v1/token",
                status=200,
                payload={"access_token": "connector-access-token", "expires_in": 3600},
            )
            mocked.post(
                f"https://oidc-create.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit",
                status=200,
                payload={"run_id": 42},
            )
            result = await connector.create(task_template)

    assert result.auth_type == "oidc_federation"
    assert result.auth_token is None
    assert result.client_id == "connector-client"
    assert result.oidc_token_file == str(token_file)
    assert result.oidc_audience == "example-audience"
