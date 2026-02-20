"""
Comprehensive tests for the Databricks per-project token support feature.

Tests cover:
- get_secret_from_k8s: Cross-namespace K8s secret reading
- get_databricks_token: Token resolution strategy (K8s -> env var fallback)
- get_header: Authorization header generation with token support
- DatabricksConnector.create/get/delete: Token persistence in job metadata
- DatabricksV2 task config: databricks_token_secret serialization
"""

import base64
import http
import json
import os
from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from aioresponses import aioresponses

from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals, task
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import Container, Resources, TaskTemplate, TaskExecutionMetadata

from flytekitplugins.spark.connector import (
    DATABRICKS_API_ENDPOINT,
    DatabricksJobMetadata,
    DatabricksConnector,
    _get_databricks_job_spec,
    get_databricks_token,
    get_header,
    get_secret_from_k8s,
)

from flytekit.extend.backend.base_agent import AgentRegistry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def task_template() -> TaskTemplate:
    """Standard Databricks task template for testing."""
    task_id = Identifier(
        resource_type=ResourceType.TASK,
        project="project",
        domain="domain",
        name="name",
        version="version",
    )
    task_metadata = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
        (),
    )
    task_config = {
        "sparkConf": {
            "spark.driver.memory": "1000M",
            "spark.executor.memory": "1000M",
            "spark.executor.cores": "1",
            "spark.executor.instances": "2",
            "spark.driver.cores": "1",
        },
        "mainApplicationFile": "dbfs:/entrypoint.py",
        "databricksConf": {
            "run_name": "flytekit databricks plugin example",
            "new_cluster": {
                "spark_version": "12.2.x-scala2.12",
                "node_type_id": "n2-highmem-4",
                "num_workers": 1,
            },
            "timeout_seconds": 3600,
            "max_retries": 1,
        },
    }
    container = Container(
        image="flyteorg/flytekit:databricks-0.18.0-py3.7",
        command=[],
        args=[
            "pyflyte-fast-execute",
            "--additional-distribution",
            "s3://my-s3-bucket/flytesnacks/development/24UYJEF2HDZQN3SG4VAZSM4PLI======/script_mode.tar.gz",
            "--dest-dir",
            "/root",
            "--",
            "pyflyte-execute",
            "--inputs",
            "s3://my-s3-bucket",
            "--output-prefix",
            "s3://my-s3-bucket",
            "--raw-output-data-prefix",
            "s3://my-s3-bucket",
            "--checkpoint-path",
            "s3://my-s3-bucket",
            "--prev-checkpoint",
            "s3://my-s3-bucket",
            "--resolver",
            "flytekit.core.python_auto_container.default_task_resolver",
            "--",
            "task-module",
            "spark_local_example",
            "task-name",
            "hello_spark",
        ],
        resources=Resources(requests=[], limits=[]),
        env={"foo": "bar"},
        config={},
    )
    return TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        container=container,
        interface=None,
        type="spark",
    )


@pytest.fixture(scope="function")
def task_template_with_custom_secret(task_template) -> TaskTemplate:
    """Task template with a custom databricksTokenSecret."""
    task_template.custom["databricksTokenSecret"] = "my-team-databricks-token"
    return task_template


def _make_task_execution_metadata(namespace: str) -> TaskExecutionMetadata:
    """Helper to create a TaskExecutionMetadata with a given namespace."""
    task_exec_id = MagicMock()
    return TaskExecutionMetadata(
        task_execution_id=task_exec_id,
        namespace=namespace,
        labels={},
        annotations={},
        k8s_service_account="default",
        environment_variables={},
        identity=MagicMock(),
    )


# ===========================================================================
# Tests for get_secret_from_k8s
# ===========================================================================

class TestGetSecretFromK8s:
    """Tests for cross-namespace Kubernetes secret reading."""

    def test_success(self):
        """Secret found via mocked Kubernetes API client."""
        mock_secret = MagicMock()
        mock_secret.data = {"token": base64.b64encode(b"dapi_real_token_xyz").decode()}

        with patch("kubernetes.client.CoreV1Api") as mock_api_cls, \
             patch("kubernetes.config.load_incluster_config"):
            mock_api_cls.return_value.read_namespaced_secret.return_value = mock_secret
            result = get_secret_from_k8s("databricks-token", "token", "production")
            assert result == "dapi_real_token_xyz"
            mock_api_cls.return_value.read_namespaced_secret.assert_called_once_with(
                name="databricks-token", namespace="production"
            )

    def test_secret_not_found_404(self):
        """Secret doesn't exist in the namespace (404)."""
        from kubernetes.client.exceptions import ApiException

        with patch("kubernetes.client.CoreV1Api") as mock_api_cls, \
             patch("kubernetes.config.load_incluster_config"):
            mock_api_cls.return_value.read_namespaced_secret.side_effect = ApiException(status=404)
            result = get_secret_from_k8s("databricks-token", "token", "staging")
            assert result is None

    def test_secret_key_missing(self):
        """Secret exists but the 'token' key is not present."""
        mock_secret = MagicMock()
        mock_secret.data = {"other_key": base64.b64encode(b"something").decode()}

        with patch("kubernetes.client.CoreV1Api") as mock_api_cls, \
             patch("kubernetes.config.load_incluster_config"):
            mock_api_cls.return_value.read_namespaced_secret.return_value = mock_secret
            result = get_secret_from_k8s("databricks-token", "token", "production")
            assert result is None

    def test_secret_data_is_none(self):
        """Secret exists but data field is None."""
        mock_secret = MagicMock()
        mock_secret.data = None

        with patch("kubernetes.client.CoreV1Api") as mock_api_cls, \
             patch("kubernetes.config.load_incluster_config"):
            mock_api_cls.return_value.read_namespaced_secret.return_value = mock_secret
            result = get_secret_from_k8s("databricks-token", "token", "production")
            assert result is None

    def test_kubernetes_import_error(self):
        """kubernetes package not installed - graceful fallback."""
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "kubernetes" or name.startswith("kubernetes."):
                raise ImportError("No module named 'kubernetes'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = get_secret_from_k8s("databricks-token", "token", "production")
            assert result is None

    def test_api_exception_non_404(self):
        """Non-404 API exception is logged as warning, returns None."""
        from kubernetes.client.exceptions import ApiException

        with patch("kubernetes.client.CoreV1Api") as mock_api_cls, \
             patch("kubernetes.config.load_incluster_config"):
            mock_api_cls.return_value.read_namespaced_secret.side_effect = ApiException(status=403)
            result = get_secret_from_k8s("databricks-token", "token", "restricted-ns")
            assert result is None

    def test_kubeconfig_fallback(self):
        """Falls back to kubeconfig when in-cluster config fails."""
        from kubernetes.config import ConfigException

        mock_secret = MagicMock()
        mock_secret.data = {"token": base64.b64encode(b"local_token").decode()}

        with patch("kubernetes.client.CoreV1Api") as mock_api_cls, \
             patch("kubernetes.config.load_incluster_config", side_effect=ConfigException("not in cluster")), \
             patch("kubernetes.config.load_kube_config"):
            mock_api_cls.return_value.read_namespaced_secret.return_value = mock_secret
            result = get_secret_from_k8s("databricks-token", "token", "dev")
            assert result == "local_token"

    def test_both_configs_fail(self):
        """Both in-cluster and kubeconfig fail - returns None."""
        from kubernetes.config import ConfigException

        with patch("kubernetes.config.load_incluster_config", side_effect=ConfigException("not in cluster")), \
             patch("kubernetes.config.load_kube_config", side_effect=Exception("no kubeconfig")):
            result = get_secret_from_k8s("databricks-token", "token", "orphan-ns")
            assert result is None


# ===========================================================================
# Tests for get_databricks_token
# ===========================================================================

class TestGetDatabricksToken:
    """Tests for the multi-tenant token resolution strategy."""

    @patch("flytekitplugins.spark.connector.get_secret_from_k8s")
    def test_token_from_namespace_secret(self, mock_k8s):
        """Token found in namespace-specific K8s secret."""
        mock_k8s.return_value = "dapi_namespace_token"
        token = get_databricks_token(namespace="team-a")
        assert token == "dapi_namespace_token"
        mock_k8s.assert_called_once_with(
            secret_name="databricks-token",
            secret_key="token",
            namespace="team-a",
        )

    @patch("flytekitplugins.spark.connector.get_secret_from_k8s")
    def test_token_with_custom_secret_name(self, mock_k8s):
        """Token found using a custom K8s secret name."""
        mock_k8s.return_value = "dapi_custom_secret_token"
        token = get_databricks_token(namespace="team-b", secret_name="my-db-token")
        assert token == "dapi_custom_secret_token"
        mock_k8s.assert_called_once_with(
            secret_name="my-db-token",
            secret_key="token",
            namespace="team-b",
        )

    @patch("flytekitplugins.spark.connector.get_connector_secret")
    @patch("flytekitplugins.spark.connector.get_secret_from_k8s")
    def test_fallback_to_env_when_namespace_secret_missing(self, mock_k8s, mock_env):
        """Falls back to FLYTE_DATABRICKS_ACCESS_TOKEN when namespace secret not found."""
        mock_k8s.return_value = None
        mock_env.return_value = "dapi_env_fallback_token"
        token = get_databricks_token(namespace="team-c")
        assert token == "dapi_env_fallback_token"
        mock_env.assert_called_once_with("FLYTE_DATABRICKS_ACCESS_TOKEN")

    @patch("flytekitplugins.spark.connector.get_connector_secret")
    def test_no_namespace_falls_back_to_env(self, mock_env):
        """When no namespace is provided, goes directly to env var."""
        mock_env.return_value = "dapi_default_token"
        token = get_databricks_token(namespace=None)
        assert token == "dapi_default_token"
        mock_env.assert_called_once_with("FLYTE_DATABRICKS_ACCESS_TOKEN")

    @patch("flytekitplugins.spark.connector.get_connector_secret")
    @patch("flytekitplugins.spark.connector.get_secret_from_k8s")
    def test_no_token_from_any_source_raises(self, mock_k8s, mock_env):
        """ValueError raised when neither K8s secret nor env var has a token."""
        mock_k8s.return_value = None
        mock_env.side_effect = Exception("Secret not found")
        with pytest.raises(ValueError, match="No Databricks token found from any source"):
            get_databricks_token(namespace="orphan-ns")

    @patch("flytekitplugins.spark.connector.get_secret_from_k8s")
    def test_empty_token_raises(self, mock_k8s):
        """ValueError raised when token is an empty string."""
        mock_k8s.return_value = ""
        with pytest.raises(ValueError, match="Databricks token is empty"):
            get_databricks_token(namespace="team-d")

    @patch("flytekitplugins.spark.connector.get_connector_secret")
    @patch("flytekitplugins.spark.connector.get_secret_from_k8s")
    def test_default_secret_name_is_databricks_token(self, mock_k8s, mock_env):
        """Default secret name is 'databricks-token' when not specified."""
        mock_k8s.return_value = "found_it"
        get_databricks_token(namespace="ns-1")
        mock_k8s.assert_called_once_with(
            secret_name="databricks-token",
            secret_key="token",
            namespace="ns-1",
        )

    @patch("flytekitplugins.spark.connector.get_connector_secret")
    def test_backward_compatibility_no_namespace_no_secret(self, mock_env):
        """Backward compatible: no namespace + no secret name = env var only."""
        mock_env.return_value = "dapi_legacy_token"
        token = get_databricks_token()
        assert token == "dapi_legacy_token"


# ===========================================================================
# Tests for get_header
# ===========================================================================

class TestGetHeader:
    """Tests for authorization header generation."""

    def test_with_preresolved_auth_token(self):
        """Header uses pre-fetched auth token directly."""
        headers = get_header(auth_token="dapi_preresolved_123")
        assert headers == {
            "Authorization": "Bearer dapi_preresolved_123",
            "content-type": "application/json",
        }

    @patch("flytekitplugins.spark.connector.get_databricks_token")
    def test_without_auth_token_resolves(self, mock_get_token):
        """Header resolves token via get_databricks_token when not provided."""
        mock_get_token.return_value = "dapi_resolved_456"
        headers = get_header()
        assert headers["Authorization"] == "Bearer dapi_resolved_456"
        mock_get_token.assert_called_once()


# ===========================================================================
# Tests for DatabricksJobMetadata
# ===========================================================================

class TestDatabricksJobMetadata:
    """Tests for token persistence in job metadata."""

    def test_metadata_stores_auth_token(self):
        """Auth token is stored in metadata."""
        meta = DatabricksJobMetadata(
            databricks_instance="test.cloud.databricks.com",
            run_id="42",
            auth_token="dapi_persistent_token",
        )
        assert meta.auth_token == "dapi_persistent_token"

    def test_metadata_auth_token_defaults_to_none(self):
        """Auth token defaults to None for backward compatibility."""
        meta = DatabricksJobMetadata(
            databricks_instance="test.cloud.databricks.com",
            run_id="42",
        )
        assert meta.auth_token is None


# ===========================================================================
# Tests for DatabricksConnector create/get/delete with token
# ===========================================================================

class TestDatabricksConnectorWithToken:
    """Integration tests for connector operations with per-project tokens."""

    @pytest.mark.asyncio
    async def test_create_uses_namespace_token(self, task_template):
        """create() fetches token from namespace and uses it in API call."""
        task_template.custom["databricksInstance"] = "test.cloud.databricks.com"
        agent = AgentRegistry.get_agent("spark")
        metadata = _make_task_execution_metadata("project-alpha")

        with patch("flytekitplugins.spark.connector.get_databricks_token") as mock_token:
            mock_token.return_value = "dapi_project_alpha_token"
            create_url = f"https://test.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit"

            with aioresponses() as mocked:
                mocked.post(create_url, status=http.HTTPStatus.OK, payload={"run_id": "999"})
                result = await agent.create(task_template, None, task_execution_metadata=metadata)

            mock_token.assert_called_once_with(
                namespace="project-alpha",
                task_template=task_template,
                secret_name=None,
            )
            assert result.auth_token == "dapi_project_alpha_token"
            assert result.run_id == "999"

    @pytest.mark.asyncio
    async def test_create_with_custom_secret_name(self, task_template_with_custom_secret):
        """create() passes custom secret name from task template."""
        tt = task_template_with_custom_secret
        tt.custom["databricksInstance"] = "test.cloud.databricks.com"
        agent = AgentRegistry.get_agent("spark")
        metadata = _make_task_execution_metadata("team-x")

        with patch("flytekitplugins.spark.connector.get_databricks_token") as mock_token:
            mock_token.return_value = "dapi_team_x_token"
            create_url = f"https://test.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit"

            with aioresponses() as mocked:
                mocked.post(create_url, status=http.HTTPStatus.OK, payload={"run_id": "888"})
                result = await agent.create(tt, None, task_execution_metadata=metadata)

            mock_token.assert_called_once_with(
                namespace="team-x",
                task_template=tt,
                secret_name="my-team-databricks-token",
            )
            assert result.auth_token == "dapi_team_x_token"

    @pytest.mark.asyncio
    async def test_create_without_metadata_uses_no_namespace(self, task_template):
        """create() works when task_execution_metadata is None (backward compat)."""
        task_template.custom["databricksInstance"] = "test.cloud.databricks.com"
        agent = AgentRegistry.get_agent("spark")

        with patch("flytekitplugins.spark.connector.get_databricks_token") as mock_token:
            mock_token.return_value = "dapi_default_token"
            create_url = f"https://test.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit"

            with aioresponses() as mocked:
                mocked.post(create_url, status=http.HTTPStatus.OK, payload={"run_id": "777"})
                result = await agent.create(task_template, None)

            mock_token.assert_called_once_with(
                namespace=None,
                task_template=task_template,
                secret_name=None,
            )

    @pytest.mark.asyncio
    async def test_create_stores_token_in_metadata(self, task_template):
        """create() persists auth_token in returned DatabricksJobMetadata."""
        task_template.custom["databricksInstance"] = "test.cloud.databricks.com"
        agent = AgentRegistry.get_agent("spark")
        metadata = _make_task_execution_metadata("data-team")

        with patch("flytekitplugins.spark.connector.get_databricks_token") as mock_token:
            mock_token.return_value = "dapi_data_team_abc"
            create_url = f"https://test.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit"

            with aioresponses() as mocked:
                mocked.post(create_url, status=http.HTTPStatus.OK, payload={"run_id": "555"})
                result = await agent.create(task_template, None, task_execution_metadata=metadata)

        assert isinstance(result, DatabricksJobMetadata)
        assert result.auth_token == "dapi_data_team_abc"
        assert result.run_id == "555"
        assert result.databricks_instance == "test.cloud.databricks.com"

    @pytest.mark.asyncio
    async def test_get_uses_stored_token(self):
        """get() uses the auth_token stored in DatabricksJobMetadata."""
        meta = DatabricksJobMetadata(
            databricks_instance="test.cloud.databricks.com",
            run_id="123",
            auth_token="dapi_stored_get_token",
        )
        agent = AgentRegistry.get_agent("spark")
        get_url = f"https://test.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/get?run_id=123"

        mock_response = {
            "job_id": "1",
            "run_id": "123",
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": "OK"},
        }

        with aioresponses() as mocked:
            mocked.get(get_url, status=http.HTTPStatus.OK, payload=mock_response)
            resource = await agent.get(meta)

            # Verify the correct token was used in the request header
            call_args = list(mocked.requests.values())[0][0]
            assert call_args.kwargs["headers"]["Authorization"] == "Bearer dapi_stored_get_token"

    @pytest.mark.asyncio
    async def test_delete_uses_stored_token(self):
        """delete() uses the auth_token stored in DatabricksJobMetadata."""
        meta = DatabricksJobMetadata(
            databricks_instance="test.cloud.databricks.com",
            run_id="456",
            auth_token="dapi_stored_delete_token",
        )
        agent = AgentRegistry.get_agent("spark")
        delete_url = f"https://test.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/cancel"

        with aioresponses() as mocked:
            mocked.post(delete_url, status=http.HTTPStatus.OK, payload={})
            await agent.delete(meta)

            call_args = list(mocked.requests.values())[0][0]
            assert call_args.kwargs["headers"]["Authorization"] == "Bearer dapi_stored_delete_token"

    @pytest.mark.asyncio
    async def test_get_with_none_token_falls_back(self):
        """get() falls back to get_header default when auth_token is None."""
        meta = DatabricksJobMetadata(
            databricks_instance="test.cloud.databricks.com",
            run_id="789",
            auth_token=None,
        )
        agent = AgentRegistry.get_agent("spark")
        get_url = f"https://test.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/get?run_id=789"

        mock_response = {
            "job_id": "2",
            "run_id": "789",
            "state": {"life_cycle_state": "RUNNING"},
        }

        with patch("flytekitplugins.spark.connector.get_databricks_token") as mock_token:
            mock_token.return_value = "dapi_fallback_env_token"
            with aioresponses() as mocked:
                mocked.get(get_url, status=http.HTTPStatus.OK, payload=mock_response)
                resource = await agent.get(meta)

                call_args = list(mocked.requests.values())[0][0]
                assert call_args.kwargs["headers"]["Authorization"] == "Bearer dapi_fallback_env_token"


# ===========================================================================
# Tests for DatabricksV2 task config token secret
# ===========================================================================

class TestDatabricksV2TokenSecret:
    """Tests for DatabricksV2 databricks_token_secret field and serialization."""

    def test_token_secret_field_exists(self):
        """DatabricksV2 has databricks_token_secret field."""
        from flytekitplugins.spark.task import DatabricksV2

        config = DatabricksV2(
            databricks_conf={"new_cluster": {"spark_version": "12.2.x"}},
            databricks_instance="test.cloud.databricks.com",
            databricks_token_secret="my-team-secret",
        )
        assert config.databricks_token_secret == "my-team-secret"

    def test_token_secret_defaults_to_none(self):
        """databricks_token_secret defaults to None."""
        from flytekitplugins.spark.task import DatabricksV2

        config = DatabricksV2(
            databricks_conf={"new_cluster": {"spark_version": "12.2.x"}},
            databricks_instance="test.cloud.databricks.com",
        )
        assert config.databricks_token_secret is None

    def test_get_custom_includes_token_secret(self):
        """get_custom() serializes databricksTokenSecret when set."""
        import flytekit
        from flytekitplugins.spark.task import DatabricksV2
        from flytekit.configuration import Image, ImageConfig, SerializationSettings

        databricks_conf = {
            "name": "test",
            "new_cluster": {
                "spark_version": "12.2.x-scala2.12",
                "node_type_id": "r3.xlarge",
                "num_workers": 1,
                "docker_image": {"url": "test:latest"},
            },
            "timeout_seconds": 3600,
            "max_retries": 1,
        }

        @flytekit.task(
            task_config=DatabricksV2(
                databricks_conf=databricks_conf,
                databricks_instance="test.cloud.databricks.com",
                databricks_token_secret="project-x-token",
            )
        )
        def my_task(x: int) -> int:
            return x

        default_img = Image(name="default", fqn="test", tag="tag")
        settings = SerializationSettings(
            project="project",
            domain="domain",
            version="version",
            env={},
            image_config=ImageConfig(default_image=default_img, images=[default_img]),
        )

        custom = my_task.get_custom(settings)
        assert "databricksTokenSecret" in custom
        assert custom["databricksTokenSecret"] == "project-x-token"

    def test_get_custom_excludes_token_secret_when_none(self):
        """get_custom() does NOT include databricksTokenSecret when None."""
        import flytekit
        from flytekitplugins.spark.task import DatabricksV2
        from flytekit.configuration import Image, ImageConfig, SerializationSettings

        databricks_conf = {
            "name": "test",
            "new_cluster": {
                "spark_version": "12.2.x-scala2.12",
                "node_type_id": "r3.xlarge",
                "num_workers": 1,
                "docker_image": {"url": "test:latest"},
            },
        }

        @flytekit.task(
            task_config=DatabricksV2(
                databricks_conf=databricks_conf,
                databricks_instance="test.cloud.databricks.com",
            )
        )
        def my_task(x: int) -> int:
            return x

        default_img = Image(name="default", fqn="test", tag="tag")
        settings = SerializationSettings(
            project="project",
            domain="domain",
            version="version",
            env={},
            image_config=ImageConfig(default_image=default_img, images=[default_img]),
        )

        custom = my_task.get_custom(settings)
        assert "databricksTokenSecret" not in custom
