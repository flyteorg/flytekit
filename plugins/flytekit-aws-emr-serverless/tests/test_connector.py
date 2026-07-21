"""
Unit tests for EMR Serverless Connector.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from flyteidl.core.execution_pb2 import TaskExecution

from flytekitplugins.awsemrserverless import (
    EMRServerlessConnector,
    EMRServerlessJobMetadata,
)
from flytekit.extend.backend.utils import convert_to_flyte_phase

from flytekitplugins.awsemrserverless.connector import (
    EMR_SERVERLESS_STATES,
    _ENV_ALLOW_CREATE_APPLICATION,
    _ENV_APPLICATION_NAME_PREFIX,
)


def _make_handler(**overrides) -> AsyncMock:
    """Build a mock EMRServerlessHandler with sensible defaults."""
    handler = AsyncMock()
    handler.ensure_application_started = AsyncMock()
    handler.start_job_run = AsyncMock(return_value="job-123")
    handler.create_application = AsyncMock(return_value="new-app-123")
    handler.get_application = AsyncMock(return_value={
        "applicationId": "00f5abc123def456",
        "state": "STARTED",
        "imageConfiguration": {},
    })
    handler.update_application = AsyncMock()
    handler.client.meta.region_name = "us-east-1"
    handler.region = "us-east-1"
    for k, v in overrides.items():
        setattr(handler, k, v)
    return handler


class TestEMRServerlessJobMetadata:
    def test_metadata_creation(self):
        metadata = EMRServerlessJobMetadata(
            application_id="app-123",
            job_run_id="job-456",
            region="us-east-1",
        )
        assert metadata.application_id == "app-123"
        assert metadata.job_run_id == "job-456"
        assert metadata.region == "us-east-1"
        assert metadata.created_application is False

    def test_metadata_with_created_app(self):
        metadata = EMRServerlessJobMetadata(
            application_id="app-123",
            job_run_id="job-456",
            region="us-west-2",
            created_application=True,
        )
        assert metadata.created_application is True


class TestEMRServerlessConnector:
    def test_connector_initialization(self):
        connector = EMRServerlessConnector()
        assert connector.name == "EMR Serverless Connector"

    @pytest.mark.asyncio
    async def test_create_with_existing_application(self, sample_config):
        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = sample_config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=sample_config):
                metadata = await connector.create(mock_template)

        assert isinstance(metadata, EMRServerlessJobMetadata)
        assert metadata.application_id == sample_config.application_id
        assert metadata.job_run_id == "job-123"
        mock_handler.ensure_application_started.assert_called_once()
        mock_handler.start_job_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_with_new_application(self, sample_spark_job_driver):
        """application_name + env var true => create succeeds."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
            application_name="test-app",
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.dict(os.environ, {_ENV_ALLOW_CREATE_APPLICATION: "true"}):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    metadata = await connector.create(mock_template)

        assert metadata.application_id == "new-app-123"
        assert metadata.created_application is True
        mock_handler.create_application.assert_called_once()
        assert mock_handler.create_application.call_args.kwargs["name"] == "test-app"

    @pytest.mark.asyncio
    async def test_create_fails_no_app_id_no_app_name(self, sample_spark_job_driver):
        """Without application_id or application_name, create must fail."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.dict(os.environ, {_ENV_ALLOW_CREATE_APPLICATION: "true"}):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    with pytest.raises(ValueError, match="No application_id provided and no application_name"):
                        await connector.create(mock_template)

    @pytest.mark.asyncio
    async def test_create_blocked_by_connector_env_var(self, sample_spark_job_driver):
        """When connector env var is false (default), creation is blocked even with application_name."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
            application_name="my-app",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.dict(os.environ, {_ENV_ALLOW_CREATE_APPLICATION: "false"}):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    with pytest.raises(ValueError, match="disabled by the connector"):
                        await connector.create(mock_template)

    @pytest.mark.asyncio
    async def test_create_blocked_when_env_var_unset(self, sample_spark_job_driver):
        """When the env var is not set, default is false => creation blocked."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
            application_name="my-app",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        env = os.environ.copy()
        env.pop(_ENV_ALLOW_CREATE_APPLICATION, None)
        with patch.dict(os.environ, env, clear=True):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    with pytest.raises(ValueError, match="disabled by the connector"):
                        await connector.create(mock_template)

    @pytest.mark.asyncio
    async def test_create_applies_application_name_prefix(self, sample_spark_job_driver):
        """FLYTE_EMR_APPLICATION_NAME_PREFIX should be prepended to the app name."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
            application_name="my-etl-app",
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.dict(os.environ, {
            _ENV_ALLOW_CREATE_APPLICATION: "true",
            _ENV_APPLICATION_NAME_PREFIX: "flyte-prod-",
        }):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    await connector.create(mock_template)

        call_kwargs = mock_handler.create_application.call_args.kwargs
        assert call_kwargs["name"] == "flyte-prod-my-etl-app"

    @pytest.mark.asyncio
    async def test_create_no_double_prefix(self, sample_spark_job_driver):
        """If application_name already starts with the prefix, don't double-prefix."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
            application_name="flyte-prod-my-app",
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.dict(os.environ, {
            _ENV_ALLOW_CREATE_APPLICATION: "true",
            _ENV_APPLICATION_NAME_PREFIX: "flyte-prod-",
        }):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    await connector.create(mock_template)

        call_kwargs = mock_handler.create_application.call_args.kwargs
        assert call_kwargs["name"] == "flyte-prod-my-app"

    @pytest.mark.asyncio
    async def test_create_no_prefix_when_unset(self, sample_spark_job_driver):
        """Without the prefix env var, the application name is used as-is."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
            application_name="my-raw-name",
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        env = os.environ.copy()
        env.pop(_ENV_APPLICATION_NAME_PREFIX, None)
        env[_ENV_ALLOW_CREATE_APPLICATION] = "true"
        with patch.dict(os.environ, env, clear=True):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    await connector.create(mock_template)

        call_kwargs = mock_handler.create_application.call_args.kwargs
        assert call_kwargs["name"] == "my-raw-name"

    @pytest.mark.asyncio
    async def test_create_passes_architecture_and_scheduler(self, sample_spark_job_driver):
        """Verify new app-creation params are forwarded to the handler."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
            application_name="test-app",
            architecture="ARM64",
            scheduler_configuration={"maxConcurrentRuns": 5},
            auto_stop_config={"enabled": True, "idleTimeoutMinutes": 30},
            runtime_configuration=[{"classification": "spark-defaults", "properties": {"spark.executor.cores": "8"}}],
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.dict(os.environ, {_ENV_ALLOW_CREATE_APPLICATION: "true"}):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    await connector.create(mock_template)

        call_kwargs = mock_handler.create_application.call_args.kwargs
        assert call_kwargs["architecture"] == "ARM64"
        assert call_kwargs["scheduler_configuration"] == {"maxConcurrentRuns": 5}
        assert call_kwargs["auto_stop_config"] == {"enabled": True, "idleTimeoutMinutes": 30}
        assert call_kwargs["runtime_configuration"][0]["classification"] == "spark-defaults"

    @pytest.mark.asyncio
    async def test_create_with_retry_policy(self, sample_config):
        """Verify retry_policy is forwarded to start_job_run."""
        from dataclasses import replace

        config = replace(sample_config, retry_policy={"maxAttempts": 3})

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        assert call_kwargs["retry_policy"] == {"maxAttempts": 3}

    @pytest.mark.asyncio
    async def test_create_syncs_image_from_container_image(self):
        """When sync_image=True and container_image differs from app, update_application is called.

        Script mode skips sync unless image_configuration is explicitly set,
        so this test uses a script-mode config with image_configuration to
        exercise the sync path.
        """
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
            image_configuration={"imageUri": "123456.dkr.ecr.us-east-1.amazonaws.com/spark:v2"},
            sync_image=True,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()
        mock_handler.get_application = AsyncMock(return_value={
            "applicationId": "00abc123",
            "state": "STARTED",
            "imageConfiguration": {"imageUri": "123456.dkr.ecr.us-east-1.amazonaws.com/spark:v1"},
        })

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = MagicMock()
        mock_template.container.image = "123456.dkr.ecr.us-east-1.amazonaws.com/spark:v2"

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        mock_handler.update_application.assert_called_once_with(
            application_id="00abc123",
            image_configuration={"imageUri": "123456.dkr.ecr.us-east-1.amazonaws.com/spark:v2"},
        )

    @pytest.mark.asyncio
    async def test_create_syncs_image_falls_back_to_image_configuration(self):
        """When container_image is absent, sync_image falls back to image_configuration."""
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
            image_configuration={"imageUri": "new-image:v2"},
            sync_image=True,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()
        mock_handler.get_application = AsyncMock(return_value={
            "applicationId": "00abc123",
            "state": "STARTED",
            "imageConfiguration": {"imageUri": "old-image:v1"},
        })

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = None

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        mock_handler.update_application.assert_called_once_with(
            application_id="00abc123",
            image_configuration={"imageUri": "new-image:v2"},
        )

    @pytest.mark.asyncio
    async def test_create_skips_image_sync_when_same(self):
        """When container_image matches the application, update is skipped."""
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
            sync_image=True,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()
        mock_handler.get_application = AsyncMock(return_value={
            "applicationId": "00abc123",
            "state": "STARTED",
            "imageConfiguration": {"imageUri": "same-image:v1"},
        })

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = MagicMock()
        mock_template.container.image = "same-image:v1"

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        mock_handler.update_application.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_skips_image_sync_when_disabled(self):
        """When sync_image=False, update_application should not be called."""
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = MagicMock()
        mock_template.container.image = "new-image:v2"

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        mock_handler.update_application.assert_not_called()
        mock_handler.get_application.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_image_sync_noop_when_no_image(self):
        """When there's no container_image or image_configuration, sync is a no-op."""
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
            sync_image=True,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = None

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        mock_handler.update_application.assert_not_called()
        mock_handler.get_application.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_merges_spark_submit_params_in_script_mode(self):
        """Top-level spark_submit_parameters should be appended to the job driver."""
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(
                entry_point="s3://bucket/main.py",
                spark_submit_parameters="--conf spark.executor.memory=4g",
            ),
            spark_submit_parameters="--conf spark.driver.memory=8g",
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        submit_params = call_kwargs["job_driver"]["sparkSubmit"]["sparkSubmitParameters"]
        assert "spark.executor.memory=4g" in submit_params
        assert "spark.driver.memory=8g" in submit_params

    @pytest.mark.asyncio
    async def test_create_merges_application_configuration(self):
        """application_configuration should be merged into configuration_overrides."""
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
            configuration_overrides={
                "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": "s3://logs/"}}
            },
            application_configuration=[
                {"classification": "spark-defaults", "properties": {"spark.executor.cores": "8"}}
            ],
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = None

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        config_overrides = call_kwargs["configuration_overrides"]
        assert "monitoringConfiguration" in config_overrides
        assert len(config_overrides["applicationConfiguration"]) == 1
        assert config_overrides["applicationConfiguration"][0]["classification"] == "spark-defaults"

    @pytest.mark.asyncio
    async def test_create_pythonic_mode(self):
        """Test create with Pythonic mode (no explicit job driver)."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_container = MagicMock()
        mock_container.image = "my-image:latest"
        mock_container.args = [
            "pyflyte-execute",
            "--task-module",
            "my.module",
            "--task-name",
            "my_task",
        ]

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = mock_container

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                with patch.object(
                    connector, "_ensure_entrypoint_on_s3", return_value="s3://bucket/flyte/entrypoint.py"
                ):
                    metadata = await connector.create(mock_template)

        assert metadata.job_run_id == "job-123"
        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        assert "sparkSubmit" in call_kwargs["job_driver"]
        assert call_kwargs["job_driver"]["sparkSubmit"]["entryPoint"] == "s3://bucket/flyte/entrypoint.py"
        assert "pyflyte-execute" in call_kwargs["job_driver"]["sparkSubmit"]["entryPointArguments"]

    @pytest.mark.asyncio
    async def test_create_pythonic_mode_with_spark_submit_params(self):
        """spark_submit_parameters should be merged in Pythonic mode."""
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_submit_parameters="--conf spark.executor.memory=16g --conf spark.executor.cores=8",
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_container = MagicMock()
        mock_container.image = "my-image:latest"
        mock_container.args = ["pyflyte-execute", "--task-module", "my.module"]

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = mock_container

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                with patch.object(
                    connector, "_ensure_entrypoint_on_s3", return_value="s3://bucket/flyte/entrypoint.py"
                ):
                    await connector.create(mock_template)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        submit_params = call_kwargs["job_driver"]["sparkSubmit"]["sparkSubmitParameters"]
        assert "spark.executor.memory=16g" in submit_params
        assert "spark.executor.cores=8" in submit_params
        assert "PYSPARK_PYTHON" in submit_params

    @pytest.mark.asyncio
    async def test_create_new_app_gets_default_image_in_pythonic_mode(self):
        """When creating an app in Pythonic mode with no image, a default is used."""
        from flytekitplugins.awsemrserverless import EMRServerless
        from flytekitplugins.awsemrserverless.task import EMR_SERVERLESS_BASE_IMAGE

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_name="pythonic-app",
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = MagicMock()
        mock_template.container.image = None
        mock_template.container.args = ["pyflyte-execute"]

        with patch.dict(os.environ, {_ENV_ALLOW_CREATE_APPLICATION: "true"}):
            with patch.object(connector, "_get_handler", return_value=mock_handler):
                with patch.object(connector, "_extract_config", return_value=config):
                    with patch.object(
                        connector, "_ensure_entrypoint_on_s3", return_value="s3://bucket/entrypoint.py"
                    ):
                        await connector.create(mock_template)

        call_kwargs = mock_handler.create_application.call_args.kwargs
        assert call_kwargs["image_configuration"]["imageUri"] == EMR_SERVERLESS_BASE_IMAGE

    @pytest.mark.asyncio
    async def test_get_running_job(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        mock_handler = AsyncMock()
        mock_handler.get_job_run = AsyncMock(
            return_value={
                "state": "RUNNING",
                "stateDetails": "Job is executing",
            }
        )

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            resource = await connector.get(sample_job_metadata)

        assert resource.phase == TaskExecution.RUNNING
        assert "RUNNING" in resource.message

    @pytest.mark.asyncio
    async def test_get_successful_job(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        mock_handler = AsyncMock()
        mock_handler.get_job_run = AsyncMock(
            return_value={
                "state": "SUCCESS",
                "stateDetails": "Job completed successfully",
            }
        )

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            resource = await connector.get(sample_job_metadata)

        assert resource.phase == TaskExecution.SUCCEEDED

    @pytest.mark.asyncio
    async def test_get_failed_job(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        mock_handler = AsyncMock()
        mock_handler.get_job_run = AsyncMock(
            return_value={
                "state": "FAILED",
                "stateDetails": "OutOfMemoryError",
            }
        )

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            resource = await connector.get(sample_job_metadata)

        assert resource.phase == TaskExecution.FAILED
        assert "OutOfMemoryError" in resource.message

    @pytest.mark.asyncio
    async def test_get_cancelled_job(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        mock_handler = AsyncMock()
        mock_handler.get_job_run = AsyncMock(
            return_value={
                "state": "CANCELLED",
                "stateDetails": "User cancelled",
            }
        )

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            resource = await connector.get(sample_job_metadata)

        assert resource.phase == TaskExecution.FAILED

    @pytest.mark.asyncio
    async def test_get_job_not_found(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        mock_handler = AsyncMock()
        mock_handler.get_job_run = AsyncMock(side_effect=RuntimeError("Job not found"))

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            resource = await connector.get(sample_job_metadata)

        assert resource.phase == TaskExecution.FAILED
        assert "not found" in resource.message.lower()

    @pytest.mark.asyncio
    async def test_get_pending_states(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        for state in ["PENDING", "SCHEDULED", "SUBMITTED"]:
            mock_handler = AsyncMock()
            mock_handler.get_job_run = AsyncMock(return_value={"state": state, "stateDetails": ""})

            with patch.object(connector, "_get_handler", return_value=mock_handler):
                resource = await connector.get(sample_job_metadata)

            assert resource.phase == TaskExecution.RUNNING, f"State {state} should map to RUNNING"

    @pytest.mark.asyncio
    async def test_delete_running_job(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        mock_handler = AsyncMock()
        mock_handler.cancel_job_run = AsyncMock()

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            await connector.delete(sample_job_metadata)

        mock_handler.cancel_job_run.assert_called_once_with(
            application_id=sample_job_metadata.application_id,
            job_run_id=sample_job_metadata.job_run_id,
        )

    @pytest.mark.asyncio
    async def test_delete_handles_errors_gracefully(self, sample_job_metadata):
        connector = EMRServerlessConnector()

        mock_handler = AsyncMock()
        mock_handler.cancel_job_run = AsyncMock(side_effect=Exception("Network error"))

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            await connector.delete(sample_job_metadata)


class TestEMRServerlessStateMapping:
    def test_all_states_mapped(self):
        expected_states = [
            "PENDING",
            "SCHEDULED",
            "SUBMITTED",
            "RUNNING",
            "SUCCESS",
            "FAILED",
            "CANCELLING",
            "CANCELLED",
        ]
        for state in expected_states:
            assert state in EMR_SERVERLESS_STATES, f"Missing mapping for {state}"

    def test_running_states_map_to_running(self):
        running_states = ["PENDING", "SCHEDULED", "SUBMITTED", "RUNNING", "CANCELLING"]
        for state in running_states:
            assert EMR_SERVERLESS_STATES[state] == "Running"
            assert convert_to_flyte_phase(EMR_SERVERLESS_STATES[state]) == TaskExecution.RUNNING

    def test_success_maps_to_succeeded(self):
        assert EMR_SERVERLESS_STATES["SUCCESS"] == "Success"
        assert convert_to_flyte_phase(EMR_SERVERLESS_STATES["SUCCESS"]) == TaskExecution.SUCCEEDED

    def test_failure_states_map_to_failed(self):
        for state in ["FAILED", "CANCELLED"]:
            assert EMR_SERVERLESS_STATES[state] == "Failed"
            assert convert_to_flyte_phase(EMR_SERVERLESS_STATES[state]) == TaskExecution.FAILED


def _make_task_execution_metadata(
    *,
    exec_name: str = "abc123def456",
    exec_project: str = "flytetest",
    exec_domain: str = "development",
    task_project: str = "flytetest",
    task_domain: str = "development",
    task_name: str = "examples.script_spark_job.spark_demo_task",
    task_version: str = "v1",
    node_id: str = "n0",
    retry_attempt: int = 0,
    environment_variables: dict = None,
) -> MagicMock:
    """Build a mock TaskExecutionMetadata with the nested id graph populated."""
    meta = MagicMock()
    meta.task_execution_id.task_id.project = task_project
    meta.task_execution_id.task_id.domain = task_domain
    meta.task_execution_id.task_id.name = task_name
    meta.task_execution_id.task_id.version = task_version
    meta.task_execution_id.node_execution_id.node_id = node_id
    meta.task_execution_id.node_execution_id.execution_id.name = exec_name
    meta.task_execution_id.node_execution_id.execution_id.project = exec_project
    meta.task_execution_id.node_execution_id.execution_id.domain = exec_domain
    meta.task_execution_id.retry_attempt = retry_attempt
    meta.environment_variables = environment_variables or {}
    return meta


class TestFlyteEnvInjection:
    """Tests for the Flyte context env var injection feature."""

    def test_build_flyte_env_vars_none_metadata(self):
        assert EMRServerlessConnector._build_flyte_env_vars(None) == {}

    def test_build_flyte_env_vars_full(self):
        meta = _make_task_execution_metadata()
        env = EMRServerlessConnector._build_flyte_env_vars(meta)
        assert env["FLYTE_INTERNAL_EXECUTION_ID"] == "abc123def456"
        assert env["FLYTE_INTERNAL_EXECUTION_PROJECT"] == "flytetest"
        assert env["FLYTE_INTERNAL_EXECUTION_DOMAIN"] == "development"
        assert env["FLYTE_INTERNAL_TASK_PROJECT"] == "flytetest"
        assert env["FLYTE_INTERNAL_TASK_DOMAIN"] == "development"
        assert env["FLYTE_INTERNAL_TASK_NAME"] == "examples.script_spark_job.spark_demo_task"
        assert env["FLYTE_INTERNAL_TASK_VERSION"] == "v1"
        assert env["FLYTE_INTERNAL_NODE_ID"] == "n0"
        assert env["FLYTE_INTERNAL_TASK_RETRY_ATTEMPT"] == "0"

    def test_build_flyte_env_vars_includes_user_env(self):
        meta = _make_task_execution_metadata(environment_variables={"MY_VAR": "hello"})
        env = EMRServerlessConnector._build_flyte_env_vars(meta)
        assert env["MY_VAR"] == "hello"

    def test_build_flyte_env_vars_drops_empty_values(self):
        meta = _make_task_execution_metadata(task_version="")
        env = EMRServerlessConnector._build_flyte_env_vars(meta)
        assert "FLYTE_INTERNAL_TASK_VERSION" not in env
        assert "FLYTE_INTERNAL_EXECUTION_ID" in env

    def test_format_env_as_spark_conf_empty(self):
        assert EMRServerlessConnector._format_env_as_spark_conf({}) == ""

    def test_format_env_as_spark_conf_produces_driver_and_executor(self):
        conf = EMRServerlessConnector._format_env_as_spark_conf(
            {"FLYTE_INTERNAL_EXECUTION_ID": "abc123"}
        )
        assert "spark.emr-serverless.driverEnv.FLYTE_INTERNAL_EXECUTION_ID=abc123" in conf
        assert "spark.executorEnv.FLYTE_INTERNAL_EXECUTION_ID=abc123" in conf

    def test_format_env_as_spark_conf_skips_unsafe_values(self):
        conf = EMRServerlessConnector._format_env_as_spark_conf(
            {"SAFE": "ok", "BAD_SPACES": "has spaces", "BAD_QUOTE": 'has"quote'}
        )
        assert "SAFE=ok" in conf
        assert "BAD_SPACES" not in conf
        assert "BAD_QUOTE" not in conf

    def test_append_flyte_env_disabled(self):
        meta = _make_task_execution_metadata()
        result = EMRServerlessConnector._append_flyte_env_to_spark_params(
            "--conf spark.executor.memory=4g", meta, enabled=False,
        )
        assert result == "--conf spark.executor.memory=4g"

    def test_append_flyte_env_none_metadata(self):
        result = EMRServerlessConnector._append_flyte_env_to_spark_params(
            "--conf spark.executor.memory=4g", None, enabled=True,
        )
        assert result == "--conf spark.executor.memory=4g"

    def test_append_flyte_env_preserves_existing_params(self):
        meta = _make_task_execution_metadata()
        result = EMRServerlessConnector._append_flyte_env_to_spark_params(
            "--conf spark.executor.memory=4g", meta, enabled=True,
        )
        assert result.startswith("--conf spark.executor.memory=4g ")
        assert "FLYTE_INTERNAL_EXECUTION_ID=abc123def456" in result

    def test_append_flyte_env_when_existing_is_none(self):
        meta = _make_task_execution_metadata()
        result = EMRServerlessConnector._append_flyte_env_to_spark_params(
            None, meta, enabled=True,
        )
        assert "FLYTE_INTERNAL_EXECUTION_ID=abc123def456" in result
        assert not result.startswith(" ")

    @pytest.mark.asyncio
    async def test_create_injects_flyte_env_in_script_mode(self):
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(
                entry_point="s3://bucket/main.py",
                spark_submit_parameters="--conf spark.executor.memory=4g",
            ),
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()
        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        meta = _make_task_execution_metadata(exec_name="exec-xyz-789")

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template, task_execution_metadata=meta)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        submit_params = call_kwargs["job_driver"]["sparkSubmit"]["sparkSubmitParameters"]
        assert "spark.executor.memory=4g" in submit_params
        assert "spark.emr-serverless.driverEnv.FLYTE_INTERNAL_EXECUTION_ID=exec-xyz-789" in submit_params
        assert "spark.executorEnv.FLYTE_INTERNAL_EXECUTION_ID=exec-xyz-789" in submit_params

    @pytest.mark.asyncio
    async def test_create_respects_inject_flyte_env_false(self):
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(
                entry_point="s3://bucket/main.py",
                spark_submit_parameters="--conf spark.executor.memory=4g",
            ),
            sync_image=False,
            region="us-east-1",
            inject_flyte_env=False,
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()
        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        meta = _make_task_execution_metadata()

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template, task_execution_metadata=meta)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        submit_params = call_kwargs["job_driver"]["sparkSubmit"]["sparkSubmitParameters"]
        assert "FLYTE_INTERNAL_EXECUTION_ID" not in submit_params

    @pytest.mark.asyncio
    async def test_create_injects_flyte_env_in_pythonic_mode(self):
        from flytekitplugins.awsemrserverless import EMRServerless

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()

        mock_container = MagicMock()
        mock_container.image = "my-image:latest"
        mock_container.args = ["pyflyte-execute", "--task-module", "m"]

        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"
        mock_template.container = mock_container

        meta = _make_task_execution_metadata(exec_name="py-exec-001")

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                with patch.object(
                    connector, "_ensure_entrypoint_on_s3",
                    return_value="s3://bucket/entrypoint.py",
                ):
                    await connector.create(mock_template, task_execution_metadata=meta)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        submit_params = call_kwargs["job_driver"]["sparkSubmit"]["sparkSubmitParameters"]
        assert "spark.emr-serverless.driverEnv.FLYTE_INTERNAL_EXECUTION_ID=py-exec-001" in submit_params
        assert "spark.executorEnv.FLYTE_INTERNAL_EXECUTION_ID=py-exec-001" in submit_params

    @pytest.mark.asyncio
    async def test_create_handles_missing_metadata_gracefully(self):
        """No task_execution_metadata (e.g. local mode) should not raise."""
        from flytekitplugins.awsemrserverless import (
            EMRServerless,
            EMRServerlessSparkJobDriver,
        )

        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_id="00abc123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://b/m.py"),
            sync_image=False,
            region="us-east-1",
        )

        connector = EMRServerlessConnector()
        mock_handler = _make_handler()
        mock_template = MagicMock()
        mock_template.custom = config.to_dict()
        mock_template.id = MagicMock()
        mock_template.id.name = "test-task"

        with patch.object(connector, "_get_handler", return_value=mock_handler):
            with patch.object(connector, "_extract_config", return_value=config):
                await connector.create(mock_template)

        call_kwargs = mock_handler.start_job_run.call_args.kwargs
        submit_params = call_kwargs["job_driver"]["sparkSubmit"].get("sparkSubmitParameters")
        if submit_params is not None:
            assert "FLYTE_INTERNAL_EXECUTION_ID" not in submit_params


class TestEntrypointBucketResolution:
    """Resolution order for the Pythonic-mode entrypoint S3 bucket.

    Documented contract (see :py:meth:`EMRServerlessConnector._resolve_entrypoint_bucket`):

      1. ``FLYTE_EMR_ENTRYPOINT_S3_BUCKET`` env var (preferred).
      2. ``configuration_overrides.monitoringConfiguration.s3MonitoringConfiguration.logUri``
         (reuses the same bucket the user already uses for monitoring logs).
      3. ``FLYTE_AWS_S3_BUCKET`` env var (Flyte's general-purpose bucket).
      4. Otherwise, raise -- Pythonic mode requires a bucket.
    """

    def test_env_var_takes_priority(self, sample_task_config):
        with patch.dict(
            os.environ,
            {"FLYTE_EMR_ENTRYPOINT_S3_BUCKET": "explicit-bucket", "FLYTE_AWS_S3_BUCKET": "fallback"},
            clear=False,
        ):
            assert (
                EMRServerlessConnector._resolve_entrypoint_bucket(sample_task_config) == "explicit-bucket"
            )

    def test_env_var_strips_s3_prefix(self, sample_task_config):
        with patch.dict(
            os.environ, {"FLYTE_EMR_ENTRYPOINT_S3_BUCKET": "s3://my-bucket/some/prefix"}, clear=False
        ):
            assert (
                EMRServerlessConnector._resolve_entrypoint_bucket(sample_task_config) == "my-bucket"
            )

    def test_falls_back_to_monitoring_log_uri(self, sample_task_config):
        sample_task_config.configuration_overrides = {
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": "s3://monitoring-bucket/logs"}
            }
        }
        with patch.dict(
            os.environ, {"FLYTE_EMR_ENTRYPOINT_S3_BUCKET": "", "FLYTE_AWS_S3_BUCKET": ""}, clear=False
        ):
            os.environ.pop("FLYTE_EMR_ENTRYPOINT_S3_BUCKET", None)
            os.environ.pop("FLYTE_AWS_S3_BUCKET", None)
            assert (
                EMRServerlessConnector._resolve_entrypoint_bucket(sample_task_config)
                == "monitoring-bucket"
            )

    def test_falls_back_to_flyte_aws_s3_bucket(self, sample_task_config):
        with patch.dict(
            os.environ, {"FLYTE_AWS_S3_BUCKET": "flyte-bucket"}, clear=False
        ):
            os.environ.pop("FLYTE_EMR_ENTRYPOINT_S3_BUCKET", None)
            assert (
                EMRServerlessConnector._resolve_entrypoint_bucket(sample_task_config) == "flyte-bucket"
            )

    def test_raises_when_no_source_configured(self, sample_task_config):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FLYTE_EMR_ENTRYPOINT_S3_BUCKET", None)
            os.environ.pop("FLYTE_AWS_S3_BUCKET", None)
            with pytest.raises(ValueError, match="entrypoint"):
                EMRServerlessConnector._resolve_entrypoint_bucket(sample_task_config)


class TestEnsureEntrypointOnS3:
    """The connector uploads the entrypoint exactly once per content-hash,
    keyed under ``flyte/emr-serverless/entrypoint-<sha256[:12]>.py``."""

    def _make_handler_with_region(self):
        h = MagicMock()
        h.region = "us-east-1"
        return h

    def test_skips_upload_when_already_present(self, sample_task_config):
        from flytekitplugins.awsemrserverless.connector import EMRServerlessConnector

        connector = EMRServerlessConnector()
        handler = self._make_handler_with_region()

        s3_client = MagicMock()
        s3_client.head_object.return_value = {"ContentLength": 1234}

        with (
            patch.dict(os.environ, {"FLYTE_EMR_ENTRYPOINT_S3_BUCKET": "test-bucket"}, clear=False),
            patch("boto3.client", return_value=s3_client),
        ):
            uri = connector._ensure_entrypoint_on_s3(handler, sample_task_config)

        assert uri.startswith("s3://test-bucket/flyte/emr-serverless/entrypoint-")
        assert uri.endswith(".py")
        s3_client.head_object.assert_called_once()
        s3_client.put_object.assert_not_called()

    def test_uploads_when_missing(self, sample_task_config):
        from flytekitplugins.awsemrserverless.connector import EMRServerlessConnector

        connector = EMRServerlessConnector()
        handler = self._make_handler_with_region()

        s3_client = MagicMock()
        s3_client.head_object.side_effect = Exception("NotFound")

        with (
            patch.dict(os.environ, {"FLYTE_EMR_ENTRYPOINT_S3_BUCKET": "test-bucket"}, clear=False),
            patch("boto3.client", return_value=s3_client),
        ):
            uri = connector._ensure_entrypoint_on_s3(handler, sample_task_config)

        s3_client.put_object.assert_called_once()
        put_kwargs = s3_client.put_object.call_args.kwargs
        assert put_kwargs["Bucket"] == "test-bucket"
        assert put_kwargs["Key"].startswith("flyte/emr-serverless/entrypoint-")
        assert put_kwargs["ContentType"] == "text/x-python"
        # The body is the byte-identical content of _entrypoint.py
        from flytekitplugins.awsemrserverless import _entrypoint
        from pathlib import Path

        assert put_kwargs["Body"] == Path(_entrypoint.__file__).read_bytes()
        assert uri == f"s3://test-bucket/{put_kwargs['Key']}"

    def test_uri_is_deterministic_across_calls(self, sample_task_config):
        """Two consecutive calls produce the same URI -- this is what
        makes EMR's job specs stable across connector restarts."""
        from flytekitplugins.awsemrserverless.connector import EMRServerlessConnector

        connector = EMRServerlessConnector()
        handler = self._make_handler_with_region()
        s3_client = MagicMock()
        s3_client.head_object.return_value = {"ContentLength": 1}

        with (
            patch.dict(os.environ, {"FLYTE_EMR_ENTRYPOINT_S3_BUCKET": "b"}, clear=False),
            patch("boto3.client", return_value=s3_client),
        ):
            uri1 = connector._ensure_entrypoint_on_s3(handler, sample_task_config)
            uri2 = connector._ensure_entrypoint_on_s3(handler, sample_task_config)

        assert uri1 == uri2
