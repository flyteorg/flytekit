"""
Unit tests for the EMR Serverless boto3 handler.

Tests the thin typed methods on :class:`EMRServerlessHandler` by patching
the single ``_call`` (and ``_paginate``) chokepoint that funnels all boto3
traffic.  This mirrors the flytekit-aws-sagemaker testing pattern
(``mock.patch("...Boto3ConnectorMixin._call")``).
"""

from unittest.mock import AsyncMock, patch

import pytest
from botocore.exceptions import ClientError

from flytekitplugins.awsemrserverless.boto_handler import EMRServerlessHandler


class TestEMRServerlessHandlerInit:
    def test_init_default(self):
        handler = EMRServerlessHandler()
        assert handler.region is None

    def test_init_with_region(self):
        handler = EMRServerlessHandler(region="us-west-2")
        assert handler.region == "us-west-2"


class TestEMRServerlessHandlerGetApplication:
    @pytest.mark.asyncio
    async def test_get_application_success(self, mock_call, mock_application_started):
        mock_call.return_value = mock_application_started

        handler = EMRServerlessHandler()
        result = await handler.get_application("00f5abc123def456")

        assert result["applicationId"] == "00f5abc123def456"
        assert result["state"] == "STARTED"
        mock_call.assert_called_once_with("get_application", applicationId="00f5abc123def456")

    @pytest.mark.asyncio
    async def test_get_application_not_found(self, mock_call):
        mock_call.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
            "GetApplication",
        )

        handler = EMRServerlessHandler()

        with pytest.raises(ClientError):
            await handler.get_application("nonexistent-app")


class TestEMRServerlessHandlerStartJobRun:
    @pytest.mark.asyncio
    async def test_start_job_run_success(self, mock_call):
        mock_call.return_value = {
            "applicationId": "app-123",
            "jobRunId": "job-456",
        }

        handler = EMRServerlessHandler()
        job_driver = {"sparkSubmit": {"entryPoint": "s3://bucket/main.py"}}

        job_run_id = await handler.start_job_run(
            application_id="app-123",
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            job_driver=job_driver,
        )

        assert job_run_id == "job-456"
        mock_call.assert_called_once()
        assert mock_call.call_args.args == ("start_job_run",)

    @pytest.mark.asyncio
    async def test_start_job_run_with_all_options(self, mock_call):
        mock_call.return_value = {
            "applicationId": "app-123",
            "jobRunId": "job-456",
        }

        handler = EMRServerlessHandler()
        job_driver = {"sparkSubmit": {"entryPoint": "s3://bucket/main.py"}}
        config_overrides = {"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": "s3://logs/"}}}
        tags = {"Environment": "test"}

        await handler.start_job_run(
            application_id="app-123",
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            job_driver=job_driver,
            configuration_overrides=config_overrides,
            tags=tags,
            execution_timeout_minutes=120,
            name="test-job",
        )

        call_kwargs = mock_call.call_args.kwargs
        assert call_kwargs["configurationOverrides"] == config_overrides
        assert call_kwargs["tags"] == tags
        assert call_kwargs["executionTimeoutMinutes"] == 120
        assert call_kwargs["name"] == "test-job"

    @pytest.mark.asyncio
    async def test_start_job_run_with_retry_policy(self, mock_call):
        mock_call.return_value = {"applicationId": "app-123", "jobRunId": "job-456"}

        handler = EMRServerlessHandler()
        await handler.start_job_run(
            application_id="app-123",
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            job_driver={"sparkSubmit": {"entryPoint": "s3://bucket/main.py"}},
            retry_policy={"maxAttempts": 3},
        )

        call_kwargs = mock_call.call_args.kwargs
        assert call_kwargs["retryPolicy"]["maxAttempts"] == 3

    @pytest.mark.asyncio
    async def test_start_job_run_without_retry_policy(self, mock_call):
        mock_call.return_value = {"applicationId": "app-123", "jobRunId": "job-456"}

        handler = EMRServerlessHandler()
        await handler.start_job_run(
            application_id="app-123",
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            job_driver={"sparkSubmit": {"entryPoint": "s3://bucket/main.py"}},
        )

        call_kwargs = mock_call.call_args.kwargs
        assert "retryPolicy" not in call_kwargs


class TestEMRServerlessHandlerGetJobRun:
    @pytest.mark.asyncio
    async def test_get_job_run_success(self, mock_call, mock_job_run_running):
        mock_call.return_value = mock_job_run_running

        handler = EMRServerlessHandler()
        result = await handler.get_job_run("app-123", "job-456")

        assert result["state"] == "RUNNING"
        mock_call.assert_called_once_with(
            "get_job_run", applicationId="app-123", jobRunId="job-456"
        )

    @pytest.mark.asyncio
    async def test_get_job_run_not_found(self, mock_call):
        mock_call.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
            "GetJobRun",
        )

        handler = EMRServerlessHandler()

        with pytest.raises(ClientError):
            await handler.get_job_run("app-123", "nonexistent-job")


class TestEMRServerlessHandlerCancelJobRun:
    @pytest.mark.asyncio
    async def test_cancel_job_run_success(self, mock_call, mock_job_run_running):
        # First _call -> get_job_run returns running, second -> cancel_job_run returns {}
        mock_call.side_effect = [mock_job_run_running, {}]

        handler = EMRServerlessHandler()
        await handler.cancel_job_run("app-123", "job-456")

        assert mock_call.call_count == 2
        assert mock_call.call_args_list[0].args == ("get_job_run",)
        assert mock_call.call_args_list[1].args == ("cancel_job_run",)

    @pytest.mark.asyncio
    async def test_cancel_job_run_already_completed(self, mock_call, mock_job_run_success):
        mock_call.return_value = mock_job_run_success

        handler = EMRServerlessHandler()
        await handler.cancel_job_run("app-123", "job-456")

        # Only the get_job_run call; cancel_job_run is skipped because it's terminal.
        mock_call.assert_called_once()
        assert mock_call.call_args.args == ("get_job_run",)

    @pytest.mark.asyncio
    async def test_cancel_job_run_not_found_is_swallowed(self, mock_call):
        mock_call.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
            "GetJobRun",
        )

        handler = EMRServerlessHandler()
        # Does not raise.
        await handler.cancel_job_run("app-123", "nonexistent-job")


class TestEMRServerlessHandlerCreateApplication:
    @pytest.mark.asyncio
    async def test_create_application_with_new_params(self, mock_call):
        mock_call.return_value = {"applicationId": "app-new"}

        handler = EMRServerlessHandler()
        app_id = await handler.create_application(
            name="test-app",
            release_label="emr-7.0.0",
            application_type="SPARK",
            architecture="ARM64",
            runtime_configuration=[{"classification": "spark-defaults", "properties": {}}],
            scheduler_configuration={"maxConcurrentRuns": 5},
            auto_stop_config={"enabled": True, "idleTimeoutMinutes": 30},
        )

        assert app_id == "app-new"
        call_kwargs = mock_call.call_args.kwargs
        assert call_kwargs["architecture"] == "ARM64"
        assert call_kwargs["runtimeConfiguration"][0]["classification"] == "spark-defaults"
        assert call_kwargs["schedulerConfiguration"]["maxConcurrentRuns"] == 5
        assert call_kwargs["autoStopConfiguration"]["enabled"] is True


class TestEMRServerlessHandlerUpdateApplication:
    @pytest.mark.asyncio
    async def test_update_application_image(self, mock_call):
        mock_call.return_value = {}

        handler = EMRServerlessHandler()
        await handler.update_application(
            application_id="app-123",
            image_configuration={"imageUri": "new-image:v2"},
        )

        mock_call.assert_called_once()
        call_kwargs = mock_call.call_args.kwargs
        assert call_kwargs["applicationId"] == "app-123"
        assert call_kwargs["imageConfiguration"]["imageUri"] == "new-image:v2"

    @pytest.mark.asyncio
    async def test_update_application_noop(self, mock_call):
        """When no mutable fields are provided, no API call is made."""
        handler = EMRServerlessHandler()
        await handler.update_application(application_id="app-123")

        mock_call.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_application_multiple_fields(self, mock_call):
        mock_call.return_value = {}

        handler = EMRServerlessHandler()
        await handler.update_application(
            application_id="app-123",
            image_configuration={"imageUri": "image:v3"},
            maximum_capacity={"cpu": "200vCPU"},
            scheduler_configuration={"maxConcurrentRuns": 10},
        )

        call_kwargs = mock_call.call_args.kwargs
        assert call_kwargs["imageConfiguration"]["imageUri"] == "image:v3"
        assert call_kwargs["maximumCapacity"]["cpu"] == "200vCPU"
        assert call_kwargs["schedulerConfiguration"]["maxConcurrentRuns"] == 10


class TestEMRServerlessHandlerFindApplicationByName:
    @pytest.mark.asyncio
    async def test_find_application_returns_id_when_present(self, mock_paginate):
        mock_paginate.return_value = [
            {"id": "00f-other", "name": "other-app", "state": "STARTED"},
            {"id": "00f-target", "name": "my-app", "state": "STARTED"},
        ]

        handler = EMRServerlessHandler()
        result = await handler.find_application_by_name("my-app")

        assert result == "00f-target"
        mock_paginate.assert_called_once_with(
            "list_applications",
            result_key="applications",
            states=["CREATED", "STARTED", "STOPPED"],
        )

    @pytest.mark.asyncio
    async def test_find_application_returns_none_when_absent(self, mock_paginate):
        mock_paginate.return_value = [
            {"id": "00f-other", "name": "other-app", "state": "STARTED"},
        ]

        handler = EMRServerlessHandler()
        result = await handler.find_application_by_name("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_find_application_empty_list(self, mock_paginate):
        mock_paginate.return_value = []

        handler = EMRServerlessHandler()
        result = await handler.find_application_by_name("anything")

        assert result is None


class TestEMRServerlessHandlerEnsureApplicationStarted:
    @pytest.mark.asyncio
    async def test_returns_immediately_when_started(self, mock_call):
        mock_call.return_value = {"application": {"applicationId": "app-1", "state": "STARTED"}}

        handler = EMRServerlessHandler()
        await handler.ensure_application_started("app-1")

        # Only GetApplication, no StartApplication.
        mock_call.assert_called_once()
        assert mock_call.call_args.args == ("get_application",)

    @pytest.mark.asyncio
    async def test_starts_a_stopped_application(self, mock_call):
        # First GetApplication -> STOPPED, StartApplication -> {}, then GetApplication -> STARTED.
        mock_call.side_effect = [
            {"application": {"applicationId": "app-1", "state": "STOPPED"}},
            {},
            {"application": {"applicationId": "app-1", "state": "STARTED"}},
        ]

        handler = EMRServerlessHandler()
        # Use a very small poll interval to keep the test fast.
        with patch("flytekitplugins.awsemrserverless.boto_handler.asyncio.sleep", new_callable=AsyncMock):
            await handler.ensure_application_started("app-1", poll_interval_seconds=0)

        methods_called = [c.args[0] for c in mock_call.call_args_list]
        assert methods_called[0] == "get_application"
        assert methods_called[1] == "start_application"
        assert methods_called[2] == "get_application"

    @pytest.mark.asyncio
    async def test_raises_on_terminal_state(self, mock_call):
        mock_call.return_value = {"application": {"applicationId": "app-1", "state": "TERMINATED"}}

        handler = EMRServerlessHandler()
        with pytest.raises(RuntimeError, match="terminal state"):
            await handler.ensure_application_started("app-1")
