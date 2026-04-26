"""
Pytest configuration and shared fixtures for EMR Serverless tests.

The fixtures here follow the flytekit-aws-sagemaker testing pattern: boto3
traffic is mocked at the single ``EMRServerlessHandler._call`` (and
``_paginate``) chokepoint, not at the boto3 client factory.  This keeps
test setup dependency-free -- no ``moto``, no ``botocore.stub`` wiring.
"""

from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture
def mock_call():
    """Patch :py:meth:`EMRServerlessHandler._call` for a test.

    Yields an :class:`~unittest.mock.AsyncMock` so tests can set
    ``return_value`` or ``side_effect`` and assert on ``call_args``.
    """
    with patch(
        "flytekitplugins.awsemrserverless.boto_handler.EMRServerlessHandler._call",
        new_callable=AsyncMock,
    ) as m:
        yield m


@pytest.fixture
def mock_paginate():
    """Patch :py:meth:`EMRServerlessHandler._paginate` for a test."""
    with patch(
        "flytekitplugins.awsemrserverless.boto_handler.EMRServerlessHandler._paginate",
        new_callable=AsyncMock,
    ) as m:
        yield m


@pytest.fixture
def sample_spark_job_driver():
    from flytekitplugins.awsemrserverless import EMRServerlessSparkJobDriver

    return EMRServerlessSparkJobDriver(
        entry_point="s3://my-bucket/scripts/main.py",
        entry_point_arguments=[
            "--input",
            "s3://data/input",
            "--output",
            "s3://data/output",
        ],
        spark_submit_parameters="--conf spark.executor.memory=4g --conf spark.executor.cores=2",
    )


@pytest.fixture
def sample_hive_job_driver():
    from flytekitplugins.awsemrserverless import EMRServerlessHiveJobDriver

    return EMRServerlessHiveJobDriver(
        query="s3://my-bucket/queries/analytics.sql",
        init_query_file="s3://my-bucket/queries/init.sql",
        parameters="--hiveconf hive.exec.parallel=true",
    )


@pytest.fixture
def sample_config(sample_spark_job_driver):
    from flytekitplugins.awsemrserverless import EMRServerless

    return EMRServerless(
        application_id="00f5abc123def456",
        execution_role_arn="arn:aws:iam::123456789012:role/EMRServerlessRole",
        spark_job_driver=sample_spark_job_driver,
        region="us-east-1",
        tags={"Environment": "test", "Team": "data-engineering"},
        execution_timeout_minutes=120,
    )


@pytest.fixture
def sample_task_config(sample_spark_job_driver):
    from flytekitplugins.awsemrserverless import EMRServerless

    return EMRServerless(
        application_id="00f5abc123def456",
        execution_role_arn="arn:aws:iam::123456789012:role/EMRServerlessRole",
        spark_job_driver=sample_spark_job_driver,
        region="us-east-1",
    )


@pytest.fixture
def sample_job_metadata():
    from flytekitplugins.awsemrserverless import EMRServerlessJobMetadata

    return EMRServerlessJobMetadata(
        application_id="00f5abc123def456",
        job_run_id="00f5xyz789ghi012",
        region="us-east-1",
        created_application=False,
    )


# ----------------------------------------------------------------------
# Boto response shape fixtures
#
# These match the shape returned by the boto3 client methods (what gets
# funneled through ``_call``).  Tests feed them directly as the ``_call``
# mock's return value.
# ----------------------------------------------------------------------


@pytest.fixture
def mock_job_run_success():
    return {
        "jobRun": {
            "applicationId": "00f5abc123def456",
            "jobRunId": "00f5xyz789ghi012",
            "state": "SUCCESS",
            "stateDetails": "Job completed successfully",
        }
    }


@pytest.fixture
def mock_job_run_running():
    return {
        "jobRun": {
            "applicationId": "00f5abc123def456",
            "jobRunId": "00f5xyz789ghi012",
            "state": "RUNNING",
            "stateDetails": "Job is running",
        }
    }


@pytest.fixture
def mock_job_run_failed():
    return {
        "jobRun": {
            "applicationId": "00f5abc123def456",
            "jobRunId": "00f5xyz789ghi012",
            "state": "FAILED",
            "stateDetails": "Job failed due to OOM",
        }
    }


@pytest.fixture
def mock_application_started():
    return {
        "application": {
            "applicationId": "00f5abc123def456",
            "state": "STARTED",
            "name": "test-app",
        }
    }
