"""
Unit tests for EMR Serverless Task and configuration.
"""

from unittest.mock import MagicMock

import pytest
from flytekit import task

from flytekitplugins.awsemrserverless import (
    EMRServerless,
    EMRServerlessHiveJobDriver,
    EMRServerlessSparkJobDriver,
    EMRServerlessTask,
)


class TestEMRServerlessSparkJobDriver:
    def test_basic_creation(self):
        driver = EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py")
        assert driver.entry_point == "s3://bucket/main.py"
        assert driver.entry_point_arguments is None
        assert driver.spark_submit_parameters is None

    def test_full_creation(self):
        driver = EMRServerlessSparkJobDriver(
            entry_point="s3://bucket/main.py",
            entry_point_arguments=["--arg1", "value1"],
            spark_submit_parameters="--conf spark.executor.memory=4g",
        )
        assert driver.entry_point_arguments == ["--arg1", "value1"]

    def test_to_dict_basic(self):
        driver = EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py")
        assert driver.to_dict() == {"entryPoint": "s3://bucket/main.py"}

    def test_to_dict_full(self):
        driver = EMRServerlessSparkJobDriver(
            entry_point="s3://bucket/main.py",
            entry_point_arguments=["--arg1", "value1"],
            spark_submit_parameters="--conf spark.executor.memory=4g",
        )
        result = driver.to_dict()
        assert result["entryPoint"] == "s3://bucket/main.py"
        assert result["entryPointArguments"] == ["--arg1", "value1"]
        assert result["sparkSubmitParameters"] == "--conf spark.executor.memory=4g"


class TestEMRServerlessHiveJobDriver:
    def test_basic_creation(self):
        driver = EMRServerlessHiveJobDriver(query="SELECT * FROM table")
        assert driver.query == "SELECT * FROM table"
        assert driver.init_query_file is None

    def test_to_dict(self):
        driver = EMRServerlessHiveJobDriver(
            query="s3://bucket/query.sql",
            init_query_file="s3://bucket/init.sql",
        )
        result = driver.to_dict()
        assert result == {
            "query": "s3://bucket/query.sql",
            "initQueryFile": "s3://bucket/init.sql",
        }


class TestEMRServerless:
    def test_basic_spark_config(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
        )
        assert config.application_type == "SPARK"
        assert config.is_script_mode is True

    def test_pythonic_mode_config(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
        )
        assert config.is_script_mode is False

    def test_hive_config(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
            application_type="HIVE",
            hive_job_driver=EMRServerlessHiveJobDriver(query="s3://bucket/query.sql"),
        )
        assert config.application_type == "HIVE"
        assert config.is_script_mode is True

    def test_missing_execution_role(self):
        with pytest.raises(ValueError, match="execution_role_arn is required"):
            EMRServerless(execution_role_arn="")

    def test_invalid_application_type(self):
        with pytest.raises(ValueError, match="application_type must be"):
            EMRServerless(
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                application_type="INVALID",
            )

    def test_missing_hive_driver(self):
        with pytest.raises(ValueError, match="hive_job_driver is required"):
            EMRServerless(
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                application_type="HIVE",
            )

    def test_both_drivers_specified(self):
        with pytest.raises(ValueError, match="Only one of"):
            EMRServerless(
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
                hive_job_driver=EMRServerlessHiveJobDriver(query="SELECT 1"),
            )

    def test_invalid_timeout(self):
        with pytest.raises(ValueError, match="execution_timeout_minutes"):
            EMRServerless(
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
                execution_timeout_minutes=0,
            )

    def test_get_job_driver_spark(self, sample_spark_job_driver):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            spark_job_driver=sample_spark_job_driver,
        )
        result = config.get_job_driver()
        assert "sparkSubmit" in result

    def test_get_job_driver_hive(self, sample_hive_job_driver):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_type="HIVE",
            hive_job_driver=sample_hive_job_driver,
        )
        result = config.get_job_driver()
        assert "hive" in result

    def test_to_dict(self, sample_config):
        result = sample_config.to_dict()
        assert result["execution_role_arn"] == "arn:aws:iam::123456789012:role/EMRServerlessRole"
        assert result["application_id"] == "00f5abc123def456"
        assert result["region"] == "us-east-1"
        assert "spark_job_driver" in result

    def test_from_dict_roundtrip(self, sample_config):
        config_dict = sample_config.to_dict()
        restored = EMRServerless.from_dict(config_dict)
        assert restored.execution_role_arn == sample_config.execution_role_arn
        assert restored.application_id == sample_config.application_id
        assert restored.region == sample_config.region

    def test_full_config_with_all_options(self, sample_spark_job_driver):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
            application_id="00abc123",
            release_label="emr-6.15.0",
            application_name="my-app",
            spark_job_driver=sample_spark_job_driver,
            configuration_overrides={
                "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": "s3://logs/"}}
            },
            tags={"Env": "prod"},
            execution_timeout_minutes=240,
            initial_capacity={"DRIVER": {"workerCount": 1}},
            maximum_capacity={"cpu": "100vCPU"},
            network_configuration={"subnetIds": ["subnet-123"]},
            image_configuration={"imageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/spark:latest"},
            region="us-west-2",
        )
        d = config.to_dict()
        assert d["release_label"] == "emr-6.15.0"
        assert d["application_name"] == "my-app"
        assert d["tags"]["Env"] == "prod"
        assert d["network_configuration"]["subnetIds"] == ["subnet-123"]

    def test_sync_image_default(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
            application_id="00abc123",
        )
        assert config.sync_image is True

    def test_architecture_validation(self):
        with pytest.raises(ValueError, match="architecture must be one of"):
            EMRServerless(
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                architecture="INVALID",
            )

    def test_valid_architectures(self):
        for arch in ("X86_64", "ARM64"):
            config = EMRServerless(
                execution_role_arn="arn:aws:iam::123456789012:role/Role",
                architecture=arch,
            )
            assert config.architecture == arch

    def test_new_fields_to_dict_roundtrip(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            sync_image=False,
            spark_submit_parameters="--conf spark.executor.memory=16g",
            application_configuration=[
                {"classification": "spark-defaults", "properties": {"spark.executor.cores": "8"}}
            ],
            runtime_configuration=[
                {"classification": "spark-defaults", "properties": {"spark.dynamicAllocation.enabled": "true"}}
            ],
            scheduler_configuration={"maxConcurrentRuns": 5, "queueTimeoutMinutes": 30},
            auto_stop_config={"enabled": True, "idleTimeoutMinutes": 30},
            architecture="ARM64",
            retry_policy={"maxAttempts": 3},
        )
        d = config.to_dict()
        restored = EMRServerless.from_dict(d)

        assert restored.sync_image is False
        assert restored.spark_submit_parameters == "--conf spark.executor.memory=16g"
        assert len(restored.application_configuration) == 1
        assert restored.application_configuration[0]["classification"] == "spark-defaults"
        assert len(restored.runtime_configuration) == 1
        assert restored.scheduler_configuration == {"maxConcurrentRuns": 5, "queueTimeoutMinutes": 30}
        assert restored.auto_stop_config == {"enabled": True, "idleTimeoutMinutes": 30}
        assert restored.architecture == "ARM64"
        assert restored.retry_policy == {"maxAttempts": 3}

    def test_effective_configuration_overrides_merge(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            configuration_overrides={
                "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": "s3://logs/"}}
            },
            application_configuration=[
                {"classification": "spark-defaults", "properties": {"spark.executor.cores": "8"}}
            ],
        )
        result = config.get_effective_configuration_overrides()
        assert "monitoringConfiguration" in result
        assert len(result["applicationConfiguration"]) == 1

    def test_effective_configuration_overrides_none(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
        )
        assert config.get_effective_configuration_overrides() is None

    def test_effective_configuration_overrides_only_app_config(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/Role",
            application_configuration=[
                {"classification": "spark", "properties": {"dynamicAllocationOptimization": "true"}}
            ],
        )
        result = config.get_effective_configuration_overrides()
        assert len(result["applicationConfiguration"]) == 1


class TestEMRServerlessTask:
    def test_task_type(self):
        assert EMRServerlessTask._TASK_TYPE == "emr_serverless"

    def test_task_creation_with_decorator(self, sample_task_config):
        @task(task_config=sample_task_config)
        def my_spark_job() -> str:
            return "done"

        assert isinstance(my_spark_job, EMRServerlessTask)
        assert my_spark_job.task_config == sample_task_config

    def test_get_custom(self, sample_task_config):
        @task(task_config=sample_task_config)
        def my_spark_job() -> str:
            return "done"

        mock_settings = MagicMock()
        custom = my_spark_job.get_custom(mock_settings)

        assert isinstance(custom, dict)
        assert custom["execution_role_arn"] == sample_task_config.execution_role_arn
        assert custom["application_id"] == sample_task_config.application_id
        assert "spark_job_driver" in custom

    def test_get_custom_with_hive(self):
        task_config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
            application_type="HIVE",
            hive_job_driver=EMRServerlessHiveJobDriver(
                query="s3://bucket/query.sql",
                init_query_file="s3://bucket/init.sql",
            ),
        )

        @task(task_config=task_config)
        def hive_task() -> str:
            return "done"

        custom = hive_task.get_custom(MagicMock())
        assert custom["application_type"] == "HIVE"
        assert custom["hive_job_driver"]["query"] == "s3://bucket/query.sql"

    def test_task_inherits_from_correct_classes(self):
        from flytekit.core.python_function_task import PythonFunctionTask

        try:
            from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
        except ModuleNotFoundError:
            from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin as AsyncConnectorExecutorMixin

        assert issubclass(EMRServerlessTask, PythonFunctionTask)
        assert issubclass(EMRServerlessTask, AsyncConnectorExecutorMixin)

    def test_task_decorator_produces_emr_serverless_task(self):
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
            application_id="app-123",
            spark_job_driver=EMRServerlessSparkJobDriver(entry_point="s3://bucket/main.py"),
        )

        @task(task_config=config)
        def my_emr_task() -> str:
            return "done"

        assert isinstance(my_emr_task, EMRServerlessTask)
        assert my_emr_task._task_type == "emr_serverless"

    def test_pythonic_mode_task(self):
        """Test that a task with no job driver creates successfully (Pythonic mode)."""
        config = EMRServerless(
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
            application_id="app-123",
            region="us-east-1",
        )

        @task(task_config=config)
        def my_pythonic_spark_job() -> str:
            return "done"

        assert isinstance(my_pythonic_spark_job, EMRServerlessTask)

        custom = my_pythonic_spark_job.get_custom(MagicMock())
        assert "spark_job_driver" not in custom
        assert custom["application_type"] == "SPARK"
