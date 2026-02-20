import http
import json
from datetime import timedelta
from unittest import mock

import pytest
from aioresponses import aioresponses
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit.core.constants import FLYTE_FAIL_ON_ERROR
from flytekitplugins.spark.connector import (
    DATABRICKS_API_ENDPOINT,
    DatabricksJobMetadata,
    get_header,
    _get_databricks_job_spec,
    _is_serverless_config,
    _configure_serverless,
    DEFAULT_DATABRICKS_INSTANCE_ENV_KEY,
)

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals, task
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import Container, Resources, TaskTemplate
import os


@pytest.fixture(scope="function")
def task_template() -> TaskTemplate:
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
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
        }
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
        resources=Resources(
            requests=[],
            limits=[],
        ),
        env={"foo": "bar"},
        config={},
    )

    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        container=container,
        interface=None,
        type="spark",
    )

    return dummy_template


@pytest.mark.asyncio
async def test_databricks_agent(task_template: TaskTemplate):
    agent = AgentRegistry.get_agent("spark")

    task_template.custom["databricksInstance"] = "test-account.cloud.databricks.com"

    mocked_token = "mocked_databricks_token"
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token

    databricks_metadata = DatabricksJobMetadata(
        databricks_instance="test-account.cloud.databricks.com",
        run_id="123",
    )

    mock_create_response = {"run_id": "123"}
    mock_get_response = {
        "job_id": "1",
        "run_id": "123",
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": "OK"},
    }
    mock_delete_response = {}
    create_url = f"https://test-account.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit"
    get_url = f"https://test-account.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/get?run_id=123"
    delete_url = f"https://test-account.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/cancel"
    with aioresponses() as mocked:
        mocked.post(create_url, status=http.HTTPStatus.OK, payload=mock_create_response)
        res = await agent.create(task_template, None)
        spec = _get_databricks_job_spec(task_template)
        data = json.dumps(spec)
        mocked.assert_called_with(create_url, method="POST", data=data, headers=get_header())
        spark_envs = spec["new_cluster"]["spark_env_vars"]
        assert spark_envs["foo"] == "bar"
        assert spark_envs[FLYTE_FAIL_ON_ERROR] == "true"
        assert res == databricks_metadata

        mocked.get(get_url, status=http.HTTPStatus.OK, payload=mock_get_response)
        resource = await agent.get(databricks_metadata)
        assert resource.phase == TaskExecution.SUCCEEDED
        assert resource.outputs is None
        assert resource.message == "OK"
        assert resource.log_links[0].name == "Databricks Console"
        assert resource.log_links[0].uri == "https://test-account.cloud.databricks.com/#job/1/run/123"

        mocked.post(delete_url, status=http.HTTPStatus.OK, payload=mock_delete_response)
        await agent.delete(databricks_metadata)

    assert get_header() == {"Authorization": f"Bearer {mocked_token}", "content-type": "application/json"}

    mock.patch.stopall()


@pytest.mark.asyncio
async def test_agent_create_with_no_instance(task_template: TaskTemplate):
    agent = AgentRegistry.get_agent("spark")

    with pytest.raises(ValueError) as e:
        await agent.create(task_template, None)


@pytest.mark.asyncio
async def test_agent_create_with_default_instance(task_template: TaskTemplate):
    agent = AgentRegistry.get_agent("spark")

    mocked_token = "mocked_databricks_token"
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token

    databricks_metadata = DatabricksJobMetadata(
        databricks_instance="test-account.cloud.databricks.com",
        run_id="123",
    )

    mock_create_response = {"run_id": "123"}

    os.environ[DEFAULT_DATABRICKS_INSTANCE_ENV_KEY] = "test-account.cloud.databricks.com"

    create_url = f"https://test-account.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit"
    with aioresponses() as mocked:
        mocked.post(create_url, status=http.HTTPStatus.OK, payload=mock_create_response)
        res = await agent.create(task_template, None)
        spec = _get_databricks_job_spec(task_template)
        data = json.dumps(spec)
        mocked.assert_called_with(create_url, method="POST", data=data, headers=get_header())
        assert res == databricks_metadata

    mock.patch.stopall()


# ==================== Serverless Compute Tests ====================


@pytest.fixture(scope="function")
def serverless_task_template_with_env_key() -> TaskTemplate:
    """Task template configured for serverless with pre-configured environment_key."""
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
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
        "sparkConf": {},
        "mainApplicationFile": "dbfs:/entrypoint.py",
        "databricksConf": {
            "run_name": "flytekit serverless job",
            "environment_key": "my-preconfigured-env",
            "timeout_seconds": 3600,
            "git_source": {
                "git_url": "https://github.com/test-org/test-repo",
                "git_provider": "gitHub",
                "git_branch": "main",
            },
            "python_file": "entrypoint_serverless.py",
        }
    }
    container = Container(
        image="flyteorg/flytekit:databricks-0.18.0-py3.7",
        command=[],
        args=["pyflyte-execute", "--inputs", "s3://my-s3-bucket"],
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
def serverless_task_template_with_inline_env() -> TaskTemplate:
    """Task template configured for serverless with inline environments spec."""
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
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
        "sparkConf": {},
        "mainApplicationFile": "dbfs:/entrypoint.py",
        "databricksConf": {
            "run_name": "flytekit serverless job with inline env",
            "environment_key": "default",
            "environments": [{
                "environment_key": "default",
                "spec": {
                    "client": "1",
                    "dependencies": ["pandas==2.0.0"],
                }
            }],
            "timeout_seconds": 3600,
            "git_source": {
                "git_url": "https://github.com/test-org/test-repo",
                "git_provider": "gitHub",
                "git_branch": "main",
            },
            "python_file": "entrypoint_serverless.py",
        }
    }
    container = Container(
        image="flyteorg/flytekit:databricks-0.18.0-py3.7",
        command=[],
        args=["pyflyte-execute", "--inputs", "s3://my-s3-bucket"],
        resources=Resources(requests=[], limits=[]),
        env={"foo": "bar", "MY_VAR": "my_value"},
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
def serverless_task_template_no_git_source() -> TaskTemplate:
    """Task template for serverless without git_source - relies on connector env vars."""
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
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
        "sparkConf": {},
        "mainApplicationFile": "dbfs:/entrypoint.py",
        "databricksConf": {
            "run_name": "flytekit serverless job - no git source",
            "environment_key": "default",
            "environments": [{
                "environment_key": "default",
                "spec": {
                    "client": "4",
                }
            }],
            "timeout_seconds": 3600,
        }
    }
    container = Container(
        image="flyteorg/flytekit:databricks-0.18.0-py3.7",
        command=[],
        args=["pyflyte-execute", "--inputs", "s3://my-s3-bucket"],
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
def invalid_task_template_no_compute() -> TaskTemplate:
    """Task template with no cluster or environment config - should fail."""
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
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
        "sparkConf": {},
        "mainApplicationFile": "dbfs:/entrypoint.py",
        "databricksConf": {
            "run_name": "invalid job - no compute config",
            "timeout_seconds": 3600,
        }
    }
    container = Container(
        image="flyteorg/flytekit:databricks-0.18.0-py3.7",
        command=[],
        args=["pyflyte-execute", "--inputs", "s3://my-s3-bucket"],
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


def test_is_serverless_config_detection():
    """Test the serverless configuration detection logic."""
    # Classic compute with existing_cluster_id
    assert _is_serverless_config({"existing_cluster_id": "abc123"}) is False

    # Classic compute with new_cluster
    assert _is_serverless_config({"new_cluster": {"spark_version": "13.3"}}) is False

    # Serverless with environment_key only
    assert _is_serverless_config({"environment_key": "my-env"}) is True

    # Serverless with environments array
    assert _is_serverless_config({"environments": [{"environment_key": "default"}]}) is True

    # Serverless with both environment_key and environments
    assert _is_serverless_config({
        "environment_key": "default",
        "environments": [{"environment_key": "default"}]
    }) is True

    # No compute config at all
    assert _is_serverless_config({"run_name": "test"}) is False

    # Has cluster AND environment (cluster takes precedence, not serverless)
    assert _is_serverless_config({
        "new_cluster": {"spark_version": "13.3"},
        "environment_key": "my-env"
    }) is False


def test_configure_serverless_with_env_key_only():
    """Test serverless configuration with environment_key only (no environments array)."""
    databricks_job = {"environment_key": "my-env"}
    envs = {"FOO": "bar", FLYTE_FAIL_ON_ERROR: "true"}

    result_key = _configure_serverless(databricks_job, envs)

    assert result_key == "my-env"
    # Databricks serverless requires environments array - it should be auto-created
    assert "environments" in databricks_job
    assert len(databricks_job["environments"]) == 1
    assert databricks_job["environments"][0]["environment_key"] == "my-env"
    # Environment variables should be injected
    env_vars = databricks_job["environments"][0]["spec"]["environment_vars"]
    assert env_vars["FOO"] == "bar"
    assert env_vars[FLYTE_FAIL_ON_ERROR] == "true"
    # environment_key should be removed from top level
    assert "environment_key" not in databricks_job


def test_configure_serverless_with_inline_env():
    """Test serverless configuration with inline environment spec."""
    databricks_job = {
        "environment_key": "default",
        "environments": [{
            "environment_key": "default",
            "spec": {
                "client": "1",
                "dependencies": ["pandas==2.0.0"],
            }
        }]
    }
    envs = {"FOO": "bar", FLYTE_FAIL_ON_ERROR: "true"}

    result_key = _configure_serverless(databricks_job, envs)

    assert result_key == "default"
    # Environment variables should be injected
    env_vars = databricks_job["environments"][0]["spec"]["environment_vars"]
    assert env_vars["FOO"] == "bar"
    assert env_vars[FLYTE_FAIL_ON_ERROR] == "true"
    # environment_key should be removed from top level
    assert "environment_key" not in databricks_job


def test_configure_serverless_creates_default_env():
    """Test that serverless creates a default environment when no environment specified."""
    databricks_job = {}  # No environment_key or environments
    envs = {"FOO": "bar"}

    result_key = _configure_serverless(databricks_job, envs)

    assert result_key == "default"
    assert len(databricks_job["environments"]) == 1
    assert databricks_job["environments"][0]["environment_key"] == "default"
    # Should have env vars injected
    assert databricks_job["environments"][0]["spec"]["environment_vars"]["FOO"] == "bar"


def test_get_databricks_job_spec_serverless_with_env_key(serverless_task_template_with_env_key: TaskTemplate):
    """Test job spec generation for serverless with environment_key only."""
    serverless_task_template_with_env_key.custom["databricksInstance"] = "test-account.cloud.databricks.com"

    spec = _get_databricks_job_spec(serverless_task_template_with_env_key)

    # Serverless uses multi-task format with tasks array
    assert "tasks" in spec
    assert len(spec["tasks"]) == 1

    task_def = spec["tasks"][0]
    assert task_def["task_key"] == "flyte_task"
    assert task_def["environment_key"] == "my-preconfigured-env"
    assert "spark_python_task" in task_def

    # Databricks serverless requires environments array - should be auto-created
    assert "environments" in spec
    assert len(spec["environments"]) == 1
    assert spec["environments"][0]["environment_key"] == "my-preconfigured-env"

    # Should NOT have spark_python_task at top level for serverless
    assert "spark_python_task" not in spec

    # Should NOT have environment_key at top level (moved to task)
    assert "environment_key" not in spec

    # Should NOT have cluster config
    assert "new_cluster" not in spec
    assert "existing_cluster_id" not in spec

    # Should have git_source
    assert "git_source" in spec


def test_get_databricks_job_spec_serverless_with_inline_env(serverless_task_template_with_inline_env: TaskTemplate):
    """Test job spec generation for serverless with inline environment spec."""
    serverless_task_template_with_inline_env.custom["databricksInstance"] = "test-account.cloud.databricks.com"

    spec = _get_databricks_job_spec(serverless_task_template_with_inline_env)

    # Serverless uses multi-task format with tasks array
    assert "tasks" in spec
    assert len(spec["tasks"]) == 1

    task_def = spec["tasks"][0]
    assert task_def["task_key"] == "flyte_task"
    assert task_def["environment_key"] == "default"
    assert "spark_python_task" in task_def

    # Should have environments array with injected env vars
    assert "environments" in spec
    env_vars = spec["environments"][0]["spec"]["environment_vars"]
    assert env_vars["foo"] == "bar"
    assert env_vars["MY_VAR"] == "my_value"
    assert env_vars[FLYTE_FAIL_ON_ERROR] == "true"

    # Should NOT have cluster config
    assert "new_cluster" not in spec
    assert "existing_cluster_id" not in spec


def test_get_databricks_job_spec_error_no_compute(invalid_task_template_no_compute: TaskTemplate):
    """Test that job spec generation fails when no compute config is provided."""
    with pytest.raises(ValueError) as exc_info:
        _get_databricks_job_spec(invalid_task_template_no_compute)

    assert "existing_cluster_id" in str(exc_info.value)
    assert "new_cluster" in str(exc_info.value)
    assert "environment_key" in str(exc_info.value)
    assert "environments" in str(exc_info.value)


@pytest.mark.asyncio
async def test_databricks_agent_serverless(serverless_task_template_with_env_key: TaskTemplate):
    """Test the full agent flow with serverless compute."""
    import copy
    agent = AgentRegistry.get_agent("spark")

    serverless_task_template_with_env_key.custom["databricksInstance"] = "test-account.cloud.databricks.com"

    # Generate spec BEFORE agent.create() mutates the template in-place
    spec_copy = copy.deepcopy(serverless_task_template_with_env_key)
    spec = _get_databricks_job_spec(spec_copy)

    # Verify serverless config uses multi-task format with environments
    assert "tasks" in spec
    task_def = spec["tasks"][0]
    assert task_def["task_key"] == "flyte_task"
    assert task_def["environment_key"] == "my-preconfigured-env"
    assert "spark_python_task" in task_def
    assert "environments" in spec  # Required for serverless
    assert "new_cluster" not in spec

    mocked_token = "mocked_databricks_token"
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token

    databricks_metadata = DatabricksJobMetadata(
        databricks_instance="test-account.cloud.databricks.com",
        run_id="456",
    )

    mock_create_response = {"run_id": "456"}
    mock_get_response = {
        "job_id": "2",
        "run_id": "456",
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": "OK"},
    }

    create_url = f"https://test-account.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/submit"
    get_url = f"https://test-account.cloud.databricks.com{DATABRICKS_API_ENDPOINT}/runs/get?run_id=456"

    with aioresponses() as mocked:
        mocked.post(create_url, status=http.HTTPStatus.OK, payload=mock_create_response)
        res = await agent.create(serverless_task_template_with_env_key, None)
        assert res == databricks_metadata

        mocked.get(get_url, status=http.HTTPStatus.OK, payload=mock_get_response)
        resource = await agent.get(databricks_metadata)
        assert resource.phase == TaskExecution.SUCCEEDED

    mock.patch.stopall()


# ==================== Default Serverless Entrypoint Tests ====================


def test_serverless_default_entrypoint_from_flytetools(serverless_task_template_no_git_source: TaskTemplate):
    """Test that serverless uses the default flytetools entrypoint when no git_source in task config."""
    spec = _get_databricks_job_spec(serverless_task_template_no_git_source)

    # Should use the same flytetools repo as classic
    assert spec["git_source"]["git_url"] == "https://github.com/flyteorg/flytetools"
    assert spec["git_source"]["git_provider"] == "gitHub"
    assert "git_commit" in spec["git_source"]

    # Should use the serverless-specific python_file
    task_def = spec["tasks"][0]
    assert task_def["spark_python_task"]["python_file"] == "flytekitplugins/databricks/entrypoint_serverless.py"

    # Should still be valid serverless format
    assert "environments" in spec
    assert "new_cluster" not in spec


def test_serverless_task_git_source_overrides_default(serverless_task_template_with_env_key: TaskTemplate):
    """Test that task-level git_source takes precedence over the flytetools default."""
    spec = _get_databricks_job_spec(serverless_task_template_with_env_key)

    # Should use the task-level git_source, NOT the flytetools default
    assert spec["git_source"]["git_url"] == "https://github.com/test-org/test-repo"
    assert spec["git_source"]["git_branch"] == "main"

    # Should use the task-level python_file
    task_def = spec["tasks"][0]
    assert task_def["spark_python_task"]["python_file"] == "entrypoint_serverless.py"


def test_classic_and_serverless_use_same_repo(task_template: TaskTemplate, serverless_task_template_no_git_source: TaskTemplate):
    """Test that both classic and serverless default to the same flytetools repo."""
    classic_spec = _get_databricks_job_spec(task_template)
    serverless_spec = _get_databricks_job_spec(serverless_task_template_no_git_source)

    # Same repo
    assert classic_spec["git_source"]["git_url"] == serverless_spec["git_source"]["git_url"]
    # Same commit
    assert classic_spec["git_source"]["git_commit"] == serverless_spec["git_source"]["git_commit"]
    # Different python_file
    assert classic_spec["spark_python_task"]["python_file"] == "flytekitplugins/databricks/entrypoint.py"
    assert serverless_spec["tasks"][0]["spark_python_task"]["python_file"] == "flytekitplugins/databricks/entrypoint_serverless.py"
