import json
from dataclasses import asdict
from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock

import grpc
import pytest
from aioresponses import aioresponses
from flyteidl.admin.agent_pb2 import SUCCEEDED
from flytekitplugins.spark.agent import Metadata

from flytekit import Secret
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals, task
from flytekit.models.core.identifier import ResourceType
from flytekit.models.security import SecurityContext
from flytekit.models.task import Container, Resources, TaskTemplate


@pytest.mark.asyncio
async def test_databricks_agent():
    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent(ctx, "spark")

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
    )
    task_config = {
        "spark_conf": {
            "spark.driver.memory": "1000M",
            "spark.executor.memory": "1000M",
            "spark.executor.cores": "1",
            "spark.executor.instances": "2",
            "spark.driver.cores": "1",
        },
        "applications_path": "dbfs:/entrypoint.py",
        "databricks_conf": {
            "run_name": "flytekit databricks plugin example",
            "new_cluster": {
                "spark_version": "12.2.x-scala2.12",
                "node_type_id": "n2-highmem-4",
                "num_workers": 1,
            },
            "timeout_seconds": 3600,
            "max_retries": 1,
        },
        "databricks_instance": "test-account.cloud.databricks.com",
        "databricks_endpoint": None,
    }
    container = Container(
        image="flyteorg/flytekit:databricks-0.18.0-py3.7",
        command=[],
        args=[
            "pyflyte-execute",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
            "--checkpoint-path",
            "{{.checkpointOutputPrefix}}",
            "--prev-checkpoint",
            "{{.prevCheckpointPrefix}}",
            "--resolver",
            "flytekit.core.python_auto_container.default_task_resolver",
            "--",
            "task-module",
            "",
            "task-name",
            "hello_spark",
        ],
        resources=Resources(
            requests=[],
            limits=[],
        ),
        env={},
        config={},
    )

    SECRET_GROUP = "token-info"
    SECRET_NAME = "token_secret"
    mocked_token = "mocked_secret_token"
    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        container=container,
        interface=None,
        type="spark",
        security_context=SecurityContext(
            secrets=Secret(
                group=SECRET_GROUP,
                key=SECRET_NAME,
                mount_requirement=Secret.MountType.ENV_VAR,
            )
        ),
    )
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token

    metadata_bytes = json.dumps(
        asdict(
            Metadata(
                databricks_endpoint=None,
                databricks_instance="test-account.cloud.databricks.com",
                token=mocked_token,
                run_id="123",
            )
        )
    ).encode("utf-8")

    mock_create_response = {"run_id": "123"}
    mock_get_response = {"run_id": "123", "state": {"result_state": "SUCCESS"}}
    mock_delete_response = {}
    create_url = "https://test-account.cloud.databricks.com/api/2.0/jobs/runs/submit"
    get_url = "https://test-account.cloud.databricks.com/api/2.0/jobs/runs/get?run_id=123"
    delete_url = "https://test-account.cloud.databricks.com/api/2.0/jobs/runs/cancel"
    with aioresponses() as mocked:
        mocked.post(create_url, status=200, payload=mock_create_response)
        res = await agent.async_create(ctx, "/tmp", dummy_template, None)
        assert res.resource_meta == metadata_bytes

        mocked.get(get_url, status=200, payload=mock_get_response)
        res = await agent.async_get(ctx, metadata_bytes)
        assert res.resource.state == SUCCEEDED
        assert res.resource.outputs == literals.LiteralMap({}).to_flyte_idl()

        mocked.post(delete_url, status=200, payload=mock_delete_response)
        await agent.async_delete(ctx, metadata_bytes)

    mock.patch.stopall()
