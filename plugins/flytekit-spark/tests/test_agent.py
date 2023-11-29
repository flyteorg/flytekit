import pickle
from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock
from flyteidl.admin.agent_pb2 import CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource

import grpc
import pytest
from flyteidl.admin.agent_pb2 import SUCCEEDED
from flytekitplugins.spark.agent import Metadata, get_header

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals, task
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import Container, Resources, TaskTemplate


@pytest.mark.asyncio
async def test_databricks_agent():
    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent("spark")

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
        "databricksInstance": "test-account.cloud.databricks.com",
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
        env={},
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
    mocked_token = "mocked_databricks_token"
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token

    metadata_bytes = pickle.dumps(
        Metadata(
            databricks_instance="test-account.cloud.databricks.com",
            run_id="123",
        )
    )

    # define mock responses
    async def mock_create_request(self, context, output_prefix, task_template, inputs=None):
        metadata = Metadata(
            databricks_instance="test-account.cloud.databricks.com",
            run_id="123",
        )
        return CreateTaskResponse(resource_meta=pickle.dumps(metadata))

    async def mock_get_request(self, context, resource_meta):
        return GetTaskResponse(
            resource=Resource(state=SUCCEEDED, outputs=literals.LiteralMap({}).to_flyte_idl(), message="OK")
        )

    async def mock_delete_request(self, context, resource_meta):
        return DeleteTaskResponse()

    # mock databricks requests
    with mock.patch("flytekitplugins.spark.agent.DatabricksAgent.async_create", new=mock_create_request):
        res = await agent.async_create(ctx, "/tmp", dummy_template, None)
        assert res.resource_meta == metadata_bytes
    with mock.patch("flytekitplugins.spark.agent.DatabricksAgent.async_get", new=mock_get_request):
        res = await agent.async_get(ctx, metadata_bytes)
        assert res.resource.state == SUCCEEDED
        assert res.resource.outputs == literals.LiteralMap({}).to_flyte_idl()
        assert res.resource.message == "OK"
    with mock.patch("flytekitplugins.spark.agent.DatabricksAgent.async_delete", new=mock_delete_request):
        await agent.async_delete(ctx, metadata_bytes)

    assert get_header() == {"Authorization": f"Bearer {mocked_token}", "content-type": "application/json"}

    mock.patch.stopall()
