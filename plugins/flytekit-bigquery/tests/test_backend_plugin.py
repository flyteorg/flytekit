from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock

import grpc
from flyteidl.service.external_plugin_service_pb2 import SUCCEEDED

import flytekit.models.interface as interface_models
from flytekit.extend.backend.base_plugin import BackendPluginRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals, task, types
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import Sql, TaskTemplate


@mock.patch("google.cloud.bigquery.job.QueryJob")
@mock.patch("google.cloud.bigquery.Client")
def test_bigquery_plugin(mock_client, mock_query_job):
    job_id = "dummy_id"
    mock_instance = mock_client.return_value
    mock_query_job_instance = mock_query_job.return_value
    mock_query_job_instance.state.return_value = "SUCCEEDED"
    mock_query_job_instance.job_id.return_value = job_id

    class MockDestination:
        def __init__(self):
            self.project = "dummy_project"
            self.dataset_id = "dummy_dataset"
            self.table_id = "dummy_table"

    class MockJob:
        def __init__(self):
            self.state = "SUCCEEDED"
            self.job_id = job_id
            self.destination = MockDestination()

    mock_instance.get_job.return_value = MockJob()
    mock_instance.query.return_value = MockJob()
    mock_instance.cancel_job.return_value = MockJob()

    ctx = MagicMock(spec=grpc.ServicerContext)
    p = BackendPluginRegistry.get_plugin(ctx, "bigquery_query_job_task")

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
        "Location": "us-central1",
        "ProjectID": "dummy_project",
    }

    int_type = types.LiteralType(types.SimpleType.INTEGER)
    interfaces = interface_models.TypedInterface(
        {
            "a": interface_models.Variable(int_type, "description1"),
            "b": interface_models.Variable(int_type, "description2"),
        },
        {},
    )
    task_inputs = literals.LiteralMap(
        {
            "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
            "b": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
        },
    )

    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=interfaces,
        type="bigquery_query_job_task",
        sql=Sql("SELECT 1"),
    )

    assert p.create(ctx, "/tmp", dummy_template, task_inputs).job_id == job_id
    res = p.get(ctx, job_id)
    assert res.state == SUCCEEDED
    assert (
        res.outputs.literals["results"].scalar.structured_dataset.uri == "bq://dummy_project:dummy_dataset.dummy_table"
    )
    p.delete(ctx, job_id)
    mock_instance.cancel_job.assert_called()
