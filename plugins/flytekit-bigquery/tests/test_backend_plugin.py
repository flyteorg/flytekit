from unittest import mock
from unittest.mock import MagicMock

import grpc
from flyteidl.service.external_plugin_service_pb2 import SUCCEEDED

from flytekit.extend.backend.base_plugin import BackendPluginRegistry
from tests.flytekit.unit.extend.test_backend_plugin import dummy_template, task_inputs


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

    p = BackendPluginRegistry.get_plugin("bigquery_query_job_task")
    ctx = MagicMock(spec=grpc.ServicerContext)
    dummy_template.type = "bigquery_query_job_task"

    assert p.create(ctx, "/tmp", dummy_template, task_inputs).job_id == job_id
    res = p.get(ctx, job_id)
    assert res.state == SUCCEEDED
    assert (
        res.outputs.literals["results"].scalar.structured_dataset.uri == "bq://dummy_project:dummy_dataset.dummy_table"
    )
    p.delete(ctx, job_id)
    mock_instance.cancel_job.assert_called()
