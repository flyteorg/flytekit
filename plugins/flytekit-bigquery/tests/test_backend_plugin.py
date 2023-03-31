from unittest import mock
from unittest.mock import MagicMock

import grpc
from flyteidl.service.external_plugin_service_pb2 import SUCCEEDED, TaskDeleteResponse

from flytekit.extend.backend.base_plugin import BackendPluginRegistry


@mock.patch("google.cloud.bigquery.Client")
def test_bigquery_plugin(client):
    client.query.return_value = job.QueryJob("dummy_id", client)
    p = BackendPluginRegistry.get_plugin("bigquery_query_job_task1")
    ctx = MagicMock(spec=grpc.ServicerContext)
    # assert p.create(ctx, "/tmp", None).job_id == "dummy_id"
    assert p.get(ctx, "id").state == SUCCEEDED
    assert p.delete(ctx, "id") == TaskDeleteResponse()
