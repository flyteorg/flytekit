from mock import MagicMock as _MagicMock
from mock import patch as _patch

from flytekit.models.admin import workflow as _workflow_models
from flytekit.models.core import identifier as _identifier
from flytekit.remote import workflow as _workflow


@_patch("flytekit.engines.flyte.engine._FlyteClientManager")
@_patch("flytekit.configuration.platform.URL")
def test_flyte_workflow_integration(mock_url, mock_client_manager):
    mock_url.get.return_value = "localhost"
    admin_workflow = _workflow_models.Workflow(
        _identifier.Identifier(_identifier.ResourceType.WORKFLOW, "p1", "d1", "n1", "v1"),
        _MagicMock(),
    )
    mock_client = _MagicMock()
    mock_client.list_workflows_paginated = _MagicMock(returnValue=([admin_workflow], ""))
    mock_client_manager.return_value.client = mock_client

    workflow = _workflow.FlyteWorkflow.fetch("p1", "d1", "n1", "v1")
    assert workflow.entity_type_text == "Workflow"
    assert workflow.id == admin_workflow.id
