from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from flytekit.configuration import Config
from flytekit.exceptions.user import FlyteAssertion
from flytekit.models.core.identifier import NodeExecutionIdentifier, WorkflowExecutionIdentifier
from flytekit.models.node_execution import (
    NodeExecutionClosure,
    WorkflowNodeMetadata,
)
from flytekit.remote.executions import FlyteNodeExecution, FlyteWorkflowExecution
from flytekit.remote.interface import TypedInterface
from flytekit.remote.remote import FlyteRemote


def _mock_launched_exec():
    exec = MagicMock(spec=FlyteWorkflowExecution)
    wf = MagicMock()
    wf.interface = MagicMock(spec=TypedInterface)
    exec._flyte_workflow = wf
    exec.is_done = False
    exec.inputs = {"a": 1}
    exec.outputs = {"b": 2}
    return exec


def _make_node_execution(node_id: str, execution_name: str, with_workflow_node_metadata: bool = False):
    wf_exec_id = WorkflowExecutionIdentifier("p1", "d1", execution_name)
    ne_id = NodeExecutionIdentifier(node_id, wf_exec_id)
    meta = MagicMock()
    meta.is_parent_node = False
    meta.is_array = False
    meta.spec_node_id = node_id

    wf_node_meta = None
    if with_workflow_node_metadata:
        wf_node_meta = WorkflowNodeMetadata(
            execution_id=WorkflowExecutionIdentifier("p1", "d1", f"launched_{execution_name}")
        )

    closure = NodeExecutionClosure(
        phase=0,
        started_at=datetime.now(timezone.utc),
        duration=timedelta(seconds=1),
        workflow_node_metadata=wf_node_meta,
    )
    return FlyteNodeExecution(id=ne_id, input_uri="s3://bucket/input", closure=closure, metadata=meta)


@pytest.fixture
def remote():
    with patch("flytekit.clients.friendly.SynchronousFlyteClient"):
        flyte_remote = FlyteRemote(
            config=Config.auto(),
            default_project="p1",
            default_domain="d1",
        )
        flyte_remote._client_initialized = True
        flyte_remote._client = MagicMock()
        return flyte_remote


def test_max_depth_raises_flyte_assertion(remote):
    ne = _make_node_execution("n1", "exec1", with_workflow_node_metadata=True)

    launched_exec = _mock_launched_exec()
    remote.fetch_execution = MagicMock(return_value=launched_exec)
    remote.client.get_node_execution_data = MagicMock(
        return_value=MagicMock(dynamic_workflow=None)
    )

    # Don't mock _sync_execution — the guard fires inside it when _depth exceeds _max_depth.
    # With _max_depth=0, the initial sync_node_execution enters at _depth=0 (passes guard).
    # It reaches the launched LP path, which calls _sync_execution with _depth=1.
    # _sync_execution then sees 1 > 0 and raises FlyteAssertion.
    with pytest.raises(FlyteAssertion, match="Nesting depth"):
        remote.sync_node_execution(ne, {"n1": MagicMock()}, _max_depth=0)


def test_reasonable_depth_does_not_raise(remote):
    ne = _make_node_execution("n1", "exec1", with_workflow_node_metadata=True)

    launched_exec = _mock_launched_exec()
    remote.fetch_execution = MagicMock(return_value=launched_exec)
    remote.client.get_node_execution_data = MagicMock(
        return_value=MagicMock(dynamic_workflow=None)
    )
    remote._sync_execution = MagicMock()

    result = remote.sync_node_execution(ne, {"n1": MagicMock()})
    assert result is ne


def test_nested_under_default_limit(remote):
    ne = _make_node_execution("n1", "exec1", with_workflow_node_metadata=True)

    launched_exec = _mock_launched_exec()
    remote.fetch_execution = MagicMock(return_value=launched_exec)
    remote.client.get_node_execution_data = MagicMock(
        return_value=MagicMock(dynamic_workflow=None)
    )
    remote._sync_execution = MagicMock()

    result = remote.sync_node_execution(ne, {"n1": MagicMock()}, _depth=1, _max_depth=50)
    assert result is ne


def test_sync_execution_depth_guard(remote):
    wf_exec = MagicMock(spec=FlyteWorkflowExecution)
    wf_exec.id = WorkflowExecutionIdentifier("p1", "d1", "deep_exec")

    with pytest.raises(FlyteAssertion, match="Nesting depth"):
        remote._sync_execution(wf_exec, sync_nodes=True, _depth=51, _max_depth=50)
