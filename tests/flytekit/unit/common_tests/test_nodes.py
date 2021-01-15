import datetime as _datetime

import pytest as _pytest

import flytekit.platform.sdk_node
from flytekit.common import component_nodes as _component_nodes
from flytekit.common import interface as _interface
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.models import literals as _literals
from flytekit.models.core import identifier as _identifier
from flytekit.models.core import workflow as _core_workflow_models
from flytekit.legacy.sdk import tasks as _tasks, workflow as _workflow, types as _types


def test_sdk_node_from_task():
    @_tasks.inputs(a=_types.Types.Integer)
    @_tasks.outputs(b=_types.Types.Integer)
    @_tasks.python_task()
    def testy_test(wf_params, a, b):
        pass

    n = flytekit.platform.sdk_node.SdkNode(
        "n",
        [],
        [
            _literals.Binding(
                "a", _interface.BindingData.from_python_std(_types.Types.Integer.to_flyte_literal_type(), 3),
            )
        ],
        _core_workflow_models.NodeMetadata("abc", _datetime.timedelta(minutes=15), _literals.RetryStrategy(3)),
        sdk_task=testy_test,
        sdk_workflow=None,
        sdk_launch_plan=None,
        sdk_branch=None,
    )

    assert n.id == "n"
    assert len(n.inputs) == 1
    assert n.inputs[0].var == "a"
    assert n.inputs[0].binding.scalar.primitive.integer == 3
    assert len(n.outputs) == 1
    assert "b" in n.outputs
    assert n.outputs["b"].node_id == "n"
    assert n.outputs["b"].var == "b"
    assert n.outputs["b"].sdk_node == n
    assert n.outputs["b"].sdk_type == _types.Types.Integer
    assert n.metadata.name == "abc"
    assert n.metadata.retries.retries == 3
    assert n.metadata.interruptible is False
    assert len(n.upstream_nodes) == 0
    assert len(n.upstream_node_ids) == 0
    assert len(n.output_aliases) == 0

    n2 = flytekit.platform.sdk_node.SdkNode(
        "n2",
        [n],
        [
            _literals.Binding(
                "a", _interface.BindingData.from_python_std(_types.Types.Integer.to_flyte_literal_type(), n.outputs.b),
            )
        ],
        _core_workflow_models.NodeMetadata("abc2", _datetime.timedelta(minutes=15), _literals.RetryStrategy(3)),
        sdk_task=testy_test,
        sdk_workflow=None,
        sdk_launch_plan=None,
        sdk_branch=None,
    )

    assert n2.id == "n2"
    assert len(n2.inputs) == 1
    assert n2.inputs[0].var == "a"
    assert n2.inputs[0].binding.promise.var == "b"
    assert n2.inputs[0].binding.promise.node_id == "n"
    assert len(n2.outputs) == 1
    assert "b" in n2.outputs
    assert n2.outputs["b"].node_id == "n2"
    assert n2.outputs["b"].var == "b"
    assert n2.outputs["b"].sdk_node == n2
    assert n2.outputs["b"].sdk_type == _types.Types.Integer
    assert n2.metadata.name == "abc2"
    assert n2.metadata.retries.retries == 3
    assert "n" in n2.upstream_node_ids
    assert n in n2.upstream_nodes
    assert len(n2.upstream_nodes) == 1
    assert len(n2.upstream_node_ids) == 1
    assert len(n2.output_aliases) == 0

    # Test right shift operator and late binding
    n3 = flytekit.platform.sdk_node.SdkNode(
        "n3",
        [],
        [
            _literals.Binding(
                "a", _interface.BindingData.from_python_std(_types.Types.Integer.to_flyte_literal_type(), 3),
            )
        ],
        _core_workflow_models.NodeMetadata("abc3", _datetime.timedelta(minutes=15), _literals.RetryStrategy(3)),
        sdk_task=testy_test,
        sdk_workflow=None,
        sdk_launch_plan=None,
        sdk_branch=None,
    )
    n2 >> n3
    n >> n2 >> n3
    n3 << n2
    n3 << n2 << n

    assert n3.id == "n3"
    assert len(n3.inputs) == 1
    assert n3.inputs[0].var == "a"
    assert n3.inputs[0].binding.scalar.primitive.integer == 3
    assert len(n3.outputs) == 1
    assert "b" in n3.outputs
    assert n3.outputs["b"].node_id == "n3"
    assert n3.outputs["b"].var == "b"
    assert n3.outputs["b"].sdk_node == n3
    assert n3.outputs["b"].sdk_type == _types.Types.Integer
    assert n3.metadata.name == "abc3"
    assert n3.metadata.retries.retries == 3
    assert "n2" in n3.upstream_node_ids
    assert n2 in n3.upstream_nodes
    assert len(n3.upstream_nodes) == 1
    assert len(n3.upstream_node_ids) == 1
    assert len(n3.output_aliases) == 0

    # Test left shift operator and late binding
    n4 = flytekit.platform.sdk_node.SdkNode(
        "n4",
        [],
        [
            _literals.Binding(
                "a", _interface.BindingData.from_python_std(_types.Types.Integer.to_flyte_literal_type(), 3),
            )
        ],
        _core_workflow_models.NodeMetadata("abc4", _datetime.timedelta(minutes=15), _literals.RetryStrategy(3)),
        sdk_task=testy_test,
        sdk_workflow=None,
        sdk_launch_plan=None,
        sdk_branch=None,
    )

    n4 << n3

    # Test that implicit dependencies don't cause direct dependencies
    n4 << n3 << n2 << n
    n >> n2 >> n3 >> n4

    assert n4.id == "n4"
    assert len(n4.inputs) == 1
    assert n4.inputs[0].var == "a"
    assert n4.inputs[0].binding.scalar.primitive.integer == 3
    assert len(n4.outputs) == 1
    assert "b" in n4.outputs
    assert n4.outputs["b"].node_id == "n4"
    assert n4.outputs["b"].var == "b"
    assert n4.outputs["b"].sdk_node == n4
    assert n4.outputs["b"].sdk_type == _types.Types.Integer
    assert n4.metadata.name == "abc4"
    assert n4.metadata.retries.retries == 3
    assert "n3" in n4.upstream_node_ids
    assert n3 in n4.upstream_nodes
    assert len(n4.upstream_nodes) == 1
    assert len(n4.upstream_node_ids) == 1
    assert len(n4.output_aliases) == 0

    # Add another dependency
    n4 << n2
    assert "n3" in n4.upstream_node_ids
    assert n3 in n4.upstream_nodes
    assert "n2" in n4.upstream_node_ids
    assert n2 in n4.upstream_nodes
    assert len(n4.upstream_nodes) == 2
    assert len(n4.upstream_node_ids) == 2


def test_sdk_task_node():
    @_tasks.inputs(a=_types.Types.Integer)
    @_tasks.outputs(b=_types.Types.Integer)
    @_tasks.python_task()
    def testy_test(wf_params, a, b):
        pass

    testy_test._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")
    n = _component_nodes.SdkTaskNode(testy_test)
    assert n.reference_id.project == "project"
    assert n.reference_id.domain == "domain"
    assert n.reference_id.name == "name"
    assert n.reference_id.version == "version"

    # Test floating ID
    testy_test._id = _identifier.Identifier(
        _identifier.ResourceType.TASK, "new_project", "new_domain", "new_name", "new_version",
    )
    assert n.reference_id.project == "new_project"
    assert n.reference_id.domain == "new_domain"
    assert n.reference_id.name == "new_name"
    assert n.reference_id.version == "new_version"


def test_sdk_node_from_lp():
    @_tasks.inputs(a=_types.Types.Integer)
    @_tasks.outputs(b=_types.Types.Integer)
    @_tasks.python_task()
    def testy_test(wf_params, a, b):
        pass

    @_workflow.workflow_class
    class test_workflow(object):
        a = _workflow.Input(_types.Types.Integer)
        test = testy_test(a=a)
        b = _workflow.Output(test.outputs.b, sdk_type=_types.Types.Integer)

    lp = test_workflow.create_launch_plan()

    n1 = flytekit.platform.sdk_node.SdkNode(
        "n1",
        [],
        [
            _literals.Binding(
                "a", _interface.BindingData.from_python_std(_types.Types.Integer.to_flyte_literal_type(), 3),
            )
        ],
        _core_workflow_models.NodeMetadata("abc", _datetime.timedelta(minutes=15), _literals.RetryStrategy(3)),
        sdk_launch_plan=lp,
    )

    assert n1.id == "n1"
    assert len(n1.inputs) == 1
    assert n1.inputs[0].var == "a"
    assert n1.inputs[0].binding.scalar.primitive.integer == 3
    assert len(n1.outputs) == 1
    assert "b" in n1.outputs
    assert n1.outputs["b"].node_id == "n1"
    assert n1.outputs["b"].var == "b"
    assert n1.outputs["b"].sdk_node == n1
    assert n1.outputs["b"].sdk_type == _types.Types.Integer
    assert n1.metadata.name == "abc"
    assert n1.metadata.retries.retries == 3
    assert len(n1.upstream_nodes) == 0
    assert len(n1.upstream_node_ids) == 0
    assert len(n1.output_aliases) == 0


def test_sdk_launch_plan_node():
    @_tasks.inputs(a=_types.Types.Integer)
    @_tasks.outputs(b=_types.Types.Integer)
    @_tasks.python_task()
    def testy_test(wf_params, a, b):
        pass

    @_workflow.workflow_class
    class test_workflow(object):
        a = _workflow.Input(_types.Types.Integer)
        test = testy_test(a=1)
        b = _workflow.Output(test.outputs.b, sdk_type=_types.Types.Integer)

    lp = test_workflow.create_launch_plan()

    lp._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")
    n = _component_nodes.SdkWorkflowNode(sdk_launch_plan=lp)
    assert n.launchplan_ref.project == "project"
    assert n.launchplan_ref.domain == "domain"
    assert n.launchplan_ref.name == "name"
    assert n.launchplan_ref.version == "version"

    # Test floating ID
    lp._id = _identifier.Identifier(
        _identifier.ResourceType.TASK, "new_project", "new_domain", "new_name", "new_version",
    )
    assert n.launchplan_ref.project == "new_project"
    assert n.launchplan_ref.domain == "new_domain"
    assert n.launchplan_ref.name == "new_name"
    assert n.launchplan_ref.version == "new_version"

    # If you specify both, you should get an exception
    with _pytest.raises(_system_exceptions.FlyteSystemException):
        _component_nodes.SdkWorkflowNode(sdk_workflow=test_workflow, sdk_launch_plan=lp)
