from __future__ import absolute_import

from datetime import timedelta

from mock import patch as _patch

from flytekit.common import workflow as _workflow_common
from flytekit.common.tasks import task as _task
from flytekit.models import interface as _interface, \
    literals as _literals, types as _types, task as _task_model
from flytekit.models.core import workflow as _workflow_model, identifier as _identifier
from flytekit.sdk import tasks as _sdk_tasks
from flytekit.sdk import workflow as _sdk_workflow
from flytekit.sdk.types import Types as _Types
from tests.flytekit.unit.common_tests import test_helpers


def get_sample_node_metadata(node_id):
    """
    :param Text node_id:
    :rtype: flytekit.models.core.workflow.NodeMetadata
    """

    return _workflow_model.NodeMetadata(
        name=node_id,
        timeout=timedelta(seconds=10),
        retries=_literals.RetryStrategy(0)
    )


def get_sample_container():
    """
    :rtype: flytekit.models.task.Container
    """
    cpu_resource = _task_model.Resources.ResourceEntry(_task_model.Resources.ResourceName.CPU, "1")
    resources = _task_model.Resources(requests=[cpu_resource], limits=[cpu_resource])

    return _task_model.Container(
        "my_image",
        ["this", "is", "a", "cmd"],
        ["this", "is", "an", "arg"],
        resources,
        {},
        {}
    )


def get_sample_task_metadata():
    """
    :rtype: flytekit.models.task.TaskMetadata
    """
    return _task_model.TaskMetadata(
        True,
        _task_model.RuntimeMetadata(_task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        _literals.RetryStrategy(3),
        "0.1.1b0",
        "This is deprecated!"
    )


@_patch("flytekit.common.tasks.task.SdkTask.fetch")
def test_basic_workflow_promote(mock_task_fetch):
    # This section defines a sample workflow from a user
    @_sdk_tasks.inputs(a=_Types.Integer)
    @_sdk_tasks.outputs(b=_Types.Integer, c=_Types.Integer)
    @_sdk_tasks.python_task()
    def demo_task_for_promote(wf_params, a, b, c):
        b.set(a + 1)
        c.set(a + 2)

    @_sdk_workflow.workflow_class()
    class TestPromoteExampleWf(object):
        wf_input = _sdk_workflow.Input(_Types.Integer, required=True)
        my_task_node = demo_task_for_promote(a=wf_input)
        wf_output_b = _sdk_workflow.Output(my_task_node.outputs.b, sdk_type=_Types.Integer)
        wf_output_c = _sdk_workflow.Output(my_task_node.outputs.c, sdk_type=_Types.Integer)

    # This section uses the TaskTemplate stored in Admin to promote back to an Sdk Workflow
    int_type = _types.LiteralType(_types.SimpleType.INTEGER)
    task_interface = _interface.TypedInterface(
        # inputs
        {'a': _interface.Variable(int_type, "description1")},
        # outputs
        {
            'b': _interface.Variable(int_type, "description2"),
            'c': _interface.Variable(int_type, "description3")
        }
    )
    # Since the promotion of a workflow requires retrieving the task from Admin, we mock the SdkTask to return
    task_template = _task_model.TaskTemplate(
        _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain",
                               "tests.flytekit.unit.common_tests.test_workflow_promote.demo_task_for_promote",
                               "version"),
        "python_container",
        get_sample_task_metadata(),
        task_interface,
        custom={},
        container=get_sample_container()
    )
    sdk_promoted_task = _task.SdkTask.promote_from_model(task_template)
    mock_task_fetch.return_value = sdk_promoted_task
    promoted_template = test_helpers.get_workflow_template()
    promoted_wf = _workflow_common.SdkWorkflow.promote_from_model(promoted_template)

    assert promoted_wf.interface.inputs["wf_input"] == TestPromoteExampleWf.interface.inputs["wf_input"]
    assert promoted_wf.interface.outputs["wf_output_b"] == TestPromoteExampleWf.interface.outputs["wf_output_b"]
    assert promoted_wf.interface.outputs["wf_output_c"] == TestPromoteExampleWf.interface.outputs["wf_output_c"]

    assert len(promoted_wf.nodes) == 1
    assert len(TestPromoteExampleWf.nodes) == 1
    assert promoted_wf.nodes[0].inputs[0] == TestPromoteExampleWf.nodes[0].inputs[0]
