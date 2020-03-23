from __future__ import absolute_import

from datetime import timedelta

from mock import patch as _patch
from os import path as _path

from flytekit.common import workflow as _workflow_common
from flytekit.common.tasks import task as _task
from flytekit.models import interface as _interface, \
    literals as _literals, types as _types, task as _task_model
from flytekit.models.core import workflow as _workflow_model, identifier as _identifier, compiler as _compiler_model
from flytekit.sdk import tasks as _sdk_tasks
from flytekit.sdk import workflow as _sdk_workflow
from flytekit.sdk.types import Types as _Types
from flyteidl.core import compiler_pb2 as _compiler_pb2, workflow_pb2 as _workflow_pb2
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input, Output
from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input, Output


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
        True,
        "0.1.1b0",
        "This is deprecated!"
    )


def get_workflow_template():
    """
    This function retrieves a TasKTemplate object from the pb file in the resources directory.
    It was created by reading from Flyte Admin, the following workflow, after registration.

    from __future__ import absolute_import

    from flytekit.common.types.primitives import Integer
    from flytekit.sdk.tasks import (
        python_task,
        inputs,
        outputs,
    )
    from flytekit.sdk.types import Types
    from flytekit.sdk.workflow import workflow_class, Input, Output


    @inputs(a=Types.Integer)
    @outputs(b=Types.Integer, c=Types.Integer)
    @python_task()
    def demo_task_for_promote(wf_params, a, b, c):
        b.set(a + 1)
        c.set(a + 2)


    @workflow_class()
    class OneTaskWFForPromote(object):
        wf_input = Input(Types.Integer, required=True)
        my_task_node = demo_task_for_promote(a=wf_input)
        wf_output_b = Output(my_task_node.outputs.b, sdk_type=Integer)
        wf_output_c = Output(my_task_node.outputs.c, sdk_type=Integer)


    :rtype: flytekit.models.core.workflow.WorkflowTemplate
    """
    workflow_template_pb = _workflow_pb2.WorkflowTemplate()
    # So that tests that use this work when run from any directory
    basepath = _path.dirname(__file__)
    filepath = _path.abspath(_path.join(basepath, "resources/protos", "OneTaskWFForPromote.pb"))
    with open(filepath, "rb") as fh:
        workflow_template_pb.ParseFromString(fh.read())

    wt = _workflow_model.WorkflowTemplate.from_flyte_idl(workflow_template_pb)
    return wt

# Commenting these tests out for now until we can find a way to ensure
# these tests pass on all flyteidl changes.

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
    workflow_template = get_workflow_template()
    promoted_wf = _workflow_common.SdkWorkflow.promote_from_model(workflow_template)

    assert promoted_wf.interface.inputs["wf_input"] == TestPromoteExampleWf.interface.inputs["wf_input"]
    assert promoted_wf.interface.outputs["wf_output_b"] == TestPromoteExampleWf.interface.outputs["wf_output_b"]
    assert promoted_wf.interface.outputs["wf_output_c"] == TestPromoteExampleWf.interface.outputs["wf_output_c"]

    assert len(promoted_wf.nodes) == 1
    assert len(TestPromoteExampleWf.nodes) == 1
    assert promoted_wf.nodes[0].inputs[0] == TestPromoteExampleWf.nodes[0].inputs[0]


def get_compiled_workflow_closure():
    """
    :rtype: flytekit.models.core.compiler.CompiledWorkflowClosure
    """
    cwc_pb = _compiler_pb2.CompiledWorkflowClosure()
    # So that tests that use this work when run from any directory
    basepath = _path.dirname(__file__)
    filepath = _path.abspath(_path.join(basepath, "resources/protos", "CompiledWorkflowClosure.pb"))
    with open(filepath, "rb") as fh:
        cwc_pb.ParseFromString(fh.read())

    return _compiler_model.CompiledWorkflowClosure.from_flyte_idl(cwc_pb)


# def test_subworkflow_promote():
#     cwc = get_compiled_workflow_closure()
#     primary = cwc.primary
#     sub_workflow_map = {sw.template.id: sw.template for sw in cwc.sub_workflows}
#     task_map = {t.template.id: t.template for t in cwc.tasks}
#     promoted_wf = _workflow_common.SdkWorkflow.promote_from_model(primary.template, sub_workflow_map, task_map)

#     # This file that the promoted_wf reads contains the compiled workflow closure protobuf retrieved from Admin
#     # after registering a workflow that basically looks like the one below.

#     @inputs(num=Types.Integer)
#     @outputs(out=Types.Integer)
#     @python_task
#     def inner_task(wf_params, num, out):
#         wf_params.logging.info("Running inner task... setting output to input")
#         out.set(num)

#     @workflow_class()
#     class IdentityWorkflow(object):
#         a = Input(Types.Integer, default=5, help="Input for inner workflow")
#         odd_nums_task = inner_task(num=a)
#         task_output = Output(odd_nums_task.outputs.out, sdk_type=Types.Integer)

#     @workflow_class()
#     class StaticSubWorkflowCaller(object):
#         outer_a = Input(Types.Integer, default=5, help="Input for inner workflow")
#         identity_wf_execution = IdentityWorkflow(a=outer_a)
#         wf_output = Output(identity_wf_execution.outputs.task_output, sdk_type=Types.Integer)

#     assert StaticSubWorkflowCaller.interface == promoted_wf.interface
#     assert StaticSubWorkflowCaller.nodes[0].id == promoted_wf.nodes[0].id
#     assert StaticSubWorkflowCaller.nodes[0].inputs == promoted_wf.nodes[0].inputs
#     assert StaticSubWorkflowCaller.outputs == promoted_wf.outputs
