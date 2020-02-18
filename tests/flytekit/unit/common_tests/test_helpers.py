from __future__ import absolute_import

from os import path as _path

from flyteidl.core import workflow_pb2 as _workflow_pb2

from flytekit.models.core import workflow as _workflow_model


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
    filepath = _path.abspath(_path.join(basepath, "resources", "OneTaskWFForPromote.pb"))
    with open(filepath, "rb") as fh:
        workflow_template_pb.ParseFromString(fh.read())

    wt = _workflow_model.WorkflowTemplate.from_flyte_idl(workflow_template_pb)
    return wt
