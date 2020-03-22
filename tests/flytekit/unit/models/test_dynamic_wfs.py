from __future__ import absolute_import
from __future__ import print_function

from flytekit.sdk import tasks as _tasks, workflow as _workflow
from flytekit.sdk.types import Types as _Types
from flytekit.common import constants as _sdk_constants


@_tasks.inputs(num=_Types.Integer)
@_tasks.outputs(out=_Types.Integer)
@_tasks.python_task
def inner_task(wf_params, num, out):
    wf_params.logging.info("Running inner task... setting output to input")
    out.set(num)


@_workflow.workflow_class()
class IdentityWorkflow(object):
    a = _workflow.Input(_Types.Integer, default=5, help="Input for inner workflow")
    odd_nums_task = inner_task(num=a)
    task_output = _workflow.Output(odd_nums_task.outputs.out, sdk_type=_Types.Integer)


id_lp = IdentityWorkflow.create_launch_plan()


@_tasks.inputs(num=_Types.Integer)
@_tasks.outputs(out=_Types.Integer)
@_tasks.dynamic_task
def lp_yield_task(wf_params, num, out):
    wf_params.logging.info("Running inner task... yielding a launchplan")
    identity_lp_execution = id_lp(a=num)
    yield identity_lp_execution
    out.set(identity_lp_execution.outputs.task_output)


def test_dynamic_launch_plan_yielding():
    outputs = lp_yield_task.unit_test(num=10)
    # TODO: Currently, Flytekit will not return early and not do anything if there are any workflow nodes detected
    #   in the output of a dynamic task.
    dj_spec = outputs[_sdk_constants.FUTURES_FILE_NAME]

    assert dj_spec.min_successes == 1

    launch_plan_node = dj_spec.nodes[0]
    node_id = launch_plan_node.id
    assert node_id.startswith("lp")
    assert node_id.endswith("-0")

    # Assert that the output of the dynamic job spec is bound to the single node in the spec, the workflow node
    # containing the launch plan
    assert dj_spec.outputs[0].var == "out"
    assert dj_spec.outputs[0].binding.promise.node_id == node_id
    assert dj_spec.outputs[0].binding.promise.var == "task_output"
