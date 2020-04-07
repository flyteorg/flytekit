from __future__ import absolute_import, division, print_function

from flytekit.sdk import tasks as _tasks, workflow as _workflow
from flytekit.sdk.types import Types as _Types
from flytekit.sdk.workflow import workflow_class, Input, Output


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


@workflow_class
class DynamicLaunchPlanCaller(object):
    outer_a = Input(_Types.Integer, default=5, help="Input for inner workflow")
    lp_task = lp_yield_task(num=outer_a)
    wf_output = Output(lp_task.outputs.out, sdk_type=_Types.Integer)
