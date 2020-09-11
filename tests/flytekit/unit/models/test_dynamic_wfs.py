from flytekit.common import constants as _sdk_constants
from flytekit.sdk import tasks as _tasks
from flytekit.sdk import workflow as _workflow
from flytekit.sdk.types import Types as _Types


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
    assert "models-test-dynamic-wfs-id-lp" in node_id
    assert node_id.endswith("-0")

    # Assert that the output of the dynamic job spec is bound to the single node in the spec, the workflow node
    # containing the launch plan
    assert dj_spec.outputs[0].var == "out"
    assert dj_spec.outputs[0].binding.promise.node_id == node_id
    assert dj_spec.outputs[0].binding.promise.var == "task_output"


@_tasks.python_task
def empty_task(wf_params):
    wf_params.logging.info("Running empty task")


@_workflow.workflow_class()
class EmptyWorkflow(object):
    empty_task_task_execution = empty_task()


constant_workflow_lp = EmptyWorkflow.create_launch_plan()


@_tasks.outputs(out=_Types.Integer)
@_tasks.dynamic_task
def lp_yield_empty_wf(wf_params, out):
    wf_params.logging.info("Running inner task... yielding a launchplan for empty workflow")
    constant_lp_yielding_task_execution = constant_workflow_lp()
    yield constant_lp_yielding_task_execution
    out.set(42)


def test_dynamic_launch_plan_yielding_of_constant_workflow():
    outputs = lp_yield_empty_wf.unit_test()
    # TODO: Currently, Flytekit will not return early and not do anything if there are any workflow nodes detected
    #   in the output of a dynamic task.
    dj_spec = outputs[_sdk_constants.FUTURES_FILE_NAME]

    assert len(dj_spec.nodes) == 1
    assert len(dj_spec.outputs) == 1
    assert dj_spec.outputs[0].var == "out"
    assert len(outputs.keys()) == 2


@_tasks.inputs(num=_Types.Integer)
@_tasks.python_task
def log_only_task(wf_params, num):
    wf_params.logging.info("{} was called".format(num))


@_workflow.workflow_class()
class InputOnlyWorkflow(object):
    a = _workflow.Input(_Types.Integer, default=5, help="Input for inner workflow")
    log_only_task_execution = log_only_task(num=a)


input_only_workflow_lp = InputOnlyWorkflow.create_launch_plan()


@_tasks.dynamic_task
def lp_yield_input_only_wf(wf_params):
    wf_params.logging.info("Running inner task... yielding a launchplan for input only workflow")
    input_only_workflow_lp_execution = input_only_workflow_lp()
    yield input_only_workflow_lp_execution


def test_dynamic_launch_plan_yielding_of_input_only_workflow():
    outputs = lp_yield_input_only_wf.unit_test()
    # TODO: Currently, Flytekit will not return early and not do anything if there are any workflow nodes detected
    #   in the output of a dynamic task.
    dj_spec = outputs[_sdk_constants.FUTURES_FILE_NAME]

    assert len(dj_spec.nodes) == 1
    assert len(dj_spec.outputs) == 0
    assert len(outputs.keys()) == 2

    # Using the id of the launch plan node, and then appending /inputs.pb to the string, should give you in the outputs
    # map the LiteralMap of the inputs of that node
    input_key = "{}/inputs.pb".format(dj_spec.nodes[0].id)
    lp_input_map = outputs[input_key]
    assert lp_input_map.literals["a"] is not None
