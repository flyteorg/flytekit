from flytekit import logger
from flytekit.annotated.sample import x
from flytekit.configuration.common import CONFIGURATION_SINGLETON
from tests.flytekit.common.workflows.simple import add_one
from flytekit.annotated.type_engine import outputs
from flytekit.annotated.stuff import task, workflow, WorkflowOutputs

CONFIGURATION_SINGLETON.x = 0


# def test_fds():
#     r = x(s=33)
#     print(r)


# def test_mcds():
#     c = add_one(a=1)
#     x = 5


# def test_www():
#     x = my_workflow()


@task()
def test_outputs(a: int, b: str) -> outputs(x_str=str, y_int=int):
    return "hello world", 5

logger.debug(f'test_outputs: {test_outputs.to_flyte_idl()}')