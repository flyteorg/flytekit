from flytekit import logger
from flytekit.annotated.stuff import task
from flytekit.annotated.type_engine import outputs
from flytekit.configuration.common import CONFIGURATION_SINGLETON

CONFIGURATION_SINGLETON.x = 0


@task()
def test_outputs(a: int, b: str) -> outputs(x_str=str, y_int=int):
    return "hello world", 5


logger.debug(f'test_outputs: {test_outputs}')
