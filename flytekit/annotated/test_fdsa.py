import typing

from flytekit import logger
from flytekit.annotated.stuff import task
from flytekit.configuration.common import CONFIGURATION_SINGLETON

CONFIGURATION_SINGLETON.x = 0


nt1 = typing.NamedTuple("NT1", x_str=str, y_int=int)


@task
def t_with_named_tuple(a: int, b: str) -> nt1:
    return nt1("hello world", 5)


# @task
# def t_with_typing_tuple(a: int, b: str) -> typing.Tuple[int, str]:
#     return 5, "hello world"
#
# @task
# def t_with_tuple(a: int, b: str) -> (int, str):
#     return 5, "hello world"


logger.debug(f'test_outputs: {t_with_named_tuple}')



