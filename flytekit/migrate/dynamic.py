from typing import Callable, Union

import flytekit

import flytekit.migrate
from flyte._task import AsyncFunctionTaskTemplate, P, R


def dynamic_shim(**kwargs) -> Union[AsyncFunctionTaskTemplate, Callable[P, R]]:
    return flytekit.migrate.task.task_shim(**kwargs)


flytekit.dynamic = dynamic_shim
