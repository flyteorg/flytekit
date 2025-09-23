from typing import Callable, Union

from flyte._task import AsyncFunctionTaskTemplate, P, R

import flytekit

import flytekit.migrate


def dynamic_shim(**kwargs) -> Union[AsyncFunctionTaskTemplate, Callable[P, R]]:
    return flytekit.migrate.task.task_shim(**kwargs)


flytekit.dynamic = dynamic_shim
