from typing import Callable, Union

from flyte._task import AsyncFunctionTaskTemplate, P, R

import flytekit

import flytekit._migrate


def dynamic_shim(**kwargs) -> Union[AsyncFunctionTaskTemplate, Callable[P, R]]:
    return flytekit._migrate.task.task_shim(**kwargs)


flytekit.dynamic = dynamic_shim
