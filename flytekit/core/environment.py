import copy
from functools import partial, wraps
from typing import Any, Callable, TypeVar, Union

from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty
from typing_extensions import Concatenate, ParamSpec

from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.task import task

P = ParamSpec("P")
T = TypeVar("T")


# basically, I want the docstring for `flyte.task` to be available for users to see
# this is "copying" the docstring from `flyte.task` to functions wrapped by `forge`
# more details here: https://github.com/python/typing/issues/270
def forge(source: Callable[Concatenate[Any, P], T]) -> Callable[[Callable], Callable[Concatenate[Any, P], T]]:
    def wrapper(target: Callable) -> Callable[Concatenate[Any, P], T]:
        @wraps(source)
        def wrapped(self, *args: P.args, **kwargs: P.kwargs) -> T:
            return target(self, *args, **kwargs)

        return wrapped

    return wrapper


def inherit(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(old)

    for key, value in new.items():
        if key in out:
            if isinstance(value, dict):
                out[key] = inherit(out[key], value)
            else:
                out[key] = value
        else:
            out[key] = value

    return out


class Environment:
    @forge(task)
    def __init__(self, **overrides: Any) -> None:
        _overrides: dict[str, Any] = {}
        for key, value in overrides.items():
            if key == "_task_function":
                raise KeyError("Cannot override task function")

            _overrides[key] = value

        self.overrides = _overrides

    @forge(task)
    def update(self, **overrides: Any) -> None:
        self.overrides = inherit(self.overrides, overrides)

    @forge(task)
    def extend(self, **overrides: Any) -> "Environment":
        return self.__class__(**inherit(self.overrides, overrides))

    @forge(task)
    def __call__(
        self, _task_function: Union[Callable, None] = None, /, **overrides
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        # no additional overrides are passed
        if _task_function is not None:
            if callable(_task_function):
                return partial(task, **self.overrides)(_task_function)

            else:
                raise ValueError("The first positional argument must be a callable")

        # additional overrides are passed
        else:

            def inner(_task_function: Callable) -> Callable:
                inherited = inherit(self.overrides, overrides)

                return partial(task, **inherited)(_task_function)

            return inner

    def show(self) -> None:
        console = Console()

        console.print(Panel.fit(Pretty(self.overrides)))

    task = __call__

    @forge(dynamic)
    def dynamic(
        self, _task_function: Union[Callable, None] = None, /, **overrides
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        # no additional overrides are passed
        if _task_function is not None:
            if callable(_task_function):
                return partial(dynamic, **self.overrides)(_task_function)

            else:
                raise ValueError("The first positional argument must be a callable")

        # additional overrides are passed
        else:

            def inner(_task_function: Callable) -> Callable:
                inherited = inherit(self.overrides, overrides)

                return partial(dynamic, **inherited)(_task_function)

            return inner
