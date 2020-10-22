import datetime as _datetime
from typing import Callable, Dict, Union

from flytekit.annotated.task import PythonFunctionPythonTask, TaskTypePlugins, metadata
from flytekit.models.core import identifier as _identifier_models


def dynamic(
    _task_function: Callable = None,
    task_type: str = "",
    cache: bool = False,
    cache_version: str = "",
    retries: int = 0,
    interruptible: bool = False,
    deprecated: str = "",
    timeout: Union[_datetime.timedelta, int] = None,
    environment: Dict[str, str] = None,
    *args,
    **kwargs
) -> Callable:
    def wrapper(fn) -> PythonFunctionPythonTask:
        _timeout = timeout
        if _timeout and not isinstance(_timeout, _datetime.timedelta):
            if isinstance(_timeout, int):
                _timeout = _datetime.timedelta(seconds=_timeout)
            else:
                raise ValueError("timeout should be duration represented as either a datetime.timedelta or int seconds")

        _metadata = metadata(cache, cache_version, retries, interruptible, deprecated, timeout)

        task_instance = TaskTypePlugins[task_type](fn, _metadata, *args, **kwargs)
        # TODO: One of the things I want to make sure to do is better naming support. At this point, we should already
        #       be able to determine the name of the task right? Can anyone think of situations where we can't?
        #       Where does the current instance tracker come into play?
        task_instance.id = _identifier_models.Identifier(
            _identifier_models.ResourceType.TASK, "proj", "dom", "blah", "1"
        )

        return task_instance

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper
