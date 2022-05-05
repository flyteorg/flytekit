"""Script used for testing local execution of functool.wraps-wrapped tasks for stacked decorators"""

import os
from functools import wraps
from typing import List


def task_decorator_1(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        print("running task_decorator_1")
        return fn(*args, **kwargs)

    return wrapper


def task_decorator_2(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        print("running task_decorator_2")
        return fn(*args, **kwargs)

    return wrapper


def task_setup(
    function: callable = None, *,
    integration_requests: List = None
) -> None:
    integration_requests = integration_requests or []

    @wraps(function)
    def wrapper(*args, **kwargs):
        # Preprocessing of task
        print("preprocessing")

        # Execute function
        output = function(*args, **kwargs)

        # Postprocessing of output
        print("postprocessing")

        return output

    return (
        functools.partial(
            task_setup, integration_requests=integration_requests)
        if function is None else wrapper
    )
