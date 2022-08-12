import importlib
import inspect

from flytekit.core.tracker import extract_task_module


def test_dont_use_wrapper_location():
    m = importlib.import_module("tests.flytekit.unit.core.flyte_functools.decorator_usage")
    get_data_task = getattr(m, "get_data")
    assert "decorator_source" not in get_data_task.name
    assert "decorator_usage" in get_data_task.name

    a, b, c, _ = extract_task_module(get_data_task)
    assert (a, b, c) == (
        "tests.flytekit.unit.core.flyte_functools.decorator_usage.get_data",
        "tests.flytekit.unit.core.flyte_functools.decorator_usage",
        "get_data",
    )


def test_imperative_wf():
    m = importlib.import_module("tests.flytekit.unit.core.flyte_functools.imperative_wf")
    imperative_wf = getattr(m, "wf")
    print(inspect.getabsfile(m))
    extract_task_module(imperative_wf)
    # variable_name = [k for k, v in locals().items() if v == getattr(m, "wf")][0]
    # print(variable_name)
    print(type(imperative_wf))
