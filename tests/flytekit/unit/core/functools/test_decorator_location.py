import importlib


def test_dont_use_wrapper_location():
    m = importlib.import_module("tests.flytekit.unit.core.functools.decorator_usage")
    get_data_task = getattr(m, "get_data")
    assert "decorator_source" not in get_data_task.name
    assert "decorator_usage" in get_data_task.name
