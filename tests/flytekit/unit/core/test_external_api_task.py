import collections
import json

import pytest

from flytekit.core.external_api_task import TASK_CONFIG, TASK_MODULE, TASK_NAME, ExternalApiTask
from flytekit.core.interface import Interface, transform_interface_to_typed_interface


class MockExternalApiTask(ExternalApiTask):
    async def do(self, test_int_input: int, **kwargs) -> int:
        return test_int_input


def test_init():
    task = MockExternalApiTask(name="test_task", return_type=int)
    assert task.name == "test_task"

    interface = Interface(
        inputs=collections.OrderedDict({"test_int_input": int, "kwargs": None}),
        outputs=collections.OrderedDict({"o0": int}),
    )
    assert task.interface == transform_interface_to_typed_interface(interface)


@pytest.mark.asyncio
async def test_do():
    input_num = 100
    task = MockExternalApiTask(name="test_task", return_type=int)
    assert input_num == await task.do(test_int_input=input_num)


def test_get_custom():
    task = MockExternalApiTask(name="test_task", config={"key": "value"})
    custom = task.get_custom()

    expected_config = json.loads('{"key": "value"}')
    assert custom[TASK_MODULE] == MockExternalApiTask.__module__
    assert custom[TASK_NAME] == MockExternalApiTask.__name__
    assert json.loads(custom[TASK_CONFIG]) == expected_config
