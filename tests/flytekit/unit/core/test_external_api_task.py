import json
import collections
from flytekit.core.external_api_task import ExternalApiTask, TASK_MODULE, TASK_NAME, TASK_CONFIG_PKL


class MockExternalApiTask(ExternalApiTask):

    async def do(self, test_int_input : int, **kwargs) -> int:
        return test_int_input

def test_init():
    task = MockExternalApiTask(name="test_task", return_type=int)
    assert task.name == "test_task"
    assert task.interface.inputs == collections.OrderedDict({"test_int_input": int})
    assert task.interface.outputs == collections.OrderedDict({"o0": int})

# use asyncio
def test_do():
    task = MockExternalApiTask(name="test_task", return_type=str)
    assert task.interface.inputs == collections.OrderedDict({"test_int_input": int})

def test_get_custom():
    task = MockExternalApiTask(name="test_task", config={"key": "value"})
    custom = task.get_custom()

    expected_config = json.loads('{"key": "value"}')
    assert custom[TASK_MODULE] == MockExternalApiTask.__module__
    assert custom[TASK_NAME] == MockExternalApiTask.__name__
    assert json.loads(custom[TASK_CONFIG_PKL]) == expected_config

