import json
import collections
from flytekit.core.external_api_task import ExternalApiTask, TASK_MODULE, TASK_NAME, TASK_CONFIG_PKL  # replace "your_module" with the actual module name where ExternalApiTask is defined


# Mocking ExternalApiTask to make it instantiable
class MockExternalApiTask(ExternalApiTask):

    async def do(self, **kwargs):
        pass


# Test for the __init__ method
def test_init():
    task = MockExternalApiTask(name="test_task", return_type=str)
    assert task.name == "test_task"
    assert task.interface.inputs == collections.OrderedDict()
    assert task.interface.outputs == collections.OrderedDict({"o0": str})

# Test for the get_custom method
def test_get_custom():
    task = MockExternalApiTask(name="test_task", config={"key": "value"})
    custom = task.get_custom()

    expected_config = json.loads('{"key": "value"}')  # replace with the expected serialized config
    assert custom[TASK_MODULE] == MockExternalApiTask.__module__
    assert custom[TASK_NAME] == MockExternalApiTask.__name__
    assert json.loads(custom[TASK_CONFIG_PKL]) == expected_config  # you might need to adjust this depending on how you expect the config to be serialized


# Run this with `pytest test_external_api_task.py`
