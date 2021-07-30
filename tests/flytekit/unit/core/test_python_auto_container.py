from typing import List

import mock

from flytekit.core.context_manager import SerializationSettings
from flytekit.core.python_auto_container import PythonAutoContainerTask


@mock.patch("flytekit.core.python_auto_container.SDK_PYTHON_VENV")
def test_get_default_command_prefix_sdk_python_venv(mock_venv):

    mock_venv.get.return_value = ["service_venv"]

    task = PythonAutoContainerTask(
        "test",
        None,
    )

    task._task_resolver._location = "location"
    task._task_resolver.loader_args = lambda a, b: ["a", "b"]

    cmd: List[str] = task.get_default_command(SerializationSettings("p", "d", "v", None))
    assert cmd[0] == "service_venv"

    mock_venv.get.return_value = []
    cmd: List[str] = task.get_default_command(SerializationSettings("p", "d", "v", None))
    assert cmd[0] == "pyflyte-execute"
