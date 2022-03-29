import typing

import pytest
import tasks

from flytekit import task
from flytekit.core.module_utils import extract_task_module


def convert_to_test(d: dict) -> typing.Tuple[typing.List[str], typing.List]:
    ids = []
    test_vals = []
    for k, v in d.items():
        ids.append(k)
        test_vals.append(v)
    return ids, test_vals


NAMES, TESTS = convert_to_test(
    {
        "local-convert_to_test": (
            convert_to_test,
            ("test_module_utils.convert_to_test", "test_module_utils", "convert_to_test"),
        ),
        "core.task": (task, ("flytekit.core.task.task", "flytekit.core.task", "task")),
        "current-mod-tasks": (tasks.tasks, ("tasks.tasks", "tasks", "tasks")),
        "tasks-core-task": (tasks.task, ("flytekit.core.task.task", "flytekit.core.task", "task")),
    }
)


@pytest.mark.parametrize(
    "test_input,expected",
    argvalues=TESTS,
    ids=NAMES,
)
def test_extract_task_module(test_input, expected):
    assert extract_task_module(test_input) == expected
