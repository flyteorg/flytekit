"""Test local execution of files that use functools to decorate tasks and workflows."""

import os
import subprocess
import sys
from pathlib import Path

test_module_dir = Path(os.path.dirname(__file__))


def test_wrapped_tasks_happy_path(capfd):
    subprocess.run(
        [sys.executable, str(test_module_dir / "simple_decorator.py")],
        env={"SCRIPT_INPUT": "10"},
        text=True,
    )
    out = capfd.readouterr().out

    assert out.strip().split("\n") == [
        "before running my_task",
        "try running my_task",
        "running my_task",
        "finally after running my_task",
        "after running my_task",
        "11",
    ]


def test_wrapped_tasks_error(capfd):
    subprocess.run(
        [sys.executable, str(test_module_dir / "simple_decorator.py")],
        env={"SCRIPT_INPUT": "0"},
        text=True,
    )
    out = capfd.readouterr().out

    assert out.strip().split("\n") == [
        "before running my_task",
        "try running my_task",
        "error running my_task: my_task failed with input: 0",
        "finally after running my_task",
        "after running my_task",
    ]
