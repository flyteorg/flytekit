"""Test local execution of files that use functools to decorate tasks and workflows."""

import os
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path

from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig

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


def test_mcklsdj():
    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    from flytekit.user_code import t

    task_spec = get_serializable(OrderedDict(), serialization_settings, t)
    assert task_spec.template.container.args == [
        "pyflyte-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "flytekit.user_code",
        "task-name",
        "inner_task_2",
    ]
