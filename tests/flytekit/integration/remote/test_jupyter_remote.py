import datetime
import json
import os
import pathlib
import subprocess
import time
import typing

import joblib
import pytest
import mock

from flytekit import LaunchPlan, kwtypes
from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.core.launch_plan import reference_launch_plan
from flytekit.core.task import reference_task
from flytekit.core.workflow import reference_workflow
from flytekit.exceptions.user import FlyteAssertion, FlyteEntityNotExistException
from flytekit.extras.sqlite3.task import SQLite3Config, SQLite3Task
from flytekit.remote.remote import FlyteRemote
from flytekit.types.schema import FlyteSchema
from flytekit.remote import init_remote

MODULE_PATH = pathlib.Path(__file__).parent / "workflows/basic"
CONFIG = os.environ.get("FLYTECTL_CONFIG", str(pathlib.Path.home() / ".flyte" / "config-sandbox.yaml"))
IMAGE = os.environ.get("FLYTEKIT_IMAGE", "localhost:30000/flytekit:dev")
PROJECT = "flytesnacks"
DOMAIN = "development"
VERSION = f"v{os.getpid()}"

@pytest.fixture(scope="session")
def register():
    out = subprocess.run(
        [
            "pyflyte",
            "--verbose",
            "-c",
            CONFIG,
            "register",
            "--image",
            IMAGE,
            "--project",
            PROJECT,
            "--domain",
            DOMAIN,
            "--version",
            VERSION,
            MODULE_PATH,
        ]
    )
    assert out.returncode == 0


def run(file_name, wf_name, *args):
    out = subprocess.run(
        [
            "pyflyte",
            "--verbose",
            "-c",
            CONFIG,
            "run",
            "--remote",
            "--image",
            IMAGE,
            "--project",
            PROJECT,
            "--domain",
            DOMAIN,
            MODULE_PATH / file_name,
            wf_name,
            *args,
        ]
    )
    assert out.returncode == 0

@mock.patch("flytekit.tools.interactive.ipython_check")
def test_jupyter_remote_workflow(mock_ipython_check):
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.child_workflow import parent_wf, double

    # child_workflow.parent_wf asynchronously register a parent wf1 with child lp from another wf2.
    future0 = double.remote(a = 3)
    future1 = parent_wf.remote(a = 3)
    future2 = parent_wf.remote(a = 2)
    assert future0.version != VERSION
    assert future1.version != VERSION
    assert future2.version != VERSION
    assert future1.version != future2.version

    out0 = future0.wait()
    assert out0.outputs["t1_int_output"] == 6
    out1 = future1.wait()
    assert out1.outputs["o0"] == 18
    out2 = future2.wait()
    assert out2.outputs["o0"] == 12


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_jupyter_python_task(mock_ipython_check, register):
    """Test execution of a @task-decorated python function that is already registered."""
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.basic_workflow import t1

    future = t1.remote(a = 10, version=VERSION)
    out = future.wait()
    assert future.version == VERSION

    assert out.outputs["t1_int_output"] == 12
    assert out.outputs["c"] == "world"

@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_python_workflow(mock_ipython_check, register):
    """Test execution of a @workflow-decorated python function."""
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.basic_workflow import my_wf

    future = my_wf.remote(a = 10, b = "xyz", version=VERSION)
    out = future.wait()
    assert out.outputs["o0"] == 12
    assert out.outputs["o1"] == "xyzworld"


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_python_workflow_dict_of_string_to_string(mock_ipython_check, register):
    """Test execution of a @workflow-decorated python function."""
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.dict_str_wf import my_wf

    d: typing.Dict[str, str] = {"k1": "v1", "k2": "v2"}
    future = my_wf.remote(d = d, version=VERSION)
    out = future.wait()
    assert json.loads(out.outputs["o0"]) == {"k1": "v1", "k2": "v2"}

@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_python_workflow_list_of_floats(mock_ipython_check, register):
    """Test execution of a @workflow-decorated python function."""
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.list_float_wf import my_wf

    xs: typing.List[float] = [42.24, 999.1, 0.0001]
    future = my_wf.remote(xs = xs, version=VERSION)
    out = future.wait()
    assert out.outputs["o0"] == "[42.24, 999.1, 0.0001]"


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_python_workflow_with_maptask(mock_ipython_check, register):
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.array_map import workflow_with_maptask

    d: typing.List[int] = [1, 2, 3]
    future = workflow_with_maptask.remote(data = d, y = 3, version=VERSION)
    out = future.wait()
    assert out.outputs["o0"] == [4, 5, 6]

@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_python_maptask(mock_ipython_check):
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.array_map import map_fn
    
    d: typing.List[int] = [1, 2, 3]
    y: typing.List[int] = [3, 3, 3]
    future = map_fn.remote(x = d, y = y)
    out = future.wait()
    assert out.outputs["o0"] == [4, 5, 6]