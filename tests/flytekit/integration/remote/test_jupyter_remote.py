import json
import os
import pathlib
import subprocess
import typing

import pytest
from mock import mock, patch

from flytekit.configuration import Config
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


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_jupyter_remote_workflow(mock_ipython_check):
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.child_workflow import parent_wf, double

    # child_workflow.parent_wf asynchronously register a parent wf1 with child lp from another wf2.
    future0 = double.remote(a=3)
    future1 = parent_wf.remote(a=3)
    future2 = parent_wf.remote(a=2)
    assert future0.version != VERSION
    assert future1.version != VERSION
    assert future2.version != VERSION
    # It should generate a new version for each execution
    assert future1.version != future2.version

    out0 = future0.wait()
    assert out0.outputs["o0"] == 6
    out1 = future1.wait()
    assert out1.outputs["o0"] == 18
    out2 = future2.wait()
    assert out2.outputs["o0"] == 12


def test_execute_jupyter_python_task(register):
    """Test execution of a @task-decorated python function that is already registered."""
    with patch("flytekit.tools.interactive.ipython_check") as mock_ipython_check:
        mock_ipython_check.return_value = True
        init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
        from .workflows.basic.basic_workflow import t1

        future = t1.remote(a=10, version=VERSION)
        out = future.wait()
        assert future.version == VERSION

        assert out.outputs["t1_int_output"] == 12
        assert out.outputs["c"] == "world"


def test_execute_jupyter_python_workflow(register):
    """Test execution of a @workflow-decorated python function."""
    with patch("flytekit.tools.interactive.ipython_check") as mock_ipython_check:
        mock_ipython_check.return_value = True
        init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
        from .workflows.basic.basic_workflow import my_basic_wf

        future = my_basic_wf.remote(a=10, b="xyz", version=VERSION)
        out = future.wait()
        assert out.outputs["o0"] == 12
        assert out.outputs["o1"] == "xyzworld"


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_fetch_execute_task_list_of_floats(mock_ipython_check):
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.list_float_wf import concat_list

    xs: typing.List[float] = [0.1, 0.2, 0.3, 0.4, -99999.7]
    future = concat_list.remote(xs=xs)
    out = future.wait()
    assert out.outputs["o0"] == "[0.1, 0.2, 0.3, 0.4, -99999.7]"


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_fetch_execute_task_convert_dict(mock_ipython_check):
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.dict_str_wf import convert_to_string

    d: typing.Dict[str, str] = {"key1": "value1", "key2": "value2"}
    future = convert_to_string.remote(d=d)
    out = future.wait()
    assert json.loads(out.outputs["o0"]) == {"key1": "value1", "key2": "value2"}


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_jupyter_python_workflow_dict_of_string_to_string(mock_ipython_check):
    """Test execution of a @workflow-decorated python function."""
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.dict_str_wf import my_dict_str_wf

    d: typing.Dict[str, str] = {"k1": "v1", "k2": "v2"}
    future = my_dict_str_wf.remote(d=d)
    out = future.wait()
    assert json.loads(out.outputs["o0"]) == {"k1": "v1", "k2": "v2"}


@mock.patch("flytekit.tools.interactive.ipython_check")
def test_execute_jupyter_python_workflow_list_of_floats(mock_ipython_check):
    """Test execution of a @workflow-decorated python function."""
    mock_ipython_check.return_value = True
    init_remote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    from .workflows.basic.list_float_wf import my_list_float_wf

    xs: typing.List[float] = [42.24, 999.1, 0.0001]
    future = my_list_float_wf.remote(xs=xs)
    out = future.wait()
    assert out.outputs["o0"] == "[42.24, 999.1, 0.0001]"
