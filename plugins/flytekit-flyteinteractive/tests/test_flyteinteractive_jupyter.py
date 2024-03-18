from collections import OrderedDict

import mock
import pytest
from flytekitplugins.flyteinteractive import jupyter

from flytekit import task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.context_manager import ExecutionState
from flytekit.tools.translator import get_serializable_task


@pytest.fixture
def mock_local_execution():
    with mock.patch.object(ExecutionState, "is_local_execution", return_value=True) as mock_func:
        yield mock_func


@pytest.fixture
def mock_remote_execution():
    with mock.patch.object(ExecutionState, "is_local_execution", return_value=False) as mock_func:
        yield mock_func


@pytest.fixture
def jupyter_patches():
    with mock.patch("multiprocessing.Process") as mock_process, mock.patch(
        "flytekitplugins.flyteinteractive.jupyter_lib.decorator.write_example_notebook"
    ) as mock_write_example_notebook, mock.patch(
        "flytekitplugins.flyteinteractive.jupyter_lib.decorator.exit_handler"
    ) as mock_exit_handler:
        yield (mock_process, mock_write_example_notebook, mock_exit_handler)


def test_jupyter_remote_execution(jupyter_patches, mock_remote_execution):
    (mock_process, mock_write_example_notebook, mock_exit_handler) = jupyter_patches

    @task
    @jupyter
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_process.assert_called_once()
    mock_write_example_notebook.assert_called_once()
    mock_exit_handler.assert_called_once()


def test_jupyter_remote_execution_but_disable(jupyter_patches, mock_remote_execution):
    (mock_process, mock_write_example_notebook, mock_exit_handler) = jupyter_patches

    @task
    @jupyter(enable=False)
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_process.assert_not_called()
    mock_write_example_notebook.assert_not_called()
    mock_exit_handler.assert_not_called()


def test_jupyter_local_execution(jupyter_patches, mock_local_execution):
    (mock_process, mock_write_example_notebook, mock_exit_handler) = jupyter_patches

    @task
    @jupyter
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_process.assert_not_called()
    mock_write_example_notebook.assert_not_called()
    mock_exit_handler.assert_not_called()


def test_jupyter_run_task_first_succeed(mock_remote_execution):
    @task
    @jupyter(run_task_first=True)
    def t(a: int, b: int) -> int:
        return a + b

    @workflow
    def wf(a: int, b: int) -> int:
        out = t(a=a, b=b)
        return out

    res = wf(a=7, b=8)
    assert res == 15


def test_jupyter_run_task_first_fail(jupyter_patches, mock_remote_execution):
    (mock_process, mock_write_example_notebook, mock_exit_handler) = jupyter_patches

    @task
    @jupyter(run_task_first=True)
    def t(a: int, b: int):
        dummy = a // b  # noqa: F841
        return

    @workflow
    def wf(a: int, b: int):
        t(a=a, b=b)

    wf(a=10, b=0)
    mock_process.assert_called_once()
    mock_write_example_notebook.assert_called_once()
    mock_exit_handler.assert_called_once()


def test_jupyter_extra_config(mock_remote_execution):
    @jupyter(
        max_idle_seconds=100,
        port=8888,
        notebook_dir="/root",
        enable=True,
        pre_execute=None,
        post_execute=None,
    )
    def t():
        return

    assert t.get_extra_config()["link_type"] == "jupyter"
    assert t.get_extra_config()["port"] == "8888"


def test_serialize_vscode(mock_remote_execution):
    @task
    @jupyter(
        max_idle_seconds=100,
        port=8889,
        notebook_dir="/root",
        enable=True,
        pre_execute=None,
        post_execute=None,
    )
    def t():
        return

    default_image = Image(name="default", fqn="docker.io/xyz", tag="some-git-hash")
    default_image_config = ImageConfig(default_image=default_image)
    default_serialization_settings = SerializationSettings(
        project="p", domain="d", version="v", image_config=default_image_config
    )

    serialized_task = get_serializable_task(OrderedDict(), default_serialization_settings, t)
    assert serialized_task.template.config == {"link_type": "jupyter", "port": "8889"}
