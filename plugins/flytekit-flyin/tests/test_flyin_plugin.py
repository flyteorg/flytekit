import mock
import pytest

from flytekitplugins.flyin import vscode, jupyter
from flytekit import task, workflow
from flytekit.core.context_manager import ExecutionState


@pytest.fixture
def mock_local_execution():
    with mock.patch.object(ExecutionState, "is_local_execution", return_value=True) as mock_func:
        yield mock_func


@pytest.fixture
def mock_remote_execution():
    with mock.patch.object(ExecutionState, "is_local_execution", return_value=False) as mock_func:
        yield mock_func


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.generate_interactive_python")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_remote_execution(mock_download_vscode, mock_exit_handler, mock_process, mock_generate_interactive_python, mock_remote_execution):
    @task
    @vscode
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_download_vscode.assert_called_once()
    mock_process.assert_called_once()
    mock_exit_handler.assert_called_once()
    mock_generate_interactive_python.assert_called_once()


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_local_execution(mock_download_vscode, mock_exit_handler, mock_process, mock_local_execution):
    @task
    @vscode
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_download_vscode.assert_not_called()
    mock_process.assert_not_called()
    mock_exit_handler.assert_not_called()


def test_vscode_run_task_first_succeed(mock_remote_execution):
    @task
    @vscode(run_task_first=True)
    def t(a: int, b: int) -> int:
        return a + b

    @workflow
    def wf(a: int, b: int) -> int:
        out = t(a=a, b=b)
        return out

    res = wf(a=10, b=5)
    assert res == 15


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_run_task_first_fail(mock_download_vscode, mock_exit_handler, mock_process, mock_remote_execution):
    @task
    @vscode
    def t(a: int, b: int):
        dummy = a // b  # noqa: F841
        return

    @workflow
    def wf(a: int, b: int):
        t(a=a, b=b)

    wf(a=10, b=0)
    mock_download_vscode.assert_called_once()
    mock_process.assert_called_once()
    mock_exit_handler.assert_called_once()


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_local_execution(mock_download_vscode, mock_exit_handler, mock_process, mock_local_execution):
    @task
    @vscode
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_download_vscode.assert_not_called()
    mock_process.assert_not_called()
    mock_exit_handler.assert_not_called()


def test_vscode_run_task_first_succeed(mock_remote_execution):
    @task
    @vscode(run_task_first=True)
    def t(a: int, b: int) -> int:
        return a + b

    @workflow
    def wf(a: int, b: int) -> int:
        out = t(a=a, b=b)
        return out

    res = wf(a=10, b=5)
    assert res == 15


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_run_task_first_fail(mock_download_vscode, mock_exit_handler, mock_process, mock_remote_execution):
    @task
    @vscode
    def t(a: int, b: int):
        dummy = a // b  # noqa: F841
        return

    @workflow
    def wf(a: int, b: int):
        t(a=a, b=b)

    wf(a=10, b=0)
    mock_download_vscode.assert_called_once()
    mock_process.assert_called_once()
    mock_exit_handler.assert_called_once()


@mock.patch("flytekitplugins.flyin.jupyter_lib.decorator.subprocess.Popen")
@mock.patch("flytekitplugins.flyin.jupyter_lib.decorator.sys.exit")
def test_jupyter(mock_exit, mock_popen):
    @task
    @jupyter
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_popen.assert_called_once()
    mock_exit.assert_called_once()
