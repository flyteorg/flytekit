import mock
import pytest

from flytekitplugins.flyin import (
    CODE_TOGETHER_CONFIG,
    CODE_TOGETHER_EXTENSION,
    COPILOT_CONFIG,
    COPILOT_EXTENSION,
    DEFAULT_CODE_SERVER_DIR_NAME,
    DEFAULT_CODE_SERVER_EXTENSIONS,
    DEFAULT_CODE_SERVER_REMOTE_PATH,
    VIM_CONFIG,
    VIM_EXTENSION,
    VscodeConfig,
    jupyter,
    vscode,
)
from flytekit import task, workflow
from flytekit.core.context_manager import ExecutionState

from flytekit.tools.translator import get_serializable_task
from flytekit.configuration import Image, ImageConfig, SerializationSettings


@pytest.fixture
def mock_local_execution():
    with mock.patch.object(ExecutionState, "is_local_execution", return_value=True) as mock_func:
        yield mock_func


@pytest.fixture
def mock_remote_execution():
    with mock.patch.object(ExecutionState, "is_local_execution", return_value=False) as mock_func:
        yield mock_func


# TODO: refactor repetitive mocks to a common mock
@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.prepare_interactive_python")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_remote_execution(
    mock_download_vscode, mock_exit_handler, mock_process, mock_prepare_interactive_python, mock_remote_execution
):
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
    mock_prepare_interactive_python.assert_called_once()


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.prepare_interactive_python")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_remote_execution_but_disable(
    mock_download_vscode, mock_exit_handler, mock_process, mock_prepare_interactive_python, mock_remote_execution
):
    @task
    @vscode(enable=False)
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_download_vscode.assert_not_called()
    mock_process.assert_not_called()
    mock_exit_handler.assert_not_called()
    mock_prepare_interactive_python.assert_not_called()


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.prepare_interactive_python")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_local_execution(
    mock_download_vscode, mock_exit_handler, mock_process, mock_prepare_interactive_python, mock_local_execution
):
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
    mock_prepare_interactive_python.assert_not_called()


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
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.prepare_interactive_python")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_run_task_first_fail(
    mock_download_vscode, mock_exit_handler, mock_process, mock_prepare_interactive_python, mock_remote_execution
):
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
    mock_prepare_interactive_python.assert_called_once()


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


def test_vscode_config():
    config = VscodeConfig()
    assert config.code_server_remote_path == DEFAULT_CODE_SERVER_REMOTE_PATH
    assert config.code_server_dir_name == DEFAULT_CODE_SERVER_DIR_NAME
    assert config.extension_remote_paths == DEFAULT_CODE_SERVER_EXTENSIONS

    code_together_config = CODE_TOGETHER_CONFIG
    assert code_together_config.code_server_remote_path == DEFAULT_CODE_SERVER_REMOTE_PATH
    assert code_together_config.code_server_dir_name == DEFAULT_CODE_SERVER_DIR_NAME
    assert code_together_config.extension_remote_paths == DEFAULT_CODE_SERVER_EXTENSIONS + [CODE_TOGETHER_EXTENSION]

    copilot_config = COPILOT_CONFIG
    assert copilot_config.code_server_remote_path == DEFAULT_CODE_SERVER_REMOTE_PATH
    assert copilot_config.code_server_dir_name == DEFAULT_CODE_SERVER_DIR_NAME
    assert copilot_config.extension_remote_paths == DEFAULT_CODE_SERVER_EXTENSIONS + [COPILOT_EXTENSION]

    vim_config = VIM_CONFIG
    assert vim_config.code_server_remote_path == DEFAULT_CODE_SERVER_REMOTE_PATH
    assert vim_config.code_server_dir_name == DEFAULT_CODE_SERVER_DIR_NAME
    assert vim_config.extension_remote_paths == DEFAULT_CODE_SERVER_EXTENSIONS + [VIM_EXTENSION]


def test_vscode_config_add_extensions():
    additional_extensions = [COPILOT_EXTENSION, VIM_EXTENSION, CODE_TOGETHER_EXTENSION]

    config = VscodeConfig()
    config.add_extensions(additional_extensions)

    for extension in additional_extensions:
        assert extension in config.extension_remote_paths

    additional_extension = "test_str_extension"
    config.add_extensions(additional_extension)
    assert additional_extension in config.extension_remote_paths


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.prepare_interactive_python")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode_with_args(
    mock_download_vscode, mock_exit_handler, mock_prepare_interactive_python, mock_process, mock_remote_execution
):
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
    mock_prepare_interactive_python.assert_called_once()


def test_vscode_extra_config(mock_remote_execution):
    @vscode(
        max_idle_seconds=100,
        port=8081,
        enable=True,
        pre_execute=None,
        post_execute=None,
        config=None,
    )
    def t():
        return

    t.get_extra_config()["link_type"] == "vscode"
    t.get_extra_config()["port"] == 8081


def test_serialize_vscode(mock_remote_execution):
    @task
    @vscode(
        max_idle_seconds=100,
        port=8081,
        enable=True,
        pre_execute=None,
        post_execute=None,
        config=None,
    )
    def t():
        return

    default_image = Image(name="default", fqn="docker.io/xyz", tag="some-git-hash")
    default_image_config = ImageConfig(default_image=default_image)
    default_serialization_settings = SerializationSettings(
        project="p", domain="d", version="v", image_config=default_image_config
    )

    serialized_task = get_serializable_task(default_serialization_settings, t)
    assert serialized_task.template.config == {"link_type": "vscode", "port": "8081"}
