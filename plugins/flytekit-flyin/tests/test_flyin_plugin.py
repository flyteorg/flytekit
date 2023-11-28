import mock
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


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
def test_vscode(mock_download_vscode, mock_exit_handler, mock_process):
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
