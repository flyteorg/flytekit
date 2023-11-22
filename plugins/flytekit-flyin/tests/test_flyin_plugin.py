import mock
from flytekitplugins.flyin import vscode

from flytekit import task, workflow


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.exit_handler")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_vscode")
@mock.patch("flytekitplugins.flyin.vscode_lib.decorator.download_extension")
def test_vscode(mock_download_extension, mock_download_vscode, mock_exit_handler, mock_process):
    @task
    @vscode
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_download_extension.assert_called_once()
    mock_download_vscode.assert_called_once()
    mock_process.assert_called_once()
    mock_exit_handler.assert_called_once()
