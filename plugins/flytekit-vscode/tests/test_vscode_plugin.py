import mock
from flytekitplugins.vscode import vscode

from flytekit import task, workflow


@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.vscode.decorator.exit_handler")
@mock.patch("flytekitplugins.vscode.decorator.download_vscode")
def test_vscode_plugin(mock_download_vscode, mock_exit_handler, mock_process):
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
