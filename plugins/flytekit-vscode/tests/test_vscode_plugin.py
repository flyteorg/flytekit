import mock
from flytekitplugins.vscode import vscode

from flytekit import task, workflow


@mock.patch("sys.exit")
@mock.patch("time.sleep")
@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.vscode.decorator.download_vscode")
def test_vscode_plugin(mock_download_vscode, mock_process, mock_sleep, mock_exit):
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
    mock_sleep.assert_called_once()
    mock_exit.assert_called_once()
