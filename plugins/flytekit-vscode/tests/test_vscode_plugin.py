import mock
from flytekitplugins.vscode import vscode

from flytekit import task, workflow


@mock.patch("sys.exit")
@mock.patch("time.sleep")
@mock.patch("multiprocessing.Process")
@mock.patch("flytekitplugins.vscode.decorator.download_vscode")
@mock.patch("flytekitplugins.vscode.decorator.download_code_together_extension")
def test_vscode_plugin(
    mock_download_code_together_extension, mock_download_vscode, mock_process, mock_sleep, mock_exit
):
    @task
    @vscode(code_together=True)
    def t():
        return

    @workflow
    def wf():
        t()

    wf()

    mock_download_code_together_extension.assert_called_once()
    mock_download_vscode.assert_called_once()
    mock_process.assert_called_once()
    mock_sleep.assert_called_once()
    mock_exit.assert_called_once()
