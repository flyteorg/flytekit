import shutil

import mock
from flytekitplugins.vscode import vscode

from flytekit import task, workflow

EXECUTABLE_NAME = "code-server"


@mock.patch("sys.exit")
def test_vscode_plugin(mock_sys_exit):
    @task
    @vscode(server_up_seconds=5)
    def t():
        return

    @workflow
    def wf():
        t()

    executable_path = shutil.which(EXECUTABLE_NAME)
    assert executable_path is None
    wf()
    executable_path = shutil.which(EXECUTABLE_NAME)
    assert executable_path is not None
