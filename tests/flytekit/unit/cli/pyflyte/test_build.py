import os

from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte

WORKFLOW_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "image_spec_wf.py")


def test_build():
    runner = CliRunner()
    result = runner.invoke(pyflyte.main, ["build", "--fast", WORKFLOW_FILE, "wf"])
    print(result.stdout)
    assert result.exit_code == 0
