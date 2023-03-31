from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte
from tests.flytekit.unit.cli.pyflyte.test_run import WORKFLOW_FILE


def test_build():
    runner = CliRunner()
    result = runner.invoke(pyflyte.main, ["build", "--file", WORKFLOW_FILE])
    assert result.output == ""
    assert result.exit_code == 0
