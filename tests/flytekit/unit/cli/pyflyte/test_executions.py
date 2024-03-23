import mock
from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte
from flytekit.interfaces import cli_identifiers


@mock.patch("flytekit.configuration.plugin.FlyteRemote")
def test_execution_recover(remote):
    runner = CliRunner()
    id = cli_identifiers.WorkflowExecutionIdentifier(project="p1", domain="d1", name="test-execution-id")

    result = runner.invoke(
        pyflyte.main,
        ["execution", "--execution-id", "test-execution-id", "--project", "p1", "--domain", "d1", "recover"],
    )
    assert "Launched execution" in result.stdout
    assert result.exit_code == 0
    remote.return_value.client.recover_execution.assert_called_once_with(id=id)


@mock.patch("flytekit.configuration.plugin.FlyteRemote")
def test_execution_relaunch(remote):
    runner = CliRunner()
    id = cli_identifiers.WorkflowExecutionIdentifier(project="p1", domain="d1", name="test-execution-id")

    result = runner.invoke(
        pyflyte.main,
        ["execution", "--execution-id", "test-execution-id", "--project", "p1", "--domain", "d1", "relaunch"],
    )
    assert "Launched execution" in result.stdout
    assert result.exit_code == 0
    remote.return_value.client.relaunch_execution.assert_called_once_with(id=id)
