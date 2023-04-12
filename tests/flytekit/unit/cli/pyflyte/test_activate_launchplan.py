from click.testing import CliRunner
from mock import mock

from flytekit.clis.sdk_in_container import pyflyte
from flytekit.remote import FlyteRemote


@mock.patch("flytekit.clis.sdk_in_container.helpers.FlyteRemote", spec=FlyteRemote)
def test_pyflyte_activate_launchplan(mock_remote):
    mock_remote.generate_console_url.return_value = "ex"
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            pyflyte.main,
            [
                "activate-launchplan",
                "-p",
                "flytesnacks",
                "-d",
                "development",
                "daily",
            ],
        )
        assert result.exit_code == 0
        assert "Launchplan was activated: " in result.output
