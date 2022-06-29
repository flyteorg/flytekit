import mock
from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context


@mock.patch("flytekit.clis.sdk_in_container.helpers.FlyteRemote")
def test_saving_remote(mock_remote):
    mock_context = mock.MagicMock
    mock_context.obj = {}
    get_and_save_remote_with_click_context(mock_context, "p", "d")
    assert mock_context.obj["flyte_remote"] is not None


def test_register_with_no_package_or_module_argument():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(pyflyte.main, ["register"])
        assert result.exit_code == -1
        assert (
            "Missing argument 'PACKAGE_OR_MODULE...', at least one PACKAGE_OR_MODULE is required but multiple can be passed"
            in result.output
        )
