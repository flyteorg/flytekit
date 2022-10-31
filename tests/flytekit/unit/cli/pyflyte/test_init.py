import tempfile

import pytest
from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte


@pytest.mark.parametrize(
    "command",
    [
        ["example"],
        ["example", "--template", "simple-example"],
        ["example", "--template", "bayesian-optimization"],
    ],
)
def test_pyflyte_init(command, monkeypatch: pytest.MonkeyPatch):
    try:
        with tempfile.TemporaryDirectory() as tdir:
            monkeypatch.chdir(tdir)
            runner = CliRunner()
            result = runner.invoke(
                pyflyte.init,
                command,
                catch_exceptions=True,
            )
            assert result.exit_code == 0
    except PermissionError as e:
        # Just ignore permission errors during cleanup.
        # This is particularyly problematic on Windows (https://bugs.python.org/issue29982),
        # so much so that starting on python>=3.10 the TemporaryDirectory constructor
        # takes an extra paramater to allow for cleanup errors to be ignored.
        print(f"Ignoring permission error = {e}")
