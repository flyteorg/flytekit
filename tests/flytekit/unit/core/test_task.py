import pytest

from flytekit.core.task import decorate_function
from flytekit.core.utils import str2bool
from flytekit.interactive import vscode
from flytekit.interactive.constants import FLYTE_ENABLE_VSCODE_KEY


def test_decorate_python_task(monkeypatch: pytest.MonkeyPatch):
    def t1(a: int, b: int) -> int:
        return a + b

    assert decorate_function(t1) is t1
    monkeypatch.setenv(FLYTE_ENABLE_VSCODE_KEY, str2bool("True"))
    assert isinstance(decorate_function(t1), vscode)
