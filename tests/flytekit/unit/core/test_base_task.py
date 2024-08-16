import pytest

from flytekit import task, PythonFunctionTask
from flytekit.core.base_task import decorate_python_task, PythonTask
from flytekit.core.utils import str2bool
from flytekit.interactive import vscode
from flytekit.interactive.constants import FLYTE_ENABLE_VSCODE_KEY


def test_decorate_python_task(monkeypatch: pytest.MonkeyPatch):
    @task
    def t1(a: int, b: int) -> int:
        return a + b

    assert isinstance(decorate_python_task(t1), PythonTask)
    monkeypatch.setenv(FLYTE_ENABLE_VSCODE_KEY, str2bool("True"))
    assert isinstance(decorate_python_task(t1), vscode)
