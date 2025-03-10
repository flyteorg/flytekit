import pytest

from flytekit import task, Resources
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


def test_task_resources():

    @task(resources=Resources(cpu=("1", "2"), mem=(1024, 2048), gpu=1))
    def my_task():
        pass

    assert my_task.resources.requests == Resources(cpu="1", mem=1024, gpu=1)
    assert my_task.resources.limits == Resources(cpu="2", mem=2048)


def test_task_resources_error():
    msg = "`resource` can not be used together with"
    with pytest.raises(ValueError, match=msg):
        @task(resources=Resources(cpu=("1", "2"), mem=(1024, 2048)), gpu=1, limits=Resources(cpu=1))
        def my_task():
            pass
