import os
import pytest

from flytekit import task, dynamic
from flytekit.core.task import decorate_function
from flytekit.core.utils import str2bool
from flytekit.interactive import vscode
from flytekit.interactive.constants import FLYTE_ENABLE_VSCODE_KEY


IMAGE = os.environ.get("FLYTEKIT_IMAGE", "localhost:30000/flytekit:dev")


def test_decorate_python_task(monkeypatch: pytest.MonkeyPatch):
    def t1(a: int, b: int) -> int:
        return a + b

    assert decorate_function(t1) is t1
    monkeypatch.setenv(FLYTE_ENABLE_VSCODE_KEY, str2bool("True"))
    assert isinstance(decorate_function(t1), vscode)


def test_image():
    # Define expected warning and error messages
    WARN_MSG = "container_image is deprecated and will be removed in the future. Please use image instead."
    ERR_MSG = (
        "Cannot specify both image and container_image. "
        "Please use image because container_image is deprecated and will be removed in the future."
    )

    # Plain tasks
    @task(image=IMAGE)
    def t1() -> str:
        return "Use image in @task."
    assert t1._container_image == IMAGE, f"_container_image of t1 should match the user-specified {IMAGE}"

    with pytest.warns(DeprecationWarning, match=WARN_MSG):
        @task(container_image=IMAGE)
        def t2() -> str:
            return "Use container_image in @task."
    assert t2._container_image == IMAGE, f"_container_image of t2 should match the user-specified {IMAGE}"

    with pytest.raises(ValueError, match=ERR_MSG):
        @task(image=IMAGE, container_image=IMAGE)
        def t3() -> str:
            return "Use both image and container_image in @task."

    # Dynamic workflow tasks
    @dynamic(image=IMAGE)
    def dy_t1(i: int) -> str:
        return "Use image in @dynamic."
    assert dy_t1._container_image == IMAGE, f"_container_image of dy_t1 should match the user-specified {IMAGE}"

    with pytest.warns(DeprecationWarning, match=WARN_MSG):
        @dynamic(container_image=IMAGE)
        def dy_t2(i: int) -> str:
            return "Use container_image in @dynamic."
    assert dy_t2._container_image == IMAGE, f"_container_image of dy_t2 should match the user-specified {IMAGE}"

    with pytest.raises(ValueError, match=ERR_MSG):
        @dynamic(image=IMAGE, container_image=IMAGE)
        def dy_t3(i: int) -> str:
            return "Use both image and container_image in @dynamic."

    # TODO: Test eager
