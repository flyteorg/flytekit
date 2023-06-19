import base64
import hashlib
import os
import pathlib
from copy import copy
from dataclasses import asdict

import pytest

from flytekit.core import context_manager
from flytekit.core.context_manager import ExecutionState
from flytekit.image_spec import ImageSpec
from flytekit.image_spec.image_spec import _F_IMG_ID, ImageBuildEngine, ImageSpecBuilder, calculate_hash_from_image_spec

REQUIREMENT_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "requirements.txt")


def test_image_spec():
    image_spec = ImageSpec(
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.8",
        registry="",
        base_image="cr.flyte.org/flyteorg/flytekit:py3.8-latest",
        cuda="11.2.2",
        cudnn="8",
        requirements=REQUIREMENT_FILE,
    )

    assert image_spec.python_version == "3.8"
    assert image_spec.base_image == "cr.flyte.org/flyteorg/flytekit:py3.8-latest"
    assert image_spec.packages == ["pandas"]
    assert image_spec.apt_packages == ["git"]
    assert image_spec.registry == ""
    assert image_spec.requirements == REQUIREMENT_FILE
    assert image_spec.cuda == "11.2.2"
    assert image_spec.cudnn == "8"
    assert image_spec.name == "flytekit"
    assert image_spec.builder == "envd"
    assert image_spec.source_root is None
    assert image_spec.env is None
    assert image_spec.pip_index is None
    assert image_spec.is_container() is True

    spec = copy(image_spec)
    spec.source_root = b""
    spec.requirements = hashlib.sha1(pathlib.Path(REQUIREMENT_FILE).read_bytes()).__str__()
    image_spec_bytes = asdict(spec).__str__().encode("utf-8")
    tag = base64.urlsafe_b64encode(hashlib.md5(image_spec_bytes).digest()).decode("ascii")
    tag = tag.replace("=", ".")
    assert image_spec.image_name() == f"flytekit:{tag}"
    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION))
    ):
        os.environ[_F_IMG_ID] = "flytekit:123"
        assert image_spec.is_container() is False

    class DummyImageSpecBuilder(ImageSpecBuilder):
        def build_image(self, img):
            ...

    ImageBuildEngine.register("dummy", DummyImageSpecBuilder())
    ImageBuildEngine._REGISTRY["dummy"].build_image(image_spec)

    assert "dummy" in ImageBuildEngine._REGISTRY
    assert calculate_hash_from_image_spec(image_spec) == tag
    assert image_spec.exist() is False

    with pytest.raises(Exception):
        image_spec.builder = "flyte"
        ImageBuildEngine.build(image_spec)
