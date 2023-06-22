import base64
import hashlib
import os
from dataclasses import asdict

import pytest

from flytekit.core import context_manager
from flytekit.core.context_manager import ExecutionState
from flytekit.image_spec import ImageSpec
from flytekit.image_spec.image_spec import _F_IMG_ID, ImageBuildEngine, ImageSpecBuilder, calculate_hash_from_image_spec


def test_image_spec():
    image_spec = ImageSpec(
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.8",
        registry="",
        base_image="cr.flyte.org/flyteorg/flytekit:py3.8-latest",
        cuda="11.2.2",
        cudnn="8",
        private_registries="registry=my-registry,ca=/etc/config/ca.pem,key=/etc/config/key.pem,cert=/etc/config/cert.pem;registry=my-registry2,ca=/etc/config/ca2.pem,key=/etc/config/key2.pem,cert=/etc/config/cert2.pem",
    )

    assert image_spec.python_version == "3.8"
    assert image_spec.base_image == "cr.flyte.org/flyteorg/flytekit:py3.8-latest"
    assert image_spec.packages == ["pandas"]
    assert image_spec.apt_packages == ["git"]
    assert image_spec.registry == ""
    assert image_spec.cuda == "11.2.2"
    assert image_spec.cudnn == "8"
    assert image_spec.name == "flytekit"
    assert image_spec.builder == "envd"
    assert image_spec.source_root is None
    assert image_spec.env is None
    assert image_spec.pip_index is None
    assert image_spec.is_container() is True
    assert (
        image_spec.private_registries
        == "registry=my-registry,ca=/etc/config/ca.pem,key=/etc/config/key.pem,cert=/etc/config/cert.pem;registry=my-registry2,ca=/etc/config/ca2.pem,key=/etc/config/key2.pem,cert=/etc/config/cert2.pem"
    )

    image_spec.source_root = b""
    image_spec_bytes = asdict(image_spec).__str__().encode("utf-8")
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
