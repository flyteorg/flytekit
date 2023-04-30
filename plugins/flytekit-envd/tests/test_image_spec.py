from pathlib import Path

from flytekitplugins.envd.image_builder import EnvdImageSpecBuilder, create_envd_config

from flytekit.image_spec.image_spec import ImageSpec


def test_image_spec():
    image_spec = ImageSpec(
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.8",
        registry="",
        base_image="cr.flyte.org/flyteorg/flytekit:py3.8-latest",
    )

    EnvdImageSpecBuilder().build_image(image_spec)
    config_path = create_envd_config(image_spec)
    contents = Path(config_path).read_text()
    assert (
        contents
        == """# syntax=v1

def build():
    base(image="cr.flyte.org/flyteorg/flytekit:py3.8-latest", dev=False)
    install.python_packages(name = ["pandas"])
    install.apt_packages(name = ["git"])
    install.python(version="3.8")
    runtime.environ(env={'PYTHONPATH': '/root', '_F_IMG_ID': 'flytekit:yZ8jICcDTLoDArmNHbWNwg..'})
"""
    )
