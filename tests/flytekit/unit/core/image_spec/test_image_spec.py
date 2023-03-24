from pathlib import Path

from flytekit.image_spec import ImageSpec
from flytekit.image_spec.image_spec import calculate_hash_from_image_spec, create_envd_config, should_build_image


def test_image_spec():
    image_spec = ImageSpec(
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.9",
        registry="",
    )

    config_path = create_envd_config(
        image_spec,
        True,
    )
    contents = Path(config_path).read_text()
    assert (
        contents
        == """# syntax=v1

def build():
    base(image="cr.flyte.org/flyteorg/flytekit:py3.9-latest", dev=False)
    install.python_packages(name = ["pandas", ])
    install.apt_packages(name = ["git", ])
    install.python(version="3.9")
"""
    )

    hash_value = calculate_hash_from_image_spec(image_spec)
    assert hash_value == "epkr42Fd9HJB-diDDGT8hA.."

    assert should_build_image("fake_registry", "epkr42Fd9H")
