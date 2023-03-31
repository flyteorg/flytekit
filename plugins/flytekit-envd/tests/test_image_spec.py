from pathlib import Path

from flytekitplugins.envd.image_spec import EnvdImageSpec

from flytekit.image_spec.image_spec import calculate_hash_from_image_spec, image_exist


def test_image_spec():
    image_spec = EnvdImageSpec(
        packages=["pandas"],
        apt_packages=["git"],
        python_version="3.8",
        registry="",
        base_image="cr.flyte.org/flyteorg/flytekit:py3.8-latest",
    )

    config_path = image_spec.create_envd_config(True, None)
    contents = Path(config_path).read_text()
    assert (
        contents
        == """# syntax=v1
    
    def build():
        base(image="cr.flyte.org/flyteorg/flytekit:py3.8-latest", dev=False)
        install.python_packages(name = ["pandas", ])
        install.apt_packages(name = ["git", ])
        install.python(version="3.8")
    """
    )

    hash_value = calculate_hash_from_image_spec(image_spec)
    assert hash_value == "KwGID--5A8Cb1SH8UUwESA.."
    assert image_exist("fake_registry", "epkr42Fd9H")
