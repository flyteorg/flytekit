import os

import py
import pytest

from flytekit.configuration import FastSerializationSettings, Image, ImageConfig, SecretsConfig, SerializationSettings
from flytekit.core.context_manager import FlyteContext, FlyteContextManager, SecretsManager


class SampleTestClass(object):
    def __init__(self, value):
        self.value = value


def test_levels():
    ctx = FlyteContextManager.current_context()
    b = ctx.new_builder()
    b.flyte_client = SampleTestClass(value=1)
    with FlyteContextManager.with_context(b) as outer:
        assert outer.flyte_client.value == 1
        b = outer.new_builder()
        b.flyte_client = SampleTestClass(value=2)
        with FlyteContextManager.with_context(b) as ctx:
            assert ctx.flyte_client.value == 2

        with FlyteContextManager.with_context(outer.with_new_compilation_state()) as ctx:
            assert ctx.flyte_client.value == 1


def test_default():
    ctx = FlyteContext.current_context()
    assert ctx.file_access is not None


def test_look_up_image_info():
    img = Image.look_up_image_info(name="x", tag="docker.io/xyz", optional_tag=True)
    assert img.name == "x"
    assert img.tag is None
    assert img.fqn == "docker.io/xyz"

    img = Image.look_up_image_info(name="x", tag="docker.io/xyz:latest", optional_tag=True)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "docker.io/xyz"

    img = Image.look_up_image_info(name="x", tag="docker.io/xyz:latest", optional_tag=False)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "docker.io/xyz"

    img = Image.look_up_image_info(name="x", tag="localhost:5000/xyz:latest", optional_tag=False)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "localhost:5000/xyz"


def test_validate_image():
    ic = ImageConfig.validate_image(None, "image", ())
    assert ic
    assert ic.default_image is None

    img1 = "xyz:latest"
    img2 = "docker.io/xyz:latest"
    img3 = "docker.io/xyz:latest"
    img3_cli = f"default={img3}"
    img4 = "docker.io/my:azb"
    img4_cli = f"my_img={img4}"

    ic = ImageConfig.validate_image(None, "image", (img1,))
    assert ic
    assert ic.default_image.full == img1

    ic = ImageConfig.validate_image(None, "image", (img2,))
    assert ic
    assert ic.default_image.full == img2

    ic = ImageConfig.validate_image(None, "image", (img3_cli,))
    assert ic
    assert ic.default_image.full == img3

    with pytest.raises(ValueError):
        ImageConfig.validate_image(None, "image", (img1, img3_cli))

    with pytest.raises(ValueError):
        ImageConfig.validate_image(None, "image", (img1, img2))

    with pytest.raises(ValueError):
        ImageConfig.validate_image(None, "image", (img1, img1))

    ic = ImageConfig.validate_image(None, "image", (img3_cli, img4_cli))
    assert ic
    assert ic.default_image.full == img3
    assert len(ic.images) == 2
    assert ic.images[1].full == img4


def test_secrets_manager_default():
    with pytest.raises(ValueError):
        sec = SecretsManager()
        sec.get("group", "key")

    with pytest.raises(ValueError):
        _ = sec.group.key


def test_secrets_manager_get_envvar():
    sec = SecretsManager()
    with pytest.raises(ValueError):
        sec.get_secrets_env_var("test", "")
    with pytest.raises(ValueError):
        sec.get_secrets_env_var("", "x")
    cfg = SecretsConfig.auto()
    assert sec.get_secrets_env_var("group", "test") == f"{cfg.env_prefix}GROUP_TEST"


def test_secrets_manager_get_file():
    sec = SecretsManager()
    with pytest.raises(ValueError):
        sec.get_secrets_file("test", "")
    with pytest.raises(ValueError):
        sec.get_secrets_file("", "x")
    cfg = SecretsConfig.auto()
    assert sec.get_secrets_file("group", "test") == os.path.join(
        cfg.default_dir,
        "group",
        f"{cfg.file_prefix}test",
    )


def test_secrets_manager_file(tmpdir: py.path.local):
    tmp = tmpdir.mkdir("file_test").dirname
    os.environ["FLYTE_SECRETS_DEFAULT_DIR"] = tmp
    sec = SecretsManager()
    f = os.path.join(tmp, "test")
    with open(f, "w+") as w:
        w.write("my-password")

    with pytest.raises(ValueError):
        sec.get("test", "")
    with pytest.raises(ValueError):
        sec.get("", "x")
    # Group dir not exists
    with pytest.raises(ValueError):
        sec.get("group", "test")

    g = os.path.join(tmp, "group")
    os.makedirs(g)
    f = os.path.join(g, "test")
    with open(f, "w+") as w:
        w.write("my-password")
    assert sec.get("group", "test") == "my-password"
    assert sec.group.test == "my-password"
    del os.environ["FLYTE_SECRETS_DEFAULT_DIR"]


def test_secrets_manager_bad_env():
    with pytest.raises(ValueError):
        os.environ["TEST"] = "value"
        sec = SecretsManager()
        sec.get("group", "test")


def test_secrets_manager_env():
    sec = SecretsManager()
    os.environ[sec.get_secrets_env_var("group", "test")] = "value"
    assert sec.get("group", "test") == "value"

    os.environ[sec.get_secrets_env_var(group="group", key="key")] = "value"
    assert sec.get(group="group", key="key") == "value"


def test_serialization_settings_transport():
    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"hello": "blah"},
        image_config=ImageConfig(
            default_image=default_img,
            images=[default_img],
        ),
        flytekit_virtualenv_root="/opt/venv/blah",
        python_interpreter="/opt/venv/bin/python3",
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir="/opt/blah/blah/blah",
            distribution_location="s3://my-special-bucket/blah/bha/asdasdasd/cbvsdsdf/asdddasdasdasdasdasdasd.tar.gz",
        ),
    )

    tp = serialization_settings.prepare_for_transport()
    ss = SerializationSettings.from_transport(tp)
    assert ss is not None
    assert ss == serialization_settings
    assert len(tp) == 376
