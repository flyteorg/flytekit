import os

import mock

from flytekit.configuration import PlatformConfig, get_config_file, read_file_if_exists
from flytekit.configuration.internal import AWS, Credentials, Images


def test_load_images():
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/images.config"))
    imgs = Images.get_specified_images(cfg)
    assert imgs == {"abc": "docker.io/abc", "xyz": "docker.io/xyz:latest"}

    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/sample.yaml"))
    imgs = Images.get_specified_images(cfg)
    assert imgs == {"abc": "docker.io/abc", "xyz": "docker.io/xyz:latest"}


def test_no_images():
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    imgs = Images.get_specified_images(cfg)
    assert imgs == {}


def test_client_secret_location():
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/sample.yaml"))
    secret_location = Credentials.CLIENT_CREDENTIALS_SECRET_LOCATION.read(cfg)
    assert secret_location is None

    cfg = get_config_file(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/creds_secret_location.yaml")
    )
    secret_location = Credentials.CLIENT_CREDENTIALS_SECRET_LOCATION.read(cfg)
    assert secret_location == "configs/fake_secret"

    # Modify the path to the secret inline
    cfg._yaml_config["admin"]["clientSecretLocation"] = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "configs/fake_secret"
    )

    # Assert secret contains a newline
    with open(cfg._yaml_config["admin"]["clientSecretLocation"], "rb") as f:
        assert f.read().decode().endswith("\n") is True

    # Assert that secret in platform config does not contain a newline
    platform_cfg = PlatformConfig.auto(cfg)
    assert platform_cfg.client_credentials_secret == "hello"


def test_read_file_if_exists():
    # Test reading full path of this file.
    first_line_of_this_file = read_file_if_exists(filename=__file__)
    assert "import os" in first_line_of_this_file  # first line of this file.

    assert read_file_if_exists(None) is None
    assert read_file_if_exists("") is None


def test_command():
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    res = Credentials.COMMAND.read(cfg)
    assert res == ["aws", "sso", "get-token"]


@mock.patch("flytekit.configuration.file.os")
def test_command_2(mocked):
    mocked.environ.get.return_value = "a,b,c"
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    res = Credentials.COMMAND.read(cfg)
    assert res == ["a", "b", "c"]


@mock.patch("flytekit.configuration.file.os")
def test_some_int(mocked):
    mocked.environ.get.side_effect = "5"
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    res = AWS.RETRIES.read(cfg)
    assert type(res) is int
    assert res == 5
