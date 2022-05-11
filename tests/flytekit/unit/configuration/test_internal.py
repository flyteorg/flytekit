import os

from flytekit.configuration import get_config_file, read_file_if_exists
from flytekit.configuration.internal import Credentials, Images


def test_load_images():
    cfg = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/images.config"))
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
    assert secret_location == "../tests/flytekit/unit/configuration/configs/fake_secret"


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
