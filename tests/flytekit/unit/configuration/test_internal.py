import os

from flytekit.configuration import Config, get_config_file
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
    assert secret_location == "configs/fake_secret"


def test_client_secret_parsing_from_location():
    cfg = Config.auto(
        config_file=os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/creds_secret_location.yaml")
    )
    assert cfg.platform.client_credentials_secret == "hello\n"
