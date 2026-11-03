from unittest.mock import MagicMock, patch

from keyring.errors import NoKeyringError

from flytekit.clients.auth.keyring import Credentials, KeyringStore

from flytekit.clients.auth_helper import upgrade_channel_to_authenticated, upgrade_channel_to_proxy_authenticated

from flytekit.configuration import PlatformConfig

import pytest

from flytekit.clients.auth.authenticator import CommandAuthenticator

from flytekit.clients.grpc_utils.auth_interceptor import AuthUnaryInterceptor

@patch("keyring.get_password")
def test_keyring_store_get(kr_get_password: MagicMock):
    kr_get_password.return_value = "t"
    assert KeyringStore.retrieve("example1.com") is not None

    kr_get_password.side_effect = NoKeyringError()
    assert KeyringStore.retrieve("example2.com") is None


@patch("keyring.delete_password")
def test_keyring_store_delete(kr_del_password: MagicMock):
    kr_del_password.return_value = None
    assert KeyringStore.delete("example1.com") is None

    kr_del_password.side_effect = NoKeyringError()
    assert KeyringStore.delete("example2.com") is None


@patch("keyring.set_password")
def test_keyring_store_set(kr_set_password: MagicMock):
    kr_set_password.return_value = None
    assert KeyringStore.store(Credentials(access_token="a", refresh_token="r", for_endpoint="f"))

    kr_set_password.side_effect = NoKeyringError()
    assert KeyringStore.retrieve("example2.com") is None

@patch("flytekit.clients.auth.authenticator.KeyringStore")
def test_upgrade_channel_to_authenticated_with_keyring_exception(mock_keyring_store):
    mock_keyring_store.retrieve.side_effect = Exception("mock exception")

    mock_channel = MagicMock()

    platform_config = PlatformConfig()

    try:
        out_ch = upgrade_channel_to_authenticated(platform_config, mock_channel)
    except Exception as e:
        pytest.fail(f"upgrade_channel_to_authenticated Exception: {e}")

    assert isinstance(out_ch._interceptor, AuthUnaryInterceptor)

@patch("flytekit.clients.auth.authenticator.KeyringStore")
def test_upgrade_channel_to_proxy_authenticated_with_keyring_exception(mock_keyring_store):
    mock_keyring_store.retrieve.side_effect = Exception("mock exception")

    mock_channel = MagicMock()

    platform_config = PlatformConfig(auth_mode="Pkce", proxy_command=["echo", "foo-bar"])

    try:
        out_ch = upgrade_channel_to_proxy_authenticated(platform_config, mock_channel)
    except Exception as e:
        pytest.fail(f"upgrade_channel_to_proxy_authenticated Exception: {e}")

    assert isinstance(out_ch._interceptor, AuthUnaryInterceptor)
    assert isinstance(out_ch._interceptor.authenticator, CommandAuthenticator)
