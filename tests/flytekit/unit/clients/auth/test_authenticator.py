import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from flytekit.clients.auth.authenticator import (
    ClientConfig,
    ClientCredentialsAuthenticator,
    CommandAuthenticator,
    PKCEAuthenticator,
    StaticClientConfigStore,
)
from flytekit.clients.auth.exceptions import AuthenticationError

ENDPOINT = "example.com"

client_config = ClientConfig(
    token_endpoint="token_endpoint",
    authorization_endpoint="auth_endpoint",
    redirect_uri="redirect_uri",
    client_id="client",
)

static_cfg_store = StaticClientConfigStore(client_config)


@patch("flytekit.clients.auth.authenticator.KeyringStore")
@patch("flytekit.clients.auth.auth_client.AuthorizationClient.get_creds_from_remote")
@patch("flytekit.clients.auth.auth_client.AuthorizationClient.refresh_access_token")
def test_pkce_authenticator(mock_refresh: MagicMock, mock_get_creds: MagicMock, mock_keyring: MagicMock):
    mock_keyring.retrieve.return_value = None
    authn = PKCEAuthenticator(ENDPOINT, static_cfg_store)
    assert authn._verify is None

    authn = PKCEAuthenticator(ENDPOINT, static_cfg_store, verify=False)
    assert authn._verify is False

    assert authn._creds is None
    assert authn._auth_client is None
    authn.refresh_credentials()
    assert authn._auth_client
    mock_get_creds.assert_called()
    mock_refresh.assert_not_called()
    mock_keyring.store.assert_called()

    authn.refresh_credentials()
    mock_refresh.assert_called()


@patch("subprocess.run")
def test_command_authenticator(mock_subprocess: MagicMock):
    with pytest.raises(AuthenticationError):
        authn = CommandAuthenticator(None)  # noqa

    authn = CommandAuthenticator(["echo"])

    authn.refresh_credentials()
    assert authn._creds
    mock_subprocess.assert_called()

    mock_subprocess.side_effect = subprocess.CalledProcessError(-1, ["x"])

    with pytest.raises(AuthenticationError):
        authn.refresh_credentials()


def test_get_basic_authorization_header():
    header = ClientCredentialsAuthenticator.get_basic_authorization_header("client_id", "abc")
    assert header == "Basic Y2xpZW50X2lkOmFiYw=="


@patch("flytekit.clients.auth.authenticator.requests")
def test_get_token(mock_requests):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = json.loads("""{"access_token": "abc", "expires_in": 60}""")
    mock_requests.post.return_value = response
    access, expiration = ClientCredentialsAuthenticator.get_token("https://corp.idp.net", "abc123", ["my_scope"])
    assert access == "abc"
    assert expiration == 60


@patch("flytekit.clients.auth.authenticator.requests")
def test_client_creds_authenticator(mock_requests):
    authn = ClientCredentialsAuthenticator(
        ENDPOINT, client_id="client", client_secret="secret", cfg_store=static_cfg_store
    )

    response = MagicMock()
    response.status_code = 200
    response.json.return_value = json.loads("""{"access_token": "abc", "expires_in": 60}""")
    mock_requests.post.return_value = response
    authn.refresh_credentials()
    assert authn._creds
