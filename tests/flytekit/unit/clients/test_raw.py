from __future__ import absolute_import
from flytekit.clients.raw import (
    RawSynchronousFlyteClient as _RawSynchronousFlyteClient,
    _refresh_credentials_basic
)
from flytekit.configuration.creds import CLIENT_CREDENTIALS_SECRET as _CREDENTIALS_SECRET
from flytekit.clis.auth.discovery import AuthorizationEndpoints as _AuthorizationEndpoints
import mock
import os
import json


@mock.patch('flytekit.clients.raw._admin_service')
@mock.patch('flytekit.clients.raw._insecure_channel')
def test_client_set_token(mock_channel, mock_admin):
    mock_channel.return_value = True
    mock_admin.AdminServiceStub.return_value = True
    client = _RawSynchronousFlyteClient(url='a.b.com', insecure=True)
    client.set_access_token('abc')
    assert client._metadata[0][1] == 'Bearer abc'


@mock.patch('flytekit.clis.sdk_in_container.basic_auth._requests')
@mock.patch('flytekit.clients.raw._credentials_access')
def test_refresh_credentials_basic(mock_credentials_access, mock_requests):
    mock_credentials_access.get_authorization_endpoints.return_value = _AuthorizationEndpoints('auth', 'token')
    response = mock.MagicMock()
    response.status_code = 200
    response.json.return_value = json.loads("""{"access_token": "abc", "expires_in": 60}""")
    mock_requests.post.return_value = response
    os.environ[_CREDENTIALS_SECRET.env_var] = "asdf12345"

    mock_client = mock.MagicMock()
    mock_client.url.return_value = 'flyte.localhost'
    _refresh_credentials_basic(mock_client)
    mock_client.set_access_token.assert_called_with('abc')
    mock_credentials_access.get_authorization_endpoints.assert_called_with(mock_client.url)
