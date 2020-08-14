from __future__ import absolute_import

import json

from mock import MagicMock, PropertyMock, patch

from flytekit.clis.flyte_cli.main import _welcome_message
from flytekit.clis.sdk_in_container import basic_auth
from flytekit.configuration.creds import \
    CLIENT_CREDENTIALS_SECRET as _CREDENTIALS_SECRET

_welcome_message()


def test_get_secret():
    import os

    os.environ[_CREDENTIALS_SECRET.env_var] = "abc"
    assert basic_auth.get_secret() == "abc"


def test_get_basic_authorization_header():
    header = basic_auth.get_basic_authorization_header("client_id", "abc")
    assert header == "Basic Y2xpZW50X2lkOmFiYw=="


@patch("flytekit.clis.sdk_in_container.basic_auth._requests")
def test_get_token(mock_requests):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = json.loads(
        """{"access_token": "abc", "expires_in": 60}"""
    )
    mock_requests.post.return_value = response
    access, expiration = basic_auth.get_token(
        "https://corp.idp.net", "abc123", "my_scope"
    )
    assert access == "abc"
    assert expiration == 60
