from __future__ import absolute_import
from flytekit.clis.auth.discovery import DiscoveryClient as _DiscoveryClient
import base64 as _base64
from flytekit.configuration.creds import (
    DISCOVERY_ENDPOINT as _DISCOVERY_ENDPOINT
)
import logging as _logging
import requests as _requests
from flytekit.common.exceptions.base import FlyteException as _FlyteException

_utf_8 = 'utf-8'


class FlyteAuthenticationException(_FlyteException):
    _ERROR_CODE = "FlyteAuthenticationFailed"


def get_token_endpoint():
    discovery_endpoint = _DISCOVERY_ENDPOINT.get()
    discovery_client = _DiscoveryClient(discovery_url=discovery_endpoint)
    return discovery_client.get_authorization_endpoints().token_endpoint


def get_file_contents(location):
    """
    This reads an input file, and returns the string contents, and should be used for reading credentials.
    This function will also strip newlines.

    :param Text location: The file path holding the client id or secret
    :rtype: Text
    """
    with open(location, 'r') as f:
        return f.read().replace('\n', '')


def get_basic_authorization_header(client_id, client_secret):
    """
    This function transforms the client id and the client secret into a header that conforms with http basic auth.
    It joins the id and the secret with a : then base64 encodes it, then adds the appropriate text.
    :param Text client_id:
    :param Text client_secret:
    :rtype: Text
    """
    concated = "{}:{}".format(client_id, client_secret)
    return "Basic {}".format(str(_base64.b64encode(concated.encode(_utf_8)), _utf_8))


def get_token(token_endpoint, authorization_header, scope):
    """
    :param token_endpoint:
    :param authorization_header:
    :param scope:
    :rtype: (Text,Int)
    """
    headers = {
        'Authorization': authorization_header,
        'Cache-Control': 'no-cache',
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    body = {
        'grant_type': 'client_credentials',
        'scope': scope,
    }
    response = _requests.post(token_endpoint, data=body, headers=headers)
    if response.status_code != 200:
        _logging.error("Non-200 ({}) received from IDP: {}".format(response.status_code, response.text))
        raise FlyteAuthenticationException('Non-200 received from IDP')

    response = response.json()
    return response['access_token'], response['expires_in']
