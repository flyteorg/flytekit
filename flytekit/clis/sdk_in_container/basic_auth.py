import base64 as _base64
import logging as _logging

import requests as _requests

from flytekit.common.exceptions.user import FlyteAuthenticationException as _FlyteAuthenticationException
from flytekit.configuration.creds import CLIENT_CREDENTIALS_SECRET as _CREDENTIALS_SECRET

_utf_8 = "utf-8"


def get_secret():
    """
    This function will either read in the password from the file path given by the CLIENT_CREDENTIALS_SECRET_LOCATION
    config object, or from the environment variable using the CLIENT_CREDENTIALS_SECRET config object.
    :rtype: Text
    """
    secret = _CREDENTIALS_SECRET.get()
    if secret:
        return secret
    raise _FlyteAuthenticationException("No secret could be found")


def get_basic_authorization_header(client_id, client_secret):
    """
    This function transforms the client id and the client secret into a header that conforms with http basic auth.
    It joins the id and the secret with a : then base64 encodes it, then adds the appropriate text.
    :param Text client_id:
    :param Text client_secret:
    :rtype: Text
    """
    concated = "{}:{}".format(client_id, client_secret)
    return "Basic {}".format(_base64.b64encode(concated.encode(_utf_8)).decode(_utf_8))


def get_token(token_endpoint, authorization_header, scope):
    """
    :param Text token_endpoint:
    :param Text authorization_header: This is the value for the "Authorization" key. (eg 'Bearer abc123')
    :param Text scope:
    :rtype: (Text,Int) The first element is the access token retrieved from the IDP, the second is the expiration
            in seconds
    """
    headers = {
        "Authorization": authorization_header,
        "Cache-Control": "no-cache",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {
        "grant_type": "client_credentials",
    }
    if scope is not None:
        body["scope"] = scope
    response = _requests.post(token_endpoint, data=body, headers=headers)
    if response.status_code != 200:
        _logging.error("Non-200 ({}) received from IDP: {}".format(response.status_code, response.text))
        raise _FlyteAuthenticationException("Non-200 received from IDP")

    response = response.json()
    return response["access_token"], response["expires_in"]
