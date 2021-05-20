import base64 as _base64
import hashlib as _hashlib
import http.server as _BaseHTTPServer
import os as _os
import re as _re
import urllib.parse as _urlparse
import webbrowser as _webbrowser
from http import HTTPStatus as _StatusCodes
from multiprocessing import get_context as _mp_get_context
from urllib.parse import urlencode as _urlencode

import google.auth
import google.auth.transport.requests
import keyring as _keyring
import requests as _requests
from google.oauth2 import service_account

from flytekit.loggers import auth_logger

_code_verifier_length = 64
_random_seed_length = 40
_utf_8 = "utf-8"


# Identifies the service used for storing passwords in keyring
_keyring_service_name = "flyteauth"
# Identifies the key used for storing and fetching from keyring. In our case, instead of a username as the keyring docs
# suggest, we are storing a user's oidc.
_keyring_access_token_storage_key = "access_token"
_keyring_refresh_token_storage_key = "refresh_token"


def _generate_code_verifier():
    """
    Generates a 'code_verifier' as described in https://tools.ietf.org/html/rfc7636#section-4.1
    Adapted from https://github.com/openstack/deb-python-oauth2client/blob/master/oauth2client/_pkce.py.
    :return str:
    """
    code_verifier = _base64.urlsafe_b64encode(_os.urandom(_code_verifier_length)).decode(_utf_8)
    # Eliminate invalid characters.
    code_verifier = _re.sub(r"[^a-zA-Z0-9_\-.~]+", "", code_verifier)
    if len(code_verifier) < 43:
        raise ValueError("Verifier too short. number of bytes must be > 30.")
    elif len(code_verifier) > 128:
        raise ValueError("Verifier too long. number of bytes must be < 97.")
    return code_verifier


def _generate_state_parameter():
    state = _base64.urlsafe_b64encode(_os.urandom(_random_seed_length)).decode(_utf_8)
    # Eliminate invalid characters.
    code_verifier = _re.sub("[^a-zA-Z0-9-_.,]+", "", state)
    return code_verifier


def _create_code_challenge(code_verifier):
    """
    Adapted from https://github.com/openstack/deb-python-oauth2client/blob/master/oauth2client/_pkce.py.
    :param str code_verifier: represents a code verifier generated by generate_code_verifier()
    :return str: urlsafe base64-encoded sha256 hash digest
    """
    code_challenge = _hashlib.sha256(code_verifier.encode(_utf_8)).digest()
    code_challenge = _base64.urlsafe_b64encode(code_challenge).decode(_utf_8)
    # Eliminate invalid characters
    code_challenge = code_challenge.replace("=", "")
    return code_challenge


class AuthorizationCode(object):
    def __init__(self, code, state):
        self._code = code
        self._state = state

    @property
    def code(self):
        return self._code

    @property
    def state(self):
        return self._state


class OAuthCallbackHandler(_BaseHTTPServer.BaseHTTPRequestHandler):
    """
    A simple wrapper around BaseHTTPServer.BaseHTTPRequestHandler that handles a callback URL that accepts an
    authorization token.
    """

    def do_GET(self):
        url = _urlparse.urlparse(self.path)
        if url.path.strip("/") == self.server.redirect_path.strip("/"):
            self.send_response(_StatusCodes.OK)
            self.end_headers()
            self.handle_login(dict(_urlparse.parse_qsl(url.query)))
        else:
            self.send_response(_StatusCodes.NOT_FOUND)

    def handle_login(self, data):
        self.server.handle_authorization_code(AuthorizationCode(data["code"], data["state"]))


class OAuthHTTPServer(_BaseHTTPServer.HTTPServer):
    """
    A simple wrapper around the BaseHTTPServer.HTTPServer implementation that binds an authorization_client for handling
    authorization code callbacks.
    """

    def __init__(
        self,
        server_address,
        RequestHandlerClass,
        bind_and_activate=True,
        redirect_path=None,
        queue=None,
    ):
        _BaseHTTPServer.HTTPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
        self._redirect_path = redirect_path
        self._auth_code = None
        self._queue = queue

    @property
    def redirect_path(self):
        return self._redirect_path

    def handle_authorization_code(self, auth_code):
        self._queue.put(auth_code)
        self.server_close()

    def handle_request(self, queue=None):
        self._queue = queue
        return super().handle_request()


class Credentials(object):
    def __init__(self, access_token=None):
        self._access_token = access_token

    @property
    def access_token(self):
        return self._access_token


class AuthorizationClient(object):
    def __init__(
        self,
        auth_endpoint=None,
        token_endpoint=None,
        scopes=None,
        client_id=None,
        redirect_uri=None,
        client_secret=None,
    ):
        self._auth_endpoint = auth_endpoint
        self._token_endpoint = token_endpoint
        self._client_id = client_id
        self._scopes = scopes
        self._redirect_uri = redirect_uri
        self._code_verifier = _generate_code_verifier()
        code_challenge = _create_code_challenge(self._code_verifier)
        self._code_challenge = code_challenge
        state = _generate_state_parameter()
        self._state = state
        self._credentials = None
        self._refresh_token = None
        self._headers = {"content-type": "application/x-www-form-urlencoded"}
        self._expired = False
        self._client_secret = client_secret

        self._params = {
            "client_id": client_id,  # This must match the Client ID of the OAuth application.
            "response_type": "code",  # Indicates the authorization code grant
            "scope": " ".join(s.strip("' ") for s in scopes).strip(
                "[]'"
            ),  # ensures that the /token endpoint returns an ID and refresh token
            # callback location where the user-agent will be directed to.
            "redirect_uri": self._redirect_uri,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }

        # Prefer to use already-fetched token values when they've been set globally.
        self._refresh_token = _keyring.get_password(_keyring_service_name, _keyring_refresh_token_storage_key)
        access_token = _keyring.get_password(_keyring_service_name, _keyring_access_token_storage_key)
        if access_token:
            self._credentials = Credentials(access_token=access_token)

    def __repr__(self):
        return f"AuthorizationClient({self._auth_endpoint}, {self._token_endpoint}, {self._client_id}, {self._scopes}, {self._redirect_uri})"

    @property
    def has_valid_credentials(self) -> bool:
        return self._credentials is not None

    @property
    def can_refresh_token(self) -> bool:
        return self._refresh_token is not None

    def start_authorization_flow(self):
        # In the absence of globally-set token values, initiate the token request flow
        ctx = _mp_get_context("fork")
        q = ctx.Queue()

        # First prepare the callback server in the background
        server = self._create_callback_server()
        server_process = ctx.Process(target=server.handle_request, args=(q,))
        server_process.daemon = True
        server_process.start()

        # Send the call to request the authorization code in the background
        self._request_authorization_code()

        # Request the access token once the auth code has been received.
        auth_code = q.get()
        server_process.terminate()
        self.request_access_token(auth_code)

    def _create_callback_server(self):
        server_url = _urlparse.urlparse(self._redirect_uri)
        server_address = (server_url.hostname, server_url.port)
        return OAuthHTTPServer(server_address, OAuthCallbackHandler, redirect_path=server_url.path)

    def _request_authorization_code(self):
        scheme, netloc, path, _, _, _ = _urlparse.urlparse(self._auth_endpoint)
        query = _urlencode(self._params)
        endpoint = _urlparse.urlunparse((scheme, netloc, path, None, query, None))
        auth_logger.debug(f"Requesting authorization code through {endpoint}")
        _webbrowser.open_new_tab(endpoint)

    def _initialize_credentials(self, auth_token_resp):

        """
        The auth_token_resp body is of the form:
        {
          "access_token": "foo",
          "refresh_token": "bar",
          "token_type": "Bearer"
        }
        """
        response_body = auth_token_resp.json()
        if "access_token" not in response_body:
            raise ValueError('Expected "access_token" in response from oauth server')
        if "refresh_token" in response_body:
            self._refresh_token = response_body["refresh_token"]

        access_token = response_body["access_token"]
        refresh_token = response_body["refresh_token"]

        _keyring.set_password(_keyring_service_name, _keyring_access_token_storage_key, access_token)
        _keyring.set_password(_keyring_service_name, _keyring_refresh_token_storage_key, refresh_token)
        self._credentials = Credentials(access_token=access_token)

    def request_access_token(self, auth_code):
        if self._state != auth_code.state:
            raise ValueError(f"Unexpected state parameter [{auth_code.state}] passed")
        self._params.update(
            {
                "code": auth_code.code,
                "code_verifier": self._code_verifier,
                "grant_type": "authorization_code",
            }
        )

        resp = _requests.post(
            url=self._token_endpoint,
            data=self._params,
            headers=self._headers,
            allow_redirects=False,
        )
        if resp.status_code != _StatusCodes.OK:
            # TODO: handle expected (?) error cases:
            #  https://auth0.com/docs/flows/guides/device-auth/call-api-device-auth#token-responses
            raise Exception(
                "Failed to request access token with response: [{}] {}".format(resp.status_code, resp.content)
            )
        self._initialize_credentials(resp)

    def refresh_access_token(self):
        if self._refresh_token is None:
            raise ValueError("no refresh token available with which to refresh authorization credentials")

        resp = _requests.post(
            url=self._token_endpoint,
            data={"grant_type": "refresh_token", "client_id": self._client_id, "refresh_token": self._refresh_token},
            headers=self._headers,
            allow_redirects=False,
        )
        if resp.status_code != _StatusCodes.OK:
            self._expired = True
            # In the absence of a successful response, assume the refresh token is expired. This should indicate
            # to the caller that the AuthorizationClient is defunct and a new one needs to be re-initialized.

            _keyring.delete_password(_keyring_service_name, _keyring_access_token_storage_key)
            _keyring.delete_password(_keyring_service_name, _keyring_refresh_token_storage_key)
            return
        self._initialize_credentials(resp)

    @property
    def credentials(self):
        """
        :return flytekit.clis.auth.auth.Credentials:
        """
        return self._credentials

    @property
    def expired(self):
        """
        :return bool:
        """
        return self._expired


class GcpAuthorizationClient(object):
    def __init__(self):
        """
         the gcp credentials will be fetched under two conditions:
         1. If the environment variable ``GOOGLE_APPLICATION_CREDENTIALS`` is set
        to the path of a valid service account JSON private key file
         2. If the `Google Cloud SDK`_ is installed and has application default
        credentials set they are loaded and returned.
        In case of not able to get the credentials, the google lib will raise an exception
        """
        self._gcp_credentials, _ = google.auth.default()
        self._credentials = None

    @property
    def has_valid_credentials(self) -> bool:
        return self._gcp_credentials is not None

    @property
    def can_refresh_token(self) -> bool:
        return self._gcp_credentials is not None

    def refresh_access_token(self):
        if isinstance(self._gcp_credentials, service_account.Credentials):
            target_audience = "https://flyte.net"
            request = google.auth.transport.requests.Request()
            id_token = google.oauth2.id_token.fetch_id_token(request, target_audience)
        else:
            """
            The `audience` in this case could be random and we need disable audience
            check in flyteadmin
            """
            auth_req = google.auth.transport.requests.Request()
            self._gcp_credentials.refresh(auth_req)
            id_token = self._gcp_credentials.id_token
        self._credentials = Credentials(access_token=id_token)

    @property
    def credentials(self):
        """
        :return flytekit.clis.auth.auth.Credentials:
        """
        return self._credentials

    @property
    def expired(self):
        """
        :return bool:
        """
        return False
