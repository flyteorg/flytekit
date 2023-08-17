import logging
import subprocess
import typing
from abc import abstractmethod
from dataclasses import dataclass

import requests as _requests
from http import HTTPStatus as _StatusCodes

import click

from . import token_client
from .auth_client import AuthorizationClient
from .exceptions import AccessTokenNotFoundError, AuthenticationError
from .keyring import Credentials, KeyringStore


@dataclass
class ClientConfig:
    """
    Client Configuration that is needed by the authenticator
    """

    token_endpoint: str
    authorization_endpoint: str
    redirect_uri: str
    client_id: str
    device_authorization_endpoint: typing.Optional[str] = None
    scopes: typing.List[str] = None
    header_key: str = "authorization"
    audience: typing.Optional[str] = None


class ClientConfigStore(object):
    """
    Client Config store retrieve client config. this can be done in multiple ways
    """

    @abstractmethod
    def get_client_config(self) -> ClientConfig:
        ...


class StaticClientConfigStore(ClientConfigStore):
    def __init__(self, cfg: ClientConfig):
        self._cfg = cfg

    def get_client_config(self) -> ClientConfig:
        return self._cfg


class Authenticator(object):
    """
    Base authenticator for all authentication flows
    """

    def __init__(
        self,
        endpoint: str,
        header_key: str,
        credentials: Credentials = None,
        http_proxy_url: typing.Optional[str] = None,
        verify: typing.Optional[typing.Union[bool, str]] = None,
    ):
        self._endpoint = endpoint
        self._creds = credentials
        self._header_key = header_key if header_key else "authorization"
        self._http_proxy_url = http_proxy_url
        self._verify = verify

    def get_credentials(self) -> Credentials:
        return self._creds

    def _set_credentials(self, creds):
        self._creds = creds

    def _set_header_key(self, h: str):
        self._header_key = h

    def fetch_grpc_call_auth_metadata(self) -> typing.Optional[typing.Tuple[str, str]]:
        if self._creds:
            return self._header_key, f"Bearer {self._creds.access_token}"
        return None

    @abstractmethod
    def refresh_credentials(self):
        ...


class PKCEAuthenticator(Authenticator):
    """
    This Authenticator encapsulates the entire PKCE flow and automatically opens a browser window for login

    For Auth0 - you will need to manually configure your config.yaml to include a scopes list of the syntax:
    admin.scopes: ["offline_access", "offline", "all", "openid"] and/or similar scopes in order to get the refresh token +
    caching. Otherwise, it will just receive the access token alone.

    Your FlyteCTL config however should only contain ["offline", "all"] - as OIDC scopes are ungrantable in Auth0
    custom APIs. They are simply requested for the token caching process.
    """

    def __init__(
        self,
        endpoint: str,
        cfg_store: ClientConfigStore,
        scopes: typing.Optional[str] = None,
        header_key: typing.Optional[str] = None,
        verify: typing.Optional[typing.Union[bool, str]] = None,
    ):
        """
        Initialize with default creds from KeyStore using the endpoint name
        """
        super().__init__(endpoint, header_key, KeyringStore.retrieve(endpoint), verify=verify)
        self._cfg_store = cfg_store
        self._auth_client = None
        self._scopes = scopes

    def _initialize_auth_client(self):
        if not self._auth_client:
            cfg = self._cfg_store.get_client_config()
            self._set_header_key(cfg.header_key)
            self._auth_client = AuthorizationClient(
                endpoint=self._endpoint,
                redirect_uri=cfg.redirect_uri,
                client_id=cfg.client_id,
                audience=cfg.audience,  # Only needed for Auth0 - Taken from client config
                scopes=self._scopes or cfg.scopes,  # If scope passed in during object instantiation, that takes precedence over cfg.scopes - FOR AUTH0
                auth_endpoint=cfg.authorization_endpoint,
                token_endpoint=cfg.token_endpoint,
                verify=self._verify,
            )

    def refresh_credentials(self):
        """ """
        self._initialize_auth_client()
        if self._creds:
            """We have an access token so lets try to refresh it"""
            try:
                self._creds = self._auth_client.refresh_access_token(self._creds)
                if self._creds:
                    KeyringStore.store(self._creds)
                return
            except AccessTokenNotFoundError:
                logging.warning("Failed to refresh token. Kicking off a full authorization flow.")
                KeyringStore.delete(self._endpoint)

        self._creds = self._auth_client.get_creds_from_remote()
        KeyringStore.store(self._creds)


class CommandAuthenticator(Authenticator):
    """
    This Authenticator retreives access_token using the provided command
    """

    def __init__(self, command: typing.List[str], header_key: str = None):
        self._cmd = command
        if not self._cmd:
            raise AuthenticationError("Command cannot be empty for command authenticator")
        super().__init__(None, header_key)

    def refresh_credentials(self):
        """
        This function is used when the configuration value for AUTH_MODE is set to 'external_process'.
        It reads an id token generated by an external process started by running the 'command'.
        """
        logging.debug("Starting external process to generate id token. Command {}".format(self._cmd))
        try:
            output = subprocess.run(self._cmd, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            logging.error("Failed to generate token from command {}".format(self._cmd))
            raise AuthenticationError("Problems refreshing token with command: " + str(e))
        self._creds = Credentials(output.stdout.strip())


class ClientCredentialsAuthenticator(Authenticator):
    """
    This Authenticator uses ClientId and ClientSecret to authenticate

    For Auth0 - you will need to manually configure your config.yaml to include a scopes list of the syntax:
    admin.scopes: ["offline_access", "offline", "all", "openid"] and/or similar scopes in order to get the refresh token +
    caching. Otherwise, it will just receive the access token alone.

    Your FlyteCTL config however should only contain ["offline", "all"] - as OIDC scopes are ungrantable in Auth0
    custom APIs. They are simply requested for the token caching process.
    """

    def __init__(
        self,
        endpoint: str,
        client_id: str,
        client_secret: str,
        cfg_store: ClientConfigStore,
        header_key: typing.Optional[str] = None,
        scopes: typing.Optional[typing.List[str]] = None,
        http_proxy_url: typing.Optional[str] = None,
        verify: typing.Optional[typing.Union[bool, str]] = None,
    ):
        if not client_id or not client_secret:
            raise ValueError("Client ID and Client SECRET both are required.")
        cfg = cfg_store.get_client_config()
        self._token_endpoint = cfg.token_endpoint
        self._scopes = scopes or cfg.scopes # Use scopes from `flytekit.configuration.PlatformConfig` if passed
        self._client_id = client_id
        self._client_secret = client_secret
        self._audience = cfg.audience

        super().__init__(
            endpoint=endpoint,
            header_key=cfg.header_key or header_key,
            credentials=KeyringStore.retrieve(endpoint),
            http_proxy_url=http_proxy_url,
            verify=verify
        )


    def refresh_credentials(self):
        """
        No refresh token for ClientCredentials OAuth2 spec, so request new access token and store in Keyring

        This function is used by the _handle_rpc_error() decorator, depending on the AUTH_MODE config object. This handler
        is meant for SDK use-cases of auth (like pyflyte, or when users call SDK functions that require access to Admin,
        like when waiting for another workflow to complete from within a task). This function uses basic auth, which means
        the credentials for basic auth must be present from wherever this code is running.
        """
        try:
            # Note that unlike the Pkce flow, the client ID does not come from Admin.
            logging.debug(f"Basic authorization flow with client id {self._client_id} scope {self._scopes}")
            authorization_header = token_client.get_basic_authorization_header(self._client_id, self._client_secret)

            access_token, refresh_token, expires_in = token_client.get_token(
                token_endpoint=self._token_endpoint,
                authorization_header=authorization_header,
                http_proxy_url=self._http_proxy_url,
                verify=self._verify,
                scopes=self._scopes,
                audience=self._audience,
            )
            logging.info("Retrieved new token, expires in {}".format(expires_in))

            self._creds = Credentials(access_token=access_token,refresh_token=refresh_token, expires_in=expires_in,for_endpoint=self._endpoint)
            KeyringStore.store(self._creds)
        except Exception:
            KeyringStore.delete(self._endpoint)
            raise


class DeviceCodeAuthenticator(Authenticator):
    """
    This Authenticator implements the Device Code authorization flow useful for headless user authentication.

    Examples described
    - https://developer.okta.com/docs/guides/device-authorization-grant/main/
    - https://auth0.com/docs/get-started/authentication-and-authorization-flow/device-authorization-flow#device-flow

    For Auth0 - you will need to manually configure your config.yaml to include a scopes list of the syntax:
    admin.scopes: ["offline_access", "offline", "all", "openid"] and/or similar scopes in order to get the refresh token +
    caching. Otherwise, it will just receive the access token alone.

    Your FlyteCTL config however should only contain ["offline", "all"] - as OIDC scopes are ungrantable in Auth0
    custom APIs. They are simply requested for the token caching process.
    """

    def __init__(
        self,
        endpoint: str,
        cfg_store: ClientConfigStore,
        header_key: typing.Optional[str] = None,
        scopes: typing.Optional[typing.List[str]] = None,
        http_proxy_url: typing.Optional[str] = None,
        verify: typing.Optional[typing.Union[bool, str]] = None,
    ):
        cfg = cfg_store.get_client_config()
        self._audience = cfg.audience
        self._client_id = cfg.client_id
        self._device_auth_endpoint = cfg.device_authorization_endpoint
        # Use "scope" from object instantiation if value is not None - otherwise, default to cfg.scopes
        self._scopes = scopes or cfg.scopes
        self._token_endpoint = cfg.token_endpoint
        if self._device_auth_endpoint is None:
            raise AuthenticationError(
                "Device Authentication is not available on the Flyte backend / authentication server"
            )

        super().__init__(
            endpoint=endpoint,
            header_key=header_key or cfg.header_key,
            credentials=KeyringStore.retrieve(endpoint),
            http_proxy_url=http_proxy_url,
            verify=verify,
        )


    def _credentials_from_response(self, auth_token_resp) -> Credentials:
        """
        The auth_token_resp body is of the form:
        {
          "access_token": "foo",
          "refresh_token": "bar",
          "token_type": "Bearer"
        }
        """
        response_body = auth_token_resp.json()
        refresh_token = None
        if "access_token" not in response_body:
            raise ValueError('Expected "access_token" in response from oauth server')
        if "refresh_token" in response_body:
            refresh_token = response_body["refresh_token"]
        if "expires_in" in response_body:
            expires_in = response_body["expires_in"]
        access_token = response_body["access_token"]

        return Credentials(access_token, refresh_token, self._endpoint, expires_in=expires_in)


    def refresh_credentials(self):
        """Attempt to use Keyring-cached access token before refreshing"""
        if self._creds:
            try:
                self._creds = self.refresh_access_token(self._creds)
                if self._creds:
                    KeyringStore.store(self._creds)
                return
            except AccessTokenNotFoundError:
                logging.warning("Failed to refresh token. Kicking off a full authorization flow.")
                KeyringStore.delete(self._endpoint)

        self._creds = self.get_creds_from_remote()
        KeyringStore.store(self._creds)


    def refresh_access_token(self, credentials: Credentials) -> Credentials:
        """This function will use the refresh token to retrieve an access token

        Args:
            credentials (Credentials): Credentials object made via KeyringStore

        Raises:
            ValueError: Wrong value for one of the tokens
            AccessTokenNotFoundError: No access token in Credentials object

        Returns:
            Credentials: Returns creds object with valid tokens
        """
        if credentials.refresh_token is None:
            raise ValueError("no refresh token available with which to refresh authorization credentials")

        resp = _requests.post(
            url=self._token_endpoint,
            data={
                "grant_type": "refresh_token",
                "client_id": self._client_id,
                "refresh_token": credentials.refresh_token,
            },
            headers=self._headers,
            allow_redirects=False,
            verify=self._verify,
        )
        if resp.status_code != _StatusCodes.OK:
            # In the absence of a successful response, assume the refresh token is expired. This should indicate
            # to the caller that the AuthorizationClient is defunct and a new one needs to be re-initialized.
            raise AccessTokenNotFoundError(f"Non-200 returned from refresh token endpoint {resp.status_code}")

        return self._credentials_from_response(resp)


    def get_creds_from_remote(self) -> Credentials:
        """Trigger the DeviceCode Authorization Flow

        Retrieves the get_device_code method from the token_client module and then uses that
        response to poll the token endpoint. The end-user will have to navigate to a URL
        printed in the terminal and authenticate with the code provided by the CLI.

        Successful auth flows will cache the token response.
        """
        resp = token_client.get_device_code(
            self._device_auth_endpoint, self._client_id, self._audience, self._scopes, self._http_proxy_url, self._verify
        )
        text = f"To Authenticate, navigate in a browser to the following URL: {click.style(resp.verification_uri, fg='blue', underline=True)} and enter code: {click.style(resp.user_code, fg='blue')}"
        click.secho(text)
        try:
            access_token, refresh_token, expires_in = token_client.poll_token_endpoint(
                resp,
                self._token_endpoint,
                client_id=self._client_id,
                audience=self._audience,
                scopes=self._scopes,
                http_proxy_url=self._http_proxy_url,
                verify=self._verify,
            )

            return Credentials(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_in=expires_in,
                for_endpoint=self._endpoint
            )
        except Exception:
            KeyringStore.delete(self._endpoint)
            raise

