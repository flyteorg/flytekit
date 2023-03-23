import os.path
from unittest.mock import MagicMock, patch

import pytest
from flyteidl.service.auth_pb2 import OAuth2MetadataResponse, PublicClientAuthConfigResponse

from flytekit.clients.auth.authenticator import (
    ClientConfig,
    ClientConfigStore,
    ClientCredentialsAuthenticator,
    CommandAuthenticator,
    PKCEAuthenticator,
)
from flytekit.clients.auth.exceptions import AuthenticationError
from flytekit.clients.auth_helper import (
    RemoteClientConfigStore,
    get_authenticator,
    load_cert,
    upgrade_channel_to_authenticated,
    wrap_exceptions_channel,
)
from flytekit.clients.grpc_utils.auth_interceptor import AuthUnaryInterceptor
from flytekit.clients.grpc_utils.wrap_exception_interceptor import RetryExceptionWrapperInterceptor
from flytekit.configuration import AuthType, PlatformConfig

REDIRECT_URI = "http://localhost:53593/callback"

TOKEN_ENDPOINT = "https://your.domain.io/oauth2/token"

CLIENT_ID = "flytectl"

OAUTH_AUTHORIZE = "https://your.domain.io/oauth2/authorize"


def get_auth_service_mock() -> MagicMock:
    auth_stub_mock = MagicMock()
    auth_stub_mock.GetPublicClientConfig.return_value = PublicClientAuthConfigResponse(
        client_id=CLIENT_ID,
        redirect_uri=REDIRECT_URI,
        scopes=["offline", "all"],
        authorization_metadata_key="flyte-authorization",
    )
    auth_stub_mock.GetOAuth2Metadata.return_value = OAuth2MetadataResponse(
        issuer="https://your.domain.io",
        authorization_endpoint=OAUTH_AUTHORIZE,
        token_endpoint=TOKEN_ENDPOINT,
        response_types_supported=["code", "token", "code token"],
        scopes_supported=["all"],
        token_endpoint_auth_methods_supported=["client_secret_basic"],
        jwks_uri="https://your.domain.io/oauth2/jwks",
        code_challenge_methods_supported=["S256"],
        grant_types_supported=["client_credentials", "refresh_token", "authorization_code"],
    )
    return auth_stub_mock


@patch("flytekit.clients.auth_helper.AuthMetadataServiceStub")
def test_remote_client_config_store(mock_auth_service: MagicMock):
    ch = MagicMock()
    cs = RemoteClientConfigStore(ch)
    mock_auth_service.return_value = get_auth_service_mock()

    ccfg = cs.get_client_config()
    assert ccfg is not None
    assert ccfg.client_id == CLIENT_ID
    assert ccfg.authorization_endpoint == OAUTH_AUTHORIZE


def get_client_config() -> ClientConfigStore:
    cfg_store = MagicMock()
    cfg_store.get_client_config.return_value = ClientConfig(
        token_endpoint=TOKEN_ENDPOINT,
        authorization_endpoint=OAUTH_AUTHORIZE,
        redirect_uri=REDIRECT_URI,
        client_id=CLIENT_ID,
    )
    return cfg_store


def test_get_authenticator_basic():
    cfg = PlatformConfig(auth_mode=AuthType.BASIC)

    with pytest.raises(ValueError, match="Client ID and Client SECRET both are required"):
        get_authenticator(cfg, None)

    cfg = PlatformConfig(auth_mode=AuthType.BASIC, client_credentials_secret="xyz", client_id="id")
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, ClientCredentialsAuthenticator)

    cfg = PlatformConfig(auth_mode=AuthType.CLIENT_CREDENTIALS, client_credentials_secret="xyz", client_id="id")
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, ClientCredentialsAuthenticator)

    cfg = PlatformConfig(auth_mode=AuthType.CLIENTSECRET, client_credentials_secret="xyz", client_id="id")
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, ClientCredentialsAuthenticator)


def test_get_authenticator_pkce():
    cfg = PlatformConfig()
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, PKCEAuthenticator)

    cfg = PlatformConfig(insecure_skip_verify=True)
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, PKCEAuthenticator)
    assert authn._verify is False

    cfg = PlatformConfig(ca_cert_file_path="/file")
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, PKCEAuthenticator)
    assert authn._verify == "/file"


def test_get_authenticator_cmd():
    cfg = PlatformConfig(auth_mode=AuthType.EXTERNAL_PROCESS)
    with pytest.raises(AuthenticationError):
        get_authenticator(cfg, get_client_config())

    cfg = PlatformConfig(auth_mode=AuthType.EXTERNAL_PROCESS, command=["echo"])
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, CommandAuthenticator)

    cfg = PlatformConfig(auth_mode=AuthType.EXTERNALCOMMAND, command=["echo"])
    authn = get_authenticator(cfg, get_client_config())
    assert authn
    assert isinstance(authn, CommandAuthenticator)
    assert authn._cmd == ["echo"]


def test_wrap_exceptions_channel():
    ch = MagicMock()
    out_ch = wrap_exceptions_channel(PlatformConfig(), ch)
    assert isinstance(out_ch._interceptor, RetryExceptionWrapperInterceptor)  # noqa


def test_upgrade_channel_to_auth():
    ch = MagicMock()
    out_ch = upgrade_channel_to_authenticated(PlatformConfig(), ch)
    assert isinstance(out_ch._interceptor, AuthUnaryInterceptor)  # noqa


def test_load_cert():
    cert_file = os.path.join(os.path.dirname(__file__), "testdata", "rootCACert.pem")
    f = load_cert(cert_file)
    assert f
    print(f)
