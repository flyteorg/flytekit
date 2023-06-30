import logging
import ssl

import grpc
from flyteidl.service.auth_pb2 import OAuth2MetadataRequest, PublicClientAuthConfigRequest
from flyteidl.service.auth_pb2_grpc import AuthMetadataServiceStub
from OpenSSL import crypto

from flytekit.clients.auth.authenticator import (
    Authenticator,
    ClientConfig,
    ClientConfigStore,
    ClientCredentialsAuthenticator,
    CommandAuthenticator,
    DeviceCodeAuthenticator,
    PKCEAuthenticator,
)
from flytekit.clients.grpc_utils.auth_interceptor import AuthUnaryInterceptor
from flytekit.clients.grpc_utils.wrap_exception_interceptor import RetryExceptionWrapperInterceptor
from flytekit.configuration import AuthType, PlatformConfig


class RemoteClientConfigStore(ClientConfigStore):
    """
    This class implements the ClientConfigStore that is served by the Flyte Server, that implements AuthMetadataService
    """

    def __init__(self, secure_channel: grpc.Channel):
        self._secure_channel = secure_channel

    def get_client_config(self) -> ClientConfig:
        """
        Retrieves the ClientConfig from the given grpc.Channel assuming  AuthMetadataService is available
        """
        metadata_service = AuthMetadataServiceStub(self._secure_channel)
        public_client_config = metadata_service.GetPublicClientConfig(PublicClientAuthConfigRequest())
        oauth2_metadata = metadata_service.GetOAuth2Metadata(OAuth2MetadataRequest())
        return ClientConfig(
            token_endpoint=oauth2_metadata.token_endpoint,
            authorization_endpoint=oauth2_metadata.authorization_endpoint,
            redirect_uri=public_client_config.redirect_uri,
            client_id=public_client_config.client_id,
            scopes=public_client_config.scopes,
            header_key=public_client_config.authorization_metadata_key or None,
            device_authorization_endpoint=oauth2_metadata.device_authorization_endpoint,
            audience=public_client_config.audience,
        )


def get_authenticator(cfg: PlatformConfig, cfg_store: ClientConfigStore) -> Authenticator:
    """
    Returns a new authenticator based on the platform config.
    """
    cfg_auth = cfg.auth_mode
    if type(cfg_auth) is str:
        try:
            cfg_auth = AuthType[cfg_auth.upper()]
        except KeyError:
            logging.warning(f"Authentication type {cfg_auth} does not exist, defaulting to standard")
            cfg_auth = AuthType.STANDARD

    verify = None
    if cfg.insecure_skip_verify:
        verify = False
    elif cfg.ca_cert_file_path:
        verify = cfg.ca_cert_file_path

    if cfg_auth == AuthType.STANDARD or cfg_auth == AuthType.PKCE:
        return PKCEAuthenticator(cfg.endpoint, cfg_store, verify=verify)
    elif cfg_auth == AuthType.BASIC or cfg_auth == AuthType.CLIENT_CREDENTIALS or cfg_auth == AuthType.CLIENTSECRET:
        return ClientCredentialsAuthenticator(
            endpoint=cfg.endpoint,
            client_id=cfg.client_id,
            client_secret=cfg.client_credentials_secret,
            cfg_store=cfg_store,
            scopes=cfg.scopes,
            audience=cfg.audience,
            http_proxy_url=cfg.http_proxy_url,
            verify=verify,
        )
    elif cfg_auth == AuthType.EXTERNAL_PROCESS or cfg_auth == AuthType.EXTERNALCOMMAND:
        client_cfg = None
        if cfg_store:
            client_cfg = cfg_store.get_client_config()
        return CommandAuthenticator(
            command=cfg.command,
            header_key=client_cfg.header_key if client_cfg else None,
        )
    elif cfg_auth == AuthType.DEVICEFLOW:
        return DeviceCodeAuthenticator(
            endpoint=cfg.endpoint,
            cfg_store=cfg_store,
            audience=cfg.audience,
            http_proxy_url=cfg.http_proxy_url,
            verify=verify,
        )
    else:
        raise ValueError(
            f"Invalid auth mode [{cfg_auth}] specified." f"Please update the creds config to use a valid value"
        )


def upgrade_channel_to_authenticated(cfg: PlatformConfig, in_channel: grpc.Channel) -> grpc.Channel:
    """
    Given a grpc.Channel, preferrably a secure channel, it returns a composed channel that uses Interceptor to
    perform an Oauth2.0 Auth flow
    :param cfg: PlatformConfig
    :param in_channel: grpc.Channel Precreated channel
    :return: grpc.Channel. New composite channel
    """
    authenticator = get_authenticator(cfg, RemoteClientConfigStore(in_channel))
    return grpc.intercept_channel(in_channel, AuthUnaryInterceptor(authenticator))


def get_authenticated_channel(cfg: PlatformConfig) -> grpc.Channel:
    """
    Returns a new channel for the given config that is authenticated
    """
    channel = (
        grpc.insecure_channel(cfg.endpoint)
        if cfg.insecure
        else grpc.secure_channel(cfg.endpoint, grpc.ssl_channel_credentials())
    )  # noqa
    return upgrade_channel_to_authenticated(cfg, channel)


def load_cert(cert_file: str) -> crypto.X509:
    """
    Given a cert-file loads the PEM certificate and returns
    """
    st_cert = open(cert_file, "rt").read()
    return crypto.load_certificate(crypto.FILETYPE_PEM, st_cert)


def bootstrap_creds_from_server(endpoint: str) -> grpc.ChannelCredentials:
    """
    Retrieves the SSL cert from the remote and uses that. should be used only if insecure-skip-verify
    """
    # Get port from endpoint or use 443
    endpoint_parts = endpoint.rsplit(":", 1)
    if len(endpoint_parts) == 2 and endpoint_parts[1].isdigit():
        server_address = (endpoint_parts[0], endpoint_parts[1])
    else:
        server_address = (endpoint, "443")
    cert = ssl.get_server_certificate(server_address)  # noqa
    return grpc.ssl_channel_credentials(str.encode(cert))


def get_channel(cfg: PlatformConfig, **kwargs) -> grpc.Channel:
    """
    Creates a new grpc.Channel given a platformConfig.
    It is possible to pass additional options to the underlying channel. Examples for various options are as below

    .. code-block:: python

        get_channel(cfg=PlatformConfig(...))

    .. code-block:: python
       :caption: Additional options to insecure / secure channel. Example `options` and `compression` refer to grpc guide

        get_channel(cfg=PlatformConfig(...), options=..., compression=...)

    .. code-block:: python
       :caption: Create secure channel with custom `grpc.ssl_channel_credentials`

        get_channel(cfg=PlatformConfig(insecure=False,...), credentials=...)


    :param cfg: PlatformConfig
    :param kwargs: Optional arguments to be passed to channel method. Refer to usage example above
    :return: grpc.Channel (secure / insecure)
    """
    if cfg.insecure:
        return grpc.insecure_channel(cfg.endpoint, **kwargs)

    credentials = None
    if "credentials" not in kwargs:
        if cfg.insecure_skip_verify:
            credentials = bootstrap_creds_from_server(cfg.endpoint)
        elif cfg.ca_cert_file_path:
            credentials = grpc.ssl_channel_credentials(
                crypto.dump_certificate(crypto.FILETYPE_PEM, load_cert(cfg.ca_cert_file_path))
            )
        else:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=kwargs.get("root_certificates", None),
                private_key=kwargs.get("private_key", None),
                certificate_chain=kwargs.get("certificate_chain", None),
            )
    else:
        credentials = kwargs["credentials"]
    return grpc.secure_channel(
        target=cfg.endpoint,
        credentials=credentials,
        options=kwargs.get("options", None),
        compression=kwargs.get("compression", None),
    )


def wrap_exceptions_channel(cfg: PlatformConfig, in_channel: grpc.Channel) -> grpc.Channel:
    """
    Wraps the input channel with RetryExceptionWrapperInterceptor. This wrapper will cover all
    exceptions and raise Exception from the Family flytekit.exceptions

    .. note:: This channel should be usually the outermost channel. This channel will raise a FlyteException

    :param cfg: PlatformConfig
    :param in_channel: grpc.Channel
    :return: grpc.Channel
    """
    return grpc.intercept_channel(in_channel, RetryExceptionWrapperInterceptor(max_retries=cfg.rpc_retries))
