import urllib.parse as _urlparse

from flytekit.clis.auth.auth import AuthorizationClient as _AuthorizationClient
from flytekit.clis.auth.discovery import DiscoveryClient as _DiscoveryClient
from flytekit.configuration.creds import CLIENT_CREDENTIALS_SECRET as _CLIENT_SECRET
from flytekit.configuration.creds import CLIENT_ID as _CLIENT_ID
from flytekit.configuration.creds import OAUTH_SCOPES as _SCOPES
from flytekit.configuration.creds import REDIRECT_URI as _REDIRECT_URI
from flytekit.configuration.platform import HTTP_URL as _HTTP_URL
from flytekit.configuration.platform import INSECURE as _INSECURE
from flytekit.configuration.platform import URL as _URL
from flytekit.loggers import auth_logger

# Default, well known-URI string used for fetching JSON metadata. See https://tools.ietf.org/html/rfc8414#section-3.
discovery_endpoint_path = "./.well-known/oauth-authorization-server"


def _get_discovery_endpoint(http_config_val, platform_url_val, insecure_val):
    if http_config_val:
        scheme, netloc, path, _, _, _ = _urlparse.urlparse(http_config_val)
        if not scheme:
            scheme = "http" if insecure_val else "https"
    else:  # Use the main _URL config object effectively
        scheme = "http" if insecure_val else "https"
        netloc = platform_url_val
        path = ""

    computed_endpoint = _urlparse.urlunparse((scheme, netloc, path, None, None, None))
    # The urljoin function needs a trailing slash in order to append things correctly. Also, having an extra slash
    # at the end is okay, it just gets stripped out.
    computed_endpoint = _urlparse.urljoin(computed_endpoint + "/", discovery_endpoint_path)
    auth_logger.debug(f"Using {computed_endpoint} as discovery endpoint")
    return computed_endpoint


# Lazy initialized authorization client singleton
_authorization_client = None


def get_client(flyte_client_url):
    global _authorization_client
    if _authorization_client is not None and not _authorization_client.expired:
        return _authorization_client
    authorization_endpoints = get_authorization_endpoints(flyte_client_url)

    _authorization_client = _AuthorizationClient(
        redirect_uri=_REDIRECT_URI.get(),
        client_id=_CLIENT_ID.get(),
        scopes=_SCOPES.get(),
        auth_endpoint=authorization_endpoints.auth_endpoint,
        token_endpoint=authorization_endpoints.token_endpoint,
        client_secret=_CLIENT_SECRET.get(),
    )

    auth_logger.debug(f"Created oauth client with redirect {_authorization_client}")

    if not _authorization_client.has_valid_credentials:
        _authorization_client.start_authorization_flow()

    return _authorization_client


def get_authorization_endpoints(flyte_client_url):
    discovery_endpoint = _get_discovery_endpoint(_HTTP_URL.get(), flyte_client_url or _URL.get(), _INSECURE.get())
    discovery_client = _DiscoveryClient(discovery_url=discovery_endpoint)
    return discovery_client.get_authorization_endpoints()
