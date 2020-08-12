from __future__ import absolute_import

import logging as _logging
import urllib.parse as _urlparse

from flytekit.clis.auth.auth import AuthorizationClient as _AuthorizationClient
from flytekit.clis.auth.discovery import DiscoveryClient as _DiscoveryClient
from flytekit.configuration.creds import CLIENT_ID as _CLIENT_ID
from flytekit.configuration.creds import REDIRECT_URI as _REDIRECT_URI
from flytekit.configuration.platform import HTTP_URL as _HTTP_URL
from flytekit.configuration.platform import INSECURE as _INSECURE
from flytekit.configuration.platform import URL as _URL

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
    _logging.info("Using {} as discovery endpoint".format(computed_endpoint))
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
        auth_endpoint=authorization_endpoints.auth_endpoint,
        token_endpoint=authorization_endpoints.token_endpoint,
    )
    return _authorization_client


def get_authorization_endpoints(flyte_client_url):
    discovery_endpoint = _get_discovery_endpoint(_HTTP_URL.get(), flyte_client_url or _URL.get(), _INSECURE.get())
    discovery_client = _DiscoveryClient(discovery_url=discovery_endpoint)
    return discovery_client.get_authorization_endpoints()
