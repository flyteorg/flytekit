from __future__ import absolute_import

from flytekit.clis.auth.auth import AuthorizationClient as _AuthorizationClient
from flytekit.clis.auth.discovery import DiscoveryClient as _DiscoveryClient

from flytekit.configuration.creds import (
    DISCOVERY_ENDPOINT as _DISCOVERY_ENDPOINT,
    REDIRECT_URI as _REDIRECT_URI,
    CLIENT_ID as _CLIENT_ID
)
from flytekit.configuration.platform import URL as _URL

try:  # Python 3
    import urllib.parse as _urlparse
except ImportError:  # Python 2
    import urlparse as _urlparse


def _is_absolute(url):
    return bool(_urlparse.urlparse(url).netloc)


def get_credentials():
    discovery_endpoint = _DISCOVERY_ENDPOINT.get()
    if not _is_absolute(discovery_endpoint):
        discovery_endpoint = _urlparse.urljoin(_URL.get(), discovery_endpoint)
    discovery_client = _DiscoveryClient(discovery_url=discovery_endpoint)
    authorization_endpoints = discovery_client.get_authorization_endpoints()

    client = _AuthorizationClient(redirect_uri=_REDIRECT_URI.get(),
                                  client_id=_CLIENT_ID.get(),
                                  auth_endpoint=authorization_endpoints.auth_endpoint,
                                  token_endpoint=authorization_endpoints.token_endpoint)
    return client.credentials
