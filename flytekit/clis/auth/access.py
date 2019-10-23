from auth import AuthorizationClient
from discovery import DiscoveryClient

from flytekit.configuration.platform import URL
from flytekit.configuration.creds import DISCOVERY_ENDPOINT, REDIRECT_URI, CLIENT_ID


try:  # Python 3
    from urllib.parse import urlparse, urljoin
except ImportError:  # Python 2
    from urlparse import urlparse, urljoin


def _is_absolute(url):
    return bool(urlparse(url).netloc)


def get_credentials():
    discovery_endpoint = DISCOVERY_ENDPOINT.get()
    if not _is_absolute(discovery_endpoint):
        discovery_endpoint = urljoin(URL.get(), discovery_endpoint)
    discovery_client = DiscoveryClient(discovery_url=discovery_endpoint)
    authorization_endpoints = discovery_client.get_authorization_endpoints()

    client = AuthorizationClient(redirect_uri=REDIRECT_URI.get(),
                                 client_id=CLIENT_ID.get(),
                                 auth_endpoint=authorization_endpoints.auth_endpoint,
                                 token_endpoint=authorization_endpoints.token_endpoint)
    return client.credentials()


if __name__ == '__main__':
    get_credentials()

