from flytekit.clis.auth.discovery import DiscoveryClient
from flytekit.clis.auth.auth import AuthorizationClient as _AuthorizationClient
from flytekit.configuration.creds import (
    DISCOVERY_ENDPOINT as _DISCOVERY_ENDPOINT,
    REDIRECT_URI as _REDIRECT_URI,
    CLIENT_ID as _CLIENT_ID
)
from flytekit.configuration.platform import URL as _URL


def test_discovery_client():
    discovery_endpoint = _DISCOVERY_ENDPOINT.get()
    discovery_client = DiscoveryClient(discovery_url=discovery_endpoint)
    authorization_endpoints = discovery_client.get_authorization_endpoints()
    print("///////////////////////////////////////|||||||||||||||||||||||||||||||||||||||||")
    print(authorization_endpoints.token_endpoint)
    token_endpoint = authorization_endpoints.token_endpoint
    scope = 'svc'


def outer_dec(fn=None, other=None):
    def wrapper(fn):
        def handler(*args, **kwargs):
            import ipdb; ipdb.set_trace()

            print('other is {}'.format(other))
            return fn(*args, **kwargs)
        return handler

    if fn:
        return wrapper(fn)
    else:
        return wrapper


@outer_dec(other=5)
def hi():
    import ipdb
    ipdb.set_trace()
    print('hi')


def test_asdfds():
    hi()
