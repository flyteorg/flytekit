from flytekit.clis.auth.discovery import DiscoveryClient
from flytekit.clis.auth.auth import AuthorizationClient as _AuthorizationClient
from flytekit.configuration.creds import (
    DISCOVERY_ENDPOINT as _DISCOVERY_ENDPOINT,
    REDIRECT_URI as _REDIRECT_URI,
    CLIENT_ID as _CLIENT_ID
)
from flytekit.configuration.platform import URL as _URL

# This will be deleted
from flytekit.clis.flyte_cli.main import _welcome_message

from flytekit.clis.sdk_in_container import oauth_manager

_welcome_message()


def test_discovery_client():
    discovery_endpoint = _DISCOVERY_ENDPOINT.get()
    discovery_client = DiscoveryClient(discovery_url=discovery_endpoint)
    authorization_endpoints = discovery_client.get_authorization_endpoints()
    print("\n///////////////////////////////////////|||||||||||||||||||||||||||||||||||||||||")
    print(authorization_endpoints.token_endpoint)
    token_endpoint = authorization_endpoints.token_endpoint
    scope = 'svc'
    client_id = oauth_manager.get_file_contents('/Users/ytong/.ssh/flyte_jenkins_id')
    client_secret = oauth_manager.get_file_contents('/Users/ytong/.ssh/flyte_jenkins_secret')
    print(client_id, client_secret)
    authorization_header = oauth_manager.get_basic_authorization_header(client_id, client_secret)
    token, expires_in = oauth_manager.get_token(token_endpoint, authorization_header, scope)
    print(token, expires_in)

def test_a():
    class A(object):
        def xx(self):
            print('hi')

    a = A()
    a.xx()

if __name__ == '__main__':
    test_discovery_client()

