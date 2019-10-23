import requests

try:  # Python 3.5+
    from http import HTTPStatus as StatusCodes
except ImportError:
    try:  # Python 3
        from http import client as StatusCodes
    except ImportError:  # Python 2
        import httplib as StatusCodes

# These response keys are defined in https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html.
authorization_endpoint_key = "authorization_endpoint"
token_endpoint_key = "token_endpoint"


class AuthorizationEndpoints(object):
    """
    A simple wrapper around commonly discovered endpoints used for the PKCE auth flow.
    """
    def __init__(self, auth_endpoint=None, token_endpoint=None):
        self._auth_endpoint = auth_endpoint
        self._token_endpoint = token_endpoint

    @property
    def auth_endpoint(self):
        return self._auth_endpoint

    @property
    def token_endpoint(self):
        return self._token_endpoint


class DiscoveryClient(object):
    """
    Discovers
    """

    def __init__(self, discovery_url=None):
        self._discovery_url = discovery_url
        self._authorization_endpoints = None

    def get_authorization_endpoints(self):
        if self._authorization_endpoints is not None:
            return self._authorization_endpoints
        resp = requests.get(
            url=self._discovery_url,
        )
        # Follow at most one redirect.
        if resp.status_code == StatusCodes.FOUND:
            redirect_location = resp.headers['Location']
            if redirect_location is None:
                raise ValueError('Received a 302 but no follow up location was provided in headers')
            resp = requests.get(
                url=redirect_location,
            )

        response_body = resp.json()
        if response_body[authorization_endpoint_key] is None:
            raise ValueError('Unable to discover authorization endpoint')

        if response_body[token_endpoint_key] is None:
            raise ValueError('Unable to discover token endpoint')

        self._authorization_endpoints = AuthorizationEndpoints(auth_endpoint=response_body[authorization_endpoint_key],
                                                               token_endpoint=response_body[token_endpoint_key])
        return self._authorization_endpoints
