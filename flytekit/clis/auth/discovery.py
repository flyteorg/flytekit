import logging

import requests as _requests

# These response keys are defined in https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html.
_authorization_endpoint_key = "authorization_endpoint"
_token_endpoint_key = "token_endpoint"


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
    Discovers well known OpenID configuration and parses out authorization endpoints required for initiating the PKCE
    auth flow.
    """

    def __init__(self, discovery_url=None):
        logging.debug("Initializing discovery client with {}".format(discovery_url))
        self._discovery_url = discovery_url
        self._authorization_endpoints = None

    @property
    def authorization_endpoints(self):
        """
        :rtype: flytekit.clis.auth.discovery.AuthorizationEndpoints:
        """
        return self._authorization_endpoints

    def get_authorization_endpoints(self):
        if self.authorization_endpoints is not None:
            return self.authorization_endpoints
        resp = _requests.get(
            url=self._discovery_url,
        )

        response_body = resp.json()

        authorization_endpoint = response_body[_authorization_endpoint_key]
        token_endpoint = response_body[_token_endpoint_key]

        if authorization_endpoint is None:
            raise ValueError("Unable to discover authorization endpoint")

        if token_endpoint is None:
            raise ValueError("Unable to discover token endpoint")

        if authorization_endpoint.startswith("/"):
            authorization_endpoint = _requests.compat.urljoin(self._discovery_url, authorization_endpoint)

        if token_endpoint.startswith("/"):
            token_endpoint = _requests.compat.urljoin(self._discovery_url, token_endpoint)

        self._authorization_endpoints = AuthorizationEndpoints(
            auth_endpoint=authorization_endpoint, token_endpoint=token_endpoint
        )
        return self.authorization_endpoints
