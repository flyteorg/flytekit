import pytest
import responses

from flytekit.clis.auth import discovery as _discovery


@responses.activate
def test_get_authorization_endpoints():
    discovery_url = "http://flyte-admin.com/discovery"

    auth_endpoint = "http://flyte-admin.com/authorization"
    token_endpoint = "http://flyte-admin.com/token"
    responses.add(
        responses.GET, discovery_url, json={"authorization_endpoint": auth_endpoint, "token_endpoint": token_endpoint},
    )

    discovery_client = _discovery.DiscoveryClient(discovery_url=discovery_url)
    assert discovery_client.get_authorization_endpoints().auth_endpoint == auth_endpoint
    assert discovery_client.get_authorization_endpoints().token_endpoint == token_endpoint


@responses.activate
def test_get_authorization_endpoints_relative():
    discovery_url = "http://flyte-admin.com/discovery"

    auth_endpoint = "/authorization"
    token_endpoint = "/token"
    responses.add(
        responses.GET, discovery_url, json={"authorization_endpoint": auth_endpoint, "token_endpoint": token_endpoint},
    )

    discovery_client = _discovery.DiscoveryClient(discovery_url=discovery_url)
    assert discovery_client.get_authorization_endpoints().auth_endpoint == "http://flyte-admin.com/authorization"
    assert discovery_client.get_authorization_endpoints().token_endpoint == "http://flyte-admin.com/token"


@responses.activate
def test_get_authorization_endpoints_missing_authorization_endpoint():
    discovery_url = "http://flyte-admin.com/discovery"
    responses.add(
        responses.GET, discovery_url, json={"token_endpoint": "http://flyte-admin.com/token"},
    )

    discovery_client = _discovery.DiscoveryClient(discovery_url=discovery_url)
    with pytest.raises(Exception):
        discovery_client.get_authorization_endpoints()


@responses.activate
def test_get_authorization_endpoints_missing_token_endpoint():
    discovery_url = "http://flyte-admin.com/discovery"
    responses.add(
        responses.GET, discovery_url, json={"authorization_endpoint": "http://flyte-admin.com/authorization"},
    )

    discovery_client = _discovery.DiscoveryClient(discovery_url=discovery_url)
    with pytest.raises(Exception):
        discovery_client.get_authorization_endpoints()
