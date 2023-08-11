import requests
import responses

from flytekit.clients.auth_helper import get_session
from flytekit.configuration import PlatformConfig


@responses.activate
def test_get_proxy_authenticated_session():
    """Test that proxy auth headers are added to http requests if the proxy command is provided in the platform config."""
    expected_token = "foo-bar"
    platform_config = PlatformConfig(
        endpoint="http://my-flyte-deployment.com",
        proxy_command=["echo", expected_token],
    )

    responses.add(responses.GET, platform_config.endpoint)

    session = get_session(platform_config)
    request = requests.Request("GET", platform_config.endpoint)
    prepared_request = session.prepare_request(request)

    # Send the request to trigger the refresh of the credentials
    session.send(prepared_request)

    # Check that the proxy-authorization header was added to the request
    assert prepared_request.headers["proxy-authorization"] == f"Bearer {expected_token}"
