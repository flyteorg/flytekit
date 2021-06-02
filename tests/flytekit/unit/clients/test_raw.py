import json
import os
from subprocess import CompletedProcess

import mock
from flyteidl.admin import project_pb2 as _project_pb2

from flytekit.clients.raw import RawSynchronousFlyteClient as _RawSynchronousFlyteClient
from flytekit.clients.raw import _get_basic_flow_scopes, _refresh_credentials_basic, _refresh_credentials_from_command
from flytekit.clis.auth.discovery import AuthorizationEndpoints as _AuthorizationEndpoints
from flytekit.configuration import TemporaryConfiguration
from flytekit.configuration.creds import CLIENT_CREDENTIALS_SECRET as _CREDENTIALS_SECRET


@mock.patch("flytekit.clients.raw.RawSynchronousFlyteClient.force_auth_flow")
@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw._insecure_channel")
@mock.patch("flytekit.clients.raw._secure_channel")
def test_client_set_token(mock_secure_channel, mock_channel, mock_admin, mock_force):
    mock_force.return_value = True
    mock_secure_channel.return_value = True
    mock_channel.return_value = True
    mock_admin.AdminServiceStub.return_value = True
    client = _RawSynchronousFlyteClient(url="a.b.com", insecure=True)
    client.set_access_token("abc")
    assert client._metadata[0][1] == "Bearer abc"


@mock.patch("flytekit.clis.sdk_in_container.basic_auth._requests")
@mock.patch("flytekit.clients.raw._credentials_access")
def test_refresh_credentials_basic(mock_credentials_access, mock_requests):
    mock_credentials_access.get_authorization_endpoints.return_value = _AuthorizationEndpoints("auth", "token")
    response = mock.MagicMock()
    response.status_code = 200
    response.json.return_value = json.loads("""{"access_token": "abc", "expires_in": 60}""")
    mock_requests.post.return_value = response
    os.environ[_CREDENTIALS_SECRET.env_var] = "asdf12345"

    mock_client = mock.MagicMock()
    mock_client.url.return_value = "flyte.localhost"
    _refresh_credentials_basic(mock_client)
    mock_client.set_access_token.assert_called_with("abc")
    mock_credentials_access.get_authorization_endpoints.assert_called_with(mock_client.url)


@mock.patch("flytekit.configuration.creds.COMMAND.get")
@mock.patch("subprocess.run")
def test_refresh_credentials_from_command(mock_call_to_external_process, mock_command_from_config):
    command = ["command", "generating", "token"]
    token = "token"

    mock_command_from_config.return_value = command
    mock_call_to_external_process.return_value = CompletedProcess(command, 0, stdout=token)
    mock_client = mock.MagicMock()

    _refresh_credentials_from_command(mock_client)

    mock_call_to_external_process.assert_called_with(command, capture_output=True, text=True, check=True)
    mock_client.set_access_token.assert_called_with(token)


@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw._insecure_channel")
def test_update_project(mock_channel, mock_admin):
    client = _RawSynchronousFlyteClient(url="a.b.com", insecure=True)
    project = _project_pb2.Project(id="foo", name="name", description="description", state=_project_pb2.Project.ACTIVE)
    client.update_project(project)
    mock_admin.AdminServiceStub().UpdateProject.assert_called_with(project)


@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw._insecure_channel")
def test_list_projects_paginated(mock_channel, mock_admin):
    client = _RawSynchronousFlyteClient(url="a.b.com", insecure=True)
    project_list_request = _project_pb2.ProjectListRequest(limit=100, token="", filters=None, sort_by=None)
    client.list_projects(project_list_request)
    mock_admin.AdminServiceStub().ListProjects.assert_called_with(project_list_request, metadata=None)


def test_scope_deprecation():
    with TemporaryConfiguration(os.path.join(os.path.dirname(__file__), "auth_deprecation.config")):
        assert _get_basic_flow_scopes() == ["custom_basic"]

    with TemporaryConfiguration(os.path.join(os.path.dirname(__file__), "auth_deprecation2.config")):
        assert _get_basic_flow_scopes() == ["custom_basic", "other_scope", "profile"]

    with TemporaryConfiguration(os.path.join(os.path.dirname(__file__), "auth_deprecation3.config")):
        assert _get_basic_flow_scopes() == ["custom_basic"]
