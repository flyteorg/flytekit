from subprocess import CompletedProcess

import mock
from flyteidl.admin import project_pb2 as _project_pb2
from flyteidl.service import auth_pb2

from flytekit.clients.raw import RawSynchronousFlyteClient as _RawSynchronousFlyteClient
from flytekit.clients.raw import _refresh_credentials_from_command


def get_admin_stub_mock() -> mock.MagicMock:
    auth_stub_mock = mock.MagicMock()
    auth_stub_mock.GetPublicClientConfig.return_value = auth_pb2.PublicClientAuthConfigResponse(
        client_id="flytectl",
        redirect_uri="http://localhost:53593/callback",
        scopes=["offline", "all"],
        authorization_metadata_key="flyte-authorization",
    )
    auth_stub_mock.GetOAuth2Metadata.return_value = auth_pb2.OAuth2MetadataResponse(
        issuer="https://your.domain.io",
        authorization_endpoint="https://your.domain.io/oauth2/authorize",
        token_endpoint="https://your.domain.io/oauth2/token",
        response_types_supported=["code", "token", "code token"],
        scopes_supported=["all"],
        token_endpoint_auth_methods_supported=["client_secret_basic"],
        jwks_uri="https://your.domain.io/oauth2/jwks",
        code_challenge_methods_supported=["S256"],
        grant_types_supported=["client_credentials", "refresh_token", "authorization_code"],
    )
    return auth_stub_mock


@mock.patch("flytekit.clients.raw.RawSynchronousFlyteClient.force_auth_flow")
@mock.patch("flytekit.clients.raw.auth_service")
@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw._insecure_channel")
@mock.patch("flytekit.clients.raw._secure_channel")
def test_client_set_token(mock_secure_channel, mock_channel, mock_admin, mock_admin_auth, mock_force):
    mock_force.return_value = True
    mock_secure_channel.return_value = True
    mock_channel.return_value = True
    mock_admin.AdminServiceStub.return_value = True
    mock_admin_auth.AuthMetadataServiceStub.return_value = get_admin_stub_mock()
    client = _RawSynchronousFlyteClient(url="a.b.com", insecure=True)
    client.set_access_token("abc")
    assert client._metadata[0][1] == "Bearer abc"


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
    mock_admin.AdminServiceStub().UpdateProject.assert_called_with(project, metadata=None)


@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw._insecure_channel")
def test_list_projects_paginated(mock_channel, mock_admin):
    client = _RawSynchronousFlyteClient(url="a.b.com", insecure=True)
    project_list_request = _project_pb2.ProjectListRequest(limit=100, token="", filters=None, sort_by=None)
    client.list_projects(project_list_request)
    mock_admin.AdminServiceStub().ListProjects.assert_called_with(project_list_request, metadata=None)
