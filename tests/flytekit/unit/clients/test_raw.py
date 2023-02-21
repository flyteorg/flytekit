import json
from subprocess import CompletedProcess

import grpc
import mock
import pytest
from flyteidl.admin import project_pb2 as _project_pb2
from flyteidl.service import auth_pb2
from mock import MagicMock, patch

from flytekit.clients.raw import (
    RawSynchronousFlyteClient,
    _handle_invalid_create_request,
    get_basic_authorization_header,
    get_token,
)
from flytekit.configuration import AuthType, PlatformConfig
from flytekit.configuration.internal import Credentials


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


@mock.patch("flytekit.clients.raw.signal_service")
@mock.patch("flytekit.clients.raw.dataproxy_service")
@mock.patch("flytekit.clients.raw.auth_service")
@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw.grpc.insecure_channel")
@mock.patch("flytekit.clients.raw.grpc.secure_channel")
def test_client_set_token(mock_secure_channel, mock_channel, mock_admin, mock_admin_auth, mock_dataproxy, mock_signal):
    mock_secure_channel.return_value = True
    mock_channel.return_value = True
    mock_admin.AdminServiceStub.return_value = True
    mock_admin_auth.AuthMetadataServiceStub.return_value = get_admin_stub_mock()
    client = RawSynchronousFlyteClient(PlatformConfig(endpoint="a.b.com", insecure=True))
    client.set_access_token("abc")
    assert client._metadata[0][1] == "Bearer abc"
    assert client.check_access_token("abc")


@mock.patch("flytekit.clients.raw.RawSynchronousFlyteClient.set_access_token")
@mock.patch("flytekit.clients.raw.auth_service")
@mock.patch("subprocess.run")
def test_refresh_credentials_from_command(mock_call_to_external_process, mock_admin_auth, mock_set_access_token):
    token = "token"
    command = ["command", "generating", "token"]

    mock_admin_auth.AuthMetadataServiceStub.return_value = get_admin_stub_mock()
    client = RawSynchronousFlyteClient(PlatformConfig(command=command))

    mock_call_to_external_process.return_value = CompletedProcess(command, 0, stdout=token)
    client._refresh_credentials_from_command()

    mock_call_to_external_process.assert_called_with(command, capture_output=True, text=True, check=True)
    mock_set_access_token.assert_called_with(token, client.public_client_config.authorization_metadata_key)


@mock.patch("flytekit.clients.raw.signal_service")
@mock.patch("flytekit.clients.raw.dataproxy_service")
@mock.patch("flytekit.clients.raw.get_basic_authorization_header")
@mock.patch("flytekit.clients.raw.get_token")
@mock.patch("flytekit.clients.raw.auth_service")
@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw.grpc.insecure_channel")
@mock.patch("flytekit.clients.raw.grpc.secure_channel")
def test_refresh_client_credentials_aka_basic(
    mock_secure_channel,
    mock_channel,
    mock_admin,
    mock_admin_auth,
    mock_get_token,
    mock_get_basic_header,
    mock_dataproxy,
    mock_signal,
):
    mock_secure_channel.return_value = True
    mock_channel.return_value = True
    mock_admin.AdminServiceStub.return_value = True
    mock_get_basic_header.return_value = "Basic 123"
    mock_get_token.return_value = ("token1", 1234567)

    mock_admin_auth.AuthMetadataServiceStub.return_value = get_admin_stub_mock()
    client = RawSynchronousFlyteClient(
        PlatformConfig(
            endpoint="a.b.com", insecure=True, client_credentials_secret="sosecret", scopes=["a", "b", "c", "d"]
        )
    )
    client._metadata = None
    assert not client.check_access_token("fdsa")
    client._refresh_credentials_basic()

    # Scopes from configuration take precendence.
    mock_get_token.assert_called_once_with("https://your.domain.io/oauth2/token", "Basic 123", "a,b,c,d")

    client.set_access_token("token")
    assert client._metadata[0][0] == "authorization"


@mock.patch("flytekit.clients.raw.signal_service")
@mock.patch("flytekit.clients.raw.dataproxy_service")
@mock.patch("flytekit.clients.raw.auth_service")
@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw.grpc.insecure_channel")
@mock.patch("flytekit.clients.raw.grpc.secure_channel")
def test_raises(mock_secure_channel, mock_channel, mock_admin, mock_admin_auth, mock_dataproxy, mock_signal):
    mock_secure_channel.return_value = True
    mock_channel.return_value = True
    mock_admin.AdminServiceStub.return_value = True

    # If the public client config is missing then raise an error
    mocked_auth = get_admin_stub_mock()
    mocked_auth.GetPublicClientConfig.return_value = None
    mock_admin_auth.AuthMetadataServiceStub.return_value = mocked_auth
    client = RawSynchronousFlyteClient(PlatformConfig(endpoint="a.b.com", insecure=True))
    assert client.public_client_config is None
    with pytest.raises(ValueError):
        client._refresh_credentials_basic()

    # If the oauth2 metadata is missing then raise an error
    mocked_auth = get_admin_stub_mock()
    mocked_auth.GetOAuth2Metadata.return_value = None
    mock_admin_auth.AuthMetadataServiceStub.return_value = mocked_auth
    client = RawSynchronousFlyteClient(PlatformConfig(endpoint="a.b.com", insecure=True))
    assert client.oauth2_metadata is None
    with pytest.raises(ValueError):
        client._refresh_credentials_basic()


@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw.grpc.insecure_channel")
def test_update_project(mock_channel, mock_admin):
    client = RawSynchronousFlyteClient(PlatformConfig(endpoint="a.b.com", insecure=True))
    project = _project_pb2.Project(id="foo", name="name", description="description", state=_project_pb2.Project.ACTIVE)
    client.update_project(project)
    mock_admin.AdminServiceStub().UpdateProject.assert_called_with(project, metadata=None)


@mock.patch("flytekit.clients.raw._admin_service")
@mock.patch("flytekit.clients.raw.grpc.insecure_channel")
def test_list_projects_paginated(mock_channel, mock_admin):
    client = RawSynchronousFlyteClient(PlatformConfig(endpoint="a.b.com", insecure=True))
    project_list_request = _project_pb2.ProjectListRequest(limit=100, token="", filters=None, sort_by=None)
    client.list_projects(project_list_request)
    mock_admin.AdminServiceStub().ListProjects.assert_called_with(project_list_request, metadata=None)


def test_get_basic_authorization_header():
    header = get_basic_authorization_header("client_id", "abc")
    assert header == "Basic Y2xpZW50X2lkOmFiYw=="


@patch("flytekit.clients.raw._requests")
def test_get_token(mock_requests):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = json.loads("""{"access_token": "abc", "expires_in": 60}""")
    mock_requests.post.return_value = response
    access, expiration = get_token("https://corp.idp.net", "abc123", "my_scope")
    assert access == "abc"
    assert expiration == 60


@patch.object(RawSynchronousFlyteClient, "_refresh_credentials_standard")
def test_refresh_standard(mocked_method):
    cc = RawSynchronousFlyteClient(PlatformConfig())
    cc.refresh_credentials()
    assert mocked_method.called


@patch.object(RawSynchronousFlyteClient, "_refresh_credentials_basic")
def test_refresh_basic(mocked_method):
    cc = RawSynchronousFlyteClient(PlatformConfig(auth_mode=AuthType.BASIC))
    cc.refresh_credentials()
    assert mocked_method.called

    cc = RawSynchronousFlyteClient(PlatformConfig(auth_mode=AuthType.CLIENT_CREDENTIALS))
    cc.refresh_credentials()
    assert mocked_method.call_count == 2


@patch.object(RawSynchronousFlyteClient, "_refresh_credentials_basic")
def test_basic_strings(mocked_method):
    cc = RawSynchronousFlyteClient(PlatformConfig(auth_mode="basic"))
    cc.refresh_credentials()
    assert mocked_method.called

    cc = RawSynchronousFlyteClient(PlatformConfig(auth_mode="client_credentials"))
    cc.refresh_credentials()
    assert mocked_method.call_count == 2


@patch.object(RawSynchronousFlyteClient, "_refresh_credentials_from_command")
def test_refresh_command(mocked_method):
    cc = RawSynchronousFlyteClient(PlatformConfig(auth_mode=AuthType.EXTERNALCOMMAND))
    cc.refresh_credentials()
    assert mocked_method.called


@patch.object(RawSynchronousFlyteClient, "_refresh_credentials_from_command")
def test_refresh_from_environment_variable(mocked_method, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(Credentials.AUTH_MODE.legacy.get_env_name(), AuthType.EXTERNAL_PROCESS.name, prepend=False)
    cc = RawSynchronousFlyteClient(PlatformConfig(auth_mode=None).auto(None))
    cc.refresh_credentials()
    assert mocked_method.called


def test__handle_invalid_create_request_decorator_happy():
    client = RawSynchronousFlyteClient(PlatformConfig(auth_mode=AuthType.CLIENT_CREDENTIALS))
    mocked_method = client._stub.CreateWorkflow = mock.Mock()
    _handle_invalid_create_request(client.create_workflow("/flyteidl.service.AdminService/CreateWorkflow"))
    mocked_method.assert_called_once()


@patch("flytekit.clients.raw.cli_logger")
@patch("flytekit.clients.raw._MessageToJson")
def test__handle_invalid_create_request_decorator_raises(mock_to_JSON, mock_logger):
    mock_to_JSON(return_value="test")
    err = grpc.RpcError()
    err.details = "There is already a workflow with different structure."
    err.code = lambda: grpc.StatusCode.INVALID_ARGUMENT
    client = RawSynchronousFlyteClient(PlatformConfig(auth_mode=AuthType.CLIENT_CREDENTIALS))
    client._stub.CreateWorkflow = mock.Mock(side_effect=err)
    with pytest.raises(grpc.RpcError):
        _handle_invalid_create_request(client.create_workflow("/flyteidl.service.AdminService/CreateWorkflow"))
    mock_logger.error.assert_called_with("There is already a workflow with different structure.")
