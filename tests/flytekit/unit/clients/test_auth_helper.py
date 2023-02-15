from unittest.mock import MagicMock, patch

from flytekit.clients.auth_helper import RemoteClientConfigStore


@patch("flytekit.clients.auth_helper.AuthMetadataServiceStub")
def test_remote_client_config_store(mock_auth_service: MagicMock):
    service_instance = MagicMock()
    ch = MagicMock()
    cs = RemoteClientConfigStore(ch)
    mock_auth_service.return_value = service_instance

    ccfg = cs.get_client_config()
    assert ccfg is not None
    service_instance.GetPublicClientConfig.assert_called()
    service_instance.GetOAuth2Metadata.assert_called()
