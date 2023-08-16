from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner
from flytekitplugins.identity_aware_proxy.cli import cli, get_gcp_secret_manager_secret
from google.api_core.exceptions import NotFound


def test_help() -> None:
    """Smoke test external command IAP ID token generator cli by printing help message."""
    runner = CliRunner()
    result = runner.invoke(cli, "--help")
    assert "Generate ID tokens" in result.output
    assert result.exit_code == 0

    result = runner.invoke(cli, ["generate-user-id-token", "--help"])
    assert "Generate a user account ID token" in result.output
    assert result.exit_code == 0


def test_get_gcp_secret_manager_secret():
    """Test retrieval of GCP secret manager secret."""
    project_id = "test_project"
    secret_id = "test_secret"
    version = "latest"
    expected_payload = "test_payload"

    mock_client = MagicMock()
    mock_client.access_secret_version.return_value.payload.data.decode.return_value = expected_payload
    with patch("google.cloud.secretmanager.SecretManagerServiceClient", return_value=mock_client):
        payload = get_gcp_secret_manager_secret(project_id, secret_id, version)
        assert payload == expected_payload

        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
        mock_client.access_secret_version.assert_called_once_with(name=name)


def test_get_gcp_secret_manager_secret_not_found():
    """Test retrieving non-existing secret from GCP secret manager."""
    project_id = "test_project"
    secret_id = "test_secret"
    version = "latest"

    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = NotFound("Secret not found")
    with patch("google.cloud.secretmanager.SecretManagerServiceClient", return_value=mock_client):
        with pytest.raises(click.BadParameter):
            get_gcp_secret_manager_secret(project_id, secret_id, version)
