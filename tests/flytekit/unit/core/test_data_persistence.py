import os

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from mock import patch
from flytekit.core.data_persistence import FileAccessProvider


def test_get_random_remote_path():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    path = fp.get_random_remote_path()
    assert path.startswith("s3://my-bucket")
    assert fp.raw_output_prefix == "s3://my-bucket/"


def test_is_remote():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    assert fp.is_remote("./checkpoint") is False
    assert fp.is_remote("/tmp/foo/bar") is False
    assert fp.is_remote("file://foo/bar") is False
    assert fp.is_remote("s3://my-bucket/foo/bar") is True


def test_initialise_azure_file_provider_with_account_key():
    with patch.dict(os.environ, {"FLYTE_AZURE_ACCOUNT_NAME": "accountname", "FLYTE_AZURE_ACCOUNT_KEY": "accountkey"}, clear=True):
        fp = FileAccessProvider("/tmp", "abfs://container/path/within/container")
        assert fp.get_filesystem().account_name == "accountname"
        assert fp.get_filesystem().account_key == "accountkey"
        assert fp.get_filesystem().sync_credential is None

def test_initialise_azure_file_provider_with_service_principal():
    with patch.dict(os.environ, {"FLYTE_AZURE_ACCOUNT_NAME": "accountname", "FLYTE_AZURE_CLIENT_SECRET": "clientsecret", "FLYTE_AZURE_CLIENT_ID": "clientid", 
                                 "FLYTE_AZURE_TENANT_ID": "tenantid"
                                 }, clear=True):
        fp = FileAccessProvider("/tmp", "abfs://container/path/within/container")
        assert fp.get_filesystem().account_name == "accountname"
        assert isinstance(fp.get_filesystem().sync_credential, ClientSecretCredential)
        assert fp.get_filesystem().client_secret == "clientsecret"
        assert fp.get_filesystem().client_id == "clientid"
        assert fp.get_filesystem().tenant_id == "tenantid"

def test_initialise_azure_file_provider_with_default_credential():
    with patch.dict(os.environ, {"FLYTE_AZURE_ACCOUNT_NAME": "accountname"}, clear=True):
        fp = FileAccessProvider("/tmp", "abfs://container/path/within/container")
        assert fp.get_filesystem().account_name == "accountname"
        assert isinstance(fp.get_filesystem().sync_credential, DefaultAzureCredential)
