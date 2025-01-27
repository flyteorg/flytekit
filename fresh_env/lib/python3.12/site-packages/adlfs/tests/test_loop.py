from concurrent.futures import ThreadPoolExecutor

import pytest
from azure.identity.aio import DefaultAzureCredential

from adlfs import AzureBlobFileSystem

URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA


def test_fs_loop(storage):
    """
    This is a test to verify that AzureBlobFilesystem can provide a
    running event loop to azure python sdk when requesting asynchronous
    credentials And running in a separate thread
    """

    def test_connect_async_credential():
        fs = AzureBlobFileSystem(  # NOQA
            account_name=storage.account_name, credential=DefaultAzureCredential()
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(test_connect_async_credential)
        assert future.result() is None


def test_file_loop(storage):
    """
    This is a test to verify that AzureBlobFile class provides a
    running event loop to the Azure python sdk when requesting asynchronous
    credentials and running in a separate thread
    """

    def test_connect_async_open_credential():
        fs = AzureBlobFileSystem(
            account_name=storage.account_name, credential=DefaultAzureCredential()
        )
        fs.open(path="")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(test_connect_async_open_credential)
        with pytest.raises(ValueError):
            future.result()
