import asyncio
import datetime

import docker
import pytest
from azure.storage.blob import BlobServiceClient

URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA
data = b"0123456789"
metadata = {"meta": "data"}


def pytest_addoption(parser):
    parser.addoption(
        "--host",
        action="store",
        default="127.0.0.1:10000",
        help="Host running azurite.",
    )


@pytest.fixture(scope="session")
def host(request):
    print("host:", request.config.getoption("--host"))
    return request.config.getoption("--host")


@pytest.fixture(scope="function")
def storage(host):
    """
    Create blob using azurite.
    """

    conn_str = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA

    bbs = BlobServiceClient.from_connection_string(conn_str=conn_str)
    bbs.create_container("data")
    container_client = bbs.get_container_client(container="data")
    bbs.insert_time = datetime.datetime.utcnow().replace(
        microsecond=0, tzinfo=datetime.timezone.utc
    )
    container_client.upload_blob("top_file.txt", data)
    container_client.upload_blob("root/rfile.txt", data)
    container_client.upload_blob("root/a/file.txt", data)
    container_client.upload_blob("root/a1/file1.txt", data)
    container_client.upload_blob("root/b/file.txt", data)
    container_client.upload_blob("root/c/file1.txt", data)
    container_client.upload_blob("root/c/file2.txt", data)
    container_client.upload_blob(
        "root/d/file_with_metadata.txt", data, metadata=metadata
    )
    container_client.upload_blob("root/e+f/file1.txt", data)
    container_client.upload_blob("root/e+f/file2.txt", data)
    yield bbs

    bbs.delete_container("data")


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    try:
        yield loop
    finally:
        loop.close()
        policy.set_event_loop(loop)


@pytest.fixture(scope="session", autouse=True)
def spawn_azurite():
    print("Starting azurite docker container")
    client = docker.from_env()
    azurite = client.containers.run(
        "mcr.microsoft.com/azure-storage/azurite",
        "azurite-blob --loose --blobHost 0.0.0.0",
        detach=True,
        ports={"10000": "10000"},
    )
    print("Successfully created azurite container...")
    yield azurite
    print("Teardown azurite docker container")
    azurite.stop()
