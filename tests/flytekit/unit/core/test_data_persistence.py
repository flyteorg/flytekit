import io
import os
import pathlib
import random
import string
import sys
import tempfile

import fsspec
import mock
import pytest
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from mock import AsyncMock

from flytekit.configuration import Config
from flytekit.core.data_persistence import FileAccessProvider, get_additional_fsspec_call_kwargs
from flytekit.core.local_fsspec import FlyteLocalFileSystem


def test_get_manual_random_remote_path():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    path = fp.join(fp.raw_output_prefix, fp.get_random_string())
    assert path.startswith("s3://my-bucket")
    assert fp.raw_output_prefix == "s3://my-bucket/"


def test_is_remote():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    assert fp.is_remote("./checkpoint") is False
    assert fp.is_remote("/tmp/foo/bar") is False
    assert fp.is_remote("file://foo/bar") is False
    assert fp.is_remote("s3://my-bucket/foo/bar") is True


@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
@mock.patch("flytekit.core.data_persistence.UUID")
def test_write_folder_put_raw(mock_uuid_class):
    import pandas as pd

    """
    A test that writes this structure
    raw/
        foo/
            a.txt
        <rand>/
            bar/
                00000
        baz/
            00000
            <a.txt but called something random>
        pd.parquet
    """
    mock_uuid_class.return_value.hex = "abcdef123"
    random_dir = tempfile.mkdtemp()
    raw = os.path.join(random_dir, "raw")
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=raw)

    sio = io.StringIO()
    sio.write("hello world")
    sio.seek(0)

    bio = io.BytesIO()
    bio.write(b"hello world bytes")

    bio2 = io.BytesIO()
    df = pd.DataFrame({"name": ["Tom", "Joseph"], "age": [20, 22]})
    df.to_parquet(bio2, engine="pyarrow")

    # Write foo/a.txt by specifying the upload prefix and a file name
    fs.put_raw_data(sio, upload_prefix="foo", file_name="a.txt")

    # Write bar/00000 by specifying the folder in the filename
    fs.put_raw_data(bio, file_name="bar/00000")

    # Write pd.parquet and baz by specifying an empty string upload prefix
    fs.put_raw_data(bio2, upload_prefix="", file_name="pd.parquet")
    fs.put_raw_data(bio, upload_prefix="", file_name="baz/00000")

    # Write sio again with known folder but random file name
    fs.put_raw_data(sio, upload_prefix="baz")

    paths = [str(p) for p in pathlib.Path(raw).rglob("*")]
    assert len(paths) == 9
    expected = [
        os.path.join(raw, "pd.parquet"),
        os.path.join(raw, "foo"),
        os.path.join(raw, "baz"),
        os.path.join(raw, "abcdef123"),
        os.path.join(raw, "foo", "a.txt"),
        os.path.join(raw, "baz", "00000"),
        os.path.join(raw, "baz", "abcdef123"),
        os.path.join(raw, "abcdef123", "bar"),
        os.path.join(raw, "abcdef123", "bar", "00000"),
    ]
    expected = [str(pathlib.Path(p)) for p in expected]
    assert sorted(paths) == sorted(expected)


@pytest.mark.asyncio
async def test_write_large_put_raw():
    """
    Test that writes a large'ish file setting block size and read size.
    """
    random_dir = tempfile.mkdtemp()
    raw = os.path.join(random_dir, "raw")
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=raw)

    arbitrary_text = "".join(random.choices(string.printable, k=5000))

    sio = io.StringIO()
    sio.write(arbitrary_text)
    sio.seek(0)

    # Write foo/a.txt by specifying the upload prefix and a file name
    await fs.async_put_raw_data(sio, upload_prefix="foo", file_name="a.txt", block_size=5, read_chunk_size_bytes=1)
    output_file = os.path.join(raw, "foo", "a.txt")
    with open(output_file, "rb") as f:
        assert f.read() == arbitrary_text.encode("utf-8")


def test_write_known_location():
    """
    Test that if given the skip_raw_data_prefix, the raw output data field is not prepended.
    """
    random_dir = tempfile.mkdtemp()
    raw = os.path.join(random_dir, "raw")
    fs = FileAccessProvider(local_sandbox_dir=random_dir, raw_output_prefix=raw)

    arbitrary_text = "".join(random.choices(string.printable, k=5))

    sio = io.StringIO()
    sio.write(arbitrary_text)
    sio.seek(0)

    # Write foo/a.txt by specifying the upload prefix and a file name
    known_dest_dir = tempfile.mkdtemp()
    set_path = fs.join(known_dest_dir, "a.txt")
    output_path = fs.put_raw_data(sio, upload_prefix=known_dest_dir, file_name="a.txt", skip_raw_data_prefix=True)
    assert output_path == set_path
    with open(output_path, "rb") as f:
        assert f.read() == arbitrary_text.encode("utf-8")


def test_generate_new_custom_path():
    """
    Test that a new path given alternate bucket and name is generated correctly
    """
    random_dir = tempfile.mkdtemp()
    fs = FileAccessProvider(
        local_sandbox_dir=random_dir,
        raw_output_prefix="s3://my-default-bucket/my-default-prefix/"
        )
    np = fs.generate_new_custom_path(alt="foo-bucket", stem="bar.txt")
    assert np == "s3://foo-bucket/my-default-prefix/bar.txt"


def test_initialise_azure_file_provider_with_account_key():
    with mock.patch.dict(
        os.environ,
        {"FLYTE_AZURE_STORAGE_ACCOUNT_NAME": "accountname", "FLYTE_AZURE_STORAGE_ACCOUNT_KEY": "accountkey"},
    ):
        fp = FileAccessProvider("/tmp", "abfs://container/path/within/container")
        assert fp.get_filesystem().account_name == "accountname"
        assert fp.get_filesystem().account_key == "accountkey"
        assert fp.get_filesystem().sync_credential is None


def test_initialise_azure_file_provider_with_service_principal():
    with mock.patch.dict(
        os.environ,
        {
            "FLYTE_AZURE_STORAGE_ACCOUNT_NAME": "accountname",
            "FLYTE_AZURE_CLIENT_SECRET": "clientsecret",
            "FLYTE_AZURE_CLIENT_ID": "clientid",
            "FLYTE_AZURE_TENANT_ID": "tenantid",
        },
    ):
        fp = FileAccessProvider("/tmp", "abfs://container/path/within/container")
        assert fp.get_filesystem().account_name == "accountname"
        assert isinstance(fp.get_filesystem().sync_credential, ClientSecretCredential)
        assert fp.get_filesystem().client_secret == "clientsecret"
        assert fp.get_filesystem().client_id == "clientid"
        assert fp.get_filesystem().tenant_id == "tenantid"


def test_initialise_azure_file_provider_with_default_credential():
    with mock.patch.dict(
            os.environ,
            {
                "FLYTE_AZURE_STORAGE_ACCOUNT_NAME": "accountname",
                "AZURE_STORAGE_ANON": "false",
            },
    ):
        fp = FileAccessProvider("/tmp", "abfs://container/path/within/container")
        assert fp.get_filesystem().account_name == "accountname"
        assert isinstance(fp.get_filesystem().sync_credential, DefaultAzureCredential)


def test_get_file_system():
    # Test that custom args are not swallowed by get_filesystem

    class MockFileSystem(FlyteLocalFileSystem):
        def __init__(self, *args, **kwargs):
            assert "test_arg" in kwargs
            del kwargs["test_arg"]
            super().__init__(*args, **kwargs)

    fsspec.register_implementation("testgetfs", MockFileSystem)

    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    fp.get_filesystem("testgetfs", test_arg="test_arg")


def test_get_additional_fsspec_call_kwargs():
    with mock.patch("flytekit.core.data_persistence._WRITE_SIZE_CHUNK_BYTES", 12345):
        kwargs = get_additional_fsspec_call_kwargs(("s3", "s3a"), "put")
        assert kwargs == {"chunksize": 12345}

        kwargs = get_additional_fsspec_call_kwargs("s3", "_put")
        assert kwargs == {"chunksize": 12345}

        kwargs = get_additional_fsspec_call_kwargs("s3", "get")
        assert kwargs == {}


@pytest.mark.asyncio
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_async_filesystem_for_path", new_callable=AsyncMock)
@mock.patch("flytekit.core.data_persistence.get_additional_fsspec_call_kwargs")
async def test_chunk_size(mock_call_kwargs, mock_get_fs):
    mock_call_kwargs.return_value = {"chunksize": 1234}
    mock_fs = mock.MagicMock()
    mock_get_fs.return_value = mock_fs

    mock_fs.protocol = ("s3", "s3a")
    fp = FileAccessProvider("/tmp", "s3://container/path/within/container")

    def put(*args, **kwargs):
        assert "chunksize" in kwargs

    mock_fs.put = put
    upload_location = await fp._put("/tmp/foo", "s3://bar")
    assert upload_location == "s3://bar"


@pytest.mark.sandbox_test
def test_put_raw_data_bytes():
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/"
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)
    prefix = provider.get_random_string()
    provider.put_raw_data(lpath=b"hello", upload_prefix=prefix, file_name="hello_bytes")
    provider.put_raw_data(lpath=io.BytesIO(b"hello"), upload_prefix=prefix, file_name="hello_bytes_io")
    provider.put_raw_data(lpath=io.StringIO("hello"), upload_prefix=prefix, file_name="hello_string_io")

    fs = provider.get_filesystem("s3")
    listing = fs.ls(f"{raw_output}{prefix}/")
    assert len(listing) == 3
