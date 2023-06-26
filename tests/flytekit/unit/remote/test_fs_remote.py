import tempfile
from base64 import b64encode

import pathlib
import mock
import pytest
import os
import shutil
import fsspec

from flytekit.configuration import Config
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.remote.remote import FlyteRemote
from flytekit.remote.remote_fs import RemoteFS

local = fsspec.filesystem("file")


@pytest.fixture
def source_folder():
    # Set up source directory for testing
    parent_temp = tempfile.mkdtemp()
    src_dir = os.path.join(parent_temp, "source", "")
    nested_dir = os.path.join(src_dir, "nested")
    local.mkdir(nested_dir)
    local.touch(os.path.join(src_dir, "original.txt"))
    with open(os.path.join(src_dir, "original.txt"), "w") as fh:
        fh.write("hello original")
    local.touch(os.path.join(nested_dir, "more.txt"))
    yield src_dir
    shutil.rmtree(parent_temp)


def test_basics():
    r = FlyteRemote(
        Config.for_sandbox(),
        default_project="flytesnacks",
        default_domain="development",
        data_upload_location="flyte://dv1/",
    )
    fs = RemoteFS(remote=r)
    assert fs.protocol == "flyte"
    assert fs.sep == "/"
    assert fs.unstrip_protocol("dv/fwu11/") == "flyte://dv/fwu11/"


@pytest.fixture
def sandbox_remote():
    r = FlyteRemote(
        Config.for_sandbox(),
        default_project="flytesnacks",
        default_domain="development",
        data_upload_location="flyte://data",
    )
    yield r


@pytest.mark.sandbox_test
def test_upl(sandbox_remote):
    encoded_md5 = b64encode(b"hi2dfsfj23333ileksa")
    xx = sandbox_remote.client.get_upload_signed_url(
        "flytesnacks", "development", content_md5=encoded_md5, filename="parent/child/1"
    )
    print(xx.native_url)


@pytest.mark.sandbox_test
@mock.patch("flytekit.core.data_persistence.UUID")
def test_remote_upload_with_fs_directly(mock_uuid_class, sandbox_remote):
    mock_uuid_class.return_value.hex = "abcdef123"
    fs = RemoteFS(remote=sandbox_remote)

    # Test uploading a folder, but without the /
    res = fs.put("/Users/ytong/temp/data/source", "flyte://data", recursive=True)
    assert res == "s3://my-s3-bucket/flytesnacks/development/abcdef123/source"

    # Test uploading a file
    res = fs.put(__file__, "flyte://data")
    assert res.startswith("s3://my-s3-bucket/flytesnacks/development")
    assert res.endswith("test_fs_remote.py")


@pytest.mark.sandbox_test
@mock.patch("flytekit.core.data_persistence.UUID")
def test_fs_direct_trailing_slash(mock_uuid_class, sandbox_remote):
    mock_uuid_class.return_value.hex = "abcdef123"
    fs = RemoteFS(remote=sandbox_remote)

    # Uploading folder with a / won't include the folder name
    res = fs.put("/Users/ytong/temp/data/source/", "flyte://data", recursive=True)
    assert res == "s3://my-s3-bucket/flytesnacks/development/abcdef123"


@pytest.mark.sandbox_test
@mock.patch("flytekit.core.data_persistence.UUID")
def test_remote_upload_with_data_persistence(mock_uuid_class, sandbox_remote):
    mock_uuid_class.return_value.hex = "abcdef123"
    sandbox_path = tempfile.mkdtemp()
    fp = FileAccessProvider(local_sandbox_dir=sandbox_path, raw_output_prefix="flyte://data/")

    # Test uploading a file and folder.
    res = fp.put("/Users/ytong/temp/data/source", "flyte://data", recursive=True)
    # Unlike using the RemoteFS directly, the trailing slash is automatically added by data persistence, not sure why
    # but preserving the behavior for now.
    assert res == "s3://my-s3-bucket/flytesnacks/development/abcdef123"


def test_common_matching():
    urls = [
        "s3://my-s3-bucket/flytesnacks/development/ABCYZWMPACZAJ2MABGMOZ6CCPY======/source/empty.md",
        "s3://my-s3-bucket/flytesnacks/development/ABCXKL5ZZWXY3PDLM3OONUHHME======/source/nested/more.txt",
        "s3://my-s3-bucket/flytesnacks/development/ABCXBAPBKONMADXVW5Q3J6YBWM======/source/original.txt",
    ]

    assert RemoteFS.extract_common(urls) == "s3://my-s3-bucket/flytesnacks/development"


def test_hashing(sandbox_remote, source_folder):
    fs = RemoteFS(remote=sandbox_remote)
    s = fs.get_hashes_and_lengths(pathlib.Path(source_folder))
    assert len(s) == 2
    lengths = set([x[1] for x in s.values()])
    assert lengths == {0, 14}
    fr = fs.get_filename_root(s)
    assert fr == "GSEYDOSFXWFB5ABZB6AHZ2HK7Y======"
