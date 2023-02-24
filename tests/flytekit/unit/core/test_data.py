import fsspec
import mock
from fsspec.implementations.arrow import ArrowFSWrapper
from pyarrow import fs
import contextlib
import os
import shutil
import tempfile

from flytekit.core.data_persistence import FileAccessProvider, default_local_file_access_provider

# def test_mlje():
#     # pyarrow stuff
#     local = fs.LocalFileSystem()
#     local_fsspec = ArrowFSWrapper(local)
#
#     s3, path = fs.FileSystem.from_uri("s3://flyte-demo/datasets/sddemo/small.parquet")
#     print(s3, path)
#     f = s3.open_input_stream(path)
#     f.readall()
#     ws3 = ArrowFSWrapper(s3)
#
#     ss3 = fs.S3FileSystem(region="us-east-2")
#
#     # base fsspec stuff
#     fs3 = fsspec.filesystem("s3")
#     fs3.cat_file("s3://flyte-demo/datasets/sddemo/small.parquet")
#
#     # Does doing this work with minio without the thing?
#     s3, path = fs.FileSystem.from_uri(
#         "s3://my-s3-bucket/metadata/flytesnacks/development/am9s9q2dfrkrfnc7x9nd/user_inputs"
#     )
#     # If you don't have http, it will try to use SSL.
#     # TODO: check the sandbox configuration to see what it uses.
#     local_s3 = fs.S3FileSystem(
#         access_key="minio", secret_key="miniostorage", endpoint_override="http://localhost:30002"
#     )
#     wr_s3 = ArrowFSWrapper(local_s3)


@mock.patch("flytekit.core.data_persistence.UUID")
def test_path_getting(mock_uuid_class):
    mock_uuid_class.return_value.hex = "abcdef123"

    # Testing with raw output prefix pointing to a local path
    local_raw_fp = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix="/tmp/unittestdata")
    assert local_raw_fp.get_random_remote_path() == "/tmp/unittestdata/abcdef123"
    assert local_raw_fp.get_random_remote_path("/fsa/blah.csv") == "/tmp/unittestdata/abcdef123/blah.csv"
    assert local_raw_fp.get_random_remote_directory() == "/tmp/unittestdata/abcdef123"

    # Test local path and directory
    assert local_raw_fp.get_random_local_path() == "/tmp/unittest/local_flytekit/abcdef123"
    assert local_raw_fp.get_random_local_path("xjiosa/blah.txt") == "/tmp/unittest/local_flytekit/abcdef123/blah.txt"
    assert local_raw_fp.get_random_local_directory() == "/tmp/unittest/local_flytekit/abcdef123"

    # Test with remote pointed to s3.
    s3_fa = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix="s3://my-s3-bucket")
    assert s3_fa.get_random_remote_path() == "s3://my-s3-bucket/abcdef123"
    assert s3_fa.get_random_remote_directory() == "s3://my-s3-bucket/abcdef123"
    # trailing slash should make no difference
    s3_fa = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix="s3://my-s3-bucket/")
    assert s3_fa.get_random_remote_path() == "s3://my-s3-bucket/abcdef123"
    assert s3_fa.get_random_remote_directory() == "s3://my-s3-bucket/abcdef123"

    # Testing with raw output prefix pointing to file://
    file_raw_fp = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix="file:///tmp/unittestdata")
    assert file_raw_fp.get_random_remote_path() == "/tmp/unittestdata/abcdef123"
    assert file_raw_fp.get_random_remote_path("/fsa/blah.csv") == "/tmp/unittestdata/abcdef123/blah.csv"
    assert file_raw_fp.get_random_remote_directory() == "/tmp/unittestdata/abcdef123"


@mock.patch("flytekit.core.data_persistence.UUID")
def test_default_file_access_instance(mock_uuid_class):
    mock_uuid_class.return_value.hex = "abcdef123"

    assert default_local_file_access_provider.get_random_local_path().endswith("/sandbox/local_flytekit/abcdef123")
    assert default_local_file_access_provider.get_random_local_path("bob.txt").endswith("abcdef123/bob.txt")

    assert default_local_file_access_provider.get_random_local_directory().endswith("sandbox/local_flytekit/abcdef123")

    x = default_local_file_access_provider.get_random_remote_path()
    assert x.startswith("file:///")
    assert x.endswith("raw/abcdef123")
    x = default_local_file_access_provider.get_random_remote_path("eve.txt")
    assert x.startswith("file:///")
    assert x.endswith("raw/abcdef123/eve.txt")
    x = default_local_file_access_provider.get_random_remote_directory()
    assert x.startswith("file:///")
    assert x.endswith("raw/abcdef123")


"""
In [4]: local_raw_fp.get_random_local_path()
Out[4]: '/tmp/unittest/local_flytekit/153b61bec01aa77222ac6f80585474f7'

In [5]: local_raw_fp.get_random_local_directory()
Out[5]: '/tmp/unittest/local_flytekit/b8dc5fa64106246a4fcc5f15c5510622'

In [6]: s3_fa = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix="s3://my-s3-bucket")

In [7]: s3_fa.get_random_remote_path()
Out[7]: 's3://my-s3-bucket/3b82914f468caff2ee2b42dc6710b9d8'

In [8]: s3_fa.get_random_remote_directory()
Out[8]: 's3://my-s3-bucket/328b65c71db29fef33e1ec5101a07ac2'

In [9]: s3_fa = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix="s3://my-s3-bucket/")

In [10]: s3_fa.get_random_remote_directory()
Out[10]: 's3://my-s3-bucket//f9ae51d910de8c66a49fc9b9e0652f9a'

In [11]: s3_fa.get_random_remote_directory()
Out[11]: 's3://my-s3-bucket//425d3af05e456772f983a19f1594c844'
"""


def test_local_only():
    dirpath = tempfile.mkdtemp()
    source = os.path.join(dirpath, "start.txt")
    file_source = "file://" + source
    dest = os.path.join(dirpath, "dest.txt")
    file_dest = f"file://{os.path.join(dirpath, 'dest2.txt')}"
    filesource_dest = os.path.join(dirpath, "dest3.txt")
    data = os.path.join(dirpath, "data")
    print(source)
    print(file_dest)
    with open(source, "w") as fh:
        fh.write("hello")

    local = FileAccessProvider(local_sandbox_dir=dirpath, raw_output_prefix=data)
    local.put_data(local_path=source, remote_path=dest)
    local.put_data(local_path=source, remote_path=file_dest)
    local.put_data(local_path=file_source, remote_path=filesource_dest)


