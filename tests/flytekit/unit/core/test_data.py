import os
import shutil
import tempfile

import fsspec
import mock
import pytest

from flytekit.configuration import Config, S3Config
from flytekit.core.data_persistence import FileAccessProvider, default_local_file_access_provider, s3_setup_args

local = fsspec.filesystem("file")
root = os.path.abspath(os.sep)


@mock.patch("google.auth.compute_engine._metadata")  # to prevent network calls
@mock.patch("flytekit.core.data_persistence.UUID")
def test_path_getting(mock_uuid_class, mock_gcs):
    mock_uuid_class.return_value.hex = "abcdef123"

    # Testing with raw output prefix pointing to a local path
    loc_sandbox = os.path.join(root, "tmp", "unittest")
    loc_data = os.path.join(root, "tmp", "unittestdata")
    local_raw_fp = FileAccessProvider(local_sandbox_dir=loc_sandbox, raw_output_prefix=loc_data)
    assert local_raw_fp.get_random_remote_path() == os.path.join(root, "tmp", "unittestdata", "abcdef123")
    assert local_raw_fp.get_random_remote_path("/fsa/blah.csv") == os.path.join(
        root, "tmp", "unittestdata", "abcdef123", "blah.csv"
    )
    assert local_raw_fp.get_random_remote_directory() == os.path.join(root, "tmp", "unittestdata", "abcdef123")

    # Test local path and directory
    assert local_raw_fp.get_random_local_path() == os.path.join(root, "tmp", "unittest", "local_flytekit", "abcdef123")
    assert local_raw_fp.get_random_local_path("xjiosa/blah.txt") == os.path.join(
        root, "tmp", "unittest", "local_flytekit", "abcdef123", "blah.txt"
    )
    assert local_raw_fp.get_random_local_directory() == os.path.join(
        root, "tmp", "unittest", "local_flytekit", "abcdef123"
    )

    # Recursive paths
    assert "file:///abc/happy/", "s3://my-s3-bucket/bucket1/" == local_raw_fp.recursive_paths(
        "file:///abc/happy/", "s3://my-s3-bucket/bucket1/"
    )
    assert "file:///abc/happy/", "s3://my-s3-bucket/bucket1/" == local_raw_fp.recursive_paths(
        "file:///abc/happy", "s3://my-s3-bucket/bucket1"
    )

    # Test with remote pointed to s3.
    s3_fa = FileAccessProvider(local_sandbox_dir=loc_sandbox, raw_output_prefix="s3://my-s3-bucket")
    assert s3_fa.get_random_remote_path() == "s3://my-s3-bucket/abcdef123"
    assert s3_fa.get_random_remote_directory() == "s3://my-s3-bucket/abcdef123"
    # trailing slash should make no difference
    s3_fa = FileAccessProvider(local_sandbox_dir=loc_sandbox, raw_output_prefix="s3://my-s3-bucket/")
    assert s3_fa.get_random_remote_path() == "s3://my-s3-bucket/abcdef123"
    assert s3_fa.get_random_remote_directory() == "s3://my-s3-bucket/abcdef123"

    # Testing with raw output prefix pointing to file://
    # Skip tests for windows
    if os.name != "nt":
        file_raw_fp = FileAccessProvider(local_sandbox_dir=loc_sandbox, raw_output_prefix="file:///tmp/unittestdata")
        assert file_raw_fp.get_random_remote_path() == os.path.join(root, "tmp", "unittestdata", "abcdef123")
        assert file_raw_fp.get_random_remote_path("/fsa/blah.csv") == os.path.join(
            root, "tmp", "unittestdata", "abcdef123", "blah.csv"
        )
        assert file_raw_fp.get_random_remote_directory() == os.path.join(root, "tmp", "unittestdata", "abcdef123")

    g_fa = FileAccessProvider(local_sandbox_dir=loc_sandbox, raw_output_prefix="gs://my-s3-bucket/")
    assert g_fa.get_random_remote_path() == "gs://my-s3-bucket/abcdef123"


@mock.patch("flytekit.core.data_persistence.UUID")
def test_default_file_access_instance(mock_uuid_class):
    mock_uuid_class.return_value.hex = "abcdef123"

    assert default_local_file_access_provider.get_random_local_path().endswith(
        os.path.join("sandbox", "local_flytekit", "abcdef123")
    )
    assert default_local_file_access_provider.get_random_local_path("bob.txt").endswith(
        os.path.join("abcdef123", "bob.txt")
    )

    assert default_local_file_access_provider.get_random_local_directory().endswith(
        os.path.join("sandbox", "local_flytekit", "abcdef123")
    )

    x = default_local_file_access_provider.get_random_remote_path()
    assert x.endswith(os.path.join("raw", "abcdef123"))
    x = default_local_file_access_provider.get_random_remote_path("eve.txt")
    assert x.endswith(os.path.join("raw", "abcdef123", "eve.txt"))
    x = default_local_file_access_provider.get_random_remote_directory()
    assert x.endswith(os.path.join("raw", "abcdef123"))


@pytest.fixture
def source_folder():
    # Set up source directory for testing
    parent_temp = tempfile.mkdtemp()
    src_dir = os.path.join(parent_temp, "source", "")
    nested_dir = os.path.join(src_dir, "nested")
    local.mkdir(nested_dir)
    local.touch(os.path.join(src_dir, "original.txt"))
    local.touch(os.path.join(nested_dir, "more.txt"))
    yield src_dir
    shutil.rmtree(parent_temp)


def test_local_raw_fsspec(source_folder):
    # Test copying using raw fsspec local filesystem, should not create a nested folder
    with tempfile.TemporaryDirectory() as dest_tmpdir:
        local.put(source_folder, dest_tmpdir, recursive=True)

    new_temp_dir_2 = tempfile.mkdtemp()
    new_temp_dir_2 = os.path.join(new_temp_dir_2, "doesnotexist")
    local.put(source_folder, new_temp_dir_2, recursive=True)
    files = local.find(new_temp_dir_2)
    assert len(files) == 2


def test_local_provider(source_folder):
    # Test that behavior putting from a local dir to a local remote dir is the same whether or not the local
    # dest folder exists.
    dc = Config.for_sandbox().data_config
    with tempfile.TemporaryDirectory() as dest_tmpdir:
        provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=dest_tmpdir, data_config=dc)
        doesnotexist = provider.get_random_remote_directory()
        provider.put_data(source_folder, doesnotexist, is_multipart=True)
        files = provider._default_remote.find(doesnotexist)
        assert len(files) == 2

        exists = provider.get_random_remote_directory()
        provider._default_remote.mkdir(exists)
        provider.put_data(source_folder, exists, is_multipart=True)
        files = provider._default_remote.find(exists)
        assert len(files) == 2


@pytest.mark.sandbox_test
def test_s3_provider(source_folder):
    # Running mkdir on s3 filesystem doesn't do anything so leaving out for now
    dc = Config.for_sandbox().data_config
    provider = FileAccessProvider(
        local_sandbox_dir="/tmp/unittest", raw_output_prefix="s3://my-s3-bucket/testdata/", data_config=dc
    )
    doesnotexist = provider.get_random_remote_directory()
    provider.put_data(source_folder, doesnotexist, is_multipart=True)
    fs = provider.get_filesystem_for_path(doesnotexist)
    files = fs.find(doesnotexist)
    assert len(files) == 2


def test_local_provider_get_empty():
    dc = Config.for_sandbox().data_config
    with tempfile.TemporaryDirectory() as empty_source:
        with tempfile.TemporaryDirectory() as dest_folder:
            provider = FileAccessProvider(
                local_sandbox_dir="/tmp/unittest", raw_output_prefix=empty_source, data_config=dc
            )
            provider.get_data(empty_source, dest_folder, is_multipart=True)
            loc = provider.get_filesystem_for_path(dest_folder)
            src_files = loc.find(empty_source)
            assert len(src_files) == 0
            dest_files = loc.find(dest_folder)
            assert len(dest_files) == 0


@mock.patch("flytekit.configuration.get_config_file")
@mock.patch("os.environ")
def test_s3_setup_args_env_empty(mock_os, mock_get_config_file):
    mock_get_config_file.return_value = None
    mock_os.get.return_value = None
    s3c = S3Config.auto()
    kwargs = s3_setup_args(s3c)
    assert kwargs == {}


@mock.patch("flytekit.configuration.get_config_file")
@mock.patch("os.environ")
def test_s3_setup_args_env_both(mock_os, mock_get_config_file):
    mock_get_config_file.return_value = None
    ee = {
        "AWS_ACCESS_KEY_ID": "ignore-user",
        "AWS_SECRET_ACCESS_KEY": "ignore-secret",
        "FLYTE_AWS_ACCESS_KEY_ID": "flyte",
        "FLYTE_AWS_SECRET_ACCESS_KEY": "flyte-secret",
    }
    mock_os.get.side_effect = lambda x, y: ee.get(x)
    kwargs = s3_setup_args(S3Config.auto())
    assert kwargs == {"key": "flyte", "secret": "flyte-secret"}


@mock.patch("flytekit.configuration.get_config_file")
@mock.patch("os.environ")
def test_s3_setup_args_env_flyte(mock_os, mock_get_config_file):
    mock_get_config_file.return_value = None
    ee = {
        "FLYTE_AWS_ACCESS_KEY_ID": "flyte",
        "FLYTE_AWS_SECRET_ACCESS_KEY": "flyte-secret",
    }
    mock_os.get.side_effect = lambda x, y: ee.get(x)
    kwargs = s3_setup_args(S3Config.auto())
    assert kwargs == {"key": "flyte", "secret": "flyte-secret"}


@mock.patch("flytekit.configuration.get_config_file")
@mock.patch("os.environ")
def test_s3_setup_args_env_aws(mock_os, mock_get_config_file):
    mock_get_config_file.return_value = None
    ee = {
        "AWS_ACCESS_KEY_ID": "ignore-user",
        "AWS_SECRET_ACCESS_KEY": "ignore-secret",
    }
    mock_os.get.side_effect = lambda x, y: ee.get(x)
    kwargs = s3_setup_args(S3Config.auto())
    # not explicitly in kwargs, since fsspec/boto3 will use these env vars by default
    assert kwargs == {}
