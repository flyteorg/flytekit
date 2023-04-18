import os
import random
import shutil
import tempfile
from uuid import UUID

import fsspec
import mock
import pytest

from flytekit.configuration import Config, S3Config
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider, default_local_file_access_provider, s3_setup_args
from flytekit.types.directory.types import FlyteDirectory

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
    with open(os.path.join(src_dir, "original.txt"), "w") as fh:
        fh.write("hello original")
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
    assert kwargs == {"cache_regions": True}


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
    assert kwargs == {"key": "flyte", "secret": "flyte-secret", "cache_regions": True}


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
    assert kwargs == {"key": "flyte", "secret": "flyte-secret", "cache_regions": True}


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
    assert kwargs == {"cache_regions": True}


def test_crawl_local_nt(source_folder):
    """
    running this to see what it prints
    """
    if os.name != "nt":  # don't
        return
    source_folder = os.path.join(source_folder, "")  # ensure there's a trailing / or \
    fd = FlyteDirectory(path=source_folder)
    res = fd.crawl()
    split = [(x, y) for x, y in res]
    print(f"NT split {split}")

    # Test crawling a directory without trailing / or \
    source_folder = source_folder[:-1]
    fd = FlyteDirectory(path=source_folder)
    res = fd.crawl()
    files = [os.path.join(x, y) for x, y in res]
    print(f"NT files joined {files}")


def test_crawl_local_non_nt(source_folder):
    """
    crawl on the source folder fixture should return for example
        ('/var/folders/jx/54tww2ls58n8qtlp9k31nbd80000gp/T/tmpp14arygf/source/', 'original.txt')
        ('/var/folders/jx/54tww2ls58n8qtlp9k31nbd80000gp/T/tmpp14arygf/source/', 'nested/more.txt')
    """
    if os.name == "nt":  # don't
        return
    source_folder = os.path.join(source_folder, "")  # ensure there's a trailing / or \
    fd = FlyteDirectory(path=source_folder)
    res = fd.crawl()
    split = [(x, y) for x, y in res]
    files = [os.path.join(x, y) for x, y in split]
    assert set(split) == {(source_folder, "original.txt"), (source_folder, os.path.join("nested", "more.txt"))}
    expected = {os.path.join(source_folder, "original.txt"), os.path.join(source_folder, "nested", "more.txt")}
    assert set(files) == expected

    # Test crawling a directory without trailing / or \
    source_folder = source_folder[:-1]
    fd = FlyteDirectory(path=source_folder)
    res = fd.crawl()
    files = [os.path.join(x, y) for x, y in res]
    assert set(files) == expected

    # Test crawling a single file
    fd = FlyteDirectory(path=os.path.join(source_folder, "original.txt"))
    res = fd.crawl()
    files = [os.path.join(x, y) for x, y in res]
    assert len(files) == 0


@pytest.mark.sandbox_test
def test_crawl_s3(source_folder):
    """
    ('s3://my-s3-bucket/testdata/5b31492c032893b515650f8c76008cf7', 'original.txt')
    ('s3://my-s3-bucket/testdata/5b31492c032893b515650f8c76008cf7', 'nested/more.txt')
    """
    # Running mkdir on s3 filesystem doesn't do anything so leaving out for now
    dc = Config.for_sandbox().data_config
    provider = FileAccessProvider(
        local_sandbox_dir="/tmp/unittest", raw_output_prefix="s3://my-s3-bucket/testdata/", data_config=dc
    )
    s3_random_target = provider.get_random_remote_directory()
    provider.put_data(source_folder, s3_random_target, is_multipart=True)
    ctx = FlyteContextManager.current_context()
    expected = {f"{s3_random_target}/original.txt", f"{s3_random_target}/nested/more.txt"}

    with FlyteContextManager.with_context(ctx.with_file_access(provider)):
        fd = FlyteDirectory(path=s3_random_target)
        res = fd.crawl()
        res = [(x, y) for x, y in res]
        files = [os.path.join(x, y) for x, y in res]
        assert set(files) == expected
        assert set(res) == {(s3_random_target, "original.txt"), (s3_random_target, os.path.join("nested", "more.txt"))}

        fd_file = FlyteDirectory(path=f"{s3_random_target}/original.txt")
        res = fd_file.crawl()
        files = [r for r in res]
        assert len(files) == 1


@pytest.mark.sandbox_test
def test_walk_local_copy_to_s3(source_folder):
    dc = Config.for_sandbox().data_config
    explicit_empty_folder = UUID(int=random.getrandbits(128)).hex
    raw_output_path = f"s3://my-s3-bucket/testdata/{explicit_empty_folder}"
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output_path, data_config=dc)

    ctx = FlyteContextManager.current_context()
    local_fd = FlyteDirectory(path=source_folder)
    local_fd_crawl = local_fd.crawl()
    local_fd_crawl = [x for x in local_fd_crawl]
    with FlyteContextManager.with_context(ctx.with_file_access(provider)):
        fd = FlyteDirectory.new_remote()
        assert raw_output_path in fd.path

        # Write source folder files to new remote path
        for root_path, suffix in local_fd_crawl:
            new_file = fd.new_file(suffix)  # noqa
            with open(os.path.join(root_path, suffix), "rb") as r:  # noqa
                with new_file.open("w") as w:
                    print(f"Writing, t {type(w)} p {new_file.path} |{suffix}|")
                    w.write(str(r.read()))

        new_crawl = fd.crawl()
        new_suffixes = [y for x, y in new_crawl]
        assert len(new_suffixes) == 2  # should have written two files
