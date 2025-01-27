import datetime
import os
import tempfile
from unittest import mock

import azure.storage.blob.aio
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from adlfs import AzureBlobFile, AzureBlobFileSystem

URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA
DEFAULT_VERSION_ID = "1970-01-01T00:00:00.0000000Z"
LATEST_VERSION_ID = "2022-01-01T00:00:00.0000000Z"


def assert_almost_equal(x, y, threshold, prop_name=None):
    if x is None and y is None:
        return
    assert abs(x - y) <= threshold


def test_connect(storage):
    AzureBlobFileSystem(account_name=storage.account_name, connection_string=CONN_STR)


def test_anon_env(storage):
    with mock.patch.dict(os.environ, {"AZURE_STORAGE_ANON": "false"}):
        # Setting cachable to false to avoid re-testing the instance from the previous test
        AzureBlobFileSystem.cachable = False
        x = AzureBlobFileSystem(
            account_name=storage.account_name, connection_string=CONN_STR
        )
        assert not x.anon
        AzureBlobFileSystem.cachable = True  # Restoring cachable value


def assert_blob_equals(blob, expected_blob):
    irregular_props = [
        "etag",
        "deleted",
    ]

    time_based_props = [
        "last_modified",
        "creation_time",
        "deleted_time",
        "last_accessed_on",
    ]
    # creating a shallow copy since we are going to pop properties
    shallow_copy = {**blob}
    for time_based_prop in time_based_props:
        time_value = shallow_copy.pop(time_based_prop, None)
        expected_time_value = expected_blob.pop(time_based_prop, None)
        assert_almost_equal(
            time_value,
            expected_time_value,
            datetime.timedelta(minutes=1),
            prop_name=time_based_prop,
        )

    for irregular_prop in irregular_props:
        shallow_copy.pop(irregular_prop, None)
        expected_blob.pop(irregular_prop, None)

    content_settings = dict(sorted(shallow_copy.pop("content_settings", {}).items()))
    expected_content_settings = dict(
        sorted(expected_blob.pop("content_settings", {}).items())
    )
    assert content_settings == expected_content_settings
    assert shallow_copy == expected_blob


def assert_blobs_equals(blobs, expected_blobs):
    assert len(blobs) == len(expected_blobs)
    for blob, expected_blob in zip(blobs, expected_blobs):
        assert_blob_equals(blob, expected_blob)


def test_ls(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )

    # these are containers
    assert fs.ls("") == ["data"]
    assert fs.ls("/") == ["data"]
    assert fs.ls(".") == ["data"]
    assert fs.ls("*") == ["data"]

    # these are top-level directories and files
    assert fs.ls("data") == ["data/root", "data/top_file.txt"]
    assert fs.ls("/data") == ["data/root", "data/top_file.txt"]

    # root contains files and directories
    assert fs.ls("data/root") == [
        "data/root/a",
        "data/root/a1",
        "data/root/b",
        "data/root/c",
        "data/root/d",
        "data/root/e+f",
        "data/root/rfile.txt",
    ]
    assert fs.ls("data/root/") == [
        "data/root/a",
        "data/root/a1",
        "data/root/b",
        "data/root/c",
        "data/root/d",
        "data/root/e+f",
        "data/root/rfile.txt",
    ]

    # slashes are not not needed, but accepted
    assert fs.ls("data/root/a") == ["data/root/a/file.txt"]
    assert fs.ls("data/root/a/") == ["data/root/a/file.txt"]
    assert fs.ls("/data/root/a") == ["data/root/a/file.txt"]
    assert fs.ls("/data/root/a/") == ["data/root/a/file.txt"]
    assert fs.ls("data/root/b") == ["data/root/b/file.txt"]
    assert fs.ls("data/root/b/") == ["data/root/b/file.txt"]
    assert fs.ls("data/root/a1") == ["data/root/a1/file1.txt"]
    assert fs.ls("data/root/a1/") == ["data/root/a1/file1.txt"]
    assert fs.ls("data/root/e+f") == [
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
    ]
    assert fs.ls("data/root/e+f/") == [
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
    ]
    # Ensure ls works with protocol prefix
    assert fs.ls("data/root/e+f/") == fs.ls("abfs://data/root/e+f/")

    # file details
    files = fs.ls("data/root/a/file.txt", detail=True)
    assert_blobs_equals(
        files,
        [
            {
                "name": "data/root/a/file.txt",
                "size": 10,
                "type": "file",
                "archive_status": None,
                "creation_time": storage.insert_time,
                "last_modified": storage.insert_time,
                "deleted_time": None,
                "last_accessed_on": None,
                "remaining_retention_days": None,
                "tag_count": None,
                "tags": None,
                "metadata": {},
                "content_settings": {
                    "content_type": "application/octet-stream",
                    "content_encoding": None,
                    "content_language": None,
                    "content_md5": bytearray(
                        b"x\x1e^$]i\xb5f\x97\x9b\x86\xe2\x8d#\xf2\xc7"
                    ),
                    "content_disposition": None,
                    "cache_control": None,
                },
            }
        ],
    )

    # c has two files
    assert_blobs_equals(
        fs.ls("data/root/c", detail=True),
        [
            {
                "name": "data/root/c/file1.txt",
                "size": 10,
                "type": "file",
                "archive_status": None,
                "creation_time": storage.insert_time,
                "last_modified": storage.insert_time,
                "deleted_time": None,
                "last_accessed_on": None,
                "remaining_retention_days": None,
                "tag_count": None,
                "tags": None,
                "metadata": {},
                "content_settings": {
                    "content_type": "application/octet-stream",
                    "content_encoding": None,
                    "content_language": None,
                    "content_md5": bytearray(
                        b"x\x1e^$]i\xb5f\x97\x9b\x86\xe2\x8d#\xf2\xc7"
                    ),
                    "content_disposition": None,
                    "cache_control": None,
                },
            },
            {
                "name": "data/root/c/file2.txt",
                "size": 10,
                "type": "file",
                "archive_status": None,
                "creation_time": storage.insert_time,
                "last_modified": storage.insert_time,
                "deleted_time": None,
                "last_accessed_on": None,
                "remaining_retention_days": None,
                "tag_count": None,
                "tags": None,
                "metadata": {},
                "content_settings": {
                    "content_type": "application/octet-stream",
                    "content_encoding": None,
                    "content_language": None,
                    "content_md5": bytearray(
                        b"x\x1e^$]i\xb5f\x97\x9b\x86\xe2\x8d#\xf2\xc7"
                    ),
                    "content_disposition": None,
                    "cache_control": None,
                },
            },
        ],
    )

    # with metadata
    assert_blobs_equals(
        fs.ls("data/root/d", detail=True),
        [
            {
                "name": "data/root/d/file_with_metadata.txt",
                "size": 10,
                "type": "file",
                "archive_status": None,
                "creation_time": storage.insert_time,
                "last_modified": storage.insert_time,
                "deleted_time": None,
                "last_accessed_on": None,
                "remaining_retention_days": None,
                "tag_count": None,
                "tags": None,
                "metadata": {"meta": "data"},
                "content_settings": {
                    "content_type": "application/octet-stream",
                    "content_encoding": None,
                    "content_language": None,
                    "content_md5": bytearray(
                        b"x\x1e^$]i\xb5f\x97\x9b\x86\xe2\x8d#\xf2\xc7"
                    ),
                    "content_disposition": None,
                    "cache_control": None,
                },
            }
        ],
    )

    # if not direct match is found throws error
    with pytest.raises(FileNotFoundError):
        fs.ls("not-a-container")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/not-a-directory/")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/root/not-a-file.txt")


def test_ls_no_listings_cache(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        use_listings_cache=False,
    )
    result = fs.ls("data/root")
    assert len(result) > 0  # some state leaking between tests


async def test_ls_versioned(storage, mocker):
    from azure.storage.blob.aio import ContainerClient

    walk_blobs = mocker.patch.object(ContainerClient, "walk_blobs")
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=False,
        skip_instance_cache=True,
    )
    with pytest.raises(ValueError):
        await fs._ls("data/root/a/file.txt", version_id=DEFAULT_VERSION_ID)
    await fs._ls("data/root/a/file.txt")
    walk_blobs.assert_called_once_with(
        include=["metadata"], name_starts_with="root/a/file.txt"
    )

    fs.version_aware = True
    walk_blobs.reset_mock()
    await fs._ls("data/root/a/file.txt", version_id=DEFAULT_VERSION_ID)
    walk_blobs.assert_called_once_with(
        include=["metadata", "versions"], name_starts_with="root/a/file.txt"
    )


@pytest.mark.parametrize("version_aware", [False, True])
def test_info(storage, version_aware):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=version_aware,
        skip_instance_cache=True,
    )

    container_info = fs.info("data")
    assert_blob_equals(
        container_info,
        {
            "name": "data",
            "type": "directory",
            "size": None,
            "last_modified": storage.insert_time,
            "metadata": None,
        },
    )

    container2_info = fs.info("data/root")
    assert_blob_equals(
        container2_info, {"name": "data/root", "type": "directory", "size": None}
    )

    dir_info = fs.info("data/root/c")
    assert_blob_equals(
        dir_info, {"name": "data/root/c", "type": "directory", "size": None}
    )

    file_info = fs.info("data/root/a/file.txt")
    expected = {
        "name": "data/root/a/file.txt",
        "size": 10,
        "type": "file",
        "archive_status": None,
        "creation_time": storage.insert_time,
        "last_modified": storage.insert_time,
        "deleted_time": None,
        "last_accessed_on": None,
        "remaining_retention_days": None,
        "tag_count": None,
        "tags": None,
        "metadata": {},
        "content_settings": {
            "content_type": "application/octet-stream",
            "content_encoding": None,
            "content_language": None,
            "content_md5": bytearray(b"x\x1e^$]i\xb5f\x97\x9b\x86\xe2\x8d#\xf2\xc7"),
            "content_disposition": None,
            "cache_control": None,
        },
    }
    if version_aware:
        expected.update({"is_current_version": None, "version_id": None})
    assert_blob_equals(file_info, expected)

    file_with_meta_info = fs.info("data/root/d/file_with_metadata.txt")
    expected = {
        "name": "data/root/d/file_with_metadata.txt",
        "size": 10,
        "type": "file",
        "archive_status": None,
        "creation_time": storage.insert_time,
        "last_modified": storage.insert_time,
        "deleted_time": None,
        "last_accessed_on": None,
        "remaining_retention_days": None,
        "tag_count": None,
        "tags": None,
        "metadata": {"meta": "data"},
        "content_settings": {
            "content_type": "application/octet-stream",
            "content_encoding": None,
            "content_language": None,
            "content_md5": bytearray(b"x\x1e^$]i\xb5f\x97\x9b\x86\xe2\x8d#\xf2\xc7"),
            "content_disposition": None,
            "cache_control": None,
        },
    }
    if version_aware:
        expected.update({"is_current_version": None, "version_id": None})
    assert_blob_equals(file_with_meta_info, expected)


@pytest.mark.parametrize(
    "path",
    [
        "does-not-exist",
        "does-not-exist/foo",
        "data/does_not_exist",
        "data/root/does_not_exist",
    ],
)
def test_info_missing(storage, path):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    with pytest.raises(FileNotFoundError):
        fs.info(path)


def test_time_info(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        max_concurrency=1,
    )

    creation_time = fs.created("data/root/d/file_with_metadata.txt")
    assert_almost_equal(
        creation_time, storage.insert_time, datetime.timedelta(seconds=1)
    )

    modified_time = fs.modified("data/root/d/file_with_metadata.txt")
    assert_almost_equal(
        modified_time, storage.insert_time, datetime.timedelta(seconds=1)
    )


def test_find(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    # just the directory name
    assert fs.find("data/root/a") == ["data/root/a/file.txt"]  # NOQA
    assert fs.find("data/root/a/") == ["data/root/a/file.txt"]  # NOQA

    assert fs.find("data/root/c") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]
    assert fs.find("data/root/c/") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]

    # all files
    assert fs.find("data/root") == [
        "data/root/a/file.txt",
        "data/root/a1/file1.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
    ]
    assert fs.find("data/root", withdirs=False) == [
        "data/root/a/file.txt",
        "data/root/a1/file1.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
    ]

    # all files and directories
    assert fs.find("data/root", withdirs=True) == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/a1",
        "data/root/a1/file1.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
    ]
    assert fs.find("data/root/", withdirs=True) == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/a1",
        "data/root/a1/file1.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
    ]

    # missing
    assert fs.find("data/missing") == []

    # prefix search
    assert fs.find("data/root", prefix="a") == [
        "data/root/a/file.txt",
        "data/root/a1/file1.txt",
    ]

    assert fs.find("data/root", prefix="a", withdirs=True) == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/a1",
        "data/root/a1/file1.txt",
    ]

    find_results = fs.find("data/root", prefix="a1", withdirs=True, detail=True)
    assert_blobs_equals(
        list(find_results.values()),
        [
            {"name": "data/root/a1", "size": 0, "type": "directory"},
            {
                "name": "data/root/a1/file1.txt",
                "size": 10,
                "type": "file",
                "archive_status": None,
                "creation_time": storage.insert_time,
                "last_modified": storage.insert_time,
                "deleted_time": None,
                "last_accessed_on": None,
                "remaining_retention_days": None,
                "tag_count": None,
                "tags": None,
                "metadata": {},
                "content_settings": {
                    "content_type": "application/octet-stream",
                    "content_encoding": None,
                    "content_language": None,
                    "content_md5": bytearray(
                        b"x\x1e^$]i\xb5f\x97\x9b\x86\xe2\x8d#\xf2\xc7"
                    ),
                    "content_disposition": None,
                    "cache_control": None,
                },
            },
        ],
    )


def test_find_missing(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    assert fs.find("data/roo") == []


def test_glob(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    # just the directory name
    assert fs.glob("data/root") == ["data/root"]
    assert fs.glob("data/root/") == ["data/root/"]

    # top-level contents of a directory
    assert fs.glob("data/root/*") == [
        "data/root/a",
        "data/root/a1",
        "data/root/b",
        "data/root/c",
        "data/root/d",
        "data/root/e+f",
        "data/root/rfile.txt",
    ]

    assert fs.glob("data/root/b/*") == ["data/root/b/file.txt"]  # NOQA
    assert fs.glob("data/root/b/**") == ["data/root/b/file.txt"]  # NOQA

    # across directories
    assert fs.glob("data/root/*/file.txt") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
    ]

    # regex match
    assert fs.glob("data/root/*/file[0-9].txt") == [
        "data/root/a1/file1.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
    ]

    # text files
    assert fs.glob("data/root/*/file*.txt") == [
        "data/root/a/file.txt",
        "data/root/a1/file1.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
    ]

    # all text files
    assert fs.glob("data/**/*.txt") == [
        "data/root/a/file.txt",
        "data/root/a1/file1.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
        "data/top_file.txt",
    ]

    # all files
    assert fs.glob("data/root/**") == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/a1",
        "data/root/a1/file1.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
    ]
    assert fs.glob("data/roo*/**") == [
        "data/root",
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/a1",
        "data/root/a1/file1.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
    ]

    # missing
    assert fs.glob("data/missing/*") == []


def test_glob_full_uri(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    assert fs.glob("abfs://account.dfs.core.windows.net/data/**/*.txt") == [
        "data/root/a/file.txt",
        "data/root/a1/file1.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
        "data/top_file.txt",
    ]

    assert fs.glob("account.dfs.core.windows.net/data/**/*.txt") == [
        "data/root/a/file.txt",
        "data/root/a1/file1.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/d/file_with_metadata.txt",
        "data/root/e+f/file1.txt",
        "data/root/e+f/file2.txt",
        "data/root/rfile.txt",
        "data/top_file.txt",
    ]


def test_open_file(storage, mocker):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    f = fs.open("/data/root/a/file.txt")

    result = f.read()
    assert result == b"0123456789"

    close = mocker.patch.object(f.container_client, "close")
    f.close()
    print(fs.ls("/data/root/a"))

    close.assert_called_once()


# def test_open_context_manager(storage, mocker):
#     """
#     Memory profiling shows this is working, but its failing the test
#     Due to the behavior of the MagicMock.  Needs to be fixed
#     """
#     # "test closing azure client with context manager"
#     fs = AzureBlobFileSystem(
#         account_name=storage.account_name, connection_string=CONN_STR
#     )

#     with fs.open("/data/root/a/file.txt") as f:
#         close = mocker.patch.object(f.container_client, "close")
#         result = f.read()
#         assert result == b"0123456789"
#     close.assert_called_once()


def test_rm(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.rm("/data/root/a/file.txt")

    with pytest.raises(FileNotFoundError):
        fs.ls("/data/root/a/file.txt", refresh=True)


@mock.patch.object(
    azure.storage.blob.aio.ContainerClient,
    "delete_blob",
    side_effect=azure.storage.blob.aio.ContainerClient.delete_blob,
    autospec=True,
)
def test_rm_recursive(mock_delete_blob, storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    assert "data/root/c" in fs.ls("/data/root")

    assert fs.ls("data/root/c") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]
    fs.rm("data/root/c", recursive=True)
    assert "data/root/c" not in fs.ls("/data/root")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/root/c")

    assert mock_delete_blob.mock_calls[-1] == mock.call(
        mock.ANY, "root/c"
    ), "The directory deletion should be the last call"


@pytest.mark.filterwarnings("error")
def test_rm_recursive2(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    assert "data/root" in fs.ls("/data")

    fs.rm("data/root", recursive=True)
    assert "data/root" not in fs.ls("/data")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/root")


async def test_rm_recursive_call_order(storage, mocker):
    from azure.storage.blob.aio import ContainerClient

    delete_blob_mock = mocker.patch.object(
        ContainerClient, "delete_blob", return_value=None
    )
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    container_name = "data"
    file_paths = [
        "root/a",
        "root/a/file.txt",
        "root/a1",
        "root/a1/file1.txt",
        "root/b",
        "root/b/file.txt",
        "root",
        "root/c",
        "root/c/file1.txt",
        "root/c/file2.txt",
        "root/d",
        "root/d/file_with_metadata.txt",
        "root/e+f",
        "root/e+f/file1.txt",
        "root/e+f/file2.txt",
        "root/rfile.txt",
    ]
    await fs._rm_files(container_name=container_name, file_paths=file_paths)
    last_deleted_paths = [call.args[0] for call in delete_blob_mock.call_args_list[-7:]]
    assert last_deleted_paths == [
        "root/e+f",
        "root/d",
        "root/c",
        "root/b",
        "root/a1",
        "root/a",
        "root",
    ], "The directory deletion should be in reverse lexographical order"


def test_rm_multiple_items(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    for i in range(2):
        fs.makedir(f"new-container{i}")
        assert f"new-container{i}" in fs.ls("")

        with fs.open(path=f"new-container{i}/file0.txt", mode="wb") as f:
            f.write(b"0123456789")

        with fs.open(f"new-container{i}/file1.txt", "wb") as f:
            f.write(b"0123456789")

        assert fs.ls(f"new-container{i}") == [
            f"new-container{i}/file0.txt",
            f"new-container{i}/file1.txt",
        ]

    fs.rm(
        [
            "new-container0/file0.txt",
            "new-container0/file1.txt",
            "new-container1/file0.txt",
            "new-container1/file1.txt",
        ],
        recursive=True,
    )

    assert fs.ls("new-container0") == []
    assert fs.ls("new-container1") == []

    for i in range(2):
        fs.rm(f"new-container{i}")


def test_mkdir(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        assume_container_exists=False,
    )

    # Verify mkdir will create a new container when create_parents is True
    fs.mkdir("new-container", create_parents=True)
    assert "new-container" in fs.ls(".")
    fs.rm("new-container")

    # Verify a new container will not be created when create_parents
    # is False
    with pytest.raises(PermissionError):
        fs.mkdir("new-container", exist_ok=False, create_parents=False)

    with pytest.raises(ValueError):
        fs.mkdir("bad_container_name")

    # Test creating subdirectory when container does not exist
    # Since mkdir is a no-op, if create_parents=True, it will create
    # the top level container, but will NOT create nested directories
    fs.mkdir("new-container/dir", create_parents=True)
    assert "new-container/dir" not in fs.ls("new-container")
    assert "new-container" in fs.ls(".")
    fs.rm("new-container", recursive=True)

    # Test that creating a directory when already exists passes
    fs.mkdir("data")
    assert "data" in fs.ls(".")

    # Test raising error when container does not exist
    with pytest.raises(PermissionError):
        fs.mkdir("new-container/dir", create_parents=False)

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        assume_container_exists=None,
    )

    with pytest.warns():
        fs.mkdir("bad_container_name")

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        assume_container_exists=True,
    )
    fs.mkdir("bad_container_name")  # should not throw as we assume it exists


def test_makedir(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )

    # Verify makedir will create a new container when create_parents is True
    with pytest.raises(FileExistsError):
        fs.makedir("data", exist_ok=False)

    # The container and directory already exist.  Should pass
    fs.makedir("data", exist_ok=True)
    assert "data" in fs.ls(".")

    # Test creating subdirectory when container does not exist.  Again
    # Since makedir is a no-op, this can create the container, but not write nested directories
    fs.makedir("new-container/dir")
    assert "new-container/dir" not in fs.ls("new-container")
    assert "new-container" in fs.ls(".")
    fs.rm("new-container", recursive=True)


def test_makedir_rmdir(storage, caplog):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )

    fs.makedir("new-container")
    assert "new-container" in fs.ls("")
    assert fs.ls("new-container") == []

    with fs.open(path="new-container/file.txt", mode="wb") as f:
        f.write(b"0123456789")

    with fs.open("new-container/dir/file.txt", "wb") as f:
        f.write(b"0123456789")

    with fs.open("new-container/dir/file2.txt", "wb") as f:
        f.write(b"0123456789")

    # Verify that mkdir will raise an exception if the directory exists
    # and exist_ok is False
    with pytest.raises(FileExistsError):
        fs.makedir("new-container/dir/file.txt", exist_ok=False)

    # mkdir should raise an error if the container exists and
    # we try to create a nested directory, with exist_ok=False
    with pytest.raises(FileExistsError):
        fs.makedir("new-container/dir2", exist_ok=False)

    # Check that trying to overwrite an existing nested file in append mode works as expected
    # if exist_ok is True
    fs.makedir("new-container/dir/file2.txt", exist_ok=True)
    assert "new-container/dir/file2.txt" in fs.ls("new-container/dir")

    # Also verify you can make a nested directory structure
    with fs.open("new-container/dir2/file.txt", "wb") as f:
        f.write(b"0123456789")
    assert "new-container/dir2/file.txt" in fs.ls("new-container/dir2")
    fs.rm("new-container/dir2", recursive=True)

    fs.rm("new-container/dir", recursive=True)
    fs.touch("new-container/file2.txt")
    assert fs.ls("new-container") == [
        "new-container/file.txt",
        "new-container/file2.txt",
    ]

    fs.rm("new-container/file.txt")
    fs.rm("new-container/file2.txt")
    fs.rmdir("new-container")

    assert "new-container" not in fs.ls("")


@pytest.mark.skip
def test_append_operation(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("append-container")

    # Check that appending to an existing file works as expected
    with fs.open("append-container/append_file.txt", "ab") as f:
        f.write(b"0123456789")
    with fs.open("append-container/append_file.txt", "ab") as f:
        f.write(b"0123456789")
    with fs.open("new-container/dir/file2.txt", "rb") as f:
        outfile = f.read()
    assert outfile == b"01234567890123456789"

    fs.rm("append-container", recursive=True)


def test_mkdir_rm_recursive(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    fs.mkdir("test-mkdir-rm-recursive")
    assert "test-mkdir-rm-recursive" in fs.ls("")

    with fs.open("test-mkdir-rm-recursive/file.txt", "wb") as f:
        f.write(b"0123456789")

    with fs.open("test-mkdir-rm-recursive/dir/file.txt", "wb") as f:
        f.write(b"ABCD")

    with fs.open("test-mkdir-rm-recursive/dir/file2.txt", "wb") as f:
        f.write(b"abcdef")

    assert fs.find("test-mkdir-rm-recursive") == [
        "test-mkdir-rm-recursive/dir/file.txt",
        "test-mkdir-rm-recursive/dir/file2.txt",
        "test-mkdir-rm-recursive/file.txt",
    ]

    fs.rm("test-mkdir-rm-recursive", recursive=True)

    assert "test-mkdir-rm-recursive" not in fs.ls("")
    # assert fs.find("test-mkdir-rm-recursive") == []


def test_deep_paths(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    fs.mkdir("test-deep")
    assert "test-deep" in fs.ls("")

    with fs.open("test-deep/a/b/c/file.txt", "wb") as f:
        f.write(b"0123456789")

    assert fs.ls("test-deep") == ["test-deep/a"]
    assert fs.ls("test-deep/") == ["test-deep/a"]
    assert fs.ls("test-deep/a") == ["test-deep/a/b"]
    assert fs.ls("test-deep/a/") == ["test-deep/a/b"]
    assert fs.find("test-deep") == ["test-deep/a/b/c/file.txt"]
    assert fs.find("test-deep/") == ["test-deep/a/b/c/file.txt"]
    assert fs.find("test-deep/a") == ["test-deep/a/b/c/file.txt"]
    assert fs.find("test-deep/a/") == ["test-deep/a/b/c/file.txt"]

    fs.rm("test-deep", recursive=True)

    assert "test-deep" not in fs.ls("")
    assert fs.find("test-deep") == []


def test_large_blob(storage):
    import hashlib
    import io
    import shutil
    from pathlib import Path

    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    # create a 20MB byte array, ensure it's larger than blocksizes to force a
    # chuncked upload
    blob_size = 120_000_000
    # blob_size = 2_684_354_560
    assert blob_size > fs.blocksize
    assert blob_size > AzureBlobFile.DEFAULT_BLOCK_SIZE

    data = b"1" * blob_size
    _hash = hashlib.md5(data)
    expected = _hash.hexdigest()

    # create container
    fs.mkdir("chunk-container")

    # upload the data using fs.open
    path = "chunk-container/large-blob.bin"
    with fs.open(path, "ab") as dst:
        dst.write(data)

    assert fs.exists(path)
    assert fs.size(path) == blob_size

    del data

    # download with fs.open
    bio = io.BytesIO()
    with fs.open(path, "rb") as src:
        shutil.copyfileobj(src, bio)

    # read back the data and calculate md5
    bio.seek(0)
    data = bio.read()
    _hash = hashlib.md5(data)
    result = _hash.hexdigest()

    assert expected == result

    # do the same but using upload/download and a tempdir
    path = path = "chunk-container/large_blob2.bin"
    with tempfile.TemporaryDirectory() as td:
        local_blob: Path = Path(td) / "large_blob2.bin"
        with local_blob.open("wb") as fo:
            fo.write(data)
        assert local_blob.exists()
        assert local_blob.stat().st_size == blob_size

        fs.upload(str(local_blob), path)
        assert fs.exists(path)
        assert fs.size(path) == blob_size

        # download now
        local_blob.unlink()
        fs.download(path, str(local_blob))
        assert local_blob.exists()
        assert local_blob.stat().st_size == blob_size


def test_large_upload_overflow(storage):
    import hashlib
    import io
    import shutil
    from pathlib import Path

    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    # create a 3 GB byte array to check if SSL overflow error occurs
    blob_size = 3 * 1024**3
    # blob_size = 3 GB

    data = b"1" * blob_size
    _hash = hashlib.md5(data)
    expected = _hash.hexdigest()

    # create container
    fs.mkdir("chunk-container")

    # upload the data using fs.open
    path = "chunk-container/large-upload.bin"
    with fs.open(path, "ab") as dst:
        dst.write(data)

    assert fs.exists(path)
    assert fs.size(path) == blob_size

    del data

    # download with fs.open
    bio = io.BytesIO()
    with fs.open(path, "rb") as src:
        shutil.copyfileobj(src, bio)

    # read back the data and calculate md5
    bio.seek(0)
    data = bio.read()
    _hash = hashlib.md5(data)
    result = _hash.hexdigest()

    assert expected == result

    # do the same but using upload/download and a tempdir
    path = path = "chunk-container/large_upload2.bin"
    with tempfile.TemporaryDirectory() as td:
        local_blob: Path = Path(td) / "large_upload2.bin"
        with local_blob.open("wb") as fo:
            fo.write(data)
        assert local_blob.exists()
        assert local_blob.stat().st_size == blob_size

        fs.upload(str(local_blob), path)
        assert fs.exists(path)
        assert fs.size(path) == blob_size

        # download now
        local_blob.unlink()
        fs.download(path, str(local_blob))
        assert local_blob.exists()
        assert local_blob.stat().st_size == blob_size


def test_dask_parquet(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("test")
    STORAGE_OPTIONS = {
        "account_name": "devstoreaccount1",
        "connection_string": CONN_STR,
    }
    df = pd.DataFrame(
        {
            "col1": [1, 2, 3, 4],
            "col2": [2, 4, 6, 8],
            "index_key": [1, 1, 2, 2],
            "partition_key": [1, 1, 2, 2],
        }
    )

    dask_dataframe = dd.from_pandas(df, npartitions=1)
    for protocol in ["abfs", "az"]:
        dask_dataframe.to_parquet(
            "{}://test/test_group.parquet".format(protocol),
            storage_options=STORAGE_OPTIONS,
            engine="pyarrow",
            write_metadata_file=True,
        )

        fs = AzureBlobFileSystem(**STORAGE_OPTIONS)
        assert fs.ls("test/test_group.parquet") == [
            "test/test_group.parquet/_common_metadata",
            "test/test_group.parquet/_metadata",
            "test/test_group.parquet/part.0.parquet",
        ]
        fs.rm("test/test_group.parquet")

    df_test = dd.read_parquet(
        "abfs://test/test_group.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df, df_test)

    A = np.random.randint(0, 100, size=(10000, 4))
    df2 = pd.DataFrame(data=A, columns=list("ABCD"))
    ddf2 = dd.from_pandas(df2, npartitions=4)
    dd.to_parquet(
        ddf2,
        "abfs://test/test_group2.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
        write_metadata_file=True,
    )
    assert fs.ls("test/test_group2.parquet") == [
        "test/test_group2.parquet/_common_metadata",
        "test/test_group2.parquet/_metadata",
        "test/test_group2.parquet/part.0.parquet",
        "test/test_group2.parquet/part.1.parquet",
        "test/test_group2.parquet/part.2.parquet",
        "test/test_group2.parquet/part.3.parquet",
    ]
    df2_test = dd.read_parquet(
        "abfs://test/test_group2.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df2, df2_test)

    a = np.full(shape=(10000, 1), fill_value=1)
    b = np.full(shape=(10000, 1), fill_value=2)
    c = np.full(shape=(10000, 1), fill_value=3)
    d = np.full(shape=(10000, 1), fill_value=4)
    B = np.concatenate((a, b, c, d), axis=1)
    df3 = pd.DataFrame(data=B, columns=list("ABCD"))
    ddf3 = dd.from_pandas(df3, npartitions=4)
    dd.to_parquet(
        ddf3,
        "abfs://test/test_group3.parquet",
        partition_on=["A", "B"],
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
        write_metadata_file=True,
    )
    assert fs.glob("test/test_group3.parquet/*") == [
        "test/test_group3.parquet/A=1",
        "test/test_group3.parquet/_common_metadata",
        "test/test_group3.parquet/_metadata",
    ]
    df3_test = dd.read_parquet(
        "abfs://test/test_group3.parquet",
        filters=[("A", "=", 1)],
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    df3_test = df3_test[["A", "B", "C", "D"]]
    df3_test = df3_test[["A", "B", "C", "D"]].astype(int)
    assert_frame_equal(df3, df3_test)

    A = np.random.randint(0, 100, size=(10000, 4))
    df4 = pd.DataFrame(data=A, columns=list("ABCD"))
    ddf4 = dd.from_pandas(df4, npartitions=4)
    dd.to_parquet(
        ddf4,
        "abfs://test/test_group4.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
        flavor="spark",
        write_statistics=False,
        write_metadata_file=True,
    )
    fs.rmdir("test/test_group4.parquet/_common_metadata", recursive=True)
    fs.rmdir("test/test_group4.parquet/_metadata", recursive=True)
    fs.rm("test/test_group4.parquet/_common_metadata")
    fs.rm("test/test_group4.parquet/_metadata")
    assert fs.ls("test/test_group4.parquet") == [
        "test/test_group4.parquet/part.0.parquet",
        "test/test_group4.parquet/part.1.parquet",
        "test/test_group4.parquet/part.2.parquet",
        "test/test_group4.parquet/part.3.parquet",
    ]
    df4_test = dd.read_parquet(
        "abfs://test/test_group4.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df4, df4_test)

    A = np.random.randint(0, 100, size=(10000, 4))
    df5 = pd.DataFrame(data=A, columns=list("ABCD"))
    ddf5 = dd.from_pandas(df5, npartitions=4)
    dd.to_parquet(
        ddf5,
        "abfs://test/test group5.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
        write_metadata_file=True,
    )
    assert fs.ls("test/test group5.parquet") == [
        "test/test group5.parquet/_common_metadata",
        "test/test group5.parquet/_metadata",
        "test/test group5.parquet/part.0.parquet",
        "test/test group5.parquet/part.1.parquet",
        "test/test group5.parquet/part.2.parquet",
        "test/test group5.parquet/part.3.parquet",
    ]
    df5_test = dd.read_parquet(
        "abfs://test/test group5.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df5, df5_test)


def test_metadata_write(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("test-metadata-write")
    data = b"0123456789"
    metadata = {"meta": "data"}

    # standard blob type
    with fs.open("test-metadata-write/file.txt", "wb", metadata=metadata) as f:
        f.write(data)
    info = fs.info("test-metadata-write/file.txt")
    assert info["metadata"] == metadata
    metadata_changed_on_write = {"meta": "datum"}
    with fs.open(
        "test-metadata-write/file.txt", "wb", metadata=metadata_changed_on_write
    ) as f:
        f.write(data)
    info = fs.info("test-metadata-write/file.txt")
    assert info["metadata"] == metadata_changed_on_write

    # append blob type
    new_metadata = {"data": "meta"}
    with fs.open("test-metadata-write/append-file.txt", "ab", metadata=metadata) as f:
        f.write(data)

    # try change metadata on block appending
    with fs.open(
        "test-metadata-write/append-file.txt", "ab", metadata=new_metadata
    ) as f:
        f.write(data)
    info = fs.info("test-metadata-write/append-file.txt")

    # azure blob client doesn't seem to support metadata mutation when appending blocks
    # lets be sure this behavior doesn't change as this would imply
    # a potential breaking change
    assert info["metadata"] == metadata

    # getxattr / setxattr
    assert fs.getxattr("test-metadata-write/file.txt", "meta") == "datum"
    fs.setxattrs("test-metadata-write/file.txt", metadata="data2")
    assert fs.getxattr("test-metadata-write/file.txt", "metadata") == "data2"
    assert fs.info("test-metadata-write/file.txt")["metadata"] == {"metadata": "data2"}

    # empty file and nested directory
    with fs.open(
        "test-metadata-write/a/b/c/nested-file.txt", "wb", metadata=metadata
    ) as f:
        f.write(b"")
    assert fs.getxattr("test-metadata-write/a/b/c/nested-file.txt", "meta") == "data"
    fs.setxattrs("test-metadata-write/a/b/c/nested-file.txt", metadata="data2")
    assert fs.info("test-metadata-write/a/b/c/nested-file.txt")["metadata"] == {
        "metadata": "data2"
    }
    fs.rmdir("test-metadata-write")


def test_put_file(storage, tmp_path):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("putdir")

    # Check that put on an empty file works
    src = tmp_path / "sample.txt"
    dest = tmp_path / "sample2.txt"
    src.write_bytes(b"")
    fs.put(str(src), "putdir/sample.txt")
    fs.get("putdir/sample.txt", str(dest))

    with open(src, "rb") as f:
        f1 = f.read()
    with open(dest, "rb") as f:
        f2 = f.read()
    assert f1 == f2

    # Check that put on a file with data works
    src2 = tmp_path / "sample3.txt"
    dest2 = tmp_path / "sample4.txt"
    src2.write_bytes(b"01234567890")
    fs.put(str(src2), "putdir/sample3.txt")
    fs.get("putdir/sample3.txt", str(dest2))

    with open(src2, "rb") as f:
        f3 = f.read()
    with open(dest2, "rb") as f:
        f4 = f.read()
    assert f3 == f4


def test_isdir(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.touch("data/root/a/file.txt")
    assert fs.isdir("data") is True
    assert fs.isdir("data/top_file.txt") is False
    assert fs.isdir("data/root") is True
    assert fs.isdir("data/root/") is True
    assert fs.isdir("data/root/rfile.txt") is False
    assert fs.isdir("data/root/a") is True
    assert fs.isdir("data/root/a/") is True


def test_isfile(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    assert fs.isfile("data") is False
    assert fs.isfile("data/top_file.txt") is True
    assert fs.isfile("data/root") is False
    assert fs.isfile("data/root/") is False
    assert fs.isfile("data/root/rfile.txt") is True
    fs.touch("data/root/null_file.txt")
    assert fs.isfile("data/root/null_file.txt") is True


async def test_isfile_versioned(storage, mocker):
    from azure.core.exceptions import HttpResponseError
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=True,
        skip_instance_cache=True,
    )
    get_blob_properties = mocker.patch.object(BlobClient, "get_blob_properties")

    await fs._isfile(f"data/root/a/file.txt?versionid={DEFAULT_VERSION_ID}")
    get_blob_properties.assert_called_once_with(version_id=DEFAULT_VERSION_ID)

    get_blob_properties.reset_mock()
    get_blob_properties.side_effect = HttpResponseError
    assert not await fs._isfile("data/root/a/file.txt?versionid=invalid_version")


def test_isdir(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.touch("data/root/a/file.txt")
    assert fs.isdir("data") is True
    assert fs.isdir("data/top_file.txt") is False
    assert fs.isdir("data/root") is True
    assert fs.isdir("data/root/") is True
    assert fs.isdir("data/root/rfile.txt") is False
    assert fs.isdir("data/root/a") is True
    assert fs.isdir("data/root/a/") is True


def test_isdir_cache(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    print("checking isdir cache")
    files = fs.ls("data/root")  # noqa: F841
    assert fs.isdir("data/root/a") is True
    assert fs.isdir("data/root/a/") is True
    assert fs.isdir("data/root/rfile.txt") is False


def test_cat(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("catdir")
    data = b"0123456789"
    with fs.open("catdir/catfile.txt", "wb") as f:
        f.write(data)
    result = fs.cat("catdir/catfile.txt")
    assert result == data
    fs.rm("catdir/catfile.txt")


def test_cat_file(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("catdir")
    data = b"0123456789"
    with fs.open("catdir/catfile.txt", "wb") as f:
        f.write(data)

    result = fs.cat_file("catdir/catfile.txt", start=1, end=2)
    assert result == b"1"

    result = fs.cat_file("catdir/catfile.txt", start=8)
    assert result == b"89"

    result = fs.cat_file("catdir/catfile.txt", end=2)
    assert result == b"01"

    result = fs.cat_file("abfs://catdir/catfile.txt")
    assert result == data
    fs.rm("catdir/catfile.txt")


def test_cat_file_missing(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("catdir")
    with pytest.raises(FileNotFoundError):
        fs.cat_file("catdir/not/exist")

    with pytest.raises(FileNotFoundError):
        fs.cat_file("does/not/exist")


async def test_cat_file_versioned(storage, mocker):
    from azure.core.exceptions import HttpResponseError
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=True,
        skip_instance_cache=True,
    )
    download_blob = mocker.patch.object(BlobClient, "download_blob")

    await fs._cat_file(f"data/root/a/file.txt?versionid={DEFAULT_VERSION_ID}")
    download_blob.assert_called_once_with(
        offset=None,
        length=None,
        version_id=DEFAULT_VERSION_ID,
        max_concurrency=fs.max_concurrency,
    )

    download_blob.reset_mock()
    download_blob.side_effect = HttpResponseError
    with pytest.raises(FileNotFoundError):
        await fs._cat_file("data/root/a/file.txt?versionid=invalid_version")


@pytest.mark.skip(
    reason="Bug in Azurite Storage Emulator v3.15.0 gives 403 status_code"
)
def test_url(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR, account_key=KEY
    )
    fs.mkdir("catdir")
    data = b"0123456789"
    with fs.open("catdir/catfile.txt", "wb") as f:
        f.write(data)

    import requests

    r = requests.get(fs.url("catdir/catfile.txt"))
    assert r.status_code == 200
    assert r.content == data

    fs.rm("catdir/catfile.txt")


async def test_url_versioned(storage, mocker):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        account_key=KEY,
        version_aware=True,
        skip_instance_cache=True,
    )
    generate_blob_sas = mocker.patch("adlfs.spec.generate_blob_sas")

    await fs._url(f"data/root/a/file.txt?versionid={DEFAULT_VERSION_ID}")
    generate_blob_sas.assert_called_once_with(
        account_name=storage.account_name,
        container_name="data",
        blob_name="root/a/file.txt",
        account_key=KEY,
        permission=mocker.ANY,
        expiry=mocker.ANY,
        version_id=DEFAULT_VERSION_ID,
        content_disposition=None,
        content_encoding=None,
        content_language=None,
        content_type=None,
    )


def test_cp_file(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("homedir")
    fs.mkdir("homedir/enddir")
    fs.touch("homedir/startdir/test_file.txt")
    fs.cp_file("homedir/startdir/test_file.txt", "homedir/enddir/test_file.txt")
    files = fs.ls("homedir/enddir")
    assert "homedir/enddir/test_file.txt" in files

    fs.rm("homedir", recursive=True)


async def test_cp_file_versioned(storage, mocker):
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=True,
        skip_instance_cache=True,
    )
    fs.mkdir("homedir")
    fs.mkdir("homedir/enddir")
    fs.touch("homedir/startdir/test_file.txt")
    start_copy_from_url = mocker.patch.object(BlobClient, "start_copy_from_url")

    try:
        await fs._cp_file(
            f"homedir/startdir/test_file.txt?versionid={DEFAULT_VERSION_ID}",
            "homedir/enddir/test_file.txt",
        )
        start_copy_from_url.assert_called_once()
        url = start_copy_from_url.call_args.args[0]
        assert url.endswith(f"?versionid={DEFAULT_VERSION_ID}")
    finally:
        fs.rm("homedir", recursive=True)


def test_exists(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    assert fs.exists("data/top_file.txt")
    assert fs.exists("data")
    assert fs.exists("data/")
    assert not fs.exists("non-existent-container")
    assert not fs.exists("non-existent-container/")
    assert not fs.exists("non-existent-container/key")
    assert fs.exists("")
    assert not fs.exists("data/not-a-key")


def test_exists_directory(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    fs.mkdir("temp-exists")
    fs.touch("temp-exists/data/data.txt")
    fs.touch("temp-exists/data/something/data.txt")
    fs.invalidate_cache()

    assert fs.exists("temp-exists/data/something/")
    assert fs.exists("temp-exists/data/something")
    assert fs.exists("temp-exists/data/")
    assert fs.exists("temp-exists/data")
    assert fs.exists("temp-exists/")
    assert fs.exists("temp-exists")


async def test_exists_versioned(storage, mocker):
    from azure.core.exceptions import HttpResponseError
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=True,
        skip_instance_cache=True,
    )
    exists = mocker.patch.object(BlobClient, "exists")

    await fs._exists(f"data/root/a/file.txt?versionid={DEFAULT_VERSION_ID}")
    exists.assert_called_once_with(version_id=DEFAULT_VERSION_ID)

    exists.reset_mock()
    exists.side_effect = HttpResponseError
    assert not await fs._exists("data/root/a/file.txt?versionid=invalid_version")


def test_find_with_prefix(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    test_bucket_name = "data"

    for cursor in range(25):
        fs.touch(test_bucket_name + f"/prefixes/test_{cursor}")

    fs.touch(test_bucket_name + "/prefixes2")
    assert len(fs.find(test_bucket_name + "/prefixes")) == 25
    assert len(fs.find(test_bucket_name, prefix="prefixes")) == 26

    # This expects `path` to be either a discrete file or a directory
    assert len(fs.find(test_bucket_name + "/prefixes/test_")) == 0
    assert len(fs.find(test_bucket_name + "/prefixes", prefix="test_")) == 25
    assert len(fs.find(test_bucket_name + "/prefixes/", prefix="test_")) == 25

    test_1s = fs.find(test_bucket_name + "/prefixes/test_1")
    assert len(test_1s) == 1
    assert test_1s[0] == test_bucket_name + "/prefixes/test_1"

    test_1s = fs.find(test_bucket_name + "/prefixes/", prefix="test_1")
    assert len(test_1s) == 11
    assert test_1s == [test_bucket_name + "/prefixes/test_1"] + [
        test_bucket_name + f"/prefixes/test_{cursor}" for cursor in range(10, 20)
    ]


@pytest.mark.parametrize("proto", [None, "abfs://", "az://"])
@pytest.mark.parametrize("path", ["container/file", "container/file?versionid=1234"])
def test_strip_protocol(proto, path):
    assert (
        AzureBlobFileSystem._strip_protocol(f"{proto}{path}" if proto else path) == path
    )


@pytest.mark.parametrize("proto", ["", "abfs://", "az://"])
@pytest.mark.parametrize("key", ["file", "dir/file"])
@pytest.mark.parametrize("version_aware", [True, False])
@pytest.mark.parametrize("version_id", [None, "1970-01-01T00:00:00.0000000Z"])
def test_split_path(storage, proto, key, version_aware, version_id):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=version_aware,
        skip_instance_cache=True,
    )
    path = "".join(
        [proto, "container/", key, f"?versionid={version_id}" if version_id else ""]
    )
    assert fs.split_path(path) == (
        "container",
        key,
        version_id if version_aware else None,
    )


async def test_details_versioned(storage):
    from azure.storage.blob import BlobProperties

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=True,
        skip_instance_cache=True,
    )

    path = "root/a/file.txt"

    blob_unversioned = BlobProperties(name=path)

    blob_latest = BlobProperties(name=path)
    blob_latest["version_id"] = LATEST_VERSION_ID
    blob_latest["is_current_version"] = True

    blob_previous = BlobProperties(name=path)
    blob_previous["version_id"] = DEFAULT_VERSION_ID
    blob_previous["is_current_version"] = None

    await fs._details(
        [blob_unversioned, blob_latest, blob_previous],
        target_path=path,
        version_id=None,
    ) == [blob_unversioned, blob_latest]
    await fs._details(
        [blob_unversioned, blob_latest, blob_previous],
        target_path=path,
        version_id=DEFAULT_VERSION_ID,
    ) == [blob_previous]
    await fs._details(
        [blob_unversioned, blob_latest, blob_previous],
        target_path=path,
        version_id=LATEST_VERSION_ID,
    ) == [blob_latest]


async def test_get_file_versioned(storage, mocker, tmp_path):
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        version_aware=True,
        skip_instance_cache=True,
    )
    download_blob = mocker.patch.object(BlobClient, "download_blob")

    await fs._get_file(
        f"data/root/a/file.txt?versionid={DEFAULT_VERSION_ID}", tmp_path / "file.txt"
    )
    download_blob.assert_called_once_with(
        raw_response_hook=mocker.ANY,
        version_id=DEFAULT_VERSION_ID,
        max_concurrency=fs.max_concurrency,
    )
    download_blob.reset_mock()
    download_blob.side_effect = ResourceNotFoundError

    dest = tmp_path / "badfile.txt"
    with pytest.raises(FileNotFoundError):
        await fs._get_file("data/root/a/file.txt?versionid=invalid_version", dest)
    assert not dest.exists()


async def test_cat_file_timeout(storage, mocker):
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )
    download_blob = mocker.patch.object(BlobClient, "download_blob")

    await fs._cat_file("data/root/a/file.txt")
    download_blob.assert_called_once_with(
        offset=None,
        length=None,
        max_concurrency=fs.max_concurrency,
        version_id=None,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )


async def test_get_file_timeout(storage, mocker, tmp_path):
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )
    download_blob = mocker.patch.object(BlobClient, "download_blob")

    await fs._get_file("data/root/a/file.txt", str(tmp_path / "out"))
    download_blob.assert_called_once_with(
        raw_response_hook=None,
        max_concurrency=fs.max_concurrency,
        version_id=None,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )


async def test_pipe_file_timeout(storage, mocker):
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )
    upload_blob = mocker.patch.object(BlobClient, "upload_blob")

    await fs._pipe_file("putdir/pipefiletimeout", b"data")
    upload_blob.assert_called_once_with(
        data=b"data",
        metadata={"is_directory": "false"},
        overwrite=True,
        max_concurrency=fs.max_concurrency,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )


async def test_put_file_timeout(storage, mocker, tmp_path):
    from azure.storage.blob.aio import BlobClient

    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )
    upload_blob = mocker.patch.object(BlobClient, "upload_blob")

    src = tmp_path / "putfiletimeout"
    src.write_bytes(b"data")

    await fs._put_file(str(src), "putdir/putfiletimeout")
    upload_blob.assert_called_once_with(
        mocker.ANY,
        metadata={"is_directory": "false"},
        overwrite=True,
        raw_response_hook=None,
        max_concurrency=fs.max_concurrency,
        timeout=11,
        connection_timeout=12,
        read_timeout=13,
    )


@pytest.mark.parametrize("key", ["hdi_isfolder", "Hdi_isfolder"])
def test_hdi_isfolder_case(storage: azure.storage.blob.BlobServiceClient, key: str):
    cc = storage.get_container_client("data")
    cc.upload_blob(b"folder", b"", metadata={key: "true"}, overwrite=True)

    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    result = fs.info("data/folder")
    assert result["type"] == "directory"
