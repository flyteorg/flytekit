from __future__ import absolute_import

import os as _os

import mock as _mock
import pytest as _pytest
from flytekit.interfaces.data.gcs import gcs_proxy as _gcs_proxy


@_pytest.fixture
def mock_update_cmd_config_and_execute():
    p = _mock.patch("flytekit.interfaces.data.gcs.gcs_proxy._update_cmd_config_and_execute")
    yield p.start()
    p.stop()


@_pytest.fixture
def gcs_proxy():
    return _gcs_proxy.GCSProxy()


def test_upload_directory(mock_update_cmd_config_and_execute, gcs_proxy):
    local_path, remote_path = "/foo/*", "gs://bar/0/"
    gcs_proxy.upload_directory(local_path, remote_path)
    mock_update_cmd_config_and_execute.assert_called_once_with(
        ["gsutil", "cp", "-r", local_path, remote_path]
    )


def test_upload_directory_padding_wildcard_for_local_path(
    mock_update_cmd_config_and_execute, gcs_proxy
):
    local_path, remote_path = "/foo", "gs://bar/0/"
    gcs_proxy.upload_directory(local_path, remote_path)
    mock_update_cmd_config_and_execute.assert_called_once_with(
        ["gsutil", "cp", "-r", _os.path.join(local_path, "*"), remote_path]
    )


def test_upload_directory_padding_slash_for_remote_path(
    mock_update_cmd_config_and_execute, gcs_proxy
):
    local_path, remote_path = "/foo/*", "gs://bar/0"
    gcs_proxy.upload_directory(local_path, remote_path)
    mock_update_cmd_config_and_execute.assert_called_once_with(
        ["gsutil", "cp", "-r", local_path, remote_path + "/"]
    )
