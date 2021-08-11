import mock

from flytekit import S3Persistence
from flytekit.extras.persistence import s3_awscli


def test_property():
    aws = S3Persistence("s3://raw-output")
    assert aws.default_prefix == "s3://raw-output"


def test_construct_path():
    aws = S3Persistence()
    p = aws.construct_path(True, False, "xyz")
    assert p == "s3://xyz"


@mock.patch("flytekit.extras.persistence.s3_awscli.S3Persistence._check_binary")
@mock.patch("flytekit.configuration.aws.BACKOFF_SECONDS")
@mock.patch("flytekit.extras.persistence.s3_awscli._subprocess")
def test_retries(mock_subprocess, mock_delay, mock_check):
    mock_delay.get.return_value = 0
    mock_subprocess.check_call.side_effect = Exception("test exception (404)")
    mock_check.return_value = True

    proxy = S3Persistence()
    assert proxy.exists("s3://test/fdsa/fdsa") is False
    assert mock_subprocess.check_call.call_count == 4


def test_extra_args():
    assert s3_awscli._extra_args({}) == []
    assert s3_awscli._extra_args({"ContentType": "ct"}) == ["--content-type", "ct"]
    assert s3_awscli._extra_args({"ContentEncoding": "ec"}) == ["--content-encoding", "ec"]
    assert s3_awscli._extra_args({"ACL": "acl"}) == ["--acl", "acl"]
    assert s3_awscli._extra_args({"ContentType": "ct", "ContentEncoding": "ec", "ACL": "acl"}) == [
        "--content-type",
        "ct",
        "--content-encoding",
        "ec",
        "--acl",
        "acl",
    ]


@mock.patch("flytekit.extras.persistence.s3_awscli._update_cmd_config_and_execute")
def test_put(mock_exec):
    proxy = S3Persistence()
    proxy.put("/test", "s3://my-bucket/k1")
    mock_exec.assert_called_with(
        ["aws", "s3", "cp", "--acl", "bucket-owner-full-control", "/test", "s3://my-bucket/k1"]
    )


@mock.patch("flytekit.extras.persistence.s3_awscli._update_cmd_config_and_execute")
def test_put_recursive(mock_exec):
    proxy = S3Persistence()
    proxy.put("/test", "s3://my-bucket/k1", True)
    mock_exec.assert_called_with(
        ["aws", "s3", "cp", "--recursive", "--acl", "bucket-owner-full-control", "/test", "s3://my-bucket/k1"]
    )


@mock.patch("flytekit.extras.persistence.s3_awscli._update_cmd_config_and_execute")
def test_get(mock_exec):
    proxy = S3Persistence()
    proxy.get("s3://my-bucket/k1", "/test")
    mock_exec.assert_called_with(["aws", "s3", "cp", "s3://my-bucket/k1", "/test"])


@mock.patch("flytekit.extras.persistence.s3_awscli._update_cmd_config_and_execute")
def test_get_recursive(mock_exec):
    proxy = S3Persistence()
    proxy.get("s3://my-bucket/k1", "/test", True)
    mock_exec.assert_called_with(["aws", "s3", "cp", "--recursive", "s3://my-bucket/k1", "/test"])
