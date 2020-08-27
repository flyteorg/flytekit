import mock as _mock

from flytekit.interfaces.data.s3.s3proxy import AwsS3Proxy as _AwsS3Proxy


def test_property():
    aws = _AwsS3Proxy("s3://raw-output")
    assert aws.raw_output_data_prefix_override == "s3://raw-output"


@_mock.patch("flytekit.configuration.aws.S3_SHARD_FORMATTER")
def test_random_path(mock_formatter):
    mock_formatter.get.return_value = "s3://flyte/{}/"

    # Without raw output data prefix override
    aws = _AwsS3Proxy()
    p = str(aws.get_random_path())
    assert p.startswith("s3://flyte")

    # With override
    aws = _AwsS3Proxy("s3://raw-output")
    p = str(aws.get_random_path())
    assert p.startswith("s3://raw-output")
