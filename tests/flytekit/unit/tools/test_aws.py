from flytekit.interfaces.data.s3.s3proxy import AwsS3Proxy


def test_aws_s3_splitting():
    (bucket, key) = AwsS3Proxy._split_s3_path_to_bucket_and_key("s3://bucket/some/key")
    assert bucket == "bucket"
    assert key == "some/key"
