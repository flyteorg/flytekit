import tarfile

from flytekit.tools.fast_registration import get_additional_distribution_loc


def test_get_additional_distribution_loc():
    assert get_additional_distribution_loc("s3://my-s3-bucket/dir", "123abc") == "s3://my-s3-bucket/dir/123abc.tar.gz"
