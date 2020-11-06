import tarfile
from pathlib import Path

from flytekit.tools.fast_registration import _filter_tar_file_fn, compute_digest, get_additional_distribution_loc


def test_compute_digest():
    test_path = Path.joinpath(Path(__file__).parent.absolute(), "testdata")
    digest = compute_digest(test_path)
    assert digest == "b9c5465a43c6d99f2efce3185ef11440"


def test_filter_tar_file_fn():
    valid_tarinfo = tarfile.TarInfo(name="foo.py")
    assert _filter_tar_file_fn(valid_tarinfo) is not None

    invalid_tarinfo = tarfile.TarInfo(name="foo.pyc")
    assert not _filter_tar_file_fn(invalid_tarinfo)

    invalid_tarinfo = tarfile.TarInfo(name=".cache/foo")
    assert not _filter_tar_file_fn(invalid_tarinfo)

    invalid_tarinfo = tarfile.TarInfo(name="__pycache__")
    assert not _filter_tar_file_fn(invalid_tarinfo)


def test_get_additional_distribution_loc():
    assert get_additional_distribution_loc("s3://my-s3-bucket/dir", "123abc") == "s3://my-s3-bucket/dir/123abc.tar.gz"
