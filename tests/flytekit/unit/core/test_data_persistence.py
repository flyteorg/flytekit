from flytekit.core.data_persistence import FileAccessProvider


def test_get_random_remote_path():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    path = fp.get_random_remote_path()
    assert path.startswith("s3://my-bucket")
