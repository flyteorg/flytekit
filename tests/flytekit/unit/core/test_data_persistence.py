from flytekit.core.data_persistence import DataPersistencePlugins, FileAccessProvider


def test_get_random_remote_path():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    path = fp.get_random_remote_path()
    assert path.startswith("s3://my-bucket")
    assert fp.raw_output_prefix == "s3://my-bucket"


def test_is_remote():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    assert fp.is_remote("./checkpoint") is False
    assert fp.is_remote("/tmp/foo/bar") is False
    assert fp.is_remote("file://foo/bar") is False
    assert fp.is_remote("s3://my-bucket/foo/bar") is True


def test_lister():
    x = DataPersistencePlugins.supported_protocols()
    main_protocols = {"file", "/", "gs", "http", "https", "s3"}
    all_protocols = set([y.replace("://", "") for y in x])
    assert main_protocols.issubset(all_protocols)
