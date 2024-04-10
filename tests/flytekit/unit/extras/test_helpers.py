from flytekit.extras.helpers import generate_remote_path, rand_str


def test_rand_str():
    assert len(rand_str(10)) == 10


def test_generate_remote_path():
    try:
        generate_remote_path("s3://bucket", "file")
    except KeyError as e:
        assert str(e) == "HOSTNAME environment variable not set"
