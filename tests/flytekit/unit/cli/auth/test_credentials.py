from flytekit.clis.auth import credentials as _credentials


def test_is_absolute():
    assert _credentials._is_absolute("http://localhost:9000/my_endpoint")
    assert _credentials._is_absolute("/my_endpoint") is False
