import pytest

from flytekit import HttpPersistence


def test_put():
    proxy = HttpPersistence()
    with pytest.raises(AssertionError):
        proxy.put("", "", recursive=True)


def test_construct_path():
    proxy = HttpPersistence()
    with pytest.raises(AssertionError):
        proxy.construct_path(True, False, "", "")


def test_exists():
    proxy = HttpPersistence()
    assert proxy.exists("https://flyte.org")
