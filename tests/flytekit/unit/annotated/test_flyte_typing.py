from flytekit.typing import FlyteFilePath


def test_filepath_equality():
    a = FlyteFilePath("/tmp")
    b = FlyteFilePath("/tmp")
    assert a == b

    a = FlyteFilePath("/tmp")
    b = FlyteFilePath["pdf"]("/tmp")
    assert a != b

    a = FlyteFilePath("/tmp")
    b = FlyteFilePath("/tmp/c")
    assert a != b

    x = "jpg"
    y = ".jpg"
    a = FlyteFilePath[x]("/tmp")
    b = FlyteFilePath[y]("/tmp")
    assert a == b


def test_fdff():
    a = FlyteFilePath["txt"]("/tmp")
    print(a)
    b = FlyteFilePath["txt"]("/tmp")
    assert a == b
