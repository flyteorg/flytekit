from flytekit.typing import FlyteFilePath, Text


def test_filepath_equality():
    a = FlyteFilePath("/tmp")
    b = FlyteFilePath("/tmp")
    assert a == b

    a = FlyteFilePath("/tmp")
    b = FlyteFilePath[int]("/tmp")
    assert a != b

    a = FlyteFilePath("/tmp")
    b = FlyteFilePath("/tmp/")
    assert a == b

    a = FlyteFilePath("/tmp")
    b = FlyteFilePath("/tmp/c")
    assert a != b

    a = FlyteFilePath[Text]("/tmp")
    b = FlyteFilePath[str]("/tmp")
    assert a != b

    alias = Text
    a = FlyteFilePath[Text]("/tmp")
    b = FlyteFilePath[alias]("/tmp")
    assert a == b
