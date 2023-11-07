import pathlib

from flytekit.types.structured.basic_dfs import _path_from_uri


def test_path_from_uri() -> None:
    assert _path_from_uri("file:///test") == pathlib.Path("/test")
    assert _path_from_uri("/test") == pathlib.Path("/test")
