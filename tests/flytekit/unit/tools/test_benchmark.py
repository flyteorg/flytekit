import pytest
import subprocess
import tarfile

from flytekit.tools.ignore import GitIgnore, GitIgnoreV1, IgnoreGroup

from tests.flytekit.unit.tools.test_ignore import make_tree


def create_archive_v1(source: str, name: str) -> None:
    ignore = IgnoreGroup(source, [GitIgnoreV1])
    with tarfile.open(name, "w:gz") as tar:
        tar.add(source, arcname="", filter=ignore.tar_filter)


def create_archive_v2(source: str, name: str) -> None:
    ignore = IgnoreGroup(source, [GitIgnore])
    with tarfile.open(name, "w:gz") as tar:
        tar.add(source, arcname="", filter=ignore.tar_filter)


@pytest.fixture
def flyte_project(tmp_path):
    tree = {
        "data": {"large.file": "", "more.files": ""},
        "src": {
            "workflows": {
                "__pycache__": {"some.pyc": ""},
                "hello_world.py": "print('Hello World!')",
            }
        },
        ".venv": {"lots": "", "of": "", "packages": ""},
        ".env": "supersecret",
        "some.bar": "",
        "some.foo": "",
        "keep.foo": "",
        ".gitignore": "\n".join([".env", ".venv", "# A comment", "data", "*.foo", "!keep.foo"]),
        ".dockerignore": "\n".join(["data", "*.bar", ".git"]),
    }

    make_tree(tmp_path, tree)
    subprocess.run(["git", "init", tmp_path])
    return tmp_path


def test_v1(benchmark, flyte_project, tmp_path):
    archive_fname = tmp_path / "archive.tar.gz"
    benchmark(create_archive_v1, source=flyte_project, name=archive_fname)


def test_v2(benchmark, flyte_project, tmp_path):
    archive_fname = tmp_path / "archive.tar.gz"
    benchmark(create_archive_v2, source=flyte_project, name=archive_fname)
