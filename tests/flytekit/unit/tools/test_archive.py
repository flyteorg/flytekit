import subprocess
import tarfile

import pytest

from flytekit.tools.package_helpers import create_archive
from tests.flytekit.unit.tools.test_ignore import make_tree


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


def test_archive(flyte_project, tmp_path):
    archive_fname = tmp_path / "archive.tar.gz"
    create_archive(source=flyte_project, name=archive_fname)
    with tarfile.open(archive_fname) as tar:
        assert tar.getnames() == [
            "",  # tar root, output removes leading '/'
            ".dockerignore",
            ".gitignore",
            "keep.foo",
            "src",
            "src/workflows",
            "src/workflows/hello_world.py",
        ]
