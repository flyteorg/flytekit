import subprocess
import tarfile

import pytest

from flytekit.tools.package_helpers import FAST_FILEENDING, create_archive, compute_digest, FAST_PREFIX
from tests.flytekit.unit.tools.test_ignore import make_tree
from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore


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
    archive_fname = create_archive(source=flyte_project, output_dir=tmp_path)
    with tarfile.open(archive_fname) as tar:
        assert tar.getnames() == [
            "",  # tar root, output removes leading '/'
            ".dockerignore",
            ".gitignore",
            ".venv",
            "keep.foo",
            "src",
            "src/workflows",
            "src/workflows/hello_world.py",
        ]
    assert str(archive_fname).startswith(FAST_PREFIX)
    assert str(archive_fname).endswith(FAST_FILEENDING)


def test_digest_ignore(flyte_project, tmp_path):
    ignore = IgnoreGroup(flyte_project, [GitIgnore, DockerIgnore, StandardIgnore])
    ignored_files = ignore.list_ignored()
    digest1 = compute_digest(flyte_project, ignored_files)
    
    change_file = flyte_project / "data" / "large.file"
    assert ignore.is_ignored(change_file)
    change_file.write_text("I don't matter")
    
    digest2 = compute_digest(flyte_project, ignored_files)
    assert digest1 == digest2
