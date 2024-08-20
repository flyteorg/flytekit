import pathlib
import pytest
import tempfile

from flytekit.tools.script_mode import ls_files


# a pytest fixture that creates a tmp directory and creates
# a small file structure in it
@pytest.fixture
def dummy_dir_structure():
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmp_path:

        # Create directories
        tmp_path = pathlib.Path(tmp_path)
        subdir1 = tmp_path / "subdir1"
        subdir2 = tmp_path / "subdir2"
        subdir1.mkdir()
        subdir2.mkdir()

        # Create files in the root of the temporary directory
        (tmp_path / "file1.txt").write_text("This is file 1")
        (tmp_path / "file2.txt").write_text("This is file 2")

        # Create files in subdir1
        (subdir1 / "file3.txt").write_text("This is file 3 in subdir1")
        (subdir1 / "file4.txt").write_text("This is file 4 in subdir1")

        # Create files in subdir2
        (subdir2 / "file5.txt").write_text("This is file 5 in subdir2")

        # Return the path to the temporary directory
        yield tmp_path


def test_list_dir(dummy_dir_structure):
    files, d = ls_files(str(dummy_dir_structure), [])
    assert len(files) == 5
    assert d == "c092f1b85f7c6b2a71881a946c00a855"
