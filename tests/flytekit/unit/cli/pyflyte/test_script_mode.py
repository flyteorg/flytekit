import os
import pathlib
import pytest
import tempfile

from flytekit.tools.script_mode import ls_files
from flytekit.constants import CopyFileDetection


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
    files, d = ls_files(str(dummy_dir_structure), CopyFileDetection.ALL)
    assert len(files) == 5
    if os.name != "nt":
        assert d == "b6907fd823a45e26c780a4ba62111243"


def test_list_filtered_on_modules(dummy_dir_structure):
    # any module will do
    import sys  # noqa
    files, d = ls_files(str(dummy_dir_structure), CopyFileDetection.LOADED_MODULES)
    # because none of the files are python modules, nothing should be returned
    assert len(files) == 0
    if os.name != "nt":
        assert d == "d41d8cd98f00b204e9800998ecf8427e"
