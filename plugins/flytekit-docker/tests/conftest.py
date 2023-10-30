import pytest
from pathlib import Path
import shutil


@pytest.fixture
def test_data_dir(tmpdir):
    """
    copy test_data dir to pytest tmpdir
    """
    test_file_dir_name = Path("test_files")
    test_file_dir_source = Path(__file__).parent / test_file_dir_name
    return shutil.copytree(test_file_dir_source, Path(tmpdir.strpath) / test_file_dir_name)
