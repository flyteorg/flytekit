import pytest

from flytekit.interfaces.data.data_proxy import FileAccessProvider


@pytest.mark.parametrize("file_path_or_file_name", [None, "file_name", "dir/file_name"])
def test_get_random_local_path(tmpdir, file_path_or_file_name):
    file_access = FileAccessProvider(tmpdir)
    local_path = file_access.get_random_local_path(file_path_or_file_name)
    for _ in range(10):
        assert local_path == file_access.get_random_local_path(file_path_or_file_name)

    # this just uses a local proxy
    remote_path = file_access.get_random_remote_path(file_path_or_file_name)
    for _ in range(10):
        assert remote_path == file_access.get_random_remote_path(file_path_or_file_name)
