import os
import shutil
import subprocess

import mock
from click.testing import CliRunner

from flytekit.clients.friendly import SynchronousFlyteClient
from flytekit.clis.sdk_in_container import pyflyte
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.remote.remote import FlyteRemote

sample_file_contents = """
from flytekit import task, workflow

@task(cache=True, cache_version="1", retries=3)
def sum(x: int, y: int) -> int:
    return x + y

@task(cache=True, cache_version="1", retries=3)
def square(z: int) -> int:
    return z*z

@workflow
def my_workflow(x: int, y: int) -> int:
    return sum(x=square(z=x), y=square(z=y))
"""


@mock.patch("flytekit.clis.sdk_in_container.helpers.FlyteRemote")
def test_saving_remote(mock_remote):
    mock_context = mock.MagicMock
    mock_context.obj = {}
    get_and_save_remote_with_click_context(mock_context, "p", "d")
    assert mock_context.obj["flyte_remote"] is not None


def test_register_with_no_package_or_module_argument():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(pyflyte.main, ["register"])
        assert result.exit_code == 1
        assert (
            "Missing argument 'PACKAGE_OR_MODULE...', at least one PACKAGE_OR_MODULE is required but multiple can be passed"
            in result.output
        )


@mock.patch("flytekit.clis.sdk_in_container.helpers.FlyteRemote", spec=FlyteRemote)
@mock.patch("flytekit.clients.friendly.SynchronousFlyteClient", spec=SynchronousFlyteClient)
def test_register_with_no_output_dir_passed(mock_client, mock_remote):
    mock_remote._client = mock_client
    mock_remote.return_value._version_from_hash.return_value = "dummy_version_from_hash"
    mock_remote.return_value._upload_file.return_value = "dummy_md5_bytes", "dummy_native_url"
    runner = CliRunner()
    with runner.isolated_filesystem():
        out = subprocess.run(["git", "init"], capture_output=True)
        assert out.returncode == 0
        os.makedirs("core", exist_ok=True)
        with open(os.path.join("core", "sample.py"), "w") as f:
            f.write(sample_file_contents)
            f.close()
        result = runner.invoke(pyflyte.main, ["register", "core"])
        assert "Output given as None, using a temporary directory at" in result.output
        shutil.rmtree("core")


@mock.patch("flytekit.clis.sdk_in_container.helpers.FlyteRemote", spec=FlyteRemote)
@mock.patch("flytekit.clients.friendly.SynchronousFlyteClient", spec=SynchronousFlyteClient)
def test_non_fast_register(mock_client, mock_remote):
    mock_remote._client = mock_client
    mock_remote.return_value._version_from_hash.return_value = "dummy_version_from_hash"
    mock_remote.return_value._upload_file.return_value = "dummy_md5_bytes", "dummy_native_url"
    runner = CliRunner()
    with runner.isolated_filesystem():
        out = subprocess.run(["git", "init"], capture_output=True)
        assert out.returncode == 0
        os.makedirs("core", exist_ok=True)
        with open(os.path.join("core", "sample.py"), "w") as f:
            f.write(sample_file_contents)
            f.close()
        result = runner.invoke(pyflyte.main, ["register", "--non-fast", "--version", "a-version", "core"])
        assert "Output given as None, using a temporary directory at" in result.output
        shutil.rmtree("core")


@mock.patch("flytekit.clis.sdk_in_container.helpers.FlyteRemote", spec=FlyteRemote)
@mock.patch("flytekit.clients.friendly.SynchronousFlyteClient", spec=SynchronousFlyteClient)
def test_non_fast_register_require_version(mock_client, mock_remote):
    mock_remote._client = mock_client
    mock_remote.return_value._version_from_hash.return_value = "dummy_version_from_hash"
    mock_remote.return_value._upload_file.return_value = "dummy_md5_bytes", "dummy_native_url"
    runner = CliRunner()
    with runner.isolated_filesystem():
        out = subprocess.run(["git", "init"], capture_output=True)
        assert out.returncode == 0
        os.makedirs("core", exist_ok=True)
        with open(os.path.join("core", "sample.py"), "w") as f:
            f.write(sample_file_contents)
            f.close()
        result = runner.invoke(pyflyte.main, ["register", "--non-fast", "core"])
        assert result.exit_code == 1
        assert str(result.exception) == "Version is a required parameter in case --non-fast is specified."
        shutil.rmtree("core")
