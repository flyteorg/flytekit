import os as _os
import posixpath
import subprocess as _subprocess
import tarfile as _tarfile
import tempfile as _tempfile
from pathlib import Path as _Path

import checksumdir

from flytekit.core.context_manager import FlyteContextManager
from flytekit.tools.package_helpers import create_archive

_tmp_versions_dir = "tmp/versions"

file_access = FlyteContextManager.current_context().file_access


def compute_digest(source_dir: _os.PathLike) -> str:
    """
    Walks the entirety of the source dir to compute a deterministic hex digest of the dir contents.
    :param _os.PathLike source_dir:
    :return Text:
    """
    return f"fast{checksumdir.dirhash(source_dir, 'md5', include_paths=True)}"


def _write_marker(marker: _os.PathLike):
    try:
        open(marker, "x")
    except FileExistsError:
        pass




def get_additional_distribution_loc(remote_location: str, identifier: str) -> str:
    """
    :param Text remote_location:
    :param Text identifier:
    :return Text:
    """
    return posixpath.join(remote_location, "{}.{}".format(identifier, "tar.gz"))


def upload_package(source_dir: _os.PathLike, identifier: str, remote_location: str, dry_run=False) -> str:
    """
    Uploads the contents of the source dir as a tar package to a destination specified by the unique identifier and
    remote_location.
    :param _os.PathLike source_dir:
    :param Text identifier:
    :param Text remote_location:
    :param bool dry_run:
    :return Text:
    """
    tmp_versions_dir = _os.path.join(_os.getcwd(), _tmp_versions_dir)
    _os.makedirs(tmp_versions_dir, exist_ok=True)
    marker = _Path(_os.path.join(tmp_versions_dir, identifier))
    full_remote_path = get_additional_distribution_loc(remote_location, identifier)
    if _os.path.exists(marker):
        print("Local marker for identifier {} already exists, skipping upload".format(identifier))
        return full_remote_path

    if file_access.exists(full_remote_path):
        print("Remote file {} already exists, skipping upload".format(full_remote_path))
        _write_marker(marker)
        return full_remote_path

    with _tempfile.NamedTemporaryFile() as fp:
        # Write using gzip
        create_archive(source_dir, fp.name)
        if dry_run:
            print("Would upload {} to {}".format(fp.name, full_remote_path))
        else:
            file_access.put_data(fp.name, full_remote_path)
            print("Uploaded {} to {}".format(fp.name, full_remote_path))

    # Finally, touch the marker file so we have a flag in the future to avoid re-uploading the package dir as an
    # optimization
    _write_marker(marker)
    return full_remote_path


def download_distribution(additional_distribution: str, destination: str):
    """
    Downloads a remote code distribution and overwrites any local files.
    :param Text additional_distribution:
    :param _os.PathLike destination:
    """
    file_access.get_data(additional_distribution, destination)
    tarfile_name = _os.path.basename(additional_distribution)
    if not tarfile_name.endswith(".tar.gz"):
        raise ValueError("Unrecognized additional distribution format for {}".format(additional_distribution))

    # This will overwrite the existing user flyte workflow code in the current working code dir.
    result = _subprocess.run(
        ["tar", "-xvf", _os.path.join(destination, tarfile_name), "-C", destination],
        stdout=_subprocess.PIPE,
    )
    result.check_returncode()
