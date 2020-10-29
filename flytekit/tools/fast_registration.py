import logging as _logging
import os
import tarfile
import tempfile
import subprocess
from pathlib import Path

import dirhash

from flytekit.interfaces.data import data_proxy as _data_proxy

from flytekit.interfaces.data.data_proxy import Data as _Data

_tmp_versions_dir = "tmp/versions"


def compute_digest(source_dir: os.PathLike) -> str:
    """
    Walks the entirety of the source dir to compute a deterministic hex digest of the dir contents.
    :param source_dir:
    :return:
    """
    return dirhash.dirhash(source_dir, "md5", match=["*.py"])


def _write_marker(marker: os.PathLike):
    try:
        open(marker, 'x')
    except FileExistsError:
        pass


def _filter_tar_file_fn(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
    """
    Excludes designated file types from tar archive
    :param tarinfo:
    :return:
    """
    if tarinfo.name.endswith(".pyc"):
        return None
    return tarinfo


def get_additional_distribution_loc(remote_location, identifier):
    return os.path.join(remote_location, "{}.{}".format(identifier, "tar.gz"))


def upload_package(source_dir: os.PathLike, identifier: str, remote_location: str, dry_run=False):
    # checks for a flag file in the current execution directory
    tmp_versions_dir = os.path.join(os.getcwd(), _tmp_versions_dir)
    os.makedirs(tmp_versions_dir, exist_ok=True)
    marker = Path(os.path.join(tmp_versions_dir, identifier))
    if os.path.exists(marker):
        print("Local marker for identifier {} already exists, skipping upload".format(identifier, remote_location))
        return
    full_remote_path = get_additional_distribution_loc(remote_location, identifier)
    if _Data.data_exists(full_remote_path):
        print("File {} already exists, skipping upload".format(full_remote_path))
        _write_marker(marker)
        return

    with tempfile.NamedTemporaryFile() as fp:
        # Write using gzip
        with tarfile.open(fp.name, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir), filter=_filter_tar_file_fn)
        if dry_run:
            print("Would upload {} to {}".format(fp.name, full_remote_path))
        else:
            _Data.put_data(fp.name, full_remote_path)
            print("Uploaded {} to {}".format(fp.name, full_remote_path))

    # Finally, touch the marker file so we have a flag in the future to avoid re-uploading the package dir as an
    # optimization
    _write_marker(marker)
    return full_remote_path


def download_distribution(additional_distribution: str, destination: os.PathLike):
    _data_proxy.Data.get_data(additional_distribution, destination)
    tarfile_name = os.path.basename(additional_distribution)
    file_suffix = Path(tarfile_name).suffixes
    if len(file_suffix) != 2 or file_suffix[0] != '.tar' or file_suffix[1] != ".gz":
        raise ValueError("Unrecognized additional distribution format for {}".format(additional_distribution))

    # This will overwrite the existing user flyte workflow code in the current working code dir.

    _logging.info("Downloading fast-registered code dir {} to {}".format(os.path.join(destination, tarfile_name), destination))
    result = subprocess.run(['tar', '-xvf', os.path.join(destination, tarfile_name), "-C", destination],
                            stdout=subprocess.PIPE)
    _logging.info("Output of call to extract tar {}".format(result))
    _logging.info("And overview of destination dir {}".format(subprocess.run(["ls", destination],
                                                                             stdout=subprocess.PIPE)))
    result.check_returncode()