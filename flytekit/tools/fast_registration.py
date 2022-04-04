import os as _os
import posixpath
import subprocess as _subprocess

from flytekit.core.context_manager import FlyteContextManager


file_access = FlyteContextManager.current_context().file_access


def get_additional_distribution_loc(remote_location: str, identifier: str) -> str:
    """
    :param Text remote_location:
    :param Text identifier:
    :return Text:
    """
    return posixpath.join(remote_location, "{}.{}".format(identifier, "tar.gz"))


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
