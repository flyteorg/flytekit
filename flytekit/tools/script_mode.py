import gzip
import hashlib
import importlib
import os
import shutil
import tarfile
import tempfile
import typing
from pathlib import Path

from flyteidl.service import dataproxy_pb2 as _data_proxy_pb2

from flytekit.core import context_manager
from flytekit.core.tracker import get_full_module_path


def compress_single_script(source_path: str, destination: str, full_module_name: str):
    """
    Compresses the single script while maintaining the folder structure for that file.

    For example, given the follow file structure:
    .
    ├── flyte
    │   ├── __init__.py
    │   └── workflows
    │       ├── example.py
    │       ├── another_example.py
    │       ├── yet_another_example.py
    │       └── __init__.py

    Let's say you want to compress `example.py`. In that case we specify the the full module name as
    flyte.workflows.example and that will produce a tar file that contains only that file alongside
    with the folder structure, i.e.:

    .
    ├── flyte
    │   ├── __init__.py
    │   └── workflows
    │       ├── example.py
    │       └── __init__.py

    Note how `another_example.py` and `yet_another_example.py` were not copied to the destination.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        destination_path = os.path.join(tmp_dir, "code")
        # This is the script relative path to the root of the project
        script_relative_path = Path()
        # For each package in pkgs, create a directory and copy the __init__.py in it.
        # Skip the last package as that is the script file.
        pkgs = full_module_name.split(".")
        for p in pkgs[:-1]:
            os.makedirs(os.path.join(destination_path, p))
            source_path = os.path.join(source_path, p)
            destination_path = os.path.join(destination_path, p)
            script_relative_path = Path(script_relative_path, p)
            init_file = Path(os.path.join(source_path, "__init__.py"))
            if init_file.exists():
                shutil.copy(init_file, Path(os.path.join(tmp_dir, "code", script_relative_path, "__init__.py")))

        # Ensure destination path exists to cover the case of a single file and no modules.
        os.makedirs(destination_path, exist_ok=True)
        script_file = Path(source_path, f"{pkgs[-1]}.py")
        script_file_destination = Path(destination_path, f"{pkgs[-1]}.py")
        # Build the final script relative path and copy it to a known place.
        shutil.copy(
            script_file,
            script_file_destination,
        )
        tar_path = os.path.join(tmp_dir, "tmp.tar")
        with tarfile.open(tar_path, "w") as tar:
            tar.add(os.path.join(tmp_dir, "code"), arcname="", filter=tar_strip_file_attributes)
        with gzip.GzipFile(filename=destination, mode="wb", mtime=0) as gzipped:
            with open(tar_path, "rb") as tar_file:
                gzipped.write(tar_file.read())


# Takes in a TarInfo and returns the modified TarInfo:
# https://docs.python.org/3/library/tarfile.html#tarinfo-objects
# intented to be passed as a filter to tarfile.add
# https://docs.python.org/3/library/tarfile.html#tarfile.TarFile.add
def tar_strip_file_attributes(tar_info: tarfile.TarInfo) -> tarfile.TarInfo:
    # set time to epoch timestamp 0, aka 00:00:00 UTC on 1 January 1970
    # note that when extracting this tarfile, this time will be shown as the modified date
    tar_info.mtime = 0

    # user/group info
    tar_info.uid = 0
    tar_info.uname = ""
    tar_info.gid = 0
    tar_info.gname = ""

    # stripping paxheaders may not be required
    # see https://stackoverflow.com/questions/34688392/paxheaders-in-tarball
    tar_info.pax_headers = {}

    return tar_info


def fast_register_single_script(
    source_path: str, module_name: str, create_upload_location_fn: typing.Callable
) -> (_data_proxy_pb2.CreateUploadLocationResponse, bytes):

    # Open a temp directory and dump the contents of the digest.
    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_fname = os.path.join(tmp_dir, "script_mode.tar.gz")
        mod = importlib.import_module(module_name)
        compress_single_script(source_path, archive_fname, get_full_module_path(mod, mod.__name__))

        flyte_ctx = context_manager.FlyteContextManager.current_context()
        md5, _ = hash_file(archive_fname)
        upload_location = create_upload_location_fn(content_md5=md5)
        flyte_ctx.file_access.put_data(archive_fname, upload_location.signed_url)

        return upload_location, md5


def hash_file(file_path: typing.Union[os.PathLike, str]) -> (bytes, str):
    """
    Hash a file and produce a digest to be used as a version
    """
    h = hashlib.md5()

    with open(file_path, "rb") as file:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)

    return h.digest(), h.hexdigest()


def _find_project_root(source_path) -> Path:
    """
    Find the root of the project.
    The root of the project is considered to be the first ancestor from source_path that does
    not contain a __init__.py file.

    N.B.: This assumption only holds for regular packages (as opposed to namespace packages)
    """
    # Start from the directory right above source_path
    path = Path(source_path).parent.resolve()
    while os.path.exists(os.path.join(path, "__init__.py")):
        path = path.parent
    return path
