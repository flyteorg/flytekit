import hashlib
import os
import shutil
import tarfile
import tempfile
import typing
from pathlib import Path

from flytekit.core import context_manager
from flytekit.core.tracker import extract_task_module
from flytekit.core.workflow import WorkflowBase


def compress_single_script(absolute_project_path: str, destination: str, version: str, full_module_name: str):
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
        source_path = os.path.join(absolute_project_path)
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
            if init_file.exists:
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
        with tarfile.open(destination, "w:gz") as tar:
            tar.add(os.path.join(tmp_dir, "code"), arcname="")


def fast_register_single_script(version: str, wf_entity: WorkflowBase, create_upload_location_fn: typing.Callable):
    _, mod_name, _, script_full_path = extract_task_module(wf_entity)
    # Find project root by moving up the folder hierarchy until you cannot find a __init__.py file.
    source_path = _find_project_root(script_full_path)

    # Open a temp directory and dump the contents of the digest.
    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_fname = os.path.join(tmp_dir, f"{version}.tar.gz")
        compress_single_script(source_path, archive_fname, version, mod_name)

        flyte_ctx = context_manager.FlyteContextManager.current_context()
        md5, _ = hash_file(archive_fname)
        upload_location = create_upload_location_fn(content_md5=md5)
        flyte_ctx.file_access.put_data(archive_fname, upload_location.signed_url)
        return upload_location


def hash_file(file_path: typing.Union[os.PathLike, str]) -> (bytes, str):
    """
    Hash a file and produce a digest to be used as a version
    """
    # TODO: take file_path as an initial parameter to ensure that moving the file will produce a different version.
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
    Traverse from current working directory until it can no longer find __init__.py files
    """
    # Start from the directory right above source_path
    path = Path(source_path).parents[0]
    while os.path.exists(os.path.join(path, "__init__.py")):
        path = path.parent
    return path
