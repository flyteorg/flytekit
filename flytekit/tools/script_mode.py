import os
import shutil
import tarfile
import tempfile
from pathlib import Path

from flytekit.core import context_manager
from flytekit.core.tracker import extract_task_module
from flytekit.core.workflow import WorkflowBase


def compress_single_script(absolute_project_path: str, archive_fname, version: str, full_module_name: str):
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
    flyte.workflows.example and that will produce a tar file that contains only that file, i.e.:

    .
    ├── flyte
    │   ├── __init__.py
    │   └── workflows
    │       ├── example.py
    │       └── __init__.py

    Note how `another_example.py` and `yet_another_example.py` were not copied to the destination.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, "code")
        pkgs = full_module_name.split(".")
        # This is the script relative path to the root of the project
        script_relative_path = Path()
        # For each package in pkgs, create a directory and touch a __init__.py in it.
        # Skip the last package as that is the script file.
        for p in pkgs[:-1]:
            os.makedirs(os.path.join(path, p))
            Path(os.path.join(path, p, "__init__.py")).touch()
            path = os.path.join(path, p)
            script_relative_path = Path(script_relative_path, p)

        # Build the final script relative path and copy it to a known place.
        script_relative_path = Path(script_relative_path, f"{pkgs[-1]}.py")
        shutil.copy(
            os.path.join(absolute_project_path, script_relative_path),
            os.path.join(tmp_dir, "code", script_relative_path),
        )
        with tarfile.open(archive_fname, "w:gz") as tar:
            tar.add(os.path.join(tmp_dir, "code"), arcname="")


def fast_register_single_script(version: str, wf_entity: WorkflowBase, full_remote_path: str):
    source_path = _find_project_root()

    # Open a temp directory and dump the contents of the digest.
    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_fname = os.path.join(tmp_dir, f"{version}.tar.gz")
        _, mod_name, _ = extract_task_module(wf_entity)
        compress_single_script(source_path, archive_fname, version, mod_name)

        flyte_ctx = context_manager.FlyteContextManager.current_context()
        flyte_ctx.file_access.put_data(archive_fname, full_remote_path)


def _find_project_root() -> Path:
    """
    Traverse from current working directory until it can no longer find __init__.py files
    """
    path = Path(os.getcwd())
    while os.path.exists(os.path.join(path, "__init__.py")):
        path = path.parent
    return path
