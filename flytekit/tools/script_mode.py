from __future__ import annotations

import gzip
import hashlib
import os
import shutil
import site
import stat
import sys
import tarfile
import tempfile
import typing
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import List, Optional, Tuple, Union

import flytekit
from flytekit.constants import CopyFileDetection
from flytekit.loggers import logger
from flytekit.tools.ignore import IgnoreGroup


def compress_scripts(source_path: str, destination: str, modules: List[ModuleType]):
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
    │       ├── unused_example.py
    │       └── __init__.py

    Let's say you want to compress `example.py` imports `another_example.py`. And `another_example.py`
    imports on `yet_another_example.py`. This will  produce a tar file that contains only that
    file alongside with the folder structure, i.e.:

    .
    ├── flyte
    │   ├── __init__.py
    │   └── workflows
    │       ├── example.py
    │       ├── another_example.py
    │       ├── yet_another_example.py
    │       └── __init__.py

    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        destination_path = os.path.join(tmp_dir, "code")
        os.mkdir(destination_path)
        add_imported_modules_from_source(source_path, destination_path, modules)

        tar_path = os.path.join(tmp_dir, "tmp.tar")
        with tarfile.open(tar_path, "w") as tar:
            tmp_path: str = os.path.join(tmp_dir, "code")
            files: typing.List[str] = os.listdir(tmp_path)
            for ws_file in files:
                tar.add(os.path.join(tmp_path, ws_file), arcname=ws_file, filter=tar_strip_file_attributes)
        with gzip.GzipFile(filename=destination, mode="wb", mtime=0) as gzipped:
            with open(tar_path, "rb") as tar_file:
                gzipped.write(tar_file.read())


# Takes in a TarInfo and returns the modified TarInfo:
# https://docs.python.org/3/library/tarfile.html#tarinfo-objects
# intended to be passed as a filter to tarfile.add
# https://docs.python.org/3/library/tarfile.html#tarfile.TarFile.add
def tar_strip_file_attributes(tar_info: tarfile.TarInfo) -> tarfile.TarInfo:
    # set time to epoch timestamp 0, aka 00:00:00 UTC on 1 January 1980
    # note that when extracting this tarfile, this time will be shown as the modified date
    tar_info.mtime = datetime(1980, 1, 1).timestamp()

    # user/group info
    tar_info.uid = 0
    tar_info.uname = ""
    tar_info.gid = 0
    tar_info.gname = ""

    # stripping paxheaders may not be required
    # see https://stackoverflow.com/questions/34688392/paxheaders-in-tarball
    tar_info.pax_headers = {}

    return tar_info


def ls_files(
    source_path: str,
    copy_file_detection: CopyFileDetection,
    deref_symlinks: bool = False,
    ignore_group: Optional[IgnoreGroup] = None,
) -> Tuple[List[str], str]:
    """
    user_modules_and_packages is a list of the Python modules and packages, expressed as absolute paths, that the
    user has run this pyflyte command with. For pyflyte run for instance, this is just a list of one.
    This is used for two reasons.
      - Everything in this list needs to be returned. Files are returned and folders are walked.
      - A common source path is derived from this is, which is just the common folder that contains everything in the
        list. For ex. if you do
        $ pyflyte --pkgs a.b,a.c package
        Then the common root is just the folder a/. The modules list is filtered against this root. Only files
        representing modules under this root are included

    If the copy enum is set to loaded_modules, then the loaded sys modules will be used.
    """

    # Unlike the below, the value error here is useful and should be returned to the user, like if absolute and
    # relative paths are mixed.

    # This is --copy auto
    if copy_file_detection == CopyFileDetection.LOADED_MODULES:
        sys_modules = list(sys.modules.values())
        all_files = list_imported_modules_as_files(source_path, sys_modules)
    # this is --copy all (--copy none should never invoke this function)
    else:
        all_files = list_all_files(source_path, deref_symlinks, ignore_group)

    all_files.sort()
    hasher = hashlib.md5()
    for abspath in all_files:
        relpath = os.path.relpath(abspath, source_path)
        _filehash_update(abspath, hasher)
        _pathhash_update(relpath, hasher)

    digest = hasher.hexdigest()

    return all_files, digest


def _filehash_update(path: Union[os.PathLike, str], hasher: hashlib._Hash) -> None:
    blocksize = 65536
    with open(path, "rb") as f:
        bytes = f.read(blocksize)
        while bytes:
            hasher.update(bytes)
            bytes = f.read(blocksize)


def _pathhash_update(path: Union[os.PathLike, str], hasher: hashlib._Hash) -> None:
    path_list = path.split(os.sep)
    hasher.update("".join(path_list).encode("utf-8"))


EXCLUDE_DIRS = {".git"}


def list_all_files(source_path: str, deref_symlinks, ignore_group: Optional[IgnoreGroup] = None) -> List[str]:
    all_files = []

    # This is needed to prevent infinite recursion when walking with followlinks
    visited_inodes = set()

    for root, dirnames, files in os.walk(source_path, topdown=True, followlinks=deref_symlinks):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        if deref_symlinks:
            inode = os.stat(root).st_ino
            if inode in visited_inodes:
                continue
            visited_inodes.add(inode)

        ff = []
        files.sort()
        for fname in files:
            abspath = os.path.join(root, fname)
            # Only consider files that exist (e.g. disregard symlinks that point to non-existent files)
            if not os.path.exists(abspath):
                logger.info(f"Skipping non-existent file {abspath}")
                continue
            # Skip socket files
            if stat.S_ISSOCK(os.stat(abspath).st_mode):
                logger.info(f"Skip socket file {abspath}")
                continue
            if ignore_group:
                if ignore_group.is_ignored(abspath):
                    continue

            ff.append(abspath)
        all_files.extend(ff)

        # Remove directories that we've already visited from dirnames
        if deref_symlinks:
            dirnames[:] = [d for d in dirnames if os.stat(os.path.join(root, d)).st_ino not in visited_inodes]

    return all_files


def _file_is_in_directory(file: str, directory: str) -> bool:
    """Return True if file is in directory and in its children."""
    try:
        return os.path.commonpath([file, directory]) == directory
    except ValueError as e:
        # ValueError is raised by windows if the paths are not from the same drive
        logger.debug(f"{file} and {directory} are not in the same drive: {str(e)}")
        return False


def list_imported_modules_as_files(source_path: str, modules: List[ModuleType]) -> List[str]:
    """Copies modules into destination that are in modules. The module files are copied only if:

    1. Not a site-packages. These are installed packages and not user files.
    2. Not in the sys.base_prefix or sys.prefix. These are also installed and not user files.
    3. Does not share a common path with the source_path.
    """
    # source path is the folder holding the main script.
    # but in register/package case, there are multiple folders.
    # identify a common root amongst the packages listed?

    files = []
    flytekit_root = os.path.dirname(flytekit.__file__)

    # These directories contain installed packages or modules from the Python standard library.
    # If a module is from these directories, then they are not user files.
    invalid_directories = [
        flytekit_root,
        sys.prefix,
        sys.base_prefix,
        site.getusersitepackages(),
    ] + site.getsitepackages()

    for mod in modules:
        try:
            mod_file = mod.__file__
        except AttributeError:
            continue

        if mod_file is None:
            continue

        if any(_file_is_in_directory(mod_file, directory) for directory in invalid_directories):
            continue

        if not _file_is_in_directory(mod_file, source_path):
            # Only upload files where the module file in the source directory
            continue

        files.append(mod_file)

    return files


def add_imported_modules_from_source(source_path: str, destination: str, modules: List[ModuleType]):
    """Copies modules into destination that are in modules. The module files are copied only if:

    1. Not a site-packages. These are installed packages and not user files.
    2. Not in the sys.base_prefix or sys.prefix. These are also installed and not user files.
    3. Does not share a common path with the source_path.
    """
    # source path is the folder holding the main script.
    # but in register/package case, there are multiple folders.
    # identify a common root amongst the packages listed?

    files = list_imported_modules_as_files(source_path, modules)
    for file in files:
        relative_path = os.path.relpath(file, start=source_path)
        new_destination = os.path.join(destination, relative_path)

        if os.path.exists(new_destination):
            # No need to copy if it already exists
            continue

        os.makedirs(os.path.dirname(new_destination), exist_ok=True)
        shutil.copy(file, new_destination)


def get_all_modules(source_path: str, module_name: Optional[str]) -> List[ModuleType]:
    """Import python file with module_name in source_path and return all modules."""
    sys_modules = list(sys.modules.values())
    if module_name is None or module_name in sys.modules:
        # module already exists, there is no need to import it again
        return sys_modules

    full_module = os.path.join(source_path, *module_name.split("."))
    full_module_path = f"{full_module}.py"

    is_python_file = os.path.exists(full_module_path) and os.path.isfile(full_module_path)
    if not is_python_file:
        return sys_modules

    # should move it here probably
    from flytekit.core.tracker import import_module_from_file

    try:
        new_module = import_module_from_file(module_name, full_module_path)
        return sys_modules + [new_module]
    except Exception as exc:
        logger.error(f"Using system modules, failed to import {module_name} from {full_module_path}: {str(exc)}")
        # Import failed so we fallback to `sys_modules`
        return sys_modules


def hash_file(file_path: typing.Union[os.PathLike, str]) -> (bytes, str, int):
    """
    Hash a file and produce a digest to be used as a version
    """
    h = hashlib.md5()
    l = 0

    with open(file_path, "rb") as file:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)
            l += len(chunk)

    return h.digest(), h.hexdigest(), l


def _find_project_root(source_path) -> str:
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
    return str(path)
