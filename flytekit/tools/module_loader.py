import contextlib
import importlib
import os
import pkgutil
import sys
import typing
from pathlib import Path
from typing import Any, Iterator, List, Union

from flytekit.loggers import logger
from flytekit.tools.script_mode import _find_project_root


def iterate_modules(pkgs):
    for package_name in pkgs:
        package = importlib.import_module(package_name)
        yield package

        # Check if package is a python file. If so, there is no reason to walk.
        if not hasattr(package, "__path__"):
            continue

        for _, name, _ in pkgutil.walk_packages(package.__path__, prefix="{}.".format(package_name)):
            yield importlib.import_module(name)


@contextlib.contextmanager
def add_sys_path(path: Union[str, os.PathLike]) -> Iterator[None]:
    """Temporarily add given path to `sys.path`."""
    path = os.fspath(path)
    try:
        sys.path.insert(0, path)
        yield
    finally:
        sys.path.remove(path)


def just_load_modules(pkgs: List[str]):
    """
    This one differs from the above in that we don't yield anything, just load all the modules.
    """
    for package_name in pkgs:
        package = importlib.import_module(package_name)

        # If it doesn't have a __path__ field, that means it's not a package, just a module
        if not hasattr(package, "__path__"):
            continue

        for _, name, _ in pkgutil.walk_packages(package.__path__, prefix="{}.".format(package_name)):
            importlib.import_module(name)


def load_object_from_module(object_location: str) -> Any:
    """
    # TODO: Handle corner cases, like where the first part is [] maybe
    """
    class_obj = object_location.split(".")
    class_obj_mod = class_obj[:-1]  # e.g. ['flytekit', 'core', 'python_auto_container']
    class_obj_key = class_obj[-1]  # e.g. 'default_task_class_obj'
    class_obj_mod = importlib.import_module(".".join(class_obj_mod))
    return getattr(class_obj_mod, class_obj_key)


def load_packages_and_modules(pkgs_or_mods: typing.List[str]) -> Path:
    """
    This is supposed to be a smarter function for loading. Given an arbitrary list of folders and files,
    this function will use the script mode function to walk up the filesystem to find the first folder
    without an init file. If all the folders and files resolve to the same root folder, then that folder
    will be added as the first entry to sys.path, and all the specified packages and modules loaded along
    with all submodules. The reason for prepending the entry is to ensure that the name that the various
    modules are loaded under are the fully-resolved name.

    For example, using flytesnacks cookbook, if you are in core/ and you call this function with
    ``flyte_basics/hello_world.py control_flow/``, the ``hello_world`` module would be loaded
    as ``core.flyte_basics.hello_world`` even though you're already in the core/ folder.

    :param pkgs_or_mods:
    :return: The common detected root path, the output of _find_project_root
    """
    project_root = None
    for pm in pkgs_or_mods:
        root = _find_project_root(pm)
        if project_root is None:
            project_root = root
        else:
            if project_root != root:
                raise ValueError(f"Specified module {pm} has root {root} but {project_root} already specified")

    logger.warning(f"Common root folder detected as {str(project_root)}")

    pkgs_and_modules = []
    for pm in pkgs_or_mods:
        p = Path(pm)
        rel_path_from_root = p.relative_to(project_root)
        dot_delineated = os.path.splitext(rel_path_from_root)[0].replace(os.path.sep, ".")  # noqa

        logger.warning(f"User specified arg {pm} has {str(rel_path_from_root)} relative path "
                       f"loading it as {dot_delineated}")
        pkgs_and_modules.append(dot_delineated)

    with add_sys_path(project_root):
        just_load_modules(pkgs_and_modules)

    return project_root
