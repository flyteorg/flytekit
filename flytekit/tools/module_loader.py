import contextlib
import importlib
import os
import pkgutil
import sys
from typing import Any, Iterator, List, Union


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
        for _, name, _ in pkgutil.walk_packages(package.__path__, prefix="{}.".format(package_name)):
            importlib.import_module(name)


def load_workflow_modules(pkgs):
    """
    Load all modules and packages at and under the given package.  Used for finding workflows/tasks to register.

    :param list[Text] pkgs: List of dot separated string containing paths folders (packages) containing
        the modules (python files)
    :raises ImportError
    """
    for _ in iterate_modules(pkgs):
        pass


def load_module_object_for_type(pkgs, t, additional_path=None):
    def iterate():
        entity_to_module_key = {}
        for m in iterate_modules(pkgs):
            for k in dir(m):
                o = m.__dict__[k]
                if isinstance(o, t):
                    entity_to_module_key[o] = (m.__name__, k)
        return entity_to_module_key

    if additional_path is not None:
        with add_sys_path(additional_path):
            return iterate()
    else:
        return iterate()


def load_object_from_module(object_location: str) -> Any:
    """
    # TODO: Handle corner cases, like where the first part is [] maybe
    """
    class_obj = object_location.split(".")
    class_obj_mod = class_obj[:-1]  # e.g. ['flytekit', 'core', 'python_auto_container']
    class_obj_key = class_obj[-1]  # e.g. 'default_task_class_obj'
    class_obj_mod = importlib.import_module(".".join(class_obj_mod))
    return getattr(class_obj_mod, class_obj_key)


def trigger_loading(
    pkgs,
    local_source_root=None,
):
    """
    This function will iterate all discovered entities in the given package list.  It will then attempt to
    topologically sort such that any entity with a dependency on another comes later in the list.  Note that workflows
    can reference other workflows and launch plans.

    :param list[Text] pkgs:
    :param Text local_source_root:
    """
    if local_source_root is not None:
        with add_sys_path(local_source_root):
            for _ in iterate_modules(pkgs):
                ...
    else:
        for _ in iterate_modules(pkgs):
            ...
