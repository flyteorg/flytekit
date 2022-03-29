import inspect
import os
import typing
from typing import Callable, Tuple

from flytekit.configuration.feature_flags import FeatureFlags


class _ModuleSanitizer(object):
    """
    Sanitizes and finds the absolute module path irrespective of the import location.
    """

    def __init__(self):
        self._module_cache = {}

    def _resolve_abs_module_name(self, basename: str, dirname: str, package_root: str) -> str:
        """
        Recursively finds the root python package under-which basename exists
        """
        # If we have already computed the module for this directory - return
        if dirname in self._module_cache:
            return self._module_cache[dirname]

        # Let us remove any extensions like .py
        basename = os.path.splitext(basename)[0]

        if dirname == package_root:
            return basename

        # If we have reached a directory with no __init__, ignore
        if "__init__.py" not in os.listdir(dirname):
            return basename

        # Now  recurse down such that we can extract the absolute module path
        mod_name = self._resolve_abs_module_name(os.path.basename(dirname), os.path.dirname(dirname), package_root)
        final_mod_name = f"{mod_name}.{basename}" if mod_name else basename
        self._module_cache[dirname] = final_mod_name
        return final_mod_name

    def get_absolute_module_name(self, path: str, package_root: typing.Optional[str] = None) -> str:
        """
        Returns the absolute module path for a given python file path. This assumes that every module correctly contains
        a __init__.py file. Absence of this file, indicates the root.
        """
        return self._resolve_abs_module_name(os.path.basename(path), os.path.dirname(path), package_root)


_mod_sanitizer = _ModuleSanitizer()


def extract_task_module(f: Callable) -> Tuple[str, str, str]:
    """
    Returns the task-name, absolute module and the string name of the callable.
    :param f: A task or any other callable
    :return: [name to use: str, module_name: str, function_name: str]
    """
    mod = inspect.getmodule(f)
    if mod is None:
        raise AssertionError(f"Unable to determine module of {f}")
    mod_name = mod.__name__
    name = f.__name__.split(".")[-1]

    if mod_name == "__main__":
        return name, "", name

    if FeatureFlags.FLYTE_PYTHON_PACKAGE_ROOT != ".":
        package_root = (
            FeatureFlags.FLYTE_PYTHON_PACKAGE_ROOT if FeatureFlags.FLYTE_PYTHON_PACKAGE_ROOT != "auto" else None
        )
        mod_name = _mod_sanitizer.get_absolute_module_name(inspect.getabsfile(f), package_root)
    return f"{mod_name}.{name}", mod_name, name
