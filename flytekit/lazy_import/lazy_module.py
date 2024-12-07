import importlib.util
import sys
import types


class _LazyModule(types.ModuleType):
    """
    `lazy_module` returns an instance of this class if the module is not found in the python environment.
    """

    def __init__(self, module_name: str):
        super().__init__(module_name)
        self._module_name = module_name

    def __getattribute__(self, attr):
        raise ImportError(f"Module {object.__getattribute__(self, '_module_name')} is not yet installed.")


def is_imported(module_name):
    """
    This function is used to check if a module has been imported by the regular import.
    Return false if module is lazy imported and not used yet.
    """
    return (
        module_name in sys.modules
        and object.__getattribute__(lazy_module(module_name), "__class__").__name__ != "_LazyModule"
    )


def lazy_module(fullname):
    """
    This function is used to lazily import modules.  It is used in the following way:
    .. code-block:: python
        from flytekit.lazy_import import lazy_module
        sklearn = lazy_module("sklearn")
        sklearn.svm.SVC()
    :param Text fullname: The full name of the module to import
    """
    if fullname in sys.modules:
        return sys.modules[fullname]
    # https://docs.python.org/3/library/importlib.html#implementing-lazy-imports
    spec = importlib.util.find_spec(fullname)
    if spec is None or spec.loader is None:
        # Return a lazy module if the module is not found in the python environment,
        # so that we can raise a proper error when the user tries to access an attribute in the module.
        # The reason to do this is because importlib.util.LazyLoader still requires
        # the module to be installed even if you don't use it.
        return _LazyModule(fullname)
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    loader.exec_module(module)
    return module
