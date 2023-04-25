import importlib.util
import sys


def lazy_module(fullname):
    """
    This function is used to lazily import modules.  It is used in the following way:
    .. code-block:: python
        from flytekit.lazy_import import lazy_module
        sklearn = lazy_module("sklearn")
        sklearn.svm.SVC()
    :param Text fullname: The full name of the module to import
    """
    # https://docs.python.org/3/library/importlib.html#implementing-lazy-imports
    spec = importlib.util.find_spec(fullname)
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    loader.exec_module(module)
    return module
