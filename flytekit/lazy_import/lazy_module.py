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
    try:
        return sys.modules[fullname]
    except KeyError:
        spec = importlib.util.find_spec(fullname)
        module = importlib.util.module_from_spec(spec)
        loader = importlib.util.LazyLoader(spec.loader)
        # Make module with proper locking and get it inserted into sys.modules.
        loader.exec_module(module)
        return module
