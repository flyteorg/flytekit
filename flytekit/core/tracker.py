import importlib as _importlib
import inspect
import inspect as _inspect
import logging as _logging
from typing import Callable

from flytekit.common.exceptions import system as _system_exceptions


class InstanceTrackingMeta(type):
    """
    Please see the original class :py:class`flytekit.common.mixins.registerable._InstanceTracker` also and also look
    at the tests in the ``tests/flytekit/unit/core/tracker/test_tracking/`` folder to see how it's used.

    Basically, this will make instances of classes that use this metaclass aware of the module (the .py file) that
    caused the instance to be created. This is useful because it means that we can then (at least try to) find the
    variable that the instance was assigned to.
    """

    @staticmethod
    def _find_instance_module():
        frame = _inspect.currentframe()
        while frame:
            if frame.f_code.co_name == "<module>":
                return frame.f_globals["__name__"]
            frame = frame.f_back
        return None

    def __call__(cls, *args, **kwargs):
        o = super(InstanceTrackingMeta, cls).__call__(*args, **kwargs)
        o._instantiated_in = InstanceTrackingMeta._find_instance_module()
        return o


class TrackedInstance(metaclass=InstanceTrackingMeta):
    """
    Please see the notes for the metaclass above first.

    This functionality has two use-cases currently,
    * Keep track of naming for non-function ``PythonAutoContainerTasks``.  That is, things like the
      :py:class:`flytekit.extras.sqlite3.task.SQLite3Task` task.
    * Task resolvers, because task resolvers are instances of :py:class:`flytekit.core.python_auto_container.TaskResolverMixin`
      classes, not the classes themselves, which means we need to look on the left hand side of them to see how to
      find them at task execution time.
    """

    def __init__(self, *args, **kwargs):
        self._instantiated_in = None
        self._lhs = None
        super().__init__(*args, **kwargs)

    @property
    def instantiated_in(self) -> str:
        return self._instantiated_in

    @property
    def location(self) -> str:
        return f"{self.instantiated_in}.{self.lhs}"

    @property
    def lhs(self):
        if self._lhs is not None:
            return self._lhs
        return self.find_lhs()

    def find_lhs(self) -> str:
        if self._lhs is not None:
            return self._lhs

        if self._instantiated_in is None or self._instantiated_in == "":
            raise _system_exceptions.FlyteSystemException(f"Object {self} does not have an _instantiated in")

        _logging.debug(f"Looking for LHS for {self} from {self._instantiated_in}")
        m = _importlib.import_module(self._instantiated_in)
        for k in dir(m):
            try:
                if getattr(m, k) is self:
                    _logging.debug(f"Found LHS for {self}, {k}")
                    self._lhs = k
                    return k
            except ValueError as err:
                # Empty pandas dataframes behave weirdly here such that calling `m.df` raises:
                # ValueError: The truth value of a {type(self).__name__} is ambiguous. Use a.empty, a.bool(), a.item(),
                #   a.any() or a.all()
                # Since dataframes aren't registrable entities to begin with we swallow any errors they raise and
                # continue looping through m.
                _logging.warning("Caught ValueError {} while attempting to auto-assign name".format(err))
                pass

        _logging.error(f"Could not find LHS for {self} in {self._instantiated_in}")
        raise _system_exceptions.FlyteSystemException(f"Error looking for LHS in {self._instantiated_in}")


def isnested(func: Callable) -> bool:
    """
    Returns true if a function is local to another function and is not accessible through a module

    This would essentially be any function with a `.<local>.` (defined within a function) e.g.

    .. code:: python

        def foo():
            def foo_inner():
                pass
            pass

    In the above example `foo_inner` is the local function or a nested function.
    """
    return func.__code__.co_flags & inspect.CO_NESTED != 0


def istestfunction(func) -> bool:
    """
    Returns true if the function is defined in a test module. A test module has to have `test_` as the prefix.
    False in all other cases
    """
    mod = inspect.getmodule(func)
    if mod:
        mod_name = mod.__name__
        if "." in mod_name:
            mod_name = mod_name.split(".")[-1]
        return mod_name.startswith("test_")
    return False
