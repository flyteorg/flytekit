import inspect as _inspect
import logging as _logging
import importlib as _importlib
from flytekit.common.exceptions import system as _system_exceptions
from abc import ABC


class InstanceTrackingMeta(type):
    @staticmethod
    def _find_instance_module():
        frame = _inspect.currentframe()
        while frame:
            if frame.f_code.co_name == "<module>":
                return frame.f_globals["__name__"]
            frame = frame.f_back
        return None

    def __call__(cls, *args, **kwargs):
        print(f"Starting I call for cls {cls}")
        o = super(InstanceTrackingMeta, cls).__call__(*args, **kwargs)
        o._instantiated_in = InstanceTrackingMeta._find_instance_module()
        print(f"In I call {type(o)}, {o._instantiated_in}")
        return o


class TrackedInstance(metaclass=InstanceTrackingMeta):
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


class B(TrackedInstance):
    def __init__(self):
        print(f"In B init")

class MyMeta(type(TrackedInstance), type(ABC)):
    ...


class C(B, metaclass=MyMeta):
    ...


if __name__ == "__main__":
    a = TrackedInstance()
    b = B()
    print(f"type {type(TrackedInstance)}")

