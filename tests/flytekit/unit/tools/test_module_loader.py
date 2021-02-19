import os
import sys

from flytekit.common import utils as _utils
from flytekit.tools import module_loader
from .test_dummies import a, b, c


def test_module_loading():
    with _utils.AutoDeletingTempDir("mypackage") as pkg:
        path = pkg.name
        # Create directories
        top_level = os.path.join(path, "top")
        middle_level = os.path.join(top_level, "middle")
        bottom_level = os.path.join(middle_level, "bottom")
        os.makedirs(bottom_level)

        # Create init files
        with open(os.path.join(path, "__init__.py"), "w"):
            pass
        with open(os.path.join(top_level, "__init__.py"), "w"):
            pass
        with open(os.path.join(top_level, "a.py"), "w"):
            pass
        with open(os.path.join(middle_level, "__init__.py"), "w"):
            pass
        with open(os.path.join(middle_level, "a.py"), "w"):
            pass
        with open(os.path.join(bottom_level, "__init__.py"), "w"):
            pass
        with open(os.path.join(bottom_level, "a.py"), "w"):
            pass

        sys.path.append(path)

        # Not a sufficient test but passes for now
        assert sum(1 for _ in module_loader.iterate_modules(["top"])) == 6
        assert [
            pkg.__file__ for pkg in module_loader.iterate_modules(["top.a", "top.middle.a", "top.middle.bottom.a"])
        ] == [os.path.join(lvl, "a.py") for lvl in (top_level, middle_level, bottom_level)]


def test_find_lhs():
    m, k = module_loader.find_lhs(a)
    assert (m, k) == ("tests.flytekit.unit.tools.test_dummies", "a")

    # Reassigning a variable makes no difference, depend on dir(m) iteration order
    m, k = module_loader.find_lhs(b)
    assert (m, k) == ("tests.flytekit.unit.tools.test_dummies", "a")

    # Reassigning here should not matter, the original module should be used.
    this_c = c
    m, k = module_loader.find_lhs(this_c)
    assert (m, k) == ("tests.flytekit.unit.tools.test_dummies", "c")
