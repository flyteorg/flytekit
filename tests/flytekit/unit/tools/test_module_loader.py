from __future__ import absolute_import
import os
import sys

from flytekit.tools import module_loader
from flytekit.common import utils as _utils


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
