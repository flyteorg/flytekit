import os
import tempfile
import pathlib

import pytest

import flytekit.configuration
from flytekit.configuration import DefaultImages, ImageConfig
from flytekit.tools.repo import find_common_root, load_packages_and_modules


def test_module_loading():
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create directories
        top_level = os.path.join(tmp_dir, "top")
        middle_level = os.path.join(top_level, "middle")
        bottom_level = os.path.join(middle_level, "bottom")
        os.makedirs(bottom_level)

        top_level_2 = os.path.join(tmp_dir, "top2")
        middle_level_2 = os.path.join(top_level_2, "middle")
        os.makedirs(middle_level_2)

        # Create init files
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
        with open(os.path.join(middle_level_2, "__init__.py"), "w"):
            pass

        # Because they have different roots
        with pytest.raises(ValueError):
            find_common_root([middle_level_2, bottom_level])

        # But now add one more init file
        with open(os.path.join(top_level_2, "__init__.py"), "w"):
            pass

        # Now it should pass
        root = find_common_root([middle_level_2, bottom_level])
        assert str(root) == tmp_dir

        # Now load them
        serialization_settings = flytekit.configuration.SerializationSettings(
            project="project",
            domain="domain",
            version="version",
            env=None,
            image_config=ImageConfig.auto(img_name=DefaultImages.default_image()),
        )

        # Not a good test but at least try to load
        load_packages_and_modules(serialization_settings, pathlib.Path(root), [bottom_level])
