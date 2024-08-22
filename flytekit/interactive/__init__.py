"""
..
currentmodule:: flytekit.interactive

This package contains flyteinteractive plugin for Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   vscode
   VscodeConfig
   DEFAULT_CODE_SERVER_DIR_NAME
   DEFAULT_CODE_SERVER_REMOTE_PATH
   DEFAULT_CODE_SERVER_EXTENSIONS
   get_task_inputs
"""

from .utils import get_task_inputs
from .vscode_lib.config import (
    VscodeConfig,
)
from .vscode_lib.decorator import vscode
from .vscode_lib.vscode_constants import (
    DEFAULT_CODE_SERVER_DIR_NAMES,
    DEFAULT_CODE_SERVER_EXTENSIONS,
    DEFAULT_CODE_SERVER_REMOTE_PATHS,
)
