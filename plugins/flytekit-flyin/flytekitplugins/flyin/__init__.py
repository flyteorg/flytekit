"""
.. currentmodule:: flytekitplugins.flyin

This package contains flyin plugin for Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   vscode
   VscodeConfig
   DEFAULT_CODE_SERVER_DIR_NAME
   DEFAULT_CODE_SERVER_REMOTE_PATH
   DEFAULT_CODE_SERVER_EXTENSIONS
   COPILOT_EXTENSION
   VIM_EXTENSION
   CODE_TOGETHER_EXTENSION
"""

from .vscode_lib.decorator import vscode
from .vscode_lib.constants import (
    DEFAULT_CODE_SERVER_DIR_NAME,
    DEFAULT_CODE_SERVER_REMOTE_PATH,
    DEFAULT_CODE_SERVER_EXTENSIONS,
)
from .vscode_lib.config import (
    VscodeConfig,
    COPILOT_EXTENSION,
    VIM_EXTENSION,
    CODE_TOGETHER_EXTENSION,
    VIM_CONFIG,
    COPILOT_CONFIG,
    CODE_TOGETHER_CONFIG,
)
from .jupyter_lib.decorator import jupyter
