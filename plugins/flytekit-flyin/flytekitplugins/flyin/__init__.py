"""
.. currentmodule:: flytekitplugins.flyin

This package contains flyin plugin for Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   vscode
   VscodeConfig
   DEFAULT_CODE_SERVER_REMOTE_PATH
   DEFAULT_CODE_SERVER_EXTENSIONS
"""

from .vscode_lib.decorator import vscode, VscodeConfig
from .vscode_lib.constants import DEFAULT_CODE_SERVER_REMOTE_PATH, DEFAULT_CODE_SERVER_EXTENSIONS
from .vscode_lib.config import VIM_CONFIG_EXTENSION, COPILOT_CONFIG_EXTENSION
