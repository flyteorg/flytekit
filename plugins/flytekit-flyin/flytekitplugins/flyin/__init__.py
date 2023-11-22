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

from .vscode_lib.decorator import vscode, VscodeConfig, VIM_CONFIG_EXTENSION
from .vscode_lib.constants import DEFAULT_CODE_SERVER_REMOTE_PATH, DEFAULT_CODE_SERVER_EXTENSIONS

