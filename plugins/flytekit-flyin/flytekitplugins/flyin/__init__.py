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
   jupyter
   get_task_inputs
"""

from .vscode_lib.decorator import vscode, VscodeConfig
from .vscode_lib.constants import DEFAULT_CODE_SERVER_REMOTE_PATH, DEFAULT_CODE_SERVER_EXTENSIONS
from .jupyter_lib.decorator import jupyter
from .utils import get_task_inputs
