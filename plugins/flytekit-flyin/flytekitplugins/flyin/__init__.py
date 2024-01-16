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
"""

from .vscode_lib.decorator import vscode, VscodeConfig
from .vscode_lib.constants import (
    DEFAULT_CODE_SERVER_EXTENSIONS,
    REMINDER_EMAIL_HOURS,
    HOURS_TO_SECONDS,
    DEFAULT_CODE_SERVER_DIR_NAMES,
    DEFAULT_CODE_SERVER_REMOTE_PATHS,
)
from .jupyter_lib.decorator import jupyter
from .notification.base_notifier import BaseNotifier, NotifierExecutor
from .notification.sendgrid_notifier import SendgridNotifier, SendgridConfig
from .notification.slack_notifier import SlackNotifier, SlackConfig
from .utils import get_task_inputs
from .vscode_lib.config import (
    CODE_TOGETHER_CONFIG,
    CODE_TOGETHER_EXTENSION,
    COPILOT_CONFIG,
    COPILOT_EXTENSION,
    VIM_CONFIG,
    VIM_EXTENSION,
    VscodeConfig,
)
