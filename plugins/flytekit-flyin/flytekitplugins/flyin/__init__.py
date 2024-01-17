"""
.. currentmodule:: flytekitplugins.flyin

This package contains flyin plugin for Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   jupyter
   BaseNotifier
   NotifierExecutor
   SendgridConfig
   SendgridNotifier
   SlackConfig
   SlackNotifier
   get_task_inputs
   VscodeConfig
   CODE_TOGETHER_CONFIG
   CODE_TOGETHER_EXTENSION
   COPILOT_CONFIG
   COPILOT_EXTENSION
   VIM_CONFIG
   VIM_EXTENSION
   DEFAULT_CODE_SERVER_DIR_NAMES
   DEFAULT_CODE_SERVER_EXTENSIONS
   DEFAULT_CODE_SERVER_REMOTE_PATHS
   HOURS_TO_SECONDS
   REMINDER_EMAIL_HOURS
   vscode
"""

from .jupyter_lib.decorator import jupyter
from .notification.base_notifier import BaseNotifier, NotifierExecutor
from .notification.sendgrid_notifier import SendgridConfig, SendgridNotifier
from .notification.slack_notifier import SlackConfig, SlackNotifier
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
from .vscode_lib.constants import (
    DEFAULT_CODE_SERVER_DIR_NAMES,
    DEFAULT_CODE_SERVER_EXTENSIONS,
    DEFAULT_CODE_SERVER_REMOTE_PATHS,
    HOURS_TO_SECONDS,
    REMINDER_EMAIL_HOURS,
)
from .vscode_lib.decorator import vscode
