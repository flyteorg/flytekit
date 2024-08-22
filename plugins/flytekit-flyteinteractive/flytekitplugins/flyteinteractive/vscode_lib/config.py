# This file has been moved to flytekit.interactive.vscode_lib.config
# Import flytekit.interactive module to keep backwards compatibility
from flytekit.interactive.vscode_lib.config import (  # noqa: F401
    DEFAULT_CODE_SERVER_DIR_NAMES,
    DEFAULT_CODE_SERVER_EXTENSIONS,
    DEFAULT_CODE_SERVER_REMOTE_PATHS,
    VscodeConfig,
)

# Extension URLs for additional extensions
COPILOT_EXTENSION = (
    "https://raw.githubusercontent.com/flyteorg/flytetools/master/flytekitplugins/flyin/GitHub.copilot-1.138.563.vsix"
)
VIM_EXTENSION = (
    "https://raw.githubusercontent.com/flyteorg/flytetools/master/flytekitplugins/flyin/vscodevim.vim-1.27.0.vsix"
)
CODE_TOGETHER_EXTENSION = "https://raw.githubusercontent.com/flyteorg/flytetools/master/flytekitplugins/flyin/genuitecllc.codetogether-2023.2.0.vsix"

# Predefined VSCode config with extensions
VIM_CONFIG = VscodeConfig(
    code_server_remote_paths=DEFAULT_CODE_SERVER_REMOTE_PATHS,
    code_server_dir_names=DEFAULT_CODE_SERVER_DIR_NAMES,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS + [VIM_EXTENSION],
)

COPILOT_CONFIG = VscodeConfig(
    code_server_remote_paths=DEFAULT_CODE_SERVER_REMOTE_PATHS,
    code_server_dir_names=DEFAULT_CODE_SERVER_DIR_NAMES,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS + [COPILOT_EXTENSION],
)

CODE_TOGETHER_CONFIG = VscodeConfig(
    code_server_remote_paths=DEFAULT_CODE_SERVER_REMOTE_PATHS,
    code_server_dir_names=DEFAULT_CODE_SERVER_DIR_NAMES,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS + [CODE_TOGETHER_EXTENSION],
)
