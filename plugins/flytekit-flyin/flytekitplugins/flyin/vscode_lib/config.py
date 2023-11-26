from .constants import DEFAULT_CODE_SERVER_DIR_NAME, DEFAULT_CODE_SERVER_EXTENSIONS, DEFAULT_CODE_SERVER_REMOTE_PATH
from .decorator import VscodeConfig

VIM_CONFIG_EXTENSION = VscodeConfig(
    code_server_remote_path=DEFAULT_CODE_SERVER_REMOTE_PATH,
    code_server_dir_name=DEFAULT_CODE_SERVER_DIR_NAME,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS
    + ["https://open-vsx.org/api/vscodevim/vim/1.27.0/file/vscodevim.vim-1.27.0.vsix"],
)

COPILOT_CONFIG_EXTENSION = VscodeConfig(
    code_server_remote_path=DEFAULT_CODE_SERVER_REMOTE_PATH,
    code_server_dir_name=DEFAULT_CODE_SERVER_DIR_NAME,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS
    + ["https://raw.githubusercontent.com/Future-Outlier/FlyinCopilotVsix/main/GitHub.copilot-1.138.563.vsix"],
)
