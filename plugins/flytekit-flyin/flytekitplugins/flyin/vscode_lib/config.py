from .constants import DEFAULT_CODE_SERVER_REMOTE_PATH, DEFAULT_CODE_SERVER_DIR_NAME, DEFAULT_CODE_SERVER_EXTENSIONS
from .decorator import VscodeConfig

VIM_CONFIG_EXTENSION=VscodeConfig(
    code_server_remote_path=DEFAULT_CODE_SERVER_REMOTE_PATH,
    code_server_dir_name=DEFAULT_CODE_SERVER_DIR_NAME,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS+["https://open-vsx.org/api/vscodevim/vim/1.27.0/file/vscodevim.vim-1.27.0.vsix"],
)

JUPYTER_CONFIG_EXTENSTION=VscodeConfig(
    code_server_remote_path=DEFAULT_CODE_SERVER_REMOTE_PATH,
    code_server_dir_name=DEFAULT_CODE_SERVER_DIR_NAME,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS+["https://open-vsx.org/api/ms-toolsai/jupyter/2023.9.100/file/ms-toolsai.jupyter-2023.9.100.vsix"],
)