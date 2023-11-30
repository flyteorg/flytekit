from dataclasses import dataclass, field
from typing import List, Optional, Union

from .constants import DEFAULT_CODE_SERVER_DIR_NAME, DEFAULT_CODE_SERVER_EXTENSIONS, DEFAULT_CODE_SERVER_REMOTE_PATH


@dataclass
class VscodeConfig:
    """
    VscodeConfig is the config contains default URLs of the VSCode server and extension remote paths.

    Args:
        code_server_remote_path (str, optional): The URL of the code-server tarball.
        code_server_dir_name (str, optional): The name of the code-server directory.
        extension_remote_paths (List[str], optional): The URLs of the VSCode extensions.
            You can find all available extensions at https://open-vsx.org/.
    """

    code_server_remote_path: Optional[str] = DEFAULT_CODE_SERVER_REMOTE_PATH
    code_server_dir_name: Optional[str] = DEFAULT_CODE_SERVER_DIR_NAME
    extension_remote_paths: Optional[List[str]] = field(default_factory=lambda: DEFAULT_CODE_SERVER_EXTENSIONS)

    def add_extensions(self, extensions: Union[str, List[str]]):
        """
        Add additional extensions to the extension_remote_paths list.
        """
        if isinstance(extensions, List):
            self.extension_remote_paths.extend(extensions)
        else:
            self.extension_remote_paths.append(extensions)


# Extension URLs for additional extensions
COPILOT_EXTENSION = (
    "https://raw.githubusercontent.com/flyteorg/flytetools/master/flytekitplugins/flyin/GitHub.copilot-1.138.563.vsix"
)
VIM_EXTENSION = "https://open-vsx.org/api/vscodevim/vim/1.27.0/file/vscodevim.vim-1.27.0.vsix"
CODE_TOGETHER_EXTENSION = "https://openvsxorg.blob.core.windows.net/resources/genuitecllc/codetogether/2023.2.0/genuitecllc.codetogether-2023.2.0.vsix"

# Predefined VSCode config with extensions
VIM_CONFIG = VscodeConfig(
    code_server_remote_path=DEFAULT_CODE_SERVER_REMOTE_PATH,
    code_server_dir_name=DEFAULT_CODE_SERVER_DIR_NAME,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS + [VIM_EXTENSION],
)

COPILOT_CONFIG = VscodeConfig(
    code_server_remote_path=DEFAULT_CODE_SERVER_REMOTE_PATH,
    code_server_dir_name=DEFAULT_CODE_SERVER_DIR_NAME,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS + [COPILOT_EXTENSION],
)

CODE_TOGETHER_CONFIG = VscodeConfig(
    code_server_remote_path=DEFAULT_CODE_SERVER_REMOTE_PATH,
    code_server_dir_name=DEFAULT_CODE_SERVER_DIR_NAME,
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS + [CODE_TOGETHER_EXTENSION],
)
