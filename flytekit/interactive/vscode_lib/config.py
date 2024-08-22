from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from flytekit.interactive.vscode_lib.vscode_constants import (
    DEFAULT_CODE_SERVER_DIR_NAMES,
    DEFAULT_CODE_SERVER_EXTENSIONS,
    DEFAULT_CODE_SERVER_REMOTE_PATHS,
)


@dataclass
class VscodeConfig:
    """
    VscodeConfig is the config contains default URLs of the VSCode server and extension remote paths.

    Args:
        code_server_remote_paths (Dict[str, str], optional): The URL of the code-server tarball.
        code_server_dir_names (Dict[str, str], optional): The name of the code-server directory.
        extension_remote_paths (List[str], optional): The URLs of the VSCode extensions.
            You can find all available extensions at https://open-vsx.org/.
    """

    code_server_remote_paths: Optional[Dict[str, str]] = field(default_factory=lambda: DEFAULT_CODE_SERVER_REMOTE_PATHS)
    code_server_dir_names: Optional[Dict[str, str]] = field(default_factory=lambda: DEFAULT_CODE_SERVER_DIR_NAMES)
    extension_remote_paths: Optional[List[str]] = field(default_factory=lambda: DEFAULT_CODE_SERVER_EXTENSIONS)

    def add_extensions(self, extensions: Union[str, List[str]]):
        """
        Add additional extensions to the extension_remote_paths list.
        """
        if isinstance(extensions, List):
            self.extension_remote_paths.extend(extensions)
        else:
            self.extension_remote_paths.append(extensions)
