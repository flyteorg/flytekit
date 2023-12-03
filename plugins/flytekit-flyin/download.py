import os
import shutil
import subprocess
import tarfile
from typing import Optional
from flytekitplugins.flyin.vscode_lib.config import VscodeConfig
from flytekitplugins.flyin.vscode_lib.constants import DOWNLOAD_DIR, EXECUTABLE_NAME
import fsspec

def download_file(url, target_dir: Optional[str] = "."):
    """
    Download a file from a given URL using fsspec.

    Args:
        url (str): The URL of the file to download.
        target_dir (str, optional): The directory where the file should be saved. Defaults to current directory.

    Returns:
        str: The path to the downloaded file.
    """
    if not url.startswith("http"):
        raise ValueError(f"URL {url} is not valid. Only http/https is supported.")

    # Derive the local filename from the URL
    local_file_name = os.path.join(target_dir, os.path.basename(url))

    fs = fsspec.filesystem("http")

    # Use fsspec to get the remote file and save it locally
    fs.get(url, local_file_name)

    return local_file_name

def download_vscode(config: VscodeConfig):
    """
    Download vscode server and extension from remote to local and add the directory of binary executable to $PATH.

    Args:
        config (VscodeConfig): VSCode config contains default URLs of the VSCode server and extension remote paths.
    """

    # If the code server already exists in the container, skip downloading
    executable_path = shutil.which(EXECUTABLE_NAME)
    if executable_path is not None:
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


    # Download remote file to local
    code_server_tar_path = download_file(config.code_server_remote_path, DOWNLOAD_DIR)

    extension_paths = []
    for extension in config.extension_remote_paths:
        file_path = download_file(extension, DOWNLOAD_DIR)
        extension_paths.append(file_path)

    # Extract the tarball
    with tarfile.open(code_server_tar_path, "r:gz") as tar:
        tar.extractall(path=DOWNLOAD_DIR)

    code_server_bin_dir = os.path.join(DOWNLOAD_DIR, config.code_server_dir_name, "bin")

    # Add the directory of code-server binary to $PATH
    os.environ["PATH"] = code_server_bin_dir + os.pathsep + os.environ["PATH"]

    for p in extension_paths:
        execute_command(f"code-server --install-extension {p}")

def execute_command(cmd):
    """
    Execute a command in the shell.
    """


    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"Command {cmd} failed with error: {stderr}")
    

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
