import multiprocessing
import os
import shutil
import subprocess
import sys
import tarfile
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable, List, Optional

import fsspec

from flytekit.loggers import logger

from .constants import (
    DEFAULT_CODE_SERVER_DIR_NAME,
    DEFAULT_CODE_SERVER_EXTENSIONS,
    DEFAULT_CODE_SERVER_REMOTE_PATH,
    DOWNLOAD_DIR,
    EXECUTABLE_NAME,
    HEARTBEAT_CHECK_SECONDS,
    HEARTBEAT_PATH,
    MAX_IDLE_SECONDS,
)


@dataclass
class VscodeConfig:
    """
    VscodeConfig is the config contains default URLs of the VSCode server and extension remote paths.

    Args:
        code_server_remote_path (str, optional): The URL of the code-server tarball.
        code_server_dir_name (str, optional): The name of the code-server directory.
        extension_remote_paths (List[str], optional): The URLs of the VSCode plugins.
            You can find all available plugins at https://open-vsx.org/.
    """

    code_server_remote_path: Optional[str] = DEFAULT_CODE_SERVER_REMOTE_PATH
    code_server_dir_name: Optional[str] = DEFAULT_CODE_SERVER_DIR_NAME
    extension_remote_paths: Optional[List[str]] = field(default_factory=lambda: DEFAULT_CODE_SERVER_EXTENSIONS)


def execute_command(cmd):
    """
    Execute a command in the shell.
    """

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info(f"cmd: {cmd}")
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"Command {cmd} failed with error: {stderr}")
    logger.info(f"stdout: {stdout}")
    logger.info(f"stderr: {stderr}")


def exit_handler(
    child_process: multiprocessing.Process, max_idle_seconds: int = 180, post_execute: Optional[Callable] = None
):
    """
    Check the modified time of ~/.local/share/code-server/heartbeat.
    If it is older than max_idle_second seconds, kill the container.
    Otherwise, check again every HEARTBEAT_CHECK_SECONDS.

    Args:
        child_process (multiprocessing.Process, optional): The process to be terminated.
        max_idle_seconds (int, optional): The duration in seconds to live after no activity detected.
        post_execute (function, optional): The function to be executed before the vscode is self-terminated.
    """
    start_time = time.time()
    delta = 0

    while True:
        if not os.path.exists(HEARTBEAT_PATH):
            delta = time.time() - start_time
            logger.info(f"Code server has not been connected since {delta} seconds ago.")
            logger.info("Please open the browser to connect to the running server.")
        else:
            delta = time.time() - os.path.getmtime(HEARTBEAT_PATH)
            logger.info(f"The latest activity on code server is {delta} seconds ago.")

        # If the time from last connection is longer than max idle seconds, terminate the vscode server.
        if delta > max_idle_seconds:
            logger.info(f"VSCode server is idle for more than {max_idle_seconds} seconds. Terminating...")
            if post_execute is not None:
                post_execute()
                logger.info("Post execute function executed successfully!")
            child_process.terminate()
            child_process.join()
            sys.exit()

        time.sleep(HEARTBEAT_CHECK_SECONDS)


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
    logger.info(f"Downloading {url}... to {os.path.abspath(local_file_name)}")
    fs.get(url, local_file_name)
    logger.info("File downloaded successfully!")

    return local_file_name

def download_vscode(vscode_config: VscodeConfig):
    """
    Download vscode server and extension from remote to local and add the directory of binary executable to $PATH.

    Args:
        vscode_config (VscodeConfig): VSCode config contains default URLs of the VSCode server and extension remote paths.
    """

    # If the code server already exists in the container, skip downloading
    executable_path = shutil.which(EXECUTABLE_NAME)
    if executable_path is not None:
        logger.info(f"Code server binary already exists at {executable_path}")
        logger.info("Skipping downloading code server...")
        return

    logger.info("Code server is not in $PATH, start downloading code server...")

    # Create DOWNLOAD_DIR if not exist
    logger.info(f"DOWNLOAD_DIR: {DOWNLOAD_DIR}")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    logger.info(f"Start downloading files to {DOWNLOAD_DIR}")

    # Download remote file to local
    code_server_tar_path = download_file(vscode_config.code_server_remote_path, DOWNLOAD_DIR)

    # Extract the tarball
    with tarfile.open(code_server_tar_path, "r:gz") as tar:
        tar.extractall(path=DOWNLOAD_DIR)

    code_server_bin_dir = os.path.join(DOWNLOAD_DIR, vscode_config.code_server_dir_name, "bin")

    # Add the directory of code-server binary to $PATH
    os.environ["PATH"] = code_server_bin_dir + os.pathsep + os.environ["PATH"]


def download_extension(extension_remote_paths: List[str]):
    extension_paths = []
    for extension in extension_remote_paths:
        file_path = download_file(extension, DOWNLOAD_DIR)
        extension_paths.append(file_path)

    for p in extension_paths:
        logger.info(f"Execute extension installation command to install extension {p}")
        execute_command(f"code-server --install-extension {p}")
    

def vscode(
    _task_function: Optional[Callable] = None,
    max_idle_seconds: Optional[int] = MAX_IDLE_SECONDS,
    port: Optional[int] = 8080,
    enable: Optional[bool] = True,
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None,
    config: Optional[VscodeConfig] = None,
):
    """
    vscode decorator modifies a container to run a VSCode server:
    1. Overrides the user function with a VSCode setup function.
    2. Download vscode server and extension from remote to local.
    3. Launches and monitors the VSCode server.
    4. Terminates if the server is idle for a set duration.

    Args:
        _task_function (function, optional): The user function to be decorated. Defaults to None.
        max_idle_seconds (int, optional): The duration in seconds to live after no activity detected.
        port (int, optional): The port to be used by the VSCode server. Defaults to 8080.
        enable (bool, optional): Whether to enable the VSCode decorator. Defaults to True.
        pre_execute (function, optional): The function to be executed before the vscode setup function.
        post_execute (function, optional): The function to be executed before the vscode is self-terminated.
        config (VscodeConfig, optional): VSCode config contains default URLs of the VSCode server and extension remote paths.
    """

    if config is None:
        config = VscodeConfig()

    def wrapper(fn):
        if not enable:
            return fn

        @wraps(fn)
        def inner_wrapper(*args, **kwargs):
            # 0. Executes the pre_execute function if provided.
            if pre_execute is not None:
                pre_execute()
                logger.info("Pre execute function executed successfully!")

            # 1. Downloads the VSCode server from Internet to local.
            download_vscode(config)
            download_extension(config.extension_remote_paths)

            # 2. Launches and monitors the VSCode server.
            # Run the function in the background
            child_process = multiprocessing.Process(
                target=execute_command, kwargs={"cmd": f"code-server --bind-addr 0.0.0.0:{port} --auth none"}
            )

            child_process.start()
            exit_handler(child_process, max_idle_seconds, post_execute)

        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper
