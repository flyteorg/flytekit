import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
from functools import wraps
from typing import Callable, List, Optional

import fsspec

from flytekit.loggers import logger

from .constants import (DEFAULT_CODE_SERVER_DIR_NAME,
                        DEFAULT_CODE_SERVER_REMOTE_PATH, DOWNLOAD_DIR,
                        EXECUTABLE_NAME)

# Default max idle seconds to terminate the vscode server
HOURS_TO_SECONDS = 60 * 60
MAX_IDLE_SECONDS = 10 * HOURS_TO_SECONDS  # 10 hours

# Regex to extract the extension name from the extension URL.
# This is to differentiate between different extensions downloaded locally
EXTRACT_EXTENSION_NAME_REGEX = r'/extension/([^/]+)/\d+\.\d+\.\d+/'

# The path is hardcoded by code-server
# https://coder.com/docs/code-server/latest/FAQ#what-is-the-heartbeat-file
HEARTBEAT_PATH = os.path.expanduser("~/.local/share/code-server/heartbeat")


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
    max_idle_seconds: int = 180,
    child_process: multiprocessing.Process = None,
    post_execute: Optional[Callable] = None
):
    """
    Check the modified time of ~/.local/share/code-server/heartbeat.
    If it is older than max_idle_second seconds, kill the container.
    Otherwise, sleep for 60 seconds and check again.

    Args:
        max_idle_seconds (int, optional): The duration in seconds to live after no activity detected.
        child_process (multiprocessing.Process, optional): The process to be terminated.
        post_execute (function, optional): The function to be executed before the vscode is self-terminated.
    """
    start_time = time.time()
    delta = 0

    while True:
        if not os.path.exists(HEARTBEAT_PATH):
            delta = (time.time() - start_time)
            logger.info(f"Code server has not been connected since {delta} seconds ago.")
            logger.info(f"Please open the browser to connect to the running server.")
        else:
            delta = (time.time() - os.path.getmtime(HEARTBEAT_PATH))
            logger.info(f"The latest activity on code server is {delta} seconds ago.")

        # if the time till last connection is longer than max idle seconds, terminate the vscode server.
        if delta > max_idle_seconds:
            logger.info(f"Container is idle for more than {max_idle_seconds} seconds. Terminating...")
            if post_execute is not None:
                post_execute()
                logger.info("Post execute function executed successfully!")
            child_process.terminate()
            child_process.join()
            sys.exit()

        time.sleep(60)


def download_file(url,
                  target_dir: Optional[str] = ".",
                  target_file_name: Optional[str] = ""):
    """
    Download a file from a given URL using fsspec.

    Args:
        url (str): The URL of the file to download.
        target_dir (str, optional): The directory where the file should be saved. Defaults to current directory.
        target_file_name (str, optional): The on-disk file name. Defaults to unspecified so the file name in original url will be used

    Returns:
        str: The path to the downloaded file.
    """

    if not url.startswith("http"):
        raise ValueError(f"URL {url} is not valid. Only http/https is supported.")

    # Derive the local filename from the URL
    local_file_name = os.path.join(target_dir, target_file_name if target_file_name else os.path.basename(url))

    fs = fsspec.filesystem("http")

    # Use fsspec to get the remote file and save it locally
    logger.info(f"Downloading {url}... to {os.path.abspath(local_file_name)}")
    fs.get(url, local_file_name)
    logger.info("File downloaded successfully!")

    return local_file_name


def download_vscode(
    code_server_remote_path: str,
    code_server_dir_name: str,
    plugins_remote_paths: List[str]
):
    """
    Download vscode server and plugins from remote to local and add the directory of binary executable to $PATH.

    Args:
        code_server_remote_path (str): The URL of the code-server tarball.
        code_server_dir_name (str): The name of the code-server directory.
        plugins_remote_paths (List[str]): The URLs of the VSCode plugins.
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
    code_server_tar_path = download_file(code_server_remote_path, DOWNLOAD_DIR)
    
    plugin_paths = []
    for plugin in plugins_remote_paths:
        match = re.search(EXTRACT_EXTENSION_NAME_REGEX, plugin)
        # Extract the extension_name if a match is found, otherwise skip this plugin
        if match:
            extension_name = match.group(1)
            logger.info(f"Extension Name from plugin url {plugin} : {extension_name}")
        else:
            logger.info(f"No match extention name found for path: {plugin}")
            continue
        extention_name = re.search(EXTRACT_EXTENSION_NAME_REGEX, plugin).group(1)
        logger.info("Extention name extracted from the path is {extention_name}")
        file_path = download_file(plugin, DOWNLOAD_DIR, f"{extention_name}.vsix")
        plugin_paths.append(file_path)

    # Extract the tarball
    with tarfile.open(code_server_tar_path, "r:gz") as tar:
        tar.extractall(path=DOWNLOAD_DIR)

    code_server_bin_dir = os.path.join(DOWNLOAD_DIR, code_server_dir_name, "bin")

    # Add the directory of code-server binary to $PATH
    os.environ["PATH"] = code_server_bin_dir + os.pathsep + os.environ["PATH"]
    
    for p in plugin_paths:
        logger.info(f"Execute plugin installation command to install plugin {p}")
        execute_command(f"code-server --install-extension {p}")


def vscode(
    _task_function: Optional[Callable] = None,
    max_idle_seconds: Optional[int] = MAX_IDLE_SECONDS,
    port: Optional[int] = 8080,
    enable: Optional[bool] = True,
    code_server_remote_path: Optional[str] = DEFAULT_CODE_SERVER_REMOTE_PATH,
    # The untarred directory name may be different from the tarball name
    code_server_dir_name: Optional[str] = DEFAULT_CODE_SERVER_DIR_NAME,
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None,
    plugins_remote_paths: List[str] = [
        "https://ms-python.gallery.vsassets.io/_apis/public/gallery/publisher/ms-python/extension/python/2023.12.0/assetbyname/Microsoft.VisualStudio.Services.VSIXPackage",
        "https://ms-toolsai.gallery.vsassets.io/_apis/public/gallery/publisher/ms-toolsai/extension/Jupyter/2023.4.1001091014/assetbyname/Microsoft.VisualStudio.Services.VSIXPackage"
    ]
):
    """
    vscode decorator modifies a container to run a VSCode server:
    1. Overrides the user function with a VSCode setup function.
    2. Download vscode server and plugins from remote to local.
    3. Launches and monitors the VSCode server.
    4. Terminates if the server is idle for a set duration.

    Args:
        _task_function (function, optional): The user function to be decorated. Defaults to None.
        max_idle_seconds (int, optional): The duration in seconds to live after no activity detected.
        port (int, optional): The port to be used by the VSCode server. Defaults to 8080.
        enable (bool, optional): Whether to enable the VSCode decorator. Defaults to True.
        code_server_remote_path (str, optional): The URL of the code-server tarball.
        code_server_dir_name (str, optional): The name of the code-server directory.
        pre_execute (function, optional): The function to be executed before the vscode setup function.
        post_execute (function, optional): The function to be executed before the vscode is self-terminated.
        plugins_remote_paths (List[str], optional): The URLs of the VSCode plugins.
    """

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
            download_vscode(
                code_server_remote_path=code_server_remote_path,
                code_server_dir_name=code_server_dir_name,
                plugins_remote_paths=plugins_remote_paths
            )

            # 2. Launches and monitors the VSCode server.
            # Run the function in the background
            child_process = multiprocessing.Process(
                target=execute_command, kwargs={"cmd": f"code-server --bind-addr 0.0.0.0:{port} --auth none"}
            )

            child_process.start()
            exit_handler(max_idle_seconds, child_process, post_execute)

            return

        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper
