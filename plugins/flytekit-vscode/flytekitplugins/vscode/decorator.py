import multiprocessing
import os
import shutil
import subprocess
import sys
import tarfile
import time
from functools import wraps
from typing import Callable, Optional

import fsspec

from flytekit.loggers import logger

from .constants import (
    DEFAULT_CODE_SERVER_DIR_NAME,
    DEFAULT_CODE_SERVER_REMOTE_PATH,
    DEFAULT_UP_SECONDS,
    DOWNLOAD_DIR,
    EXECUTABLE_NAME,
)


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


def download_file(url, target_dir="."):
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


def download_vscode(
    code_server_remote_path: str,
    code_server_dir_name: str,
):
    """
    Download vscode server and plugins from remote to local and add the directory of binary executable to $PATH.

    Args:
        code_server_remote_path (str): The URL of the code-server tarball.
        code_server_dir_name (str): The name of the code-server directory.
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

    # Extract the tarball
    with tarfile.open(code_server_tar_path, "r:gz") as tar:
        tar.extractall(path=DOWNLOAD_DIR)

    code_server_dir_path = os.path.join(DOWNLOAD_DIR, code_server_dir_name)

    code_server_bin_dir = os.path.join(code_server_dir_path, "bin")

    # Add the directory of code-server binary to $PATH
    os.environ["PATH"] = code_server_bin_dir + os.pathsep + os.environ["PATH"]


def vscode(
    _task_function: Optional[Callable] = None,
    server_up_seconds: Optional[int] = DEFAULT_UP_SECONDS,
    port: Optional[int] = 8080,
    enable: Optional[bool] = True,
    code_server_remote_path: Optional[str] = DEFAULT_CODE_SERVER_REMOTE_PATH,
    # The untarred directory name may be different from the tarball name
    code_server_dir_name: Optional[str] = DEFAULT_CODE_SERVER_DIR_NAME,
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None,
):
    """
    vscode decorator modifies a container to run a VSCode server:
    1. Overrides the user function with a VSCode setup function.
    2. Download vscode server and plugins from remote to local.
    3. Launches and monitors the VSCode server.
    4. Terminates after server_up_seconds seconds.

    Args:
        _task_function (function, optional): The user function to be decorated. Defaults to None.
        port (int, optional): The port to be used by the VSCode server. Defaults to 8080.
        enable (bool, optional): Whether to enable the VSCode decorator. Defaults to True.
        code_server_remote_path (str, optional): The URL of the code-server tarball.
        code_server_dir_name (str, optional): The name of the code-server directory.
        pre_execute (function, optional): The function to be executed before the vscode setup function.
        post_execute (function, optional): The function to be executed before the vscode is self-terminated.
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
            )

            # 2. Launches and monitors the VSCode server.
            # Run the function in the background
            logger.info(f"Start the server for {server_up_seconds} seconds...")
            child_process = multiprocessing.Process(
                target=execute_command, kwargs={"cmd": f"code-server --bind-addr 0.0.0.0:{port} --auth none"}
            )

            child_process.start()
            time.sleep(server_up_seconds)

            # 3. Terminates the server after server_up_seconds
            logger.info(f"{server_up_seconds} seconds passed. Terminating...")
            if post_execute is not None:
                post_execute()
                logger.info("Post execute function executed successfully!")
            child_process.terminate()
            child_process.join()
            sys.exit(0)

        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper
