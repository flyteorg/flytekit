import json
import multiprocessing
import os
import shutil
import subprocess
import sys
import tarfile
import time
from typing import Callable, Optional

import fsspec

import flytekit
from flytekit.core.context_manager import FlyteContextManager

from .config import VscodeConfig
from .constants import (
    DOWNLOAD_DIR,
    EXECUTABLE_NAME,
    HEARTBEAT_CHECK_SECONDS,
    HEARTBEAT_PATH,
    INTERACTIVE_DEBUGGING_FILE_NAME,
    MAX_IDLE_SECONDS,
)

from flytekit.core.utils import ClassDecorator


def execute_command(cmd):
    """
    Execute a command in the shell.
    """

    logger = flytekit.current_context().logging

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info(f"cmd: {cmd}")
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"Command {cmd} failed with error: {stderr}")
    logger.info(f"stdout: {stdout}")
    logger.info(f"stderr: {stderr}")


def exit_handler(
    child_process: multiprocessing.Process,
    max_idle_seconds: int = 180,
    post_execute: Optional[Callable] = None,
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

    logger = flytekit.current_context().logging
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
    logger = flytekit.current_context().logging
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


def download_vscode(config: VscodeConfig):
    """
    Download vscode server and extension from remote to local and add the directory of binary executable to $PATH.

    Args:
        config (VscodeConfig): VSCode config contains default URLs of the VSCode server and extension remote paths.
    """
    logger = flytekit.current_context().logging

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
        logger.info(f"Execute extension installation command to install extension {p}")
        execute_command(f"code-server --install-extension {p}")


def prepare_interactive_python(task_function):
    """
    1. Copy the original task file to the context working directory. This ensures that the inputs.pb can be loaded, as loading requires the original task interface.
       By doing so, even if users change the task interface in their code, we can use the copied task file to load the inputs as native Python objects.
    2. Generate a Python script and a launch.json for users to debug interactively.

    Args:
        task_function (function): User's task function.
    """

    context_working_dir = FlyteContextManager.current_context().execution_state.working_dir

    # Copy the user's Python file to the working directory.
    shutil.copy(
        f"{task_function.__module__}.py",
        os.path.join(context_working_dir, f"{task_function.__module__}.py"),
    )

    # Generate a Python script
    task_module_name, task_name = task_function.__module__, task_function.__name__
    python_script = f"""# This file is auto-generated by flyin

from {task_module_name} import {task_name}
from flytekitplugins.flyin import get_task_inputs

if __name__ == "__main__":
    inputs = get_task_inputs(
        task_module_name="{task_module_name}",
        task_name="{task_name}",
        context_working_dir="{context_working_dir}",
    )
    # You can modify the inputs! Ex: inputs['a'] = 5
    print({task_name}(**inputs))
"""

    with open(INTERACTIVE_DEBUGGING_FILE_NAME, "w") as file:
        file.write(python_script)

    # Generate a launch.json
    launch_json = {
        "version": "0.2.0",
        "configurations": [
            {
                "name": "Interactive Debugging",
                "type": "python",
                "request": "launch",
                "program": os.path.join(os.getcwd(), INTERACTIVE_DEBUGGING_FILE_NAME),
                "console": "integratedTerminal",
                "justMyCode": True,
            }
        ],
    }

    vscode_directory = ".vscode"
    if not os.path.exists(vscode_directory):
        os.makedirs(vscode_directory)

    with open(os.path.join(vscode_directory, "launch.json"), "w") as file:
        json.dump(launch_json, file, indent=4)


VSCODE_TYPE_VALUE = "vscode"


class vscode(ClassDecorator):
    def __init__(
        self,
        fn: Optional[Callable] = None,
        max_idle_seconds: Optional[int] = MAX_IDLE_SECONDS,
        port: int = 8080,
        enable: bool = True,
        run_task_first: bool = False,
        pre_execute: Optional[Callable] = None,
        post_execute: Optional[Callable] = None,
        config: Optional[VscodeConfig] = None,
    ):
        """
        vscode decorator modifies a container to run a VSCode server:
        1. Overrides the user function with a VSCode setup function.
        2. Download vscode server and extension from remote to local.
        3. Prepare the interactive debugging Python script and launch.json.
        4. Launches and monitors the VSCode server.
        5. Terminates if the server is idle for a set duration.

        Args:
            fn (function, optional): The user function to be decorated. Defaults to None.
            max_idle_seconds (int, optional): The duration in seconds to live after no activity detected.
            port (int, optional): The port to be used by the VSCode server. Defaults to 8080.
            enable (bool, optional): Whether to enable the VSCode decorator. Defaults to True.
            run_task_first (bool, optional): Executes the user's task first when True. Launches the VSCode server only if the user's task fails. Defaults to False.
            pre_execute (function, optional): The function to be executed before the vscode setup function.
            post_execute (function, optional): The function to be executed before the vscode is self-terminated.
            config (VscodeConfig, optional): VSCode config contains default URLs of the VSCode server and extension remote paths.
        """

        # these names cannot conflict with base_task method or member variables
        # otherwise, the base_task method will be overwritten
        # for example, base_task also has "pre_execute", so we name it "_pre_execute" here
        self.fn = fn
        self.max_idle_seconds = max_idle_seconds
        self.port = port
        self.enable = enable
        self.run_task_first = run_task_first
        self._pre_execute = pre_execute
        self._post_execute = post_execute

        if config is None:
            config = VscodeConfig()
        self._config = config

        # arguments are required to be passed in order to access from _wrap_call
        super().__init__(
            self.fn,
            max_idle_seconds=max_idle_seconds,
            port=port,
            enable=enable,
            run_task_first=run_task_first,
            pre_execute=pre_execute,
            post_execute=post_execute,
            config=config,
        )

    def _wrap_call(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        logger = flytekit.current_context().logging

        # 1. If the decorator is disabled, we don't launch the VSCode server.
        # 2. When user use pyflyte run or python to execute the task, we don't launch the VSCode server.
        #   Only when user use pyflyte run --remote to submit the task to cluster, we launch the VSCode server.
        if not self.enable or ctx.execution_state.is_local_execution():
            return self.fn(*args, **kwargs)

        if self.run_task_first:
            logger.info("Run user's task first")
            try:
                return self.fn(*args, **kwargs)
            except Exception as e:
                logger.error(f"Task Error: {e}")
                logger.info("Launching VSCode server")

        # 0. Executes the pre_execute function if provided.
        if self._pre_execute is not None:
            self._pre_execute()
            logger.info("Pre execute function executed successfully!")

        # 1. Downloads the VSCode server from Internet to local.
        download_vscode(self._config)

        # 2. Prepare the interactive debugging Python script and launch.json.
        prepare_interactive_python(self.fn)

        # 3. Launches and monitors the VSCode server.
        # Run the function in the background
        child_process = multiprocessing.Process(
            target=execute_command,
            kwargs={"cmd": f"code-server --bind-addr 0.0.0.0:{self.port} --auth none"},
        )

        child_process.start()
        exit_handler(child_process, self.max_idle_seconds, self._post_execute)

    def get_extra_config(self):
        return {self.LINK_TYPE_KEY: VSCODE_TYPE_VALUE, self.PORT_KEY: str(self.port)}
