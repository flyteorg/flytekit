import subprocess
from functools import wraps
import time
import os
import multiprocessing
import sys
import fsspec
import ssl
from typing import List, Optional, Callable
import tarfile


# The path is hardcoded by code-server
# https://coder.com/docs/code-server/latest/FAQ#what-is-the-heartbeat-file
HEARTBEAT_PATH = os.path.expanduser("~/.local/share/code-server/heartbeat")

# Where the code-server tar and plugins are downloaded to
DOWNLOAD_DIR = os.path.expanduser("~/.local/lib")
HOURS_TO_SECONDS = 60 * 60
MAX_IDLE_SECONDS = 10 * HOURS_TO_SECONDS  # 10 hours

def print_flush(*args, **kwargs):
    """
    Print and flush the output.
    """
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), end=" ")
    print(*args, **kwargs, flush=True)

def execute_command(cmd):
    """
    Execute a command in the shell.
    """
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print_flush("cmd: ", cmd)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"Command {cmd} failed with error: {stderr}")
    print_flush("stdout: ", stdout)
    print_flush("stderr: ", stderr)

def exit_handler(
    max_idle_seconds: int = 180,
    child_process: multiprocessing.Process = None,
    post_execute: Optional[Callable] = None
):
    """
    Check the modified time of ~/.local/share/code-server/heartbeat.
    If it is older than max_idle_second seconds, kill the container.
    Otherwise, sleep for 60 seconds and check again.

    Parameters:
    - max_idle_seconds (int, optional): The duration in seconds to live after no activity detected.
    - child_process (multiprocessing.Process, optional): The process to be terminated.
    - post_execute (function, optional): The function to be executed before the vscode is self-terminated.
    """
    
    start_time = time.time()
    delta = 0

    while True:
        if not os.path.exists(HEARTBEAT_PATH):
            delta = (time.time() - start_time)
            print_flush(f"Code server has not been connected since {delta} seconds ago.")
        else:
            delta = (time.time() - os.path.getmtime(HEARTBEAT_PATH))
            print_flush(f"The latest activity on code server is {delta} seconds ago.")

        # Send termination email and terminate process
        if delta > max_idle_seconds:
            print_flush(f"Container is idle for more than {max_idle_seconds} seconds. Terminating...")
            if post_execute is not None:
                post_execute()
                print_flush("Post execute function executed successfully!")
            child_process.terminate()
            child_process.join()
            sys.exit()

        time.sleep(60)

def download_file(url, target_dir='.'):
    """
    Download a file from a given URL using fsspec.

    Parameters:
    - url (str): The URL of the file to download.
    - target_dir (str, optional): The directory where the file should be saved. Defaults to current directory.

    Returns:
    - str: The path to the downloaded file.
    """

    # Derive the local filename from the URL
    local_file_name = os.path.join(target_dir, os.path.basename(url))

    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations('XXX')
    fs = fsspec.filesystem("https")

    if not url.startswith("http"):
        raise ValueError(f"URL {url} is not valid. Only http/https is supported.")

    # Use fsspec to get the remote file and save it locally
    fs.get(url, local_file_name, ssl_context=ssl_context)
    print_flush(f"Downloading {url}... to {os.path.abspath(local_file_name)}")

    print_flush("File downloaded successfully!")
    return local_file_name



def download_vscode(
    code_server_remote_path: str,
    code_server_dir_name: str,
    plugins_remote_paths: List[str]
) -> str:
    """
    Download vscode server and plugins from remote to local.

    Parameters:
    - code_server_remote_path (str): The URL of the code-server tarball.
    - code_server_dir_name (str): The name of the code-server directory.
    - plugins_remote_paths (List[str]): The URLs of the VSCode plugins.
    Return:
    - str: The path to the code-server binary.
    """

    # If the code server already exists in the container, skip downloading
    if os.path.exists(os.path.join(DOWNLOAD_DIR, code_server_dir_name)):
        print_flush(f"Code server already exists at {os.path.join(DOWNLOAD_DIR, code_server_dir_name)}")
        print_flush(f"Skipping downloading code server...")
        return os.path.join(DOWNLOAD_DIR, code_server_dir_name, "bin", "code-server")

    # Create DOWNLOAD_DIR if not exist
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    code_server_tar_path = download_file(code_server_remote_path, DOWNLOAD_DIR)
    plugin_paths = []
    for plugin in plugins_remote_paths:
        plugin_paths.append(download_file(plugin, DOWNLOAD_DIR))

    # Extract the tarball
    with tarfile.open(code_server_tar_path, "r:gz") as tar:
        tar.extractall(path=DOWNLOAD_DIR)

    code_server_dir_path = code_server_tar_path.replace(os.path.basename(code_server_tar_path), code_server_dir_name)

    code_server_bin = os.path.join(code_server_dir_path, "bin", "code-server")

    for p in plugin_paths:
        execute_command(f"{code_server_bin} --install-extension {p}")

    return code_server_bin

def vscode(
    _task_function: Optional[Callable] = None,
    max_idle_seconds: Optional[int] = MAX_IDLE_SECONDS,  # 10 hours
    port: Optional[int] = 8001,
    enable: Optional[bool] = True,
    code_server_remote_path: str = "XXX",
    # The untarred directory name may be different from the tarball name
    code_server_dir_name: str = "code-server-4.16.1-linux-amd64",
    plugins_remote_paths: List[str] = [],
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None
):
    """
    vscode decorator modifies a container to run a VSCode server:
    1. Overrides the user function with a VSCode setup function.
    2. Download vscode server and plugins from remote to local.
    3. Launches and monitors the VSCode server.
    4. Terminates if the server is idle for a set duration.

    Parameters:
    - _task_function (function, optional): The user function to be decorated. Defaults to None.
    - max_idle_seconds (int, optional): The duration in seconds to live after no activity detected.
    - port (int, optional): The port to be used by the VSCode server. Defaults to 8080.
    - enable (bool, optional): Whether to enable the VSCode decorator. Defaults to True.
    - code_server_remote_path (str, optional): The URL of the code-server tarball.
    - code_server_dir_name (str, optional): The name of the code-server directory.
    - plugins_remote_paths (List[str], optional): The URLs of the VSCode plugins.
    - pre_execute (function, optional): The function to be executed before the vscode setup function.
    - post_execute (function, optional): The function to be executed before the vscode is self-terminated.
    """
    def wrapper(fn):
        if not enable:
            return fn

        @wraps(fn)
        def inner_wrapper(*args, **kwargs):
            # 0. Executes the pre_execute function if provided.
            if pre_execute is not None:
                pre_execute()

            # 1. Downloads the VSCode server from Internet to local.
            code_server_bin = download_vscode(
                code_server_remote_path=code_server_remote_path,
                code_server_dir_name=code_server_dir_name,
                plugins_remote_paths=plugins_remote_paths
            )

            # 2. Launches and monitors the VSCode server.
            # Run the function in the background

            child_process = multiprocessing.Process(
                target=execute_command,
                kwargs={"cmd": f"{code_server_bin} --bind-addr 0.0.0.0:{port} --auth none"}
            )

            child_process.start()

            # 3. Terminates if the server is idle for a set duration.
            exit_handler(max_idle_seconds, child_process, post_execute)

            return

        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper