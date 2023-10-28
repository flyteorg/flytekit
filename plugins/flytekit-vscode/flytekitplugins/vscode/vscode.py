import subprocess
from functools import wraps
import time
import os
import multiprocessing
import fsspec
from typing import Optional, Callable
import tarfile

# Where the code-server tar and plugins are downloaded to
DOWNLOAD_DIR = os.path.expanduser("/tmp")
HOURS_TO_SECONDS = 60 * 60
DEFAULT_UP_SECONDS = 10 * HOURS_TO_SECONDS  # 10 hours

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

def download_file(url, target_dir='.'):
    """
    Download a file from a given URL using fsspec.

    Parameters:
    - url (str): The URL of the file to download.
    - target_dir (str, optional): The directory where the file should be saved. Defaults to current directory.

    Returns:
    - str: The path to the downloaded file.
    """

    if not url.startswith("http"):
        raise ValueError(f"URL {url} is not valid. Only http/https is supported.")

    # Derive the local filename from the URL
    local_file_name = os.path.join(target_dir, os.path.basename(url))

    fs = fsspec.filesystem("http")

    # Use fsspec to get the remote file and save it locally
    print_flush(f"Downloading {url}... to {os.path.abspath(local_file_name)}")
    fs.get(url, local_file_name)
    print_flush("File downloaded successfully!")

    return local_file_name

def download_vscode(
    code_server_remote_path: str,
    code_server_dir_name: str,
) -> str:
    """
    Download vscode server and plugins from remote to local.

    Parameters:
    - code_server_remote_path (str): The URL of the code-server tarball.
    - code_server_dir_name (str): The name of the code-server directory.
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

    print_flush(f"Start downloading files to {DOWNLOAD_DIR}")
    code_server_tar_path = download_file(code_server_remote_path, DOWNLOAD_DIR)

    # Extract the tarball
    with tarfile.open(code_server_tar_path, "r:gz") as tar:
        tar.extractall(path=DOWNLOAD_DIR)

    code_server_dir_path = os.path.join(DOWNLOAD_DIR, code_server_dir_name)

    code_server_bin = os.path.join(code_server_dir_path, "bin", "code-server")

    return code_server_bin

def vscode(
    _task_function: Optional[Callable] = None,
    server_up_seconds: Optional[int] = DEFAULT_UP_SECONDS,
    port: Optional[int] = 8080,
    enable: Optional[bool] = True,
    code_server_remote_path: Optional[str] = "https://github.com/coder/code-server/releases/download/v4.18.0/code-server-4.18.0-linux-amd64.tar.gz",
    # The untarred directory name may be different from the tarball name
    code_server_dir_name: Optional[str] = "code-server-4.18.0-linux-amd64",
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None
):
    """
    vscode decorator modifies a container to run a VSCode server:
    1. Overrides the user function with a VSCode setup function.
    2. Download vscode server and plugins from remote to local.
    3. Launches and monitors the VSCode server.
    4. Terminates after 

    Parameters:
    - _task_function (function, optional): The user function to be decorated. Defaults to None.
    - port (int, optional): The port to be used by the VSCode server. Defaults to 8080.
    - enable (bool, optional): Whether to enable the VSCode decorator. Defaults to True.
    - code_server_remote_path (str, optional): The URL of the code-server tarball.
    - code_server_dir_name (str, optional): The name of the code-server directory.
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
            )

            # 2. Launches and monitors the VSCode server.
            # Run the function in the background

            child_process = multiprocessing.Process(
                target=execute_command,
                kwargs={"cmd": f"{code_server_bin} --bind-addr 0.0.0.0:{port} --auth none"}
            )

            print_flush(f"Start the server for {server_up_seconds} seconds...")
            child_process.start()
            time.sleep(server_up_seconds)

            # 3. Terminates the server after server_up_seconds
            print_flush(f"{server_up_seconds} seconds passed. Terminating...")
            if post_execute is not None:
                post_execute()
                print_flush("Post execute function executed successfully!")
            child_process.terminate()
            child_process.join()

            return

        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper