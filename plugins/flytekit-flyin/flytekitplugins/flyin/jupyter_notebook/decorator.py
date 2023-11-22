import multiprocessing
import socket
import subprocess
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Callable, Optional

from flytekit.loggers import logger
from .constants import DEFAULT_CONFIG_FILE_PATH, WS_PING_TIMEOUT, HEARTBEAT_CHECK_SECONDS


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


def is_jupyter_notebook_running(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def exit_handler(
    child_process: multiprocessing.Process, port: int = 8888, post_execute: Optional[Callable] = None
):
    """
    Args:
        child_process (multiprocessing.Process, optional): The process to be terminated.
        port (int, optional): The port of the running jupyter notebook.
        post_execute (function, optional): The function to be executed before the vscode is self-terminated.
    """
    start_time = time.time()
    delta = 0

    while True:
        if is_jupyter_notebook_running(port):
            delta = time.time() - start_time
            logger.info(f"The latest activity on jupyter notebook is {delta} seconds ago.")
        else:
            logger.info(f"Jupyter notebook server is timeout. Terminating...")
            if post_execute is not None:
                post_execute()
                logger.info("Post execute function executed successfully!")
            child_process.terminate()
            child_process.join()
            sys.exit()

        time.sleep(HEARTBEAT_CHECK_SECONDS)


def create_jupyter_notebook_config():
    subprocess.run(["jupyter", "notebook", "--generate-config"], check=True)


def set_jupyter_notebook_timeout(ws_ping_timeout: Optional[int]):
    # Generate config file if not exists
    config_file = Path.home() / DEFAULT_CONFIG_FILE_PATH
    if not config_file.is_file():
        create_jupyter_notebook_config()

    with open(config_file, "r") as file:
        lines = file.readlines()

    with open(config_file, "w") as file:
        for line in lines:
            if line.strip() == "#c.NotebookApp.tornado_settings = {}":
                line = """
                c.NotebookApp.tornado_settings = {{
                    'ws_ping_interval': 1000,
                    'ws_ping_timeout': ws_ping_timeout,
                }}
                """.format(ws_ping_interval=ws_ping_timeout)
                file.write(line)


def jupyter(
    _task_function: Optional[Callable] = None,
    ws_ping_timeout: Optional[int] = WS_PING_TIMEOUT,
    token: Optional[str] = "",
    port: Optional[int] = 8888,
    enable: Optional[bool] = True,
    notebook_dir: Optional[str] = "/root",
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None,
):
    def wrapper(fn):
        if not enable:
            return fn

        @wraps(fn)
        def inner_wrapper(*args, **kwargs):
            # 0. Executes the pre_execute function if provided.
            if pre_execute is not None:
                pre_execute()
                logger.info("Pre execute function executed successfully!")

            # 1. Set the idle timeout for Jupyter Notebook.
            set_jupyter_notebook_timeout(ws_ping_timeout)

            # 2. Launches and monitors the Jupyter Notebook server.
            # Run the function in the background
            logger.info(f"Start the server for {ws_ping_timeout} seconds...")
            cmd = f"jupyter notebook --port {port} --NotebookApp.token={token}"
            if notebook_dir:
                cmd += f" --notebook-dir={notebook_dir}"
            child_process = multiprocessing.Process(target=execute_command, kwargs={"cmd": cmd})

            child_process.start()
            exit_handler(child_process, port, post_execute)

        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper
