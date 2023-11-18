import multiprocessing
import subprocess
import sys
import time
from functools import wraps
from typing import Callable, Optional

import fsspec

from flytekit.loggers import logger

from .constants import (
    DEFAULT_UP_SECONDS,
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


def jupyter(
    _task_function: Optional[Callable] = None,
    server_up_seconds: Optional[int] = DEFAULT_UP_SECONDS,
    token: Optional[str] = '',
    port: Optional[int] = 8888,
    enable: Optional[bool] = True,
    no_browser: Optional[bool] = False,
    # The untarred directory name may be different from the tarball name
    pre_execute: Optional[Callable] = None,
    post_execute: Optional[Callable] = None,
):

    def wrapper(fn):
        if not enable:
            return fn

        @wraps(fn)
        def inner_wrapper(*args, **kwargs):
            print("@@@@@ Jason Jupyter Notebook Test")
            # 0. Executes the pre_execute function if provided.
            if pre_execute is not None:
                pre_execute()
                logger.info("Pre execute function executed successfully!")

            # 2. Launches and monitors the VSCode server.
            # Run the function in the background
            logger.info(f"Start the server for {server_up_seconds} seconds...")
            cmd = f"jupyter notebook --port {port} --NotebookApp.token={token}"
            if no_browser:
                cmd = f"jupyter notebook --no-browser --port {port} --NotebookApp.token={token}"
            child_process = multiprocessing.Process(
                target=execute_command, kwargs={"cmd": cmd}
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
