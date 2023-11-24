import subprocess
import sys
from functools import wraps
from pathlib import Path
from typing import Callable, Optional

from flytekit.loggers import logger
from .constants import DEFAULT_CONFIG_FILE_PATH, WS_PING_TIMEOUT


def create_jupyter_notebook_config():
    subprocess.run(["jupyter", "notebook", "--generate-config"], check=True)


def set_jupyter_notebook_timeout(ws_ping_timeout: Optional[int]):
    logger.info("set_jupyter_notebook_timeout")
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
                    'ws_ping_timeout': 60000,
                }}
                """
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
            print("jupyter decorator start")
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

            process = subprocess.Popen(cmd, shell=True)

            # 3. Wait for the process to finish
            process.wait()

            # 4. Exit after subprocess has finished
            if post_execute is not None:
                post_execute()
                logger.info("Post execute function executed successfully!")
            sys.exit()

        return inner_wrapper

    # for the case when the decorator is used without arguments
    if _task_function is not None:
        return wrapper(_task_function)
    # for the case when the decorator is used with arguments
    else:
        return wrapper
