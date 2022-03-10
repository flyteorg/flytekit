import json
import logging
import subprocess
from typing import List


def run_cli(cmd: List[str]) -> (int, List[str]):
    """
    Execute a CLI command in a subprocess

    Parameters
    ----------
    cmd : list of str
        Command to be executed.

    Returns
    -------
    int
        Command's exit code.
    list of str
        Logs produced by the command execution.
    """

    logs = []
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for raw_line in process.stdout or []:
        line = raw_line.decode("utf-8")
        try:
            json_line = json.loads(line)
        except json.JSONDecodeError:
            logging.info(line.rstrip())
        else:
            logs.append(json_line)
            level = json_line.get("levelname", "").lower()
            if hasattr(logging, level):
                getattr(logging, level)(json_line.get("message", ""))
            else:
                logging.info(line.rstrip())

    process.wait()
    return process.returncode, logs
