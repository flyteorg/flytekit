import subprocess
from typing import List, Optional

from flytekit.loggers import logger


class RuntimeEnv:
    """
    This class is used to define a runtime environment for a task.
    """

    def __init__(self, pip_packages: Optional[List[str]] = None):
        # TODO: 1. requirement.txt 2. apt install ...
        self._pip_packages = pip_packages

    def install_dependencies(self):
        # pip options
        #
        # --disable-pip-version-check
        #   Don't periodically check PyPI to determine whether a new version
        #   of pip is available for download.
        #
        # --no-cache-dir
        #   Disable the cache, the pip runtime env is a one-time installation,
        #   and we don't need to handle the pip cache broken.
        pip_install_cmd = [
            "python",
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-cache-dir",
        ]

        for package in self._pip_packages:
            pip_install_cmd.append(" " + package)

        print(pip_install_cmd)
        try:
            subprocess.run(pip_install_cmd, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            logger.error("Failed to install python packages with error: {}".format(e))
