import pathlib
import shutil
import subprocess
from abc import ABC
from typing import Optional

import click

from flytekit.configuration import DefaultImages
from flytekit.core import context_manager
from flytekit.image_spec import ImageSpec


class EnvdImageSpec(ImageSpec, ABC):
    def build_image(self, name: str, tag: str, fast_register: bool, source_root: Optional[str] = None):
        cfg_path = self.create_envd_config(fast_register, source_root)
        click.secho("Building image...", fg="blue")
        command = f"envd build --path {pathlib.Path(cfg_path).parent}"
        if self.registry:
            command += f" --output type=image,name={name}:{tag},push=true"
        click.secho(f"Run command: {command} ", fg="blue")
        p = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in iter(p.stdout.readline, ""):
            if p.poll() is not None:
                break
            if line.decode().strip() != "":
                click.secho(line.decode().strip(), fg="blue")

        if p.returncode != 0:
            _, stderr = p.communicate()
            raise Exception(
                f"failed to build the imageSpec at {cfg_path} with error {stderr}",
            )

    def create_envd_config(self, fast_register: bool, source_root: Optional[str] = None) -> str:
        if self.base_image is None:
            self.base_image = DefaultImages.default_image()
        if self.packages is None:
            self.packages = []
        if self.apt_packages is None:
            self.apt_packages = []

        packages_list = ""
        for pkg in self.packages:
            packages_list += f'"{pkg}", '

        apt_packages_list = ""
        for pkg in self.apt_packages:
            apt_packages_list += f'"{pkg}", '

        envd_config = f"""# syntax=v1

def build():
    base(image="{self.base_image}", dev=False)
    install.python_packages(name = [{packages_list}])
    install.apt_packages(name = [{apt_packages_list}])
    install.python(version="{self.python_version}")
"""

        ctx = context_manager.FlyteContextManager.current_context()
        cfg_path = ctx.file_access.get_random_local_path("build.envd")
        pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

        if fast_register is False:
            shutil.copytree(source_root, pathlib.Path(cfg_path).parent, dirs_exist_ok=True)
            envd_config += f'    io.copy(host_path="./", envd_path="{self.destination_dir}")'

        with open(cfg_path, "w+") as f:
            f.write(envd_config)

        return cfg_path
