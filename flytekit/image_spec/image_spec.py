import base64
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Optional

import click
from dataclasses_json import dataclass_json

from flytekit.configuration.default_images import DefaultImages
from flytekit.core import context_manager

IMAGE_LOCK = f"{os.path.expanduser('~')}{os.path.sep}.flyte{os.path.sep}image.lock"


@dataclass_json
@dataclass
class ImageSpec:
    """
    Args:
        registry: docker registry. if it's specified, flytekit will push the image.
        packages: list of python packages that will be installed in the image.
        apt_packages: list of ubuntu packages that will be installed in the image.
        base_image: base image of the docker container.
        python_version: python version in the image.
        destination_dir: This is the location that the code should be copied into. This must be the same as the WORKING_DIR in the base image.
    """

    registry: Optional[str] = None
    packages: Optional[List[str]] = None
    apt_packages: Optional[List[str]] = None
    base_image: Optional[str] = None
    python_version: str = f"{sys.version_info.major}.{sys.version_info.minor}"
    destination_dir: str = "/root"


def create_envd_config(image_spec: ImageSpec, fast_register: bool, source_root: Optional[str] = None) -> str:
    if image_spec.base_image is None:
        image_spec.base_image = DefaultImages.default_image()
    if image_spec.packages is None:
        image_spec.packages = []
    if image_spec.apt_packages is None:
        image_spec.apt_packages = []

    packages_list = ""
    for pkg in image_spec.packages:
        packages_list += f'"{pkg}", '

    apt_packages_list = ""
    for pkg in image_spec.apt_packages:
        apt_packages_list += f'"{pkg}", '

    envd_config = f"""# syntax=v1

def build():
    base(image="{image_spec.base_image}", dev=False)
    install.python_packages(name = [{packages_list}])
    install.apt_packages(name = [{apt_packages_list}])
    install.python(version="{image_spec.python_version}")
"""

    ctx = context_manager.FlyteContextManager.current_context()
    cfg_path = ctx.file_access.get_random_local_path("build.envd")
    pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

    if fast_register is False:
        print(source_root)
        print(pathlib.Path(cfg_path).parent)
        shutil.copytree(source_root, pathlib.Path(cfg_path).parent, dirs_exist_ok=True)
        envd_config += f'    io.copy(host_path="./", envd_path="{image_spec.destination_dir}")'

    with open(cfg_path, "w+") as f:
        f.write(envd_config)

    return cfg_path


def build_docker_image(image_spec: ImageSpec, name: str, tag: str, fast_register: bool, source_root: str):
    if should_build_image(image_spec.registry, tag) is False and fast_register:
        click.secho("The image has already been pushed. Skip building the image.", fg="blue")
        return

    cfg_path = create_envd_config(image_spec, fast_register, source_root)
    click.secho("Building image...", fg="blue")
    command = f"envd build --path {pathlib.Path(cfg_path).parent}"
    if image_spec.registry:
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

    update_lock_file(image_spec.registry, tag)


def calculate_hash_from_image_spec(image_spec: ImageSpec):
    h = hashlib.md5(bytes(image_spec.to_json(), "utf-8"))
    tag = base64.urlsafe_b64encode(h.digest()).decode("ascii")
    # docker tag can't contain "="
    return tag.replace("=", ".")


def should_build_image(registry: str, tag: str) -> bool:
    if os.path.isfile(IMAGE_LOCK) is False:
        return True

    with open(IMAGE_LOCK, "r") as f:
        checkpoints = json.load(f)
        return registry not in checkpoints or tag not in checkpoints[registry]


def update_lock_file(registry: str, tag: str):
    """
    Update the ~/.flyte/image.lock. It will contains all the image names we have pushed.
    If not exists, create a new file.
    """
    data = {}
    if os.path.isfile(IMAGE_LOCK) is False:
        open(IMAGE_LOCK, "a").close()
    else:
        with open(IMAGE_LOCK, "r") as f:
            data = json.load(f)

    if registry not in data:
        data[registry] = [tag]
    else:
        data[registry].append(tag)
    with open(IMAGE_LOCK, "w") as o:
        o.write(json.dumps(data, indent=2))
