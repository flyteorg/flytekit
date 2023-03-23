import base64
import hashlib
import json
import os
import pathlib
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Optional

import click
from dataclasses_json import dataclass_json

from flytekit.configuration.default_images import DefaultImages

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

    registry: str
    packages: Optional[List[str]] = None
    apt_packages: Optional[List[str]] = None
    base_image: Optional[str] = None
    python_version: str = f"{sys.version_info.major}.{sys.version_info.minor}"
    destination_dir: str = "/root"


def create_envd_config(image_spec: ImageSpec) -> str:
    packages_list = ""
    for pkg in image_spec.packages:
        packages_list += f'"{pkg}", '

    apt_packages_list = ""
    for pkg in image_spec.apt_packages:
        apt_packages_list += f'"{pkg}", '

    if image_spec.base_image is None:
        image_spec.base_image = DefaultImages.default_image()

    envd_config = f"""# syntax=v1

def build():
    base(image="{image_spec.base_image}", dev=False)
    install.python_packages(name = [{packages_list}])
    install.apt_packages(name = [{apt_packages_list}])
    install.python(version="{image_spec.python_version}")
"""
    from flytekit.core import context_manager

    ctx = context_manager.FlyteContextManager.current_context()
    cfg_path = ctx.file_access.get_random_local_path("build.envd")
    pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

    with open(cfg_path, "x") as f:
        f.write(envd_config)

    return cfg_path


def build_docker_image(image_spec: ImageSpec, name: str, tag: str, fast_register: bool):
    if should_build_image(image_spec.registry, tag) is False:
        click.secho("The image has already been pushed. Skip building the image.", fg="blue")
        return

    cfg_path = create_envd_config(image_spec)
    click.secho("Building image...", fg="blue")
    command = f"envd build --path {pathlib.Path(cfg_path).parent} --output type=image,name={name}:{tag},push=true"
    click.secho(f"Run command: {command} ", fg="blue")
    p = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    for line in iter(p.stdout.readline, ""):
        if line.decode().strip() != "":
            click.secho(line.decode().strip(), fg="blue")
        if p.poll() is not None:
            break

    if p.stderr:
        raise Exception(
            f"failed to build the imageSpec at {cfg_path} with error {p.stderr}",
        )

    update_lock_file(image_spec.registry, tag, fast_register)


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


def update_lock_file(registry: str, tag: str, fast_register: bool):
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
