import base64
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

import click
from dataclasses_json import dataclass_json

IMAGE_LOCK = f"{os.path.expanduser('~')}{os.path.sep}.flyte{os.path.sep}image.lock"


@dataclass_json
@dataclass
class ImageSpec:
    """
    This class is used to specify the docker image that will be used to run the task.

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

    def build_image(self, name: str, tag: str, fast_register: bool, source_root: Optional[str] = None):
        raise NotImplementedError("This method is not implemented in the base class.")


def build_docker_image(
    image_spec: ImageSpec, name: str, tag: str, fast_register: bool, source_root: Optional[str] = None
):
    """
    Build the docker image and push it to the registry.
    """
    if not image_exist(image_spec.registry, tag) and fast_register:
        click.secho("The image has already been pushed. Skip building the image.", fg="blue")
        return

    image_spec.build_image(name, tag, fast_register, source_root)
    update_lock_file(image_spec.registry, tag)


def calculate_hash_from_image_spec(image_spec: ImageSpec):
    """
    Calculate the hash from the image spec.
    """
    h = hashlib.md5(bytes(image_spec.to_json(), "utf-8"))
    tag = base64.urlsafe_b64encode(h.digest()).decode("ascii")
    # replace "=" with "." to make it a valid tag
    return tag.replace("=", ".")


def image_exist(registry: str, tag: str) -> bool:
    """
    Check if the image has been pushed to the registry.
    """
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
