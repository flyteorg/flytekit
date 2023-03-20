import base64
import hashlib
import json
import os
import pathlib
import subprocess
from dataclasses import dataclass
from typing import List, Optional

from dataclasses_json import dataclass_json

IMAGE_LOCK = f"{os.path.expanduser('~')}{os.path.sep}.flyte{os.path.sep}image.lock"


@dataclass_json
@dataclass
class ImageSpec:
    """
    Args:
        packages: list of packages that will be installed in the image.
        os: operating system. by default is ubuntu 20.04.
        registry: docker registry. if it's specified, flytekit will push the image.
        python_version: python version in the image.
    """

    packages: List[str]
    base_image: str = "pingsutw/envd_base:v8"
    registry: Optional[str] = None
    python_version: Optional[str] = "3.9"


def create_envd_config(image_spec: ImageSpec) -> str:
    packages_list = ""
    for pkg in image_spec.packages:
        packages_list += f'"{pkg}", '

    envd_config = f"""# syntax=v1

def build():
    base(image="{image_spec.base_image}", dev=False)
    install.python_packages(name = [{packages_list}])
    install.python(version="{image_spec.python_version}")
    runtime.environ(env={{"PYTHONPATH": "/"}})
"""
    from flytekit.core import context_manager

    ctx = context_manager.FlyteContextManager.current_context()
    cfg_path = ctx.file_access.get_random_local_path("build.envd")
    pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

    with open(cfg_path, "x") as f:
        f.write(envd_config)

    return cfg_path


def build_docker_image(image_spec: ImageSpec, name: str, tag: str):
    if should_build_image(image_spec.registry, tag) is False:
        return

    cfg_path = create_envd_config(image_spec)
    print("building image...")
    p = subprocess.run(
        [
            "envd",
            "build",
            "--path",
            f"{pathlib.Path(cfg_path).parent}",
            "--output",
            f"type=image,name={name}:{tag},push=true",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if p.stderr:
        raise Exception(
            f"failed to build the imageSpec at {cfg_path} with error {p.stderr}",
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
    with open(IMAGE_LOCK, "r") as f:
        checkpoints = json.load(f)
        if registry not in checkpoints:
            checkpoints[registry] = [tag]
        else:
            checkpoints[registry].append(tag)
        with open(IMAGE_LOCK, "w") as o:
            o.write(json.dumps(checkpoints))
