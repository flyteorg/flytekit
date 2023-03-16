import pathlib
import subprocess
from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json


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

    packages: list[str]
    os: str = "ubuntu:20.04"
    base_image: str = "pingsutw/envd_base:v4"
    registry: Optional[str] = None
    python_version: Optional[str] = (None,)


def create_envd_config(image_spec: ImageSpec) -> str:
    packages_list = ""
    for pkg in image_spec.packages:
        packages_list += f'"{pkg}", '

    envd_config = f"""
def build():
    base(image="{image_spec.base_image}", language="python3")
    install.python_packages(name = [{packages_list}])
"""
    from flytekit.core import context_manager

    ctx = context_manager.FlyteContextManager.current_context()
    cfg_path = ctx.file_access.get_random_local_path("build.envd")
    pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

    with open(cfg_path, "x") as f:
        f.write(envd_config)

    return cfg_path


def build_docker_image(image_spec: ImageSpec, name: str, tag: str):
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
    print(p.stdout.decode())
    if p.stderr:
        print(p.stderr.decode())
        raise Exception("failed to build the image.")
