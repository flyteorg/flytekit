import pathlib
from typing import List, Optional
from dataclasses import dataclass
import docker
import subprocess
from flytekit.loggers import logger

client = docker.from_env()


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
    os: str = "pingsutw/envd_base:v2"
    base_image: str = "ubuntu:20.04"
    registry: Optional[str] = None
    python_version: Optional[str] = None,


def create_envd_config(image_spec: ImageSpec) -> str:
    packages_list = ""
    for pkg in image_spec.packages:
        packages_list += f'"{pkg}", '

    envd_config = f"""
def build():
    base(os="{image_spec.os}", language="python3")
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
    logger.info("building image...")
    p = subprocess.run(["envd", "build",
                        "--path", f"{pathlib.Path(cfg_path).parent}",
                        "--output", f"type=image,name={name}:{tag},push=true"
                        ],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info("pushed image")
    if p.stderr:
        print(p.stderr.decode())
