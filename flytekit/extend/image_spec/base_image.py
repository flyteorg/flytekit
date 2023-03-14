import pathlib
from typing import List
from dataclasses import dataclass
import docker
from flytekit.core import context_manager
import subprocess

client = docker.from_env()


@dataclass
class ImageSpec:
    os: str = "ubuntu20.04"
    python_version: str = "3.9"


def create_envd_config(os: str, packages: List[str]) -> str:
    packages_list = ""
    for pkg in packages:
        packages_list += f'"{pkg}", '

    return f"""
def build():
    base(os="{os}", language="python3")
    install.python_packages(name = [{packages_list}])
"""


def build_docker_image(envd_config: str):
    ctx = context_manager.FlyteContextManager.current_context()
    cfg_path = ctx.file_access.get_random_local_path("build.envd")
    pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

    with open(cfg_path, "x") as f:
        f.write(envd_config)

    p = subprocess.run(["envd", "build",
                        "--path", f"{pathlib.Path(cfg_path).parent}",
                        ],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("=====stdout=====")
    print(p.stdout.decode())
    print("=====stderr=====")
    print(p.stderr.decode())


if __name__ == '__main__':
    cfg = create_envd_config("ubuntu20.04", ["pandas"])
    build_docker_image(cfg)
