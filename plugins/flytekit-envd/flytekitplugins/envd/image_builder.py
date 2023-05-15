import pathlib
import shutil
import subprocess

import click

from flytekit.configuration import DefaultImages
from flytekit.core import context_manager
from flytekit.image_spec.image_spec import _F_IMG_ID, ImageBuildEngine, ImageSpec, ImageSpecBuilder


class EnvdImageSpecBuilder(ImageSpecBuilder):
    """
    This class is used to build a docker image using envd.
    """

    def build_image(self, image_spec: ImageSpec):
        cfg_path = create_envd_config(image_spec)
        command = f"envd build --path {pathlib.Path(cfg_path).parent}  --platform {image_spec.platform}"
        if image_spec.registry:
            command += f" --output type=image,name={image_spec.image_name()},push=true"
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


def create_envd_config(image_spec: ImageSpec) -> str:
    base_image = DefaultImages.default_image() if image_spec.base_image is None else image_spec.base_image
    packages = [] if image_spec.packages is None else image_spec.packages
    apt_packages = [] if image_spec.apt_packages is None else image_spec.apt_packages
    env = {"PYTHONPATH": "/root", _F_IMG_ID: image_spec.image_name()}
    if image_spec.env:
        env.update(image_spec.env)

    envd_config = f"""# syntax=v1

def build():
    base(image="{base_image}", dev=False)
    install.python_packages(name = [{', '.join(map(str, map(lambda x: f'"{x}"', packages)))}])
    install.apt_packages(name = [{', '.join(map(str, map(lambda x: f'"{x}"', apt_packages)))}])
    runtime.environ(env={env})
"""

    if image_spec.python_version:
        # Indentation is required by envd
        envd_config += f'    install.python(version="{image_spec.python_version}")\n'

    ctx = context_manager.FlyteContextManager.current_context()
    cfg_path = ctx.file_access.get_random_local_path("build.envd")
    pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

    if image_spec.source_root:
        shutil.copytree(image_spec.source_root, pathlib.Path(cfg_path).parent, dirs_exist_ok=True)
        # Indentation is required by envd
        envd_config += '    io.copy(host_path="./", envd_path="/root")'

    with open(cfg_path, "w+") as f:
        f.write(envd_config)

    return cfg_path


ImageBuildEngine.register("envd", EnvdImageSpecBuilder())
