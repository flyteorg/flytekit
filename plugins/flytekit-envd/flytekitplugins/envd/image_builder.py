import os
import pathlib
import shutil
import subprocess
from dataclasses import asdict
from importlib import metadata

import click
from packaging.version import Version
from rich import print
from rich.pretty import Pretty

from flytekit.configuration import DefaultImages
from flytekit.constants import CopyFileDetection
from flytekit.core import context_manager
from flytekit.core.constants import REQUIREMENTS_FILE_NAME
from flytekit.image_spec.image_spec import _F_IMG_ID, ImageBuildEngine, ImageSpec, ImageSpecBuilder
from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore
from flytekit.tools.script_mode import ls_files

FLYTE_LOCAL_REGISTRY = "localhost:30000"
FLYTE_ENVD_CONTEXT = "FLYTE_ENVD_CONTEXT"


class EnvdImageSpecBuilder(ImageSpecBuilder):
    """
    This class is used to build a docker image using envd.
    """

    def build_image(self, image_spec: ImageSpec):
        cfg_path = create_envd_config(image_spec)

        if image_spec.registry_config:
            bootstrap_command = f"envd bootstrap --registry-config {image_spec.registry_config}"
            execute_command(bootstrap_command)

        build_command = f"envd build --path {pathlib.Path(cfg_path).parent}  --platform {image_spec.platform}"
        if image_spec.registry and os.getenv("FLYTE_PUSH_IMAGE_SPEC", "True").lower() in ("true", "1"):
            build_command += f" --output type=image,name={image_spec.image_name()},push=true"
        else:
            build_command += f" --tag {image_spec.image_name()}"
        envd_context_switch(image_spec.registry)
        try:
            execute_command(build_command)
        except Exception as e:
            click.secho("❌ Failed to build image spec:", fg="red")
            print(
                Pretty(
                    asdict(image_spec, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}), indent_size=2
                )
            )
            raise e from None


def envd_context_switch(registry: str):
    if os.getenv(FLYTE_ENVD_CONTEXT):
        execute_command(f"envd context use --name {os.getenv(FLYTE_ENVD_CONTEXT)}")
        return
    if registry == FLYTE_LOCAL_REGISTRY:
        # Assume buildkit daemon is running within the sandbox and exposed on port 30003
        command = "envd context create --name flyte-sandbox --builder tcp --builder-address localhost:30003 --use"
        p = subprocess.run(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.returncode != 0:
            # Assume the context already exists
            execute_command("envd context use --name flyte-sandbox")
    else:
        command = "envd context create --name flyte --builder docker-container --builder-address buildkitd-demo -use"
        p = subprocess.run(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.returncode != 0:
            # Assume the context already exists
            execute_command("envd context use --name flyte")


def execute_command(command: str):
    click.secho(f"Run command: {command} ", fg="blue")
    p = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    result = []
    for line in iter(p.stdout.readline, ""):
        if p.poll() is not None:
            break
        if line.decode().strip() != "":
            output = line.decode().strip()
            click.secho(output, fg="blue")
            result.append(output)

    if p.returncode != 0:
        _, stderr = p.communicate()
        raise RuntimeError(f"failed to run command {command} with error:\n {stderr.decode()}")

    return result


def _create_str_from_package_list(packages):
    if packages is None:
        return ""
    return ", ".join(f'"{name}"' for name in packages)


def create_envd_config(image_spec: ImageSpec) -> str:
    if image_spec.python_exec is not None:
        raise ValueError("python_exec is not supported with the envd image builder")

    base_image = DefaultImages.default_image() if image_spec.base_image is None else image_spec.base_image
    if image_spec.cuda:
        if image_spec.python_version is None:
            raise ValueError("python_version is required when cuda and cudnn are specified")
        base_image = "ubuntu20.04"

    python_packages = _create_str_from_package_list(image_spec.packages)
    conda_packages = _create_str_from_package_list(image_spec.conda_packages)
    run_commands = _create_str_from_package_list(image_spec.commands)
    conda_channels = _create_str_from_package_list(image_spec.conda_channels)
    apt_packages = _create_str_from_package_list(image_spec.apt_packages)
    env = {"PYTHONPATH": "/root:", _F_IMG_ID: image_spec.id}

    if image_spec.env:
        env.update(image_spec.env)
    pip_index = "https://pypi.org/simple" if image_spec.pip_index is None else image_spec.pip_index

    envd_config = f"""# syntax=v1

def build():
    base(image="{base_image}", dev=False)
    run(commands=[{run_commands}])
    install.python_packages(name=[{python_packages}])
    install.apt_packages(name=[{apt_packages}])
    runtime.environ(env={env}, extra_path=['/root'])
"""
    if image_spec.pip_extra_index_url is None:
        envd_config += f'    config.pip_index(url="{pip_index}")\n'
    else:
        pip_extra_index_url = " ".join(image_spec.pip_extra_index_url)
        envd_config += f'    config.pip_index(url="{pip_index}", extra_url="{pip_extra_index_url}")\n'

    ctx = context_manager.FlyteContextManager.current_context()
    cfg_path = ctx.file_access.get_random_local_path("build.envd")
    pathlib.Path(cfg_path).parent.mkdir(parents=True, exist_ok=True)

    if conda_packages:
        envd_config += "    install.conda(use_mamba=True)\n"
        envd_config += f"    install.conda_packages(name=[{conda_packages}], channel=[{conda_channels}])\n"

    if image_spec.requirements:
        requirement_path = f"{pathlib.Path(cfg_path).parent}{os.sep}{REQUIREMENTS_FILE_NAME}"
        shutil.copyfile(image_spec.requirements, requirement_path)
        envd_config += f'    install.python_packages(requirements="{REQUIREMENTS_FILE_NAME}")\n'

    if image_spec.python_version:
        # Indentation is required by envd
        envd_config += f'    install.python(version="{image_spec.python_version}")\n'

    if image_spec.cuda:
        cudnn = image_spec.cudnn if image_spec.cudnn else ""
        envd_config += f'    install.cuda(version="{image_spec.cuda}", cudnn="{cudnn}")\n'

    if image_spec.source_copy_mode is not None and image_spec.source_copy_mode != CopyFileDetection.NO_COPY:
        if not image_spec.source_root:
            raise ValueError(f"Field source_root for {image_spec} must be set when copy is set")
        # todo: See note in we should pipe through ignores from the command line here at some point.
        #  what about deref_symlink?
        ignore = IgnoreGroup(image_spec.source_root, [GitIgnore, DockerIgnore, StandardIgnore])

        dst = pathlib.Path(cfg_path).parent

        ls, _ = ls_files(
            str(image_spec.source_root), image_spec.source_copy_mode, deref_symlinks=False, ignore_group=ignore
        )

        for file_to_copy in ls:
            rel_path = os.path.relpath(file_to_copy, start=str(image_spec.source_root))
            pathlib.Path(dst / rel_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(file_to_copy, dst / rel_path)

        envd_version = metadata.version("envd")
        # Indentation is required by envd
        if Version(envd_version) <= Version("0.3.37"):
            envd_config += '    io.copy(host_path="./", envd_path="/root")\n'
        else:
            envd_config += '    io.copy(source="./", target="/root")\n'

    if image_spec.copy:

        def add_envd_copy_command(src_path: pathlib.Path, envd_config: str) -> str:
            envd_version = metadata.version("envd")
            if Version(envd_version) <= Version("0.3.37"):
                if src_path.is_dir():
                    envd_config += (
                        f'    io.copy(host_path="{src_path.as_posix()}", envd_path="/root/{src_path.as_posix()}/")\n'
                    )
                else:
                    envd_config += f'    io.copy(host_path="{src_path.as_posix()}", envd_path="/root/{src_path.parent.as_posix()}/")\n'
            else:
                if src_path.is_dir():
                    envd_config += (
                        f'    io.copy(source="{src_path.as_posix()}", target="/root/{src_path.as_posix()}/")\n'
                    )
                else:
                    envd_config += (
                        f'    io.copy(source="{src_path.as_posix()}", target="/root/{src_path.parent.as_posix()}/")\n'
                    )
            return envd_config

        for src in image_spec.copy:
            src_path = pathlib.Path(src)

            if src_path.is_absolute() or ".." in src_path.parts:
                raise ValueError("Absolute paths or paths with '..' are not allowed in COPY command.")

            dst_path = pathlib.Path(cfg_path).parent / src_path
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            if src_path.is_dir():
                shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
            else:
                shutil.copy(src_path, dst_path)

            envd_config = add_envd_copy_command(src_path, envd_config)

    with open(cfg_path, "w+") as f:
        f.write(envd_config)

    return cfg_path


ImageBuildEngine.register("envd", EnvdImageSpecBuilder())
