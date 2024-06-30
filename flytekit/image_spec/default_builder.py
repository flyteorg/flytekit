import os
import shutil
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from string import Template
from typing import ClassVar

import click

from flytekit.image_spec.image_spec import (
    _F_IMG_ID,
    ImageSpec,
    ImageSpecBuilder,
)
from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore

UV_PYTHON_INSTALL_COMMAND_TEMPLATE = Template("""\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    --mount=type=bind,target=requirements_uv.txt,src=requirements_uv.txt \
    /usr/bin/uv \
    pip install --python /root/micromamba/envs/dev/bin/python $PIP_EXTRA \
    --requirement requirements_uv.txt
""")

PIP_PYTHON_INSTALL_COMMAND_TEMPLATE = Template("""\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/pip,id=pip \
    --mount=type=bind,target=requirements_pip.txt,src=requirements_pip.txt \
    /root/micromamba/envs/dev/bin/python -m pip install $PIP_EXTRA \
    --requirement requirements_pip.txt
""")

APT_INSTALL_COMMAND_TEMPLATE = Template(
    """\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/var/cache/apt,id=apt \
    apt-get update && apt-get install -y --no-install-recommends \
    $APT_PACKAGES
"""
)

DOCKER_FILE_TEMPLATE = Template(
    """\
#syntax=docker/dockerfile:1.5
FROM ghcr.io/astral-sh/uv:0.2.13 as uv
FROM mambaorg/micromamba:1.5.8-bookworm-slim as micromamba

FROM $BASE_IMAGE

USER root
$APT_INSTALL_COMMAND
RUN update-ca-certificates

RUN id -u flytekit || useradd --create-home --shell /bin/bash flytekit
RUN chown -R flytekit /root && chown -R flytekit /home

RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/micromamba/pkgs,\
id=micromamba \
    --mount=from=micromamba,source=/usr/bin/micromamba,target=/usr/bin/micromamba \
    /usr/bin/micromamba create -n dev -c conda-forge $CONDA_CHANNELS \
    python=$PYTHON_VERSION $CONDA_PACKAGES

$UV_PYTHON_INSTALL_COMMAND
$PIP_PYTHON_INSTALL_COMMAND

# Configure user space
ENV PATH="/root/micromamba/envs/dev/bin:$$PATH"
ENV FLYTE_SDK_RICH_TRACEBACKS=0 SSL_CERT_DIR=/etc/ssl/certs $ENV

# Adds nvidia just in case it exists
ENV PATH="$$PATH:/usr/local/nvidia/bin:/usr/local/cuda/bin" \
    LD_LIBRARY_PATH="/usr/local/nvidia/lib64:$$LD_LIBRARY_PATH"

$COPY_COMMAND_RUNTIME
RUN $RUN_COMMANDS

WORKDIR /root
SHELL ["/bin/bash", "-c"]

USER flytekit
RUN echo "export PATH=$$PATH" >> $$HOME/.profile
"""
)


def get_flytekit_for_pypi():
    """Get flytekit version on PyPI."""
    from flytekit import __version__

    if not __version__ or "dev" in __version__:
        return "flytekit"
    else:
        return f"flytekit=={__version__}"


def create_docker_context(image_spec: ImageSpec, tmp_dir: Path):
    """Populate tmp_dir with Dockerfile as specified by the `image_spec`."""
    base_image = image_spec.base_image or "debian:bookworm-slim"

    requirements = [get_flytekit_for_pypi()]

    if image_spec.cuda is not None or image_spec.cudnn is not None:
        msg = (
            "cuda and cudnn do not need to be specified. If you are installed "
            "a GPU accelerated library on PyPI, then it likely will install cuda "
            "from PyPI."
            "With conda you can installed cuda from the `nvidia` channel by adding `nvidia` to "
            "ImageSpec.conda_channels and adding packages from "
            "https://anaconda.org/nvidia into ImageSpec.conda_packages. If you require "
            "cuda for non-python dependencies, you can set a `base_image` with cuda "
            "preinstalled."
        )
        raise ValueError(msg)

    if image_spec.requirements:
        with open(image_spec.requirements) as f:
            requirements.extend([line.strip() for line in f.readlines()])

    if image_spec.packages:
        requirements.extend(image_spec.packages)

    uv_requirements = []

    # uv does not support git + subdirectory, so we use pip to install them instead
    pip_requirements = []

    for requirement in requirements:
        if "git" in requirement and "subdirectory" in requirement:
            pip_requirements.append(requirement)
        else:
            uv_requirements.append(requirement)

    requirements_uv_path = tmp_dir / "requirements_uv.txt"
    requirements_uv_path.write_text("\n".join(uv_requirements))

    pip_extra = f"--index-url {image_spec.pip_index}" if image_spec.pip_index else ""
    uv_python_install_command = UV_PYTHON_INSTALL_COMMAND_TEMPLATE.substitute(PIP_EXTRA=pip_extra)

    if pip_requirements:
        requirements_uv_path = tmp_dir / "requirements_pip.txt"
        requirements_uv_path.write_text(os.linesep.join(pip_requirements))

        pip_python_install_command = PIP_PYTHON_INSTALL_COMMAND_TEMPLATE.substitute(PIP_EXTRA=pip_extra)
    else:
        pip_python_install_command = ""

    env_dict = {"PYTHONPATH": "/root", _F_IMG_ID: image_spec.image_name()}

    if image_spec.env:
        env_dict.update(image_spec.env)

    env = " ".join(f"{k}={v}" for k, v in env_dict.items())

    apt_packages = ["ca-certificates"]
    if image_spec.apt_packages:
        apt_packages.extend(image_spec.apt_packages)

    apt_install_command = APT_INSTALL_COMMAND_TEMPLATE.substitute(APT_PACKAGES=" ".join(apt_packages))

    if image_spec.source_root:
        source_path = tmp_dir / "src"

        ignore = IgnoreGroup(image_spec.source_root, [GitIgnore, DockerIgnore, StandardIgnore])
        shutil.copytree(
            image_spec.source_root,
            source_path,
            ignore=shutil.ignore_patterns(*ignore.list_ignored()),
            dirs_exist_ok=True,
        )
        copy_command_runtime = "COPY --chown=flytekit ./src /root"
    else:
        copy_command_runtime = ""

    conda_packages = image_spec.conda_packages or []
    conda_channels = image_spec.conda_channels or []

    if conda_packages:
        conda_packages_concat = " ".join(conda_packages)
    else:
        conda_packages_concat = ""

    if conda_channels:
        conda_channels_concat = " ".join(f"-c {channel}" for channel in conda_channels)
    else:
        conda_channels_concat = ""

    if image_spec.python_version:
        python_version = image_spec.python_version
    else:
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

    if image_spec.commands:
        run_commands = " && ".join(image_spec.commands)
    else:
        run_commands = ""

    docker_content = DOCKER_FILE_TEMPLATE.substitute(
        PYTHON_VERSION=python_version,
        UV_PYTHON_INSTALL_COMMAND=uv_python_install_command,
        PIP_PYTHON_INSTALL_COMMAND=pip_python_install_command,
        CONDA_PACKAGES=conda_packages_concat,
        CONDA_CHANNELS=conda_channels_concat,
        APT_INSTALL_COMMAND=apt_install_command,
        BASE_IMAGE=base_image,
        ENV=env,
        COPY_COMMAND_RUNTIME=copy_command_runtime,
        RUN_COMMANDS=run_commands,
    )

    dockerfile_path = tmp_dir / "Dockerfile"
    dockerfile_path.write_text(docker_content)


class DefaultImageBuilder(ImageSpecBuilder):
    """Image builder using Docker and buildkit."""

    _SUPPORTED_IMAGE_SPEC_PARAMETERS: ClassVar[set] = {
        "name",
        "python_version",
        "builder",
        "source_root",
        "env",
        "registry",
        "packages",
        "conda_packages",
        "conda_channels",
        "requirements",
        "apt_packages",
        "platform",
        "cuda",
        "cudnn",
        "base_image",
        "pip_index",
        # "registry_config",
        "commands",
    }

    def build_image(self, image_spec: ImageSpec) -> str:
        return self._build_image(image_spec)

    def _build_image(self, image_spec: ImageSpec, *, push: bool = True) -> str:
        # For testing, set `push=False`` to just build the image locally and not push to
        # registry
        unsupported_parameters = [
            name
            for name, value in vars(image_spec).items()
            if value is not None and name not in self._SUPPORTED_IMAGE_SPEC_PARAMETERS and not name.startswith("_")
        ]
        if unsupported_parameters:
            msg = f"The following parameters are unsupported and ignored: " f"{unsupported_parameters}"
            warnings.warn(msg, UserWarning, stacklevel=2)

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            create_docker_context(image_spec, tmp_path)

            command = [
                "docker",
                "image",
                "build",
                "--tag",
                f"{image_spec.image_name()}",
                "--platform",
                image_spec.platform,
            ]

            if image_spec.registry and push:
                command.append("--push")
            command.append(tmp_dir)

            concat_command = " ".join(command)
            click.secho(f"Run command: {concat_command} ", fg="blue")
            subprocess.run(command, check=True)
