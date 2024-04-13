import os
import shutil
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from string import Template
from typing import ClassVar, Optional

import click

from flytekit.image_spec.image_spec import _F_IMG_ID, ImageSpec, ImageSpecBuilder

# The default image builder base image is located at Dockerfile.default-image-builder.
# For local testing, build the base image by running:
# 1. Set environment variable `DEFAULT_BUILDER_BASE_IMAGE=localhost:30000/default-image-builder-base`
# 2. make setup-multiarch-builder
# 3. make build-default-image-builder-image
DEFAULT_BUILDER_BASE_IMAGE_ENV = "DEFAULT_BUILDER_BASE_IMAGE"
DEFAULT_BUILDER_BASE_IMAGE = "thomasjpfan/default-image-builder-base:0.0.3"

BASE_IMAGE_BUILDER = os.getenv(DEFAULT_BUILDER_BASE_IMAGE_ENV, DEFAULT_BUILDER_BASE_IMAGE)


PYTHON_INSTALL_COMMAND = """\
RUN --mount=type=cache,target=/root/.cache/uv,id=uv \
    --mount=type=bind,target=requirements.txt,src=requirements.txt \
    /opt/conda/bin/uv \
    pip install --python /opt/conda/envs/dev/bin/python $PIP_INDEX \
    --requirement requirements.txt
"""

APT_INSTALL_COMMAND_TEMPLATE = Template(
    """\
RUN --mount=type=cache,target=/var/cache/apt,id=apt \
    apt-get update && apt-get install -y --no-install-recommends \
    $APT_PACKAGES
"""
)

DOCKER_FILE_TEMPLATE = Template(
    """\
#syntax=docker/dockerfile:1.5
FROM $BASE_IMAGE_BUILDER as build

RUN --mount=type=cache,target=/opt/conda/pkgs,id=conda \
    mamba create \
        -c conda-forge $CONDA_CHANNELS \
        -n dev -y python=$PYTHON_VERSION $CONDA_PACKAGES

WORKDIR /root

$COPY_COMMAND_BUILDER

$PYTHON_INSTALL_COMMAND

RUN /opt/conda/bin/conda-pack -n dev -o /tmp/env.tar && \
    mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
    rm /tmp/env.tar

RUN /venv/bin/conda-unpack

FROM $BASE_IMAGE AS runtime

RUN --mount=type=cache,target=/var/cache/apt,id=apt \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates
RUN update-ca-certificates

$APT_INSTALL_COMMAND

COPY --from=build /venv /venv
ENV PATH="/venv/bin:$$PATH"

WORKDIR /root
ENV FLYTE_SDK_RICH_TRACEBACKS=0 SSL_CERT_DIR=/etc/ssl/certs $ENV

RUN useradd --create-home --shell /bin/bash -u 1000 flytekit \
    && chown -R flytekit /root \
    && chown -R flytekit /home

$COPY_COMMAND_RUNTIME

$RUN_COMMANDS

RUN echo "source /venv/bin/activate" >> /home/flytekit/.bashrc
SHELL ["/bin/bash", "-c"]

USER flytekit
"""
)


def create_docker_context(image_spec: ImageSpec, tmp_dir: Path):
    """Populate tmp_dir with Dockerfile as specified by the `image_spec`."""
    base_image = image_spec.base_image or "debian:bookworm-slim"

    if image_spec.cuda:
        # Base image requires an NVIDIA driver. cuda and cudnn will be installed with conda
        base_image = "nvcr.io/nvidia/driver:535-5.15.0-1048-nvidia-ubuntu22.04"

    pip_index = f"--index-url {image_spec.pip_index}" if image_spec.pip_index else ""

    requirements = ["flytekit"]
    if image_spec.requirements:
        with open(image_spec.requirements) as f:
            requirements.extend([line.strip() for line in f.readlines()])

    if image_spec.packages:
        requirements.extend(image_spec.packages)

    requirements_path = tmp_dir / "requirements.txt"
    requirements_path.write_text("\n".join(requirements))
    python_install_command = PYTHON_INSTALL_COMMAND
    env_dict = {"PYTHONPATH": "/root", _F_IMG_ID: image_spec.image_name()}

    if image_spec.env:
        env_dict.update(image_spec.env)

    env = " ".join(f"{k}={v}" for k, v in env_dict.items())

    if image_spec.apt_packages:
        apt_install_command = APT_INSTALL_COMMAND_TEMPLATE.substitute(APT_PACKAGES=" ".join(image_spec.apt_packages))
    else:
        apt_install_command = ""

    if image_spec.source_root:
        source_path = tmp_dir / "src"
        shutil.copytree(image_spec.source_root, source_path)
        copy_command_builder = "COPY ./src /root"
        copy_command_runtime = "COPY --chown=flytekit ./src /root"
    else:
        copy_command_builder = ""
        copy_command_runtime = ""

    conda_packages = image_spec.conda_packages or []
    conda_channels = image_spec.conda_channels or []

    if image_spec.cuda:
        conda_packages.append(f"cuda={image_spec.cuda}")
        if image_spec.cudnn:
            conda_packages.append(f"cudnn={image_spec.cudnn}")

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
        run_commands = "\n".join(f"RUN {command}" for command in image_spec.commands)
    else:
        run_commands = ""

    docker_content = DOCKER_FILE_TEMPLATE.substitute(
        BASE_IMAGE_BUILDER=BASE_IMAGE_BUILDER,
        PYTHON_VERSION=python_version,
        PYTHON_INSTALL_COMMAND=python_install_command,
        CONDA_PACKAGES=conda_packages_concat,
        CONDA_CHANNELS=conda_channels_concat,
        APT_INSTALL_COMMAND=apt_install_command,
        BASE_IMAGE=base_image,
        PIP_INDEX=pip_index,
        ENV=env,
        COPY_COMMAND_BUILDER=copy_command_builder,
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

    def build_image(self, image_spec: ImageSpec) -> Optional[str]:
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

            if image_spec.registry:
                command.append("--push")
            command.append(tmp_dir)

            concat_command = " ".join(command)
            click.secho(f"Run command: {concat_command} ", fg="blue")
            subprocess.run(command, check=True)
