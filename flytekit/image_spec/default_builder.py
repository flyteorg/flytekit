import json
import os
import re
import shutil
import sys
import tempfile
import warnings
from pathlib import Path
from string import Template
from subprocess import run
from typing import ClassVar

import click

from flytekit.image_spec.image_spec import (
    _F_IMG_ID,
    ImageSpec,
    ImageSpecBuilder,
)
from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore

UV_PYTHON_INSTALL_COMMAND_TEMPLATE = Template(
    """\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    --mount=type=bind,target=requirements_uv.txt,src=requirements_uv.txt \
    /usr/bin/uv \
    pip install --python /opt/micromamba/envs/runtime/bin/python $PIP_EXTRA \
    --requirement requirements_uv.txt
"""
)

APT_INSTALL_COMMAND_TEMPLATE = Template("""\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/var/cache/apt,id=apt \
    apt-get update && apt-get install -y --no-install-recommends \
    $APT_PACKAGES
""")

DOCKER_FILE_TEMPLATE = Template("""\
#syntax=docker/dockerfile:1.5
FROM ghcr.io/astral-sh/uv:0.2.37 as uv
FROM mambaorg/micromamba:1.5.8-bookworm-slim as micromamba

FROM $BASE_IMAGE

USER root
$APT_INSTALL_COMMAND
RUN update-ca-certificates

RUN id -u flytekit || useradd --create-home --shell /bin/bash flytekit
RUN chown -R flytekit /root && chown -R flytekit /home

RUN --mount=type=cache,sharing=locked,mode=0777,target=/opt/micromamba/pkgs,\
id=micromamba \
    --mount=from=micromamba,source=/usr/bin/micromamba,target=/usr/bin/micromamba \
    micromamba config set use_lockfiles False && \
    micromamba create -n runtime --root-prefix /opt/micromamba \
    -c conda-forge $CONDA_CHANNELS \
    python=$PYTHON_VERSION $CONDA_PACKAGES

# Configure user space
ENV PATH="/opt/micromamba/envs/runtime/bin:$$PATH" \
    UV_LINK_MODE=copy \
    FLYTE_SDK_RICH_TRACEBACKS=0 \
    SSL_CERT_DIR=/etc/ssl/certs \
    $ENV

$UV_PYTHON_INSTALL_COMMAND

# Adds nvidia just in case it exists
ENV PATH="$$PATH:/usr/local/nvidia/bin:/usr/local/cuda/bin" \
    LD_LIBRARY_PATH="/usr/local/nvidia/lib64:$$LD_LIBRARY_PATH"

$ENTRYPOINT

$COPY_COMMAND_RUNTIME
RUN $RUN_COMMANDS

WORKDIR /root
SHELL ["/bin/bash", "-c"]

USER flytekit
RUN mkdir -p $$HOME && \
    echo "export PATH=$$PATH" >> $$HOME/.profile
""")


def get_flytekit_for_pypi():
    """Get flytekit version on PyPI."""
    from flytekit import __version__

    if not __version__ or "dev" in __version__:
        return "flytekit"
    else:
        return f"flytekit=={__version__}"


_PACKAGE_NAME_RE = re.compile(r"^[\w-]+")


def _is_flytekit(package: str) -> bool:
    """Return True if `package` is flytekit. `package` is expected to be a valid version
    spec. i.e. `flytekit==1.12.3`, `flytekit`, `flytekit~=1.12.3`.
    """
    m = _PACKAGE_NAME_RE.match(package)
    if not m:
        return False
    name = m.group()
    return name == "flytekit"


def create_docker_context(image_spec: ImageSpec, tmp_dir: Path):
    """Populate tmp_dir with Dockerfile as specified by the `image_spec`."""
    base_image = image_spec.base_image or "debian:bookworm-slim"

    requirements = []

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

    # Adds flytekit if it is not specified
    if not any(_is_flytekit(package) for package in requirements):
        requirements.append(get_flytekit_for_pypi())

    requirements_uv_path = tmp_dir / "requirements_uv.txt"
    requirements_uv_path.write_text("\n".join(requirements))

    pip_extra_args = ""

    if image_spec.pip_index:
        pip_extra_args += f"--index-url {image_spec.pip_index}"
    if image_spec.pip_extra_index_url:
        extra_urls = [f"--extra-index-url {url}" for url in image_spec.pip_extra_index_url]
        pip_extra_args += " ".join(extra_urls)

    uv_python_install_command = UV_PYTHON_INSTALL_COMMAND_TEMPLATE.substitute(PIP_EXTRA=pip_extra_args)

    env_dict = {"PYTHONPATH": "/root", _F_IMG_ID: image_spec.id}

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

    if image_spec.entrypoint is None:
        entrypoint = ""
    else:
        entrypoint = f"ENTRYPOINT {json.dumps(image_spec.entrypoint)}"

    if image_spec.commands:
        run_commands = " && ".join(image_spec.commands)
    else:
        run_commands = ""

    docker_content = DOCKER_FILE_TEMPLATE.substitute(
        PYTHON_VERSION=python_version,
        UV_PYTHON_INSTALL_COMMAND=uv_python_install_command,
        CONDA_PACKAGES=conda_packages_concat,
        CONDA_CHANNELS=conda_channels_concat,
        APT_INSTALL_COMMAND=apt_install_command,
        BASE_IMAGE=base_image,
        ENV=env,
        COPY_COMMAND_RUNTIME=copy_command_runtime,
        ENTRYPOINT=entrypoint,
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
        "pip_extra_index_url",
        # "registry_config",
        "commands",
    }

    def build_image(self, image_spec: ImageSpec) -> str:
        return self._build_image(
            image_spec,
            push=os.getenv("FLYTE_PUSH_IMAGE_SPEC", "True").lower() in ("true", "1"),
        )

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
            run(command, check=True)
