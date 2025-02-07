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
from typing import ClassVar, List, NamedTuple, Tuple

import click

from flytekit.constants import CopyFileDetection
from flytekit.image_spec.image_spec import (
    _F_IMG_ID,
    ImageSpec,
    ImageSpecBuilder,
)
from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore
from flytekit.tools.script_mode import ls_files

UV_LOCK_INSTALL_TEMPLATE = Template(
    """\
WORKDIR /root
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    --mount=type=bind,target=uv.lock,src=uv.lock \
    --mount=type=bind,target=pyproject.toml,src=pyproject.toml \
    $PIP_SECRET_MOUNT \
    uv sync $PIP_INSTALL_ARGS
WORKDIR /

# Update PATH and UV_PYTHON to point to the venv created by uv sync
ENV PATH="/root/.venv/bin:$$PATH" \
    UV_PYTHON=/root/.venv/bin/python
"""
)

POETRY_LOCK_TEMPLATE = Template(
    """\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    uv pip install poetry

ENV POETRY_CACHE_DIR=/tmp/poetry_cache \
    POETRY_VIRTUALENVS_IN_PROJECT=true

# poetry install does not work running in /, so we move to /root to create the venv
WORKDIR /root

RUN --mount=type=cache,sharing=locked,mode=0777,target=/tmp/poetry_cache,id=poetry \
    --mount=type=bind,target=poetry.lock,src=poetry.lock \
    --mount=type=bind,target=pyproject.toml,src=pyproject.toml \
    $PIP_SECRET_MOUNT \
    poetry install $PIP_INSTALL_ARGS

WORKDIR /

# Update PATH and UV_PYTHON to point to venv
ENV PATH="/root/.venv/bin:$$PATH" \
    UV_PYTHON=/root/.venv/bin/python
"""
)

UV_PYTHON_INSTALL_COMMAND_TEMPLATE = Template(
    """\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    --mount=type=bind,target=requirements_uv.txt,src=requirements_uv.txt \
    $PIP_SECRET_MOUNT \
    uv pip install $PIP_INSTALL_ARGS
"""
)


APT_INSTALL_COMMAND_TEMPLATE = Template("""\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/var/cache/apt,id=apt \
    apt-get update && apt-get install -y --no-install-recommends \
    $APT_PACKAGES
""")

MICROMAMBA_INSTALL_COMMAND_TEMPLATE = Template("""\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/opt/micromamba/pkgs,\
id=micromamba \
    --mount=from=micromamba,source=/usr/bin/micromamba,target=/usr/bin/micromamba \
    micromamba config set use_lockfiles False && \
    micromamba create -n runtime --root-prefix /opt/micromamba \
    -c conda-forge $CONDA_CHANNELS \
    python=$PYTHON_VERSION $CONDA_PACKAGES
""")

DOCKER_FILE_TEMPLATE = Template("""\
#syntax=docker/dockerfile:1.5
FROM ghcr.io/astral-sh/uv:0.5.1 as uv
FROM mambaorg/micromamba:2.0.3-debian12-slim as micromamba

FROM $BASE_IMAGE

USER root
$APT_INSTALL_COMMAND
RUN --mount=from=micromamba,source=/etc/ssl/certs/ca-certificates.crt,target=/tmp/ca-certificates.crt \
    [ -f /etc/ssl/certs/ca-certificates.crt ] || \
    mkdir -p /etc/ssl/certs/ && cp /tmp/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN id -u flytekit || useradd --create-home --shell /bin/bash flytekit
RUN chown -R flytekit /root && chown -R flytekit /home

$INSTALL_PYTHON_TEMPLATE

# Configure user space
ENV PATH="$EXTRA_PATH:$$PATH" \
    UV_PYTHON=$PYTHON_EXEC \
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

$EXTRA_COPY_CMDS

RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv $RUN_COMMANDS

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


def _copy_lock_files_into_context(image_spec: ImageSpec, lock_file: str, tmp_dir: Path):
    if image_spec.packages is not None:
        msg = f"Support for {lock_file} files and packages is mutually exclusive"
        raise ValueError(msg)

    lock_path = tmp_dir / lock_file
    shutil.copy2(image_spec.requirements, lock_path)

    # lock requires pyproject.toml to be included
    pyproject_toml_path = tmp_dir / "pyproject.toml"
    dir_name = os.path.dirname(image_spec.requirements)

    pyproject_toml_src = os.path.join(dir_name, "pyproject.toml")
    if not os.path.exists(pyproject_toml_src):
        msg = f"To use {lock_file}, a pyproject.toml file must be in the same directory as the lock file"
        raise ValueError(msg)

    shutil.copy2(pyproject_toml_src, pyproject_toml_path)


def _secret_id(index: int) -> str:
    return f"secret_{index}"


def prepare_uv_lock_command(image_spec: ImageSpec, tmp_dir: Path) -> Tuple[Template, List[str]]:
    # uv sync is experimental, so our uv.lock support is also experimental
    # the parameters we pass into install args could be different
    warnings.warn("uv.lock support is experimental", UserWarning)

    _copy_lock_files_into_context(image_spec, "uv.lock", tmp_dir)

    # --locked: Assert that the `uv.lock` will remain unchanged
    # --no-dev: Omit the development dependency group
    # --no-install-project: Do not install the current project
    additional_pip_install_args = ["--locked", "--no-dev", "--no-install-project"]

    return UV_LOCK_INSTALL_TEMPLATE, additional_pip_install_args


def prepare_poetry_lock_command(image_spec: ImageSpec, tmp_dir: Path) -> Tuple[Template, List[str]]:
    _copy_lock_files_into_context(image_spec, "poetry.lock", tmp_dir)

    # --no-root: Do not install the current project
    additional_pip_install_args = ["--no-root"]
    return POETRY_LOCK_TEMPLATE, additional_pip_install_args


def prepare_python_install(image_spec: ImageSpec, tmp_dir: Path) -> str:
    pip_install_args = []
    if image_spec.pip_index:
        pip_install_args.append(f"--index-url {image_spec.pip_index}")

    if image_spec.pip_extra_index_url:
        extra_urls = [f"--extra-index-url {url}" for url in image_spec.pip_extra_index_url]
        pip_install_args.extend(extra_urls)

    pip_secret_mount = ""
    if image_spec.pip_secret_mounts:
        pip_secret_mount = " ".join(
            f"--mount=type=secret,id={_secret_id(i)},target={dst}"
            for i, (_, dst) in enumerate(image_spec.pip_secret_mounts)
        )

    if image_spec.pip_extra_args:
        pip_install_args.append(image_spec.pip_extra_args)

    requirements = []
    template = None
    if image_spec.requirements:
        requirement_basename = os.path.basename(image_spec.requirements)
        if requirement_basename == "uv.lock":
            template, additional_pip_install_args = prepare_uv_lock_command(image_spec, tmp_dir)
            pip_install_args.extend(additional_pip_install_args)
        elif requirement_basename == "poetry.lock":
            template, additional_pip_install_args = prepare_poetry_lock_command(image_spec, tmp_dir)
            pip_install_args.extend(additional_pip_install_args)
        else:
            with open(image_spec.requirements) as f:
                requirements.extend([line.strip() for line in f.readlines()])

    if template is None:
        template = UV_PYTHON_INSTALL_COMMAND_TEMPLATE
        if image_spec.packages:
            requirements.extend(image_spec.packages)

        # Adds flytekit if it is not specified
        if not any(_is_flytekit(package) for package in requirements):
            requirements.append(get_flytekit_for_pypi())

        requirements_uv_path = tmp_dir / "requirements_uv.txt"
        requirements_uv_path.write_text("\n".join(requirements))
        pip_install_args.extend(["--requirement", "requirements_uv.txt"])

    pip_install_args = " ".join(pip_install_args)

    return template.substitute(
        PIP_INSTALL_ARGS=pip_install_args,
        PIP_SECRET_MOUNT=pip_secret_mount,
    )


class _PythonInstallTemplate(NamedTuple):
    python_exec: str
    template: str
    extra_path: str


def prepare_python_executable(image_spec: ImageSpec) -> _PythonInstallTemplate:
    if image_spec.python_exec:
        if image_spec.conda_channels:
            raise ValueError("conda_channels is not supported with python_exec")
        if image_spec.conda_packages:
            raise ValueError("conda_packages is not supported with python_exec")
        return _PythonInstallTemplate(python_exec=image_spec.python_exec, template="", extra_path="")

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

    template = MICROMAMBA_INSTALL_COMMAND_TEMPLATE.substitute(
        PYTHON_VERSION=python_version,
        CONDA_PACKAGES=conda_packages_concat,
        CONDA_CHANNELS=conda_channels_concat,
    )
    return _PythonInstallTemplate(
        python_exec="/opt/micromamba/envs/runtime/bin/python",
        template=template,
        extra_path="/opt/micromamba/envs/runtime/bin",
    )


def create_docker_context(image_spec: ImageSpec, tmp_dir: Path):
    """Populate tmp_dir with Dockerfile as specified by the `image_spec`."""
    base_image = image_spec.base_image or "debian:bookworm-slim"

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

    uv_python_install_command = prepare_python_install(image_spec, tmp_dir)
    env_dict = {"PYTHONPATH": "/root", _F_IMG_ID: image_spec.id}

    if image_spec.env:
        env_dict.update(image_spec.env)

    env = " ".join(f"{k}={v}" for k, v in env_dict.items())

    apt_packages = []
    if image_spec.apt_packages:
        apt_packages.extend(image_spec.apt_packages)

    if apt_packages:
        apt_install_command = APT_INSTALL_COMMAND_TEMPLATE.substitute(APT_PACKAGES=" ".join(apt_packages))
    else:
        apt_install_command = ""

    if image_spec.source_copy_mode is not None and image_spec.source_copy_mode != CopyFileDetection.NO_COPY:
        if not image_spec.source_root:
            raise ValueError(f"Field source_root for {image_spec} must be set when copy is set")

        source_path = tmp_dir / "src"
        source_path.mkdir(parents=True, exist_ok=True)
        # todo: See note in we should pipe through ignores from the command line here at some point.
        #  what about deref_symlink?
        ignore = IgnoreGroup(image_spec.source_root, [GitIgnore, DockerIgnore, StandardIgnore])

        ls, _ = ls_files(
            str(image_spec.source_root), image_spec.source_copy_mode, deref_symlinks=False, ignore_group=ignore
        )

        for file_to_copy in ls:
            rel_path = os.path.relpath(file_to_copy, start=str(image_spec.source_root))
            Path(source_path / rel_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(
                file_to_copy,
                source_path / rel_path,
            )

        copy_command_runtime = "COPY --chown=flytekit ./src /root"
    else:
        copy_command_runtime = ""

    python_install_template = prepare_python_executable(image_spec=image_spec)

    if image_spec.entrypoint is None:
        entrypoint = ""
    else:
        entrypoint = f"ENTRYPOINT {json.dumps(image_spec.entrypoint)}"

    if image_spec.commands:
        run_commands = " && ".join(image_spec.commands)
    else:
        run_commands = ""

    if image_spec.copy:
        copy_commands = []
        for src in image_spec.copy:
            src_path = Path(src)

            if src_path.is_absolute() or ".." in src_path.parts:
                raise ValueError("Absolute paths or paths with '..' are not allowed in COPY command.")

            dst_path = tmp_dir / src_path
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            if src_path.is_dir():
                shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
                copy_commands.append(f"COPY --chown=flytekit {src_path.as_posix()} /root/{src_path.as_posix()}/")
            else:
                shutil.copy(src_path, dst_path)
                copy_commands.append(f"COPY --chown=flytekit {src_path.as_posix()} /root/{src_path.parent.as_posix()}/")

        extra_copy_cmds = "\n".join(copy_commands)
    else:
        extra_copy_cmds = ""

    docker_content = DOCKER_FILE_TEMPLATE.substitute(
        UV_PYTHON_INSTALL_COMMAND=uv_python_install_command,
        APT_INSTALL_COMMAND=apt_install_command,
        INSTALL_PYTHON_TEMPLATE=python_install_template.template,
        EXTRA_PATH=python_install_template.extra_path,
        PYTHON_EXEC=python_install_template.python_exec,
        BASE_IMAGE=base_image,
        ENV=env,
        COPY_COMMAND_RUNTIME=copy_command_runtime,
        ENTRYPOINT=entrypoint,
        RUN_COMMANDS=run_commands,
        EXTRA_COPY_CMDS=extra_copy_cmds,
    )

    dockerfile_path = tmp_dir / "Dockerfile"
    dockerfile_path.write_text(docker_content)


class DefaultImageBuilder(ImageSpecBuilder):
    """Image builder using Docker and buildkit."""

    _SUPPORTED_IMAGE_SPEC_PARAMETERS: ClassVar[set] = {
        "id",
        "name",
        "python_version",
        "builder",
        "source_root",
        "source_copy_mode",
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
        "pip_secret_mounts",
        # "registry_config",
        "commands",
        "copy",
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
            msg = f"The following parameters are unsupported and ignored: {unsupported_parameters}"
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

            if image_spec.pip_secret_mounts:
                for i, (src, _) in enumerate(image_spec.pip_secret_mounts):
                    command.extend(["--secret", f"id={_secret_id(i)},src={src}"])

            if image_spec.registry and push:
                command.append("--push")
            command.append(tmp_dir)

            concat_command = " ".join(command)
            click.secho(f"Run command: {concat_command} ", fg="blue")
            run(command, check=True)
