import base64
import copy
import dataclasses
import hashlib
import os
import pathlib
import re
import sys
import typing
from abc import abstractmethod
from dataclasses import asdict, dataclass
from functools import cached_property, lru_cache
from importlib import metadata
from typing import Dict, List, Optional, Tuple, Union

import click
import requests
from packaging.version import Version

from flytekit.constants import CopyFileDetection
from flytekit.exceptions.user import FlyteAssertion

DOCKER_HUB = "docker.io"
_F_IMG_ID = "_F_IMG_ID"
FLYTE_FORCE_PUSH_IMAGE_SPEC = "FLYTE_FORCE_PUSH_IMAGE_SPEC"


@dataclass
class ImageSpec:
    """
    This class is used to specify the docker image that will be used to run the task.

    Args:
        name: name of the image.
        python_version: python version of the image. Use default python in the base image if None.
        builder: Type of plugin to build the image. Use envd by default.
        source_root: source root of the image.
        env: environment variables of the image.
        registry: registry of the image.
        packages: list of python packages to install.
        conda_packages: list of conda packages to install.
        conda_channels: list of conda channels.
        requirements: path to the requirements.txt file.
        apt_packages: list of apt packages to install.
        cuda: version of cuda to install.
        cudnn: version of cudnn to install.
        base_image: base image of the image.
        platform: Specify the target platforms for the build output (for example, windows/amd64 or linux/amd64,darwin/arm64
        pip_index: Specify the custom pip index url
        pip_extra_index_url: Specify one or more pip index urls as a list
        pip_secret_mounts: Specify a list of tuples to mount secret for pip install. Each tuple should contain the path to
            the secret file and the mount path. For example, [(".gitconfig", "/etc/gitconfig")]. This is experimental and
            the interface may change in the future. Configuring this should not change the built image.
        pip_extra_args: Specify one or more extra pip install arguments as a space-delimited string
        registry_config: Specify the path to a JSON registry config file
        entrypoint: List of strings to overwrite the entrypoint of the base image with, set to [] to remove the entrypoint.
        commands: Command to run during the building process
        tag_format: Custom string format for image tag. The ImageSpec hash passed in as `spec_hash`. For example,
            to add a "dev" suffix to the image tag, set `tag_format="{spec_hash}-dev"`
        source_copy_mode: This option allows the user to specify which source files to copy from the local host, into the image.
            Not setting this option means to use the default flytekit behavior. The default behavior is:
                - if fast register is used, source files are not copied into the image (because they're already copied
                  into the fast register tar layer).
                - if fast register is not used, then the LOADED_MODULES (aka 'auto') option is used to copy loaded
                  Python files into the image.

            If the option is set by the user, then that option is of course used.
        copy: List of files/directories to copy to /root. e.g. ["src/file1.txt", "src/file2.txt"]
        python_exec: Python executable to use for install packages
    """

    name: str = "flytekit"
    python_version: str = None  # Use default python in the base image if None.
    builder: Optional[str] = None
    source_root: Optional[str] = None  # a.txt:auto
    env: Optional[typing.Dict[str, str]] = None
    registry: Optional[str] = None
    packages: Optional[List[str]] = None
    conda_packages: Optional[List[str]] = None
    conda_channels: Optional[List[str]] = None
    requirements: Optional[str] = None
    apt_packages: Optional[List[str]] = None
    cuda: Optional[str] = None
    cudnn: Optional[str] = None
    base_image: Optional[Union[str, "ImageSpec"]] = None
    platform: str = "linux/amd64"
    pip_index: Optional[str] = None
    pip_extra_index_url: Optional[List[str]] = None
    pip_secret_mounts: Optional[List[Tuple[str, str]]] = None
    pip_extra_args: Optional[str] = None
    registry_config: Optional[str] = None
    entrypoint: Optional[List[str]] = None
    commands: Optional[List[str]] = None
    tag_format: Optional[str] = None
    source_copy_mode: Optional[CopyFileDetection] = None
    copy: Optional[List[str]] = None
    python_exec: Optional[str] = None

    def __post_init__(self):
        self.name = self.name.lower()
        self._is_force_push = os.environ.get(FLYTE_FORCE_PUSH_IMAGE_SPEC, False)  # False by default
        if self.registry:
            self.registry = self.registry.lower()
            if not validate_container_registry_name(self.registry):
                raise ValueError(
                    f"Invalid container registry name: '{self.registry}'.\n Expected formats:\n"
                    f"- 'localhost:30000' (for local registries)\n"
                    f"- 'ghcr.io/username' (for GitHub Container Registry)\n"
                    f"- 'docker.io/username' (for docker hub)\n"
                )

        # If not set, help the user set this option as well, to support the older default behavior where existence
        # of the source root implied that copying of files was needed.
        if self.source_root is not None:
            self.source_copy_mode = self.source_copy_mode or CopyFileDetection.LOADED_MODULES

        parameters_str_list = [
            "packages",
            "conda_channels",
            "conda_packages",
            "apt_packages",
            "pip_extra_index_url",
            "entrypoint",
            "commands",
        ]
        for parameter in parameters_str_list:
            attr = getattr(self, parameter)
            parameter_is_none = attr is None
            parameter_is_list_string = isinstance(attr, list) and all(isinstance(v, str) for v in attr)
            if not (parameter_is_none or parameter_is_list_string):
                error_msg = f"{parameter} must be a list of strings or None"
                raise ValueError(error_msg)

        if self.pip_secret_mounts is not None:
            pip_secret_mounts_is_list_tuple = isinstance(self.pip_secret_mounts, list) and all(
                isinstance(v, tuple) and len(v) == 2 and all(isinstance(vv, str) for vv in v)
                for v in self.pip_secret_mounts
            )
            if not pip_secret_mounts_is_list_tuple:
                error_msg = "pip_secret_mounts must be a list of tuples of two strings or None"
                raise ValueError(error_msg)

    @cached_property
    def id(self) -> str:
        """
        Calculate a unique hash as the ID for the ImageSpec, and it will be used to
        1. Identify the imageSpec in the ImageConfig in the serialization context.
        2. Check if the current container image in the pod is built from this image spec in `is_container()`.

        ImageConfig:
        - deduced abc: flyteorg/flytekit:123
        - deduced xyz: flyteorg/flytekit:456

        The result of this property also depends on whether or not update_image_spec_copy_handling was called.

        :return: a unique identifier of the ImageSpec
        """
        parameters_to_exclude = ["pip_secret_mounts"]
        # Only get the non-None values in the ImageSpec to ensure the hash is consistent across different Flytekit versions.
        image_spec_dict = asdict(
            self, dict_factory=lambda x: {k: v for (k, v) in x if v is not None and k not in parameters_to_exclude}
        )
        image_spec_bytes = image_spec_dict.__str__().encode("utf-8")
        return base64.urlsafe_b64encode(hashlib.md5(image_spec_bytes).digest()).decode("ascii").rstrip("=")

    def __hash__(self):
        return hash(self.id)

    @property
    def tag(self) -> str:
        """
        Calculate a hash from the image spec. The hash will be the tag of the image.
        We will also read the content of the requirement file and the source root to calculate the hash.
        Therefore, it will generate different hash if new dependencies are added or the source code is changed.

        Keep in mind the fields source_root and copy may be changed by update_image_spec_copy_handling, so when
        you call this property in relation to that function matter will change the output.
        """

        # copy the image spec to avoid modifying the original image spec. otherwise, the hash will be different.
        spec = copy.deepcopy(self)
        if isinstance(spec.base_image, ImageSpec):
            spec = dataclasses.replace(spec, base_image=spec.base_image.image_name())

        if self.source_copy_mode is not None and self.source_copy_mode != CopyFileDetection.NO_COPY:
            if not self.source_root:
                raise ValueError(f"Field source_root for image spec {self.name} must be set when copy is set")

            # Imports of flytekit.tools are circular
            from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore
            from flytekit.tools.script_mode import ls_files

            # todo: we should pipe through ignores from the command line here at some point.
            #  what about deref_symlink?
            ignore = IgnoreGroup(self.source_root, [GitIgnore, DockerIgnore, StandardIgnore])

            _, ls_digest = ls_files(
                str(self.source_root), self.source_copy_mode, deref_symlinks=False, ignore_group=ignore
            )

            # Since the source root is supposed to represent the files, store the digest into the source root as a
            # shortcut to represent all the files.
            spec = dataclasses.replace(spec, source_root=ls_digest)

        if self.copy:
            from flytekit.tools.fast_registration import compute_digest

            digest = compute_digest(self.copy, None)
            spec = dataclasses.replace(spec, copy=digest)

        if spec.requirements:
            requirements = hashlib.sha1(pathlib.Path(spec.requirements).read_bytes().strip()).hexdigest()
            spec = dataclasses.replace(spec, requirements=requirements)

        # won't rebuild the image if we change the registry_config path
        spec = dataclasses.replace(spec, registry_config=None)
        tag = spec.id.replace("-", "_")
        if self.tag_format:
            return self.tag_format.format(spec_hash=tag)
        return tag

    def image_name(self) -> str:
        """Full image name with tag."""
        image_name = f"{self.name}:{self.tag}"
        if self.registry:
            image_name = f"{self.registry}/{image_name}"
        try:
            return ImageBuildEngine._IMAGE_NAME_TO_REAL_NAME[image_name]
        except KeyError:
            return image_name

    def is_container(self) -> bool:
        """
        Check if the current container image in the pod is built from current image spec.
        :return: True if the current container image in the pod is built from current image spec, False otherwise.
        """
        from flytekit.core.context_manager import ExecutionState, FlyteContextManager

        state = FlyteContextManager.current_context().execution_state
        if state and state.mode and state.mode != ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            return os.environ.get(_F_IMG_ID) == self.id
        return True

    def exist(self) -> Optional[bool]:
        """
        Check if the image exists in the registry.
        Return True if the image exists in the registry, False otherwise.
        Return None if failed to check if the image exists due to the permission issue or other reasons.
        """
        import docker
        from docker.errors import APIError, ImageNotFound

        try:
            client = docker.from_env()
            if self.registry:
                client.images.get_registry_data(self.image_name())
            else:
                client.images.get(self.image_name())
            return True
        except APIError as e:
            if e.response.status_code == 404:
                return False

            if re.match(
                f"unknown: (artifact|repository) .*{self.name}(|:{self.tag}) not found",
                e.explanation,
            ):
                click.secho(f"Received 500 error with explanation: {e.explanation}", fg="yellow")
                return False

            click.secho(f"Failed to check if the image exists with error:\n {e}", fg="red")
            return None
        except ImageNotFound:
            return False
        except Exception as e:
            # if docker engine is not running locally, use requests to check if the image exists.
            if self.registry is None:
                container_registry = None
            elif "localhost:" in self.registry:
                container_registry = self.registry
            elif "/" in self.registry:
                container_registry = self.registry.split("/")[0]
            else:
                # Assume the image is in docker hub if users don't specify a registry, such as ghcr.io, docker.io.
                container_registry = DOCKER_HUB
            if container_registry == DOCKER_HUB:
                url = f"https://hub.docker.com/v2/repositories/{self.registry}/{self.name}/tags/{self.tag}"
                response = requests.get(url)
                if response.status_code == 200:
                    return True

                if response.status_code == 404 and "not found" in str(response.content):
                    return False

            if "Not supported URL scheme http+docker" in str(e):
                raise RuntimeError(
                    f"{str(e)}\n"
                    f"Error: Incompatible Docker package version.\n"
                    f"Current version: {docker.__version__}\n"
                    f"Please upgrade the Docker package to version 7.1.0 or higher.\n"
                    f"You can upgrade the package by running:\n"
                    f"    pip install --upgrade docker"
                )

            click.secho(f"Failed to check if the image exists with error:\n {e}", fg="red")
            return None

    def _update_attribute(self, attr_name: str, values: Union[str, List[str]]) -> "ImageSpec":
        """
        Generic method to update a specified list attribute, either appending or extending.
        """
        current_value = copy.deepcopy(getattr(self, attr_name)) or []

        if isinstance(values, str):
            current_value.append(values)
        elif isinstance(values, list):
            current_value.extend(values)

        return dataclasses.replace(self, **{attr_name: current_value})

    def with_commands(self, commands: Union[str, List[str]]) -> "ImageSpec":
        """
        Builder that returns a new image spec with an additional list of commands that will be executed during the building process.
        """
        return self._update_attribute("commands", commands)

    def with_packages(self, packages: Union[str, List[str]]) -> "ImageSpec":
        """
        Builder that returns a new image speck with additional python packages that will be installed during the building process.
        """
        new_image_spec = self._update_attribute("packages", packages)
        return new_image_spec

    def with_apt_packages(self, apt_packages: Union[str, List[str]]) -> "ImageSpec":
        """
        Builder that returns a new image spec with an additional list of apt packages that will be executed during the building process.
        """
        new_image_spec = self._update_attribute("apt_packages", apt_packages)
        return new_image_spec

    def with_copy(self, src: Union[str, List[str]]) -> "ImageSpec":
        """
        Builder that returns a new image spec with the source files copied to the destination directory.
        """
        return self._update_attribute("copy", src)

    def force_push(self) -> "ImageSpec":
        """
        Builder that returns a new image spec with force push enabled.
        """
        copied_image_spec = copy.deepcopy(self)
        copied_image_spec._is_force_push = True

        return copied_image_spec

    @classmethod
    def from_env(cls, *, pinned_packages: Optional[List[str]] = None, **kwargs) -> "ImageSpec":
        """Create ImageSpec with the environment's Python version and packages pinned to the ones in the environment."""

        from importlib.metadata import version

        # Invalid kwargs when using `ImageSpec.from_env`
        invalid_kwargs = ["python_version"]
        for invalid_kwarg in invalid_kwargs:
            if invalid_kwarg in kwargs and kwargs[invalid_kwarg] is not None:
                msg = (
                    f"{invalid_kwarg} can not be used with `from_env` because it will be inferred from the environment"
                )
                raise ValueError(msg)

        version_info = sys.version_info
        python_version = f"{version_info.major}.{version_info.minor}"

        if "packages" in kwargs:
            packages = kwargs.pop("packages")
        else:
            packages = []

        pinned_packages = pinned_packages or []

        for package_to_pin in pinned_packages:
            package_version = version(package_to_pin)
            packages.append(f"{package_to_pin}=={package_version}")

        return ImageSpec(packages=packages, python_version=python_version, **kwargs)


class ImageSpecBuilder:
    @abstractmethod
    def build_image(self, image_spec: ImageSpec) -> Optional[str]:
        """
        Build the docker image and push it to the registry.

        Args:
            image_spec: image spec of the task.

        Returns:
            fully_qualified_image_name: Fully qualified image name. If None, then `image_spec.image_name()` is used.
        """
        raise NotImplementedError("This method is not implemented in the base class.")

    def should_build(self, image_spec: ImageSpec) -> bool:
        """
        Whether or not the builder should build the ImageSpec.

        Args:
            image_spec: image spec of the task.

        Returns:
            True if the image should be built, otherwise it returns False.
        """
        img_name = image_spec.image_name()
        exist = image_spec.exist()
        if exist is False:
            click.secho(f"Image {img_name} not found. building...", fg="blue")
            return True
        elif exist is True:
            if image_spec._is_force_push:
                click.secho(f"Overwriting existing image {img_name}.", fg="blue")
                return True
            click.secho(f"Image {img_name} found. Skip building.", fg="blue")
        else:
            click.secho(f"Flytekit assumes the image {img_name} already exists.", fg="blue")
        return False


class ImageBuildEngine:
    """
    ImageBuildEngine contains a list of builders that can be used to build an ImageSpec.
    """

    _REGISTRY: typing.Dict[str, Tuple[ImageSpecBuilder, int]] = {}
    # _IMAGE_NAME_TO_REAL_NAME is used to keep track of the fully qualified image name
    # returned by the image builder. This allows ImageSpec to map from `image_spc.image_name()`
    # to the real qualified name.
    _IMAGE_NAME_TO_REAL_NAME: Dict[str, str] = {}

    @classmethod
    def register(cls, builder_type: str, image_spec_builder: ImageSpecBuilder, priority: int = 5):
        cls._REGISTRY[builder_type] = (image_spec_builder, priority)

    @classmethod
    @lru_cache
    def build(cls, image_spec: ImageSpec):
        from flytekit.core.context_manager import FlyteContextManager

        execution_mode = FlyteContextManager.current_context().execution_state.mode
        # Do not build in executions
        if execution_mode is not None:
            return

        spec = copy.deepcopy(image_spec)

        if isinstance(spec.base_image, ImageSpec):
            cls.build(spec.base_image)
            spec.base_image = spec.base_image.image_name()

        if spec.builder is None and cls._REGISTRY:
            builder = max(cls._REGISTRY, key=lambda name: cls._REGISTRY[name][1])
        else:
            builder = spec.builder

        img_name = spec.image_name()
        img_builder = cls._get_builder(builder)
        if img_builder.should_build(spec):
            fully_qualified_image_name = img_builder.build_image(spec)
            if fully_qualified_image_name is not None:
                cls._IMAGE_NAME_TO_REAL_NAME[img_name] = fully_qualified_image_name

    @classmethod
    def _get_builder(cls, builder: str) -> ImageSpecBuilder:
        if builder is None:
            raise AssertionError("There is no image builder registered.")
        if builder not in cls._REGISTRY:
            raise AssertionError(f"Image builder {builder} is not registered.")
        if builder == "envd":
            envd_version = metadata.version("envd")
            # flytekit v1.10.2+ copies the workflow code to the WorkDir specified in the Dockerfile. However, envd<0.3.39
            # overwrites the WorkDir when building the image, resulting in a permission issue when flytekit downloads the file.
            if Version(envd_version) < Version("0.3.39"):
                raise FlyteAssertion(
                    f"envd version {envd_version} is not compatible with flytekit>v1.10.2."
                    f" Please upgrade envd to v0.3.39+."
                )
        return cls._REGISTRY[builder][0]


def validate_container_registry_name(name: str) -> bool:
    """Validate Docker container registry name."""
    # Define the regular expression for the registry name
    registry_pattern = r"^(localhost:\d{1,5}|([a-z\d\._-]+)(:\d{1,5})?)(/[\w\.-]+)*$"

    # Use regex to validate the given name
    return bool(re.match(registry_pattern, name))
