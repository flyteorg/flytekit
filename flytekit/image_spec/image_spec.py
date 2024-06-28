import base64
import copy
import hashlib
import os
import pathlib
import typing
from abc import abstractmethod
from dataclasses import asdict, dataclass
from functools import lru_cache
from importlib import metadata
from typing import Dict, List, Optional, Tuple, Union

import click
import requests
from packaging.version import Version

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
        registry_config: Specify the path to a JSON registry config file
        commands: Command to run during the building process
        tag_format: Custom string format for image tag. The ImageSpec hash passed in as `spec_hash`. For example,
            to add a "dev" suffix to the image tag, set `tag_format="{spec_hash}-dev"`
    """

    name: str = "flytekit"
    python_version: str = None  # Use default python in the base image if None.
    builder: Optional[str] = None
    source_root: Optional[str] = None
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
    registry_config: Optional[str] = None
    commands: Optional[List[str]] = None
    tag_format: Optional[str] = None

    def __post_init__(self):
        self.name = self.name.lower()
        self._is_force_push = os.environ.get(FLYTE_FORCE_PUSH_IMAGE_SPEC, False)  # False by default
        if self.registry:
            self.registry = self.registry.lower()

    def image_name(self) -> str:
        """Full image name with tag."""
        image_name = self._image_name()
        try:
            return ImageBuildEngine._IMAGE_NAME_TO_REAL_NAME[image_name]
        except KeyError:
            return image_name

    def _image_name(self) -> str:
        """Construct full image name with tag."""
        tag = calculate_hash_from_image_spec(self)
        if self.tag_format:
            tag = self.tag_format.format(spec_hash=tag)

        container_image = f"{self.name}:{tag}"
        if self.registry:
            container_image = f"{self.registry}/{container_image}"
        return container_image

    def is_container(self) -> bool:
        from flytekit.core.context_manager import ExecutionState, FlyteContextManager

        state = FlyteContextManager.current_context().execution_state
        if state and state.mode and state.mode != ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            return os.environ.get(_F_IMG_ID) == self.image_name()
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

            click.secho(f"Failed to check if the image exists with error:\n {e}", fg="red")
            return None
        except ImageNotFound:
            return False
        except Exception as e:
            tag = calculate_hash_from_image_spec(self)
            # if docker engine is not running locally, use requests to check if the image exists.
            if "localhost:" in self.registry:
                container_registry = self.registry
            elif self.registry and "/" in self.registry:
                container_registry = self.registry.split("/")[0]
            else:
                # Assume the image is in docker hub if users don't specify a registry, such as ghcr.io, docker.io.
                container_registry = DOCKER_HUB
            if container_registry == DOCKER_HUB:
                url = f"https://hub.docker.com/v2/repositories/{self.registry}/{self.name}/tags/{tag}"
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

    def __hash__(self):
        return hash(asdict(self).__str__())

    def with_commands(self, commands: Union[str, List[str]]) -> "ImageSpec":
        """
        Builder that returns a new image spec with an additional list of commands that will be executed during the building process.
        """
        new_image_spec = copy.deepcopy(self)
        if new_image_spec.commands is None:
            new_image_spec.commands = []

        if isinstance(commands, List):
            new_image_spec.commands.extend(commands)
        else:
            new_image_spec.commands.append(commands)

        return new_image_spec

    def with_packages(self, packages: Union[str, List[str]]) -> "ImageSpec":
        """
        Builder that returns a new image speck with additional python packages that will be installed during the building process.
        """
        new_image_spec = copy.deepcopy(self)
        if new_image_spec.packages is None:
            new_image_spec.packages = []

        if isinstance(packages, List):
            new_image_spec.packages.extend(packages)
        else:
            new_image_spec.packages.append(packages)

        return new_image_spec

    def with_apt_packages(self, apt_packages: Union[str, List[str]]) -> "ImageSpec":
        """
        Builder that returns a new image spec with additional list of apt packages that will be executed during the building process.
        """
        new_image_spec = copy.deepcopy(self)
        if new_image_spec.apt_packages is None:
            new_image_spec.apt_packages = []

        if isinstance(apt_packages, List):
            new_image_spec.apt_packages.extend(apt_packages)
        else:
            new_image_spec.apt_packages.append(apt_packages)

        return new_image_spec

    def force_push(self) -> "ImageSpec":
        """
        Builder that returns a new image spec with force push enabled.
        """
        new_image_spec = copy.deepcopy(self)
        new_image_spec._is_force_push = True

        return new_image_spec


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

        if isinstance(image_spec.base_image, ImageSpec):
            cls.build(image_spec.base_image)
            image_spec.base_image = image_spec.base_image.image_name()

        if image_spec.builder is None and cls._REGISTRY:
            builder = max(cls._REGISTRY, key=lambda name: cls._REGISTRY[name][1])
        else:
            builder = image_spec.builder

        img_name = image_spec.image_name()
        if cls._get_builder(builder).should_build(image_spec):
            cls._build_image(builder, image_spec, img_name)

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

    @classmethod
    def _build_image(cls, builder: str, image_spec: ImageSpec, img_name: str):
        fully_qualified_image_name = cls._get_builder(builder).build_image(image_spec)
        if fully_qualified_image_name is not None:
            cls._IMAGE_NAME_TO_REAL_NAME[img_name] = fully_qualified_image_name


@lru_cache
def _calculate_deduped_hash_from_image_spec(image_spec: ImageSpec):
    """
    Calculate this special hash from the image spec,
    and it used to identify the imageSpec in the ImageConfig in the serialization context.

    ImageConfig:
    - deduced hash 1: flyteorg/flytekit: 123
    - deduced hash 2: flyteorg/flytekit: 456
    """
    image_spec_bytes = asdict(image_spec).__str__().encode("utf-8")
    # copy the image spec to avoid modifying the original image spec. otherwise, the hash will be different.
    return base64.urlsafe_b64encode(hashlib.md5(image_spec_bytes).digest()).decode("ascii").rstrip("=")


@lru_cache
def calculate_hash_from_image_spec(image_spec: ImageSpec):
    """
    Calculate the hash from the image spec.
    """
    # copy the image spec to avoid modifying the original image spec. otherwise, the hash will be different.
    spec = copy.deepcopy(image_spec)
    if isinstance(spec.base_image, ImageSpec):
        spec.base_image = spec.base_image.image_name()

    if image_spec.source_root:
        from flytekit.tools.fast_registration import compute_digest
        from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore

        ignore = IgnoreGroup(image_spec.source_root, [GitIgnore, DockerIgnore, StandardIgnore])
        digest = compute_digest(image_spec.source_root, ignore.is_ignored)
        spec.source_root = digest

    if spec.requirements:
        spec.requirements = hashlib.sha1(pathlib.Path(spec.requirements).read_bytes()).hexdigest()
    # won't rebuild the image if we change the registry_config path
    spec.registry_config = None
    image_spec_bytes = asdict(spec).__str__().encode("utf-8")
    tag = base64.urlsafe_b64encode(hashlib.md5(image_spec_bytes).digest()).decode("ascii").rstrip("=")
    # replace "-" with "_" to make it a valid tag
    return tag.replace("-", "_")


def hash_directory(path):
    """
    Return the SHA-256 hash of the directory at the given path.
    """
    hasher = hashlib.sha256()
    for root, dirs, files in os.walk(path):
        for file in files:
            with open(os.path.join(root, file), "rb") as f:
                while True:
                    # Read file in small chunks to avoid loading large files into memory all at once
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    hasher.update(chunk)
    return bytes(hasher.hexdigest(), "utf-8")
