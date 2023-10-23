import base64
import hashlib
import os
import pathlib
import typing
from abc import abstractmethod
from copy import copy
from dataclasses import asdict, dataclass
from functools import lru_cache
from typing import List, Optional

import click
import requests

DOCKER_HUB = "docker.io"
_F_IMG_ID = "_F_IMG_ID"


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
        requirements: path to the requirements.txt file.
        apt_packages: list of apt packages to install.
        cuda: version of cuda to install.
        cudnn: version of cudnn to install.
        base_image: base image of the image.
        platform: Specify the target platforms for the build output (for example, windows/amd64 or linux/amd64,darwin/arm64
        pip_index: Specify the custom pip index url
        registry_config: Specify the path to a JSON registry config file
    """

    name: str = "flytekit"
    python_version: str = None  # Use default python in the base image if None.
    builder: str = "envd"
    source_root: Optional[str] = None
    env: Optional[typing.Dict[str, str]] = None
    registry: Optional[str] = None
    packages: Optional[List[str]] = None
    requirements: Optional[str] = None
    apt_packages: Optional[List[str]] = None
    cuda: Optional[str] = None
    cudnn: Optional[str] = None
    base_image: Optional[str] = None
    platform: str = "linux/amd64"
    pip_index: Optional[str] = None
    registry_config: Optional[str] = None

    def __post_init__(self):
        self.name = self.name.lower()
        if self.registry:
            self.registry = self.registry.lower()

    def image_name(self) -> str:
        """
        return full image name with tag.
        """
        tag = calculate_hash_from_image_spec(self)
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

    @lru_cache
    def exist(self) -> bool:
        """
        Check if the image exists in the registry.
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
        except ImageNotFound:
            return False
        except Exception as e:
            tag = calculate_hash_from_image_spec(self)
            # if docker engine is not running locally
            container_registry = DOCKER_HUB
            if self.registry and "/" in self.registry:
                container_registry = self.registry.split("/")[0]
            if container_registry == DOCKER_HUB:
                url = f"https://hub.docker.com/v2/repositories/{self.registry}/{self.name}/tags/{tag}"
                response = requests.get(url)
                if response.status_code == 200:
                    return True

                if response.status_code == 404:
                    return False

            click.secho(f"Failed to check if the image exists with error : {e}", fg="red")
            click.secho("Flytekit assumes that the image already exists.", fg="blue")
            return True

    def __hash__(self):
        return hash(asdict(self).__str__())


class ImageSpecBuilder:
    @abstractmethod
    def build_image(self, image_spec: ImageSpec):
        """
        Build the docker image and push it to the registry.

        Args:
            image_spec: image spec of the task.
        """
        raise NotImplementedError("This method is not implemented in the base class.")


class ImageBuildEngine:
    """
    ImageBuildEngine contains a list of builders that can be used to build an ImageSpec.
    """

    _REGISTRY: typing.Dict[str, ImageSpecBuilder] = {}
    _BUILT_IMAGES: typing.Set[str] = set()

    @classmethod
    def register(cls, builder_type: str, image_spec_builder: ImageSpecBuilder):
        cls._REGISTRY[builder_type] = image_spec_builder

    @classmethod
    def build(cls, image_spec: ImageSpec):
        if image_spec.builder not in cls._REGISTRY:
            raise Exception(f"Builder {image_spec.builder} is not registered.")
        img_name = image_spec.image_name()
        if img_name in cls._BUILT_IMAGES or image_spec.exist():
            click.secho(f"Image {img_name} found. Skip building.", fg="blue")
        else:
            click.secho(f"Image {img_name} not found. Building...", fg="blue")
            cls._REGISTRY[image_spec.builder].build_image(image_spec)
            cls._BUILT_IMAGES.add(img_name)


@lru_cache
def calculate_hash_from_image_spec(image_spec: ImageSpec):
    """
    Calculate the hash from the image spec.
    """
    # copy the image spec to avoid modifying the original image spec. otherwise, the hash will be different.
    spec = copy(image_spec)
    spec.source_root = hash_directory(image_spec.source_root) if image_spec.source_root else b""
    if spec.requirements:
        spec.requirements = hashlib.sha1(pathlib.Path(spec.requirements).read_bytes()).hexdigest()
    # won't rebuild the image if we change the registry_config path
    spec.registry_config = None
    image_spec_bytes = asdict(spec).__str__().encode("utf-8")
    tag = base64.urlsafe_b64encode(hashlib.md5(image_spec_bytes).digest()).decode("ascii")
    # replace "=" with "." and replace "-" with "_" to make it a valid tag
    return tag.replace("=", ".").replace("-", "_")


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
