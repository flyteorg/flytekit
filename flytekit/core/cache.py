import hashlib
from dataclasses import dataclass
from typing import Callable, Generic, List, Optional, Protocol, Tuple, Union, runtime_checkable

from typing_extensions import ParamSpec, TypeVar

from flytekit.core.pod_template import PodTemplate
from flytekit.image_spec.image_spec import ImageSpec

P = ParamSpec("P")
FuncOut = TypeVar("FuncOut")


@dataclass
class VersionParameters(Generic[P, FuncOut]):
    """
    Parameters used for version hash generation.

    param func: The function to generate a version for. This is an optional parameter and can be any callable
                 that matches the specified parameter and return types.
    :type func: Optional[Callable[P, FuncOut]]
    :param container_image: The container image to generate a version for. This can be a string representing the
                            image name or an ImageSpec object.
    :type container_image: Optional[Union[str, ImageSpec]]
    """

    func: Callable[P, FuncOut]
    container_image: Optional[Union[str, ImageSpec]] = None
    pod_template: Optional[PodTemplate] = None
    pod_template_name: Optional[str] = None


@runtime_checkable
class CachePolicy(Protocol):
    def get_version(self, salt: str, params: VersionParameters) -> str: ...


@dataclass
class Cache:
    """
    Cache configuration for a task.

    :param version: The version of the task. If not provided, the version will be generated based on the cache policies.
    :type version: Optional[str]
    :param serialize: Boolean that indicates if identical (ie. same inputs) instances of this task should be executed in
          serial when caching is enabled. This means that given multiple concurrent executions over identical inputs,
          only a single instance executes and the rest wait to reuse the cached results.
    :type serialize: bool
    :param ignored_inputs: A tuple of input names to ignore when generating the version hash.
    :type ignored_inputs: Union[Tuple[str, ...], str]
    :param salt: A salt used in the hash generation.
    :type salt: str
    :param policies: A list of cache policies to generate the version hash.
    :type policies: Optional[Union[List[CachePolicy], CachePolicy]]
    """

    version: Optional[str] = None
    serialize: bool = False
    ignored_inputs: Union[Tuple[str, ...], str] = ()
    salt: str = ""
    policies: Optional[Union[List[CachePolicy], CachePolicy]] = None

    def __post_init__(self):
        if isinstance(self.ignored_inputs, str):
            self._ignored_inputs = (self.ignored_inputs,)
        else:
            self._ignored_inputs = self.ignored_inputs

        # Normalize policies so that self._policies is always a list
        if self.policies is None:
            from flytekit.configuration.plugin import get_plugin

            self._policies = get_plugin().get_default_cache_policies()
        elif isinstance(self.policies, CachePolicy):
            self._policies = [self.policies]

        if self.version is None and not self._policies:
            raise ValueError("If version is not defined then at least one cache policy needs to be set")

    def get_ignored_inputs(self) -> Tuple[str, ...]:
        return self._ignored_inputs

    def get_version(self, params: VersionParameters) -> str:
        if self.version is not None:
            return self.version

        task_hash = ""
        for policy in self._policies:
            try:
                task_hash += policy.get_version(self.salt, params)
            except Exception as e:
                raise ValueError(
                    f"Failed to generate version for cache policy {policy}. Please consider setting the version in the Cache definition, e.g. Cache(version='v1.2.3')"
                ) from e

        hash_obj = hashlib.sha256(task_hash.encode())
        return hash_obj.hexdigest()
