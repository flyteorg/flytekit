import hashlib
from typing import Callable, Generic, List, Optional, ParamSpec, Protocol, Tuple, TypeVar, Union, runtime_checkable
from dataclasses import dataclass
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


@runtime_checkable
class CachePolicy(Protocol):
    def get_version(self, salt: str, params: VersionParameters) -> str:
        ...


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

        if self.policies is not None and isinstance(self.policies, CachePolicy):
            self._policies = [self.policies]
        if self.policies is None:
            # TODO: Fetch default policies instead
            self._policies = []

    def get_ignored_inputs(self) -> Tuple[str, ...]:
        return self._ignored_inputs

    def get_version(self, params: VersionParameters) -> str:
        if self.version is not None:
            return self.version

        # If the list of policies is empty, raise an error
        if not self._policies:
            raise ValueError("No cache policies provided to generate version")

        task_hash = ""
        for cache_instance in self._policies:
            task_hash += cache_instance.get_version(self.salt, params)

        hash_obj = hashlib.sha256(task_hash.encode())
        return hash_obj.hexdigest()
