# Flyte Auto Cache Plugin

This plugin provides a caching mechanism for Flyte tasks that generates a version hash based on the source code of the task and its dependencies. It allows users to manage the cache behavior.

## Usage

To install the plugin, run the following command:

```bash
pip install flytekitplugins-auto-cache
```

To use the caching mechanism in a Flyte task, you can define a `CachePolicy` that combines multiple caching strategies. Here’s an example of how to set it up:

```python
from flytekit import task
from flytekit.core.auto_cache import CachePolicy
from flytekitplugins.auto_cache import CacheFunctionBody, CachePrivateModules

cache_policy = CachePolicy(
    auto_cache_policies = [
        CacheFunctionBody(),
        CachePrivateModules(root_dir="../my_package"),
        ...,
    ],
    salt="my_salt"
)

@task(cache=cache_policy)
def task_fn():
    ...

@task(cache=CacheFunctionBody())
def other_task_fn():
    ...
```

### Salt Parameter

The `salt` parameter in the `CachePolicy` adds uniqueness to the generated hash. It can be used to differentiate between different versions of the same task. This ensures that even if the underlying code remains unchanged, the hash will vary if a different salt is provided. This feature is particularly useful for invalidating the cache for specific versions of a task.

## Cache Implementations

Users can add any number of cache policies that implement the `AutoCache` protocol defined in `@auto_cache.py`. Below are the implementations available so far:

### 1. CacheFunctionBody

This implementation hashes the contents of the function of interest, ignoring any formatting or comment changes. It ensures that the core logic of the function is considered for versioning.

### 2. CacheImage

This implementation includes the hash of the `container_image` object passed. If the image is specified as a name, that string is hashed. If it is an `ImageSpec`, the parametrization of the `ImageSpec` is hashed, allowing for precise versioning of the container image used in the task.

### 3. CachePrivateModules

This implementation recursively searches the task of interest for all callables and constants used. The contents of any callable (function or class) utilized by the task are hashed, ignoring formatting or comments. The values of the literal constants used are also included in the hash.

It accounts for both `import` and `from-import` statements at the global and local levels within a module or function. Any callables that are within site-packages (i.e., external libraries) are ignored.

### 4. CacheExternalDependencies

This implementation recursively searches through all the callables like `CachePrivateModules`, but when an external package is found, it records the version of the package, which is included in the hash. This ensures that changes in external dependencies are reflected in the task's versioning.
