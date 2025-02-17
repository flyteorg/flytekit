"""Defines a plugin API allowing other libraries to modify the behavior of flytekit.

Libraries can register by defining an object that follows the same API as FlytekitPlugin
and providing an entrypoint with the group name "flytekit.plugin". In `setuptools`,
you can specific them with:

```python
setup(entry_points={
    "flytekit.configuration.plugin": ["my_plugin=my_module:MyCustomPlugin"]
})
```

or in pyproject.toml:

```toml
[project.entry-points."flytekit.configuration.plugin"]
my_plugin = "my_module:MyCustomPlugin"
```
"""

import os
from typing import List, Optional, Protocol, runtime_checkable

from click import Group
from importlib_metadata import entry_points

from flytekit import CachePolicy
from flytekit.configuration import Config, get_config_file
from flytekit.loggers import logger
from flytekit.remote import FlyteRemote


@runtime_checkable
class FlytekitPluginProtocol(Protocol):
    @staticmethod
    def get_remote(
        config: Optional[str], project: str, domain: str, data_upload_location: Optional[str] = None
    ) -> FlyteRemote:
        """Get FlyteRemote object for CLI session."""

    @staticmethod
    def configure_pyflyte_cli(main: Group) -> Group:
        """Configure pyflyte's CLI."""

    @staticmethod
    def secret_requires_group() -> bool:
        """Return True if secrets require group entry."""

    @staticmethod
    def get_default_image() -> Optional[str]:
        """Get default image. Return None to use the images from flytekit.configuration.DefaultImages"""

    @staticmethod
    def get_auth_success_html(endpoint: str) -> Optional[str]:
        """Get default success html for auth. Return None to use flytekit's default success html."""

    @staticmethod
    def get_default_cache_policies() -> List[CachePolicy]:
        """Get default cache policies for tasks."""


class FlytekitPlugin:
    @staticmethod
    def get_remote(
        config: Optional[str], project: str, domain: str, data_upload_location: Optional[str] = None
    ) -> FlyteRemote:
        """Get FlyteRemote object for CLI session."""

        cfg_file = get_config_file(config)

        # The assumption here (if there's no config file that means we want sandbox) is too broad.
        # todo: can improve this in the future, rather than just checking one env var, auto() with
        #   nothing configured should probably not return sandbox but can consider
        if cfg_file is None:
            # We really are just looking for endpoint, client_id, and client_secret. These correspond to the env vars
            # FLYTE_PLATFORM_URL, FLYTE_CREDENTIALS_CLIENT_ID, FLYTE_CREDENTIALS_CLIENT_SECRET
            # auto() should pick these up.
            if "FLYTE_PLATFORM_URL" in os.environ:
                cfg_obj = Config.auto(None)
                logger.warning(f"Auto-created config object to pick up env vars {cfg_obj}")
            else:
                cfg_obj = Config.for_sandbox()
                logger.info("No config files found, creating remote with sandbox config")
        else:  # pragma: no cover
            cfg_obj = Config.auto(config)
            logger.debug(f"Creating remote with config {cfg_obj}" + (f" with file {config}" if config else ""))
        return FlyteRemote(
            cfg_obj, default_project=project, default_domain=domain, data_upload_location=data_upload_location
        )

    @staticmethod
    def configure_pyflyte_cli(main: Group) -> Group:
        """Configure pyflyte's CLI."""
        return main

    @staticmethod
    def secret_requires_group() -> bool:
        """Return True if secrets require group entry during registration time."""
        return True

    @staticmethod
    def get_default_image() -> Optional[str]:
        """Get default image. Return None to use the images from flytekit.configuration.DefaultImages"""
        return None

    @staticmethod
    def get_auth_success_html(endpoint: str) -> Optional[str]:
        """Get default success html. Return None to use flytekit's default success html."""
        return None

    @staticmethod
    def get_default_cache_policies() -> List[CachePolicy]:
        """Get default cache policies for tasks."""
        return []


def _get_plugin_from_entrypoint():
    """Get plugin from entrypoint."""
    group = "flytekit.configuration.plugin"
    plugins = list(entry_points(group=group))

    if not plugins:
        return FlytekitPlugin

    if len(plugins) >= 2:
        plugin_names = [p.name for p in plugins]
        raise ValueError(
            f"Multiple plugins installed: {plugin_names}. flytekit only supports one installed plugin at a time."
        )

    plugin_to_load = plugins[0]
    logger.info(f"Loading plugin: {plugin_to_load.name}")
    return plugin_to_load.load()


_GLOBAL_CONFIG = {"plugin": _get_plugin_from_entrypoint()}


def get_plugin():
    """Get current plugin"""
    return _GLOBAL_CONFIG["plugin"]
