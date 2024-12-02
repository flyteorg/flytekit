import hashlib
import importlib
import sys
from pathlib import Path
from typing import Any, Optional

import click
from flytekitplugins.auto_cache.cache_private_modules import CachePrivateModules, temporarily_add_to_syspath

from flytekit.core.auto_cache import VersionParameters


class CacheExternalDependencies(CachePrivateModules):
    """
    A cache implementation that tracks external package dependencies and their versions.
    Inherits the dependency traversal logic from CachePrivateModules but focuses on external packages.
    """

    def __init__(self, salt: str, root_dir: str):
        super().__init__(salt=salt, root_dir=root_dir)
        self._package_versions = {}  # Cache for package versions
        self._external_dependencies = set()

    def get_version_dict(self) -> dict[str, str]:
        """
        Get a dictionary mapping package names to their versions.

        Returns:
            dict[str, str]: Dictionary mapping package names to version strings
        """
        versions = {}
        for package in sorted(self._external_dependencies):
            version = self._get_package_version(package)
            if version:
                versions[package] = version
        return versions

    def get_version(self, params: VersionParameters) -> str:
        if params.func is None:
            raise ValueError("Function-based cache requires a function parameter")

        # Get all dependencies including nested function calls
        _ = self._get_function_dependencies(params.func, set())

        # Get package versions and create version string
        versions = self.get_version_dict()
        version_components = [f"{pkg}=={ver}" for pkg, ver in versions.items()]

        # Combine package versions with salt
        combined_data = "|".join(version_components).encode("utf-8") + self.salt.encode("utf-8")
        return hashlib.sha256(combined_data).hexdigest()

    def _is_user_defined(self, obj: Any) -> bool:
        """
        Similar to the parent, this method checks if a callable or class is user-defined within the package.
        If it identifies a non-user-defined package, it adds the external dependency to a list of packages
        for which we will check their versions and hash.
        """
        if isinstance(obj, type(sys)):  # Check if the object is a module
            module_name = obj.__name__
        else:
            module_name = getattr(obj, "__module__", None)
        if not module_name:
            return False

        # Retrieve the module specification to get its path
        with temporarily_add_to_syspath(self.root_dir):
            spec = importlib.util.find_spec(module_name)
            if not spec or not spec.origin:
                return False

            module_path = Path(spec.origin).resolve()

            site_packages_paths = {Path(p).resolve() for p in sys.path if "site-packages" in p}
            is_in_site_packages = any(sp in module_path.parents for sp in site_packages_paths)

            # If it's in site-packages, add the module name to external dependencies
            if is_in_site_packages:
                root_package = module_name.split(".")[0]
                self._external_dependencies.add(root_package)

            # Check if the module is within the root directory but not in site-packages
            if self.root_dir in module_path.parents:
                # Exclude standard library or site-packages by checking common paths but return True if within root_dir but not in site-packages
                return not is_in_site_packages

            return False

    def _get_package_version(self, package_name: str) -> str:
        """
        Get the version of an installed package.

        Args:
            package_name: Name of the package

        Returns:
            str: Version string of the package or "unknown" if version cannot be determined
        """
        if package_name in self._package_versions:
            return self._package_versions[package_name]

        version: Optional[str] = None
        try:
            # Try importlib.metadata first (most reliable)
            version = importlib.metadata.version(package_name)
        except Exception as e:
            click.secho(f"Could not get version for {package_name} using importlib.metadata: {str(e)}", fg="yellow")
            try:
                # Fall back to checking package attributes
                package = importlib.import_module(package_name)
                version = getattr(package, "__version__", None)
                if not version:
                    version = getattr(package, "version", None)
                click.secho(f"Found by {package_name} importing module.", fg="yellow")
            except ImportError as e:
                click.secho(f"Could not import {package_name}: {str(e)}", fg="yellow")

        if not version:
            click.secho(
                f"Could not determine version for package {package_name}. " "This may affect cache invalidation.",
                fg="yellow",
            )
            version = "unknown"

        self._package_versions[package_name] = version
        return version
