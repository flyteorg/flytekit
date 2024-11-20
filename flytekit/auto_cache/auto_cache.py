from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import os
import site
from dataclasses import dataclass
from typing import Optional

import pkg_resources

from flytekit.image_spec import ImageSpec

# Path to the root of the current repo

# Get paths to all site-packages or dist-packages directories
site_package_paths = set(site.getsitepackages() + [site.getusersitepackages()])

# A cache to store already visited modules to prevent cycles
visited_modules = set()


@dataclass
class AutoCacheConfig:
    check_task: bool = False
    check_packages: bool = False
    check_modules: bool = False
    image_spec: Optional[ImageSpec] = None


def find_repo_root_by_init(path):
    current_dir = os.path.dirname(os.path.abspath(path))

    while current_dir != os.path.dirname(current_dir):
        # Check if __init__.py exists in the current directory
        init_file = os.path.join(current_dir, "__init__.py")
        if not os.path.exists(init_file):
            return current_dir
        current_dir = os.path.dirname(current_dir)

    return None


class AutoCache:
    def __init__(self, config: AutoCacheConfig):
        self.detected_root = None
        self.config = config

    def hash_task(self, func):
        self.detected_root = find_repo_root_by_init(func.__code__.co_filename)

        function_hash = ""

        if self.config.check_task:
            # Hash the AST of the main function
            function_hash = self.combine_hashes(function_hash, self.hash_ast(func))

        if self.config.image_spec:
            function_hash = self.combine_hashes(function_hash, self.config.image_spec.tag)

        # Find all imported modules and their source code
        imports = self.find_imports(func)
        for imported_func in imports:
            try:
                # Recursively hash internal modules, hash version of external packages
                module = inspect.getmodule(imported_func)
                if module is not None:
                    module_hash = ""
                    if self.is_internal_module(module) and self.config.check_modules:
                        module_hash = self.hash_module_recursive(module)
                    elif self.config.check_packages:
                        module_hash = self.hash_external_package_version(module)

                    function_hash = self.combine_hashes(function_hash, module_hash)
            except (TypeError, OSError):
                # Handle cases where the source code is not available (e.g., compiled extensions)
                pass

        return function_hash

    def is_internal_module(self, module):
        """Checks if the module is part of the current repo or an external package."""
        module_path = os.path.abspath(inspect.getfile(module))

        # Check if the module is within any of the site-packages directories
        if any(module_path.startswith(site_package_path) for site_package_path in site_package_paths):
            return False  # It's an external package

        return module_path.startswith(self.detected_root)

    def hash_module_recursive(self, module):
        """Recursively hash the module and its dependencies if it's internal."""
        if module.__name__ in visited_modules:
            return ""  # Skip if already visited

        visited_modules.add(module.__name__)

        try:
            # Get the source of the module
            source = inspect.getsource(module)
            module_hash = hashlib.sha256(source.encode("utf-8")).hexdigest()

            # Find imports in the module and hash them recursively
            module_imports = self.find_module_imports(module)
            for sub_module in module_imports:
                sub_module_hash = ""
                if self.is_internal_module(sub_module) and self.config.check_modules:
                    sub_module_hash = self.hash_module_recursive(sub_module)
                elif self.config.check_packages:
                    sub_module_hash = self.hash_external_package_version(sub_module)

                module_hash = self.combine_hashes(module_hash, sub_module_hash)

            return module_hash
        except (TypeError, OSError):
            return ""

    def hash_external_package_version(self, module):
        """Hashes the version of an external package."""
        try:
            package_name = module.__name__.split(".")[0]
            version = pkg_resources.get_distribution(package_name).version
            package_name_version = package_name + version
            return hashlib.sha256(package_name_version.encode("utf-8")).hexdigest()
        except pkg_resources.DistributionNotFound:
            return ""  # If we can't find the version (e.g., built-in libraries), return empty hash

    def combine_hashes(self, existing_hash, new_hash):
        """Combine existing hash with new hash and return a new hash."""
        combined_hash = hashlib.sha256()
        combined_hash.update(existing_hash.encode("utf-8"))
        combined_hash.update(new_hash.encode("utf-8"))
        return combined_hash.hexdigest()

    def find_module_imports(self, module):
        """Finds all the modules that are imported within the given module."""
        imported_modules = []
        try:
            # Get the source code of the module
            source = inspect.getsource(module)
            tree = ast.parse(source)

            class ModuleImportVisitor(ast.NodeVisitor):
                def visit_Import(self, node):
                    for alias in node.names:
                        imported_module = importlib.import_module(alias.name)
                        imported_modules.append(imported_module)

                def visit_ImportFrom(self, node):
                    imported_module = importlib.import_module(node.module)
                    imported_modules.append(imported_module)

            visitor = ModuleImportVisitor()
            visitor.visit(tree)
        except Exception:
            pass

        return imported_modules

    def find_imports(self, func):
        source = inspect.getsource(func)
        tree = ast.parse(source)
        imported_funcs = []

        class ImportVisitor(ast.NodeVisitor):
            def __init__(self):
                self.imports = {}

            def visit_Import(self, node):
                for alias in node.names:
                    module = importlib.import_module(alias.name)
                    self.imports[alias.asname or alias.name] = module

            def visit_ImportFrom(self, node):
                module = importlib.import_module(node.module)
                for alias in node.names:
                    obj = getattr(module, alias.name)
                    self.imports[alias.asname or alias.name] = obj

            def visit_Attribute(self, node):
                if isinstance(node.value, ast.Name) and node.value.id in self.imports:
                    obj = getattr(self.imports[node.value.id], node.attr)
                    imported_funcs.append(obj)

            def visit_Call(self, node):
                # Handle direct function calls (like some_function())
                if isinstance(node.func, ast.Name) and node.func.id in self.imports:
                    imported_funcs.append(self.imports[node.func.id])
                self.generic_visit(node)  # Continue visiting other nodes

        visitor = ImportVisitor()
        visitor.visit(tree)

        return imported_funcs

    def hash_ast(self, func):
        source = inspect.getsource(func)
        parsed_ast = ast.parse(source)
        ast_bytes = ast.dump(parsed_ast).encode("utf-8")
        return hashlib.sha256(ast_bytes).hexdigest()
