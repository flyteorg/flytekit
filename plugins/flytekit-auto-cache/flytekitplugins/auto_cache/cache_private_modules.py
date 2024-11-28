import ast
import hashlib
import importlib.util
import inspect
import sys
import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Set, Union

from flytekit.core.auto_cache import VersionParameters


@contextmanager
def temporarily_add_to_syspath(path):
    """Temporarily add the given path to sys.path."""
    sys.path.insert(0, str(path))
    try:
        yield
    finally:
        sys.path.pop(0)


class CachePrivateModules:
    def __init__(self, salt: str, root_dir: str):
        self.salt = salt
        self.root_dir = Path(root_dir).resolve()

    def get_version(self, params: VersionParameters) -> str:
        if params.func is None:
            raise ValueError("Function-based cache requires a function parameter")

        hash_components = [self._get_version(params.func)]
        dependencies = self._get_function_dependencies(params.func, set())
        for dep in dependencies:
            hash_components.append(self._get_version(dep))
        # Combine all component hashes into a single version hash
        combined_hash = hashlib.sha256("".join(hash_components).encode("utf-8")).hexdigest()
        return combined_hash

    def _get_version(self, func: Callable[..., Any]) -> str:
        source = inspect.getsource(func)
        dedented_source = textwrap.dedent(source)
        parsed_ast = ast.parse(dedented_source)
        ast_bytes = ast.dump(parsed_ast).encode("utf-8")
        combined_data = ast_bytes + self.salt.encode("utf-8")
        return hashlib.sha256(combined_data).hexdigest()

    def _get_function_dependencies(self, func: Callable[..., Any], visited: Set[str]) -> Set[Callable[..., Any]]:
        """Recursively gather all functions, methods, and classes used within `func` and defined in the user’s package."""
        dependencies = set()
        # Dedent the source code to handle class method indentation
        source = textwrap.dedent(inspect.getsource(func))
        parsed_ast = ast.parse(source)

        # Build a locals dictionary for function-level imports
        locals_dict = {}
        for node in ast.walk(parsed_ast):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module = importlib.import_module(alias.name)
                    locals_dict[alias.asname or alias.name] = module
            elif isinstance(node, ast.ImportFrom):
                module = importlib.import_module(node.module)
                for alias in node.names:
                    # Resolve attributes or submodules
                    imported_obj = getattr(module, alias.name, None)
                    if imported_obj:
                        locals_dict[alias.asname or alias.name] = imported_obj
                    else:
                        # Fallback: attempt to import as submodule. e.g. `from PIL import Image`
                        submodule = importlib.import_module(f"{node.module}.{alias.name}")
                        locals_dict[alias.asname or alias.name] = submodule

        # Check each function call in the AST
        for node in ast.walk(parsed_ast):
            if isinstance(node, ast.Call):
                func_name = self._get_callable_name(node.func)
                if func_name and func_name not in visited:
                    visited.add(func_name)
                    try:
                        # Attempt to resolve using locals first, then globals
                        # func_obj = locals_dict.get(func_name) or self._resolve_callable(func_name, func.__globals__)
                        func_obj = self._resolve_callable(func_name, locals_dict) or self._resolve_callable(
                            func_name, func.__globals__
                        )
                        if inspect.isclass(func_obj) and self._is_user_defined(func_obj):
                            # Add class methods as dependencies
                            for name, method in inspect.getmembers(func_obj, predicate=inspect.isfunction):
                                if method not in visited:
                                    visited.add(method.__qualname__)
                                    dependencies.add(method)
                                    dependencies.update(self._get_function_dependencies(method, visited))
                        elif (inspect.isfunction(func_obj) or inspect.ismethod(func_obj)) and self._is_user_defined(
                            func_obj
                        ):
                            dependencies.add(func_obj)
                            dependencies.update(self._get_function_dependencies(func_obj, visited))
                    except (NameError, AttributeError):
                        pass
        return dependencies

    def _get_callable_name(self, node: ast.AST) -> Union[str, None]:
        """Retrieve the name of the callable from an AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{node.value.id}.{node.attr}" if isinstance(node.value, ast.Name) else node.attr
        return None

    def _resolve_callable(self, func_name: str, globals_dict: dict) -> Callable[..., Any]:
        """Resolve a callable from its name within the given globals dictionary, handling modules as entry points."""
        parts = func_name.split(".")

        # First, try resolving directly from globals_dict for a straightforward reference
        obj = globals_dict.get(parts[0], None)
        for part in parts[1:]:
            if obj is None:
                break
            obj = getattr(obj, part, None)

        # If not found, iterate through modules in globals_dict and attempt resolution from them
        if not callable(obj):
            for module in globals_dict.values():
                if isinstance(module, type(sys)):  # Check if the global value is a module
                    obj = module
                    for part in parts:
                        obj = getattr(obj, part, None)
                        if obj is None:
                            break
                    if callable(obj):  # Exit if we successfully resolve the callable
                        break
                obj = None  # Reset if we didn't find the callable in this module

        # Return the callable if successfully resolved; otherwise, None
        return obj if callable(obj) else None

    def _is_user_defined(self, obj: Any) -> bool:
        """Check if a callable or class is user-defined within the package."""
        module_name = getattr(obj, "__module__", None)
        if not module_name:
            return False

        # Retrieve the module specification to get its path
        with temporarily_add_to_syspath(self.root_dir):
            spec = importlib.util.find_spec(module_name)
            if not spec or not spec.origin:
                return False

            module_path = Path(spec.origin).resolve()

            # Check if the module is within the root directory but not in site-packages
            if self.root_dir in module_path.parents:
                # Exclude standard library or site-packages by checking common paths
                site_packages_paths = {Path(p).resolve() for p in sys.path if "site-packages" in p}
                is_in_site_packages = any(sp in module_path.parents for sp in site_packages_paths)

                # Return True if within root_dir but not in site-packages
                return not is_in_site_packages

            return False
