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
        self.constants = {}

    def get_version(self, params: VersionParameters) -> str:
        if params.func is None:
            raise ValueError("Function-based cache requires a function parameter")

        hash_components = [self._get_version(params.func)]
        dependencies = self._get_function_dependencies(params.func, set())
        for dep in dependencies:
            hash_components.append(self._get_version(dep))
        for key, value in self.constants.items():
            hash_components.append(f"{key}={value}")
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

    def _get_function_dependencies(
        self, func: Callable[..., Any], visited: Set[str], class_attributes: dict = None
    ) -> Set[Callable[..., Any]]:
        """
        Recursively gather all functions, methods, and classes used within `func` and defined in the user’s package.

        This method walks through the Abstract Syntax Tree (AST) of the given function to identify all imported modules,
        functions, methods, and classes. It then checks each function call to identify the dependencies. The method
        also extracts literal constants from the function's global namespace and imported modules.

        Parameters:
        - func: The function for which to gather dependencies.
        - visited: A set to keep track of visited functions to avoid infinite recursion.
        - class_attributes: A dictionary of attributes if the func is a method from a class.

        Returns:
        - A set of all dependencies found.
        """

        dependencies = set()
        # Dedent the source code to handle class method indentation
        source = textwrap.dedent(inspect.getsource(func))
        parsed_ast = ast.parse(source)

        # Build a locals dictionary for function-level imports
        locals_dict = {}
        constant_imports = {}
        if class_attributes:
            constant_imports.update(class_attributes)

        for node in ast.walk(parsed_ast):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module = importlib.import_module(alias.name)
                    locals_dict[alias.asname or alias.name] = module
                    module_constants = self.get_module_literal_constants(module)
                    constant_imports.update(
                        {f"{alias.asname or alias.name}.{name}": value for name, value in module_constants.items()}
                    )
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module
                module = importlib.import_module(module_name)
                for alias in node.names:
                    # Resolve attributes or submodules
                    imported_obj = getattr(module, alias.name, None)
                    if imported_obj:
                        locals_dict[alias.asname or alias.name] = imported_obj
                    else:
                        # Fallback: attempt to import as submodule. e.g. `from PIL import Image`
                        submodule = importlib.import_module(f"{module_name}.{alias.name}")
                        locals_dict[alias.asname or alias.name] = submodule
                        # If it's a module, find its constants
                        if inspect.ismodule(imported_obj):
                            module_constants = self.get_module_literal_constants(imported_obj)
                            constant_imports.update(
                                {
                                    f"{alias.asname or alias.name}.{name}": value
                                    for name, value in module_constants.items()
                                }
                            )
                        # If the import itself is a constant, add it
                        elif self.is_literal_constant(imported_obj):
                            constant_imports.update({f"{module_name}.{alias.asname or alias.name}": imported_obj})

        global_constants = {key: value for key, value in func.__globals__.items() if self.is_literal_constant(value)}

        # Check each function call in the AST
        for node in ast.walk(parsed_ast):
            if isinstance(node, ast.Call):
                func_name = self._get_callable_name(node.func)
                if func_name and func_name not in visited:
                    visited.add(func_name)
                    try:
                        # Attempt to resolve using locals first, then globals
                        func_obj = self._resolve_callable(func_name, locals_dict) or self._resolve_callable(
                            func_name, func.__globals__
                        )
                        if inspect.isclass(func_obj) and self._is_user_defined(func_obj):
                            # Add class attributes as potential constants
                            current_class_attributes = {
                                f"class.{func_name}.{name}": value for name, value in func_obj.__dict__.items()
                            }
                            # Add class methods as dependencies
                            for name, method in inspect.getmembers(func_obj, predicate=inspect.isfunction):
                                if method not in visited:
                                    visited.add(method.__qualname__)
                                    dependencies.add(method)
                                    dependencies.update(
                                        self._get_function_dependencies(method, visited, current_class_attributes)
                                    )
                        elif (inspect.isfunction(func_obj) or inspect.ismethod(func_obj)) and self._is_user_defined(
                            func_obj
                        ):
                            dependencies.add(func_obj)
                            dependencies.update(self._get_function_dependencies(func_obj, visited))
                    except (NameError, AttributeError):
                        pass

        referenced_constants = self.get_referenced_constants(
            func=func, constant_imports=constant_imports, global_constants=global_constants
        )
        self.constants.update(referenced_constants)

        return dependencies

    def is_literal_constant(self, value):
        """
        Check if a value is a literal constant

        Supports basic immutable types and nested structures of those types
        """
        # Basic immutable types
        literal_types = (int, float, str, bool, type(None), complex, tuple, frozenset)

        # Recursively check for literals
        def _is_literal(val):
            # Direct type check
            if isinstance(val, literal_types):
                return True

            # Check nested structures
            if isinstance(val, (tuple, list, frozenset)):
                return all(_is_literal(item) for item in val)

            return False

        return _is_literal(value)

    def get_module_literal_constants(self, module):
        """
        Find all literal constants in a module

        Uses module's __dict__ to find uppercase attributes that are literals
        """
        constants = {}
        for name, value in module.__dict__.items():
            # Check for uppercase name (convention for constants)
            # and verify it's a literal
            if self.is_literal_constant(value):
                constants[name] = value
        return constants

    def get_referenced_constants(self, func, constant_imports=None, global_constants=None):
        """
        Find constants that are actually referenced in the function

        :param func: The function to analyze
        :param constant_imports: Dictionary of potential constant imports
        :param global_constants: Dictionary of global constants
        :return: Dictionary of referenced constants
        """
        referenced_constants = {}
        source_code = inspect.getsource(func)
        dedented_source = textwrap.dedent(source_code)

        # Check imported constants
        if constant_imports:
            for name, value in constant_imports.items():
                name_to_search, final_name = name, name
                name_parts = name.split(".")
                # If the constant is a class attribute, use the class name in the final constant name but search for "self.<attr-name>"
                if len(name_parts) == 3 and name_parts[0] == "class":
                    name_to_search = f"self.{name_parts[2]}"
                    final_name = f"{name_parts[1]}.{name_parts[2]}"
                if name_to_search in dedented_source:
                    referenced_constants[final_name] = str(value)

        # Check global constants
        if global_constants:
            for name, value in global_constants.items():
                if name in dedented_source:
                    referenced_constants[name] = str(value)

        return referenced_constants

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

            # Check if the module is within the root directory but not in site-packages
            if self.root_dir in module_path.parents:
                # Exclude standard library or site-packages by checking common paths
                site_packages_paths = {Path(p).resolve() for p in sys.path if "site-packages" in p}
                is_in_site_packages = any(sp in module_path.parents for sp in site_packages_paths)

                # Return True if within root_dir but not in site-packages
                return not is_in_site_packages

            return False
