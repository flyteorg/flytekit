import ast
import hashlib
import importlib.util
import inspect
import sys
import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Set, Union

import click
from flytekitplugins.auto_cache.cache_function_body import CacheFunctionBody

from flytekit.core.auto_cache import VersionParameters


@contextmanager
def temporarily_add_to_syspath(path):
    """Temporarily add the given path to sys.path."""
    sys.path.insert(0, str(path))
    try:
        yield
    finally:
        sys.path.pop(0)


class CachePrivateModules(CacheFunctionBody):
    """
    A class that extends CacheFunctionBody to cache private modules and their dependencies.

    It extends the functionality to recursively follow all callables, making a list of all functions,
    classes, and methods that are eventually used by the initial function of interest. Only functions
    internal to this package are included, while externally imported packages are ignored. It handles
    both import and from-import statements, as well as aliases, for both top-level imports global to
    the module and local imports to the function or method. Additionally, it identifies constants at
    the same import levels described above, accounting for both constants defined in the internal
    package and external packages. The contents of all these functions are hashed using the same
    logic as CacheFunctionBody, and the constants and their values are also fed into the hash.

    Attributes:
        salt (str): A string used to add uniqueness to the generated hash.
        root_dir (Path): The root directory of the project, used to resolve module paths.
        constants (dict): A dictionary to store constants that are part of the versioning process.
    """

    def __init__(self, root_dir: str, salt: str = ""):
        """
        Initialize the CachePrivateModules instance with a salt value and a root directory.

        Args:
            salt (str): A string to be used as the salt in the hashing process.
            root_dir (str): The root directory of the project, used to resolve module paths.
        """
        self.salt = salt
        self.root_dir = Path(root_dir).resolve()
        self.constants = {}

    def get_version(self, params: VersionParameters) -> str:
        """
        Generates a version hash for the provided function and its dependencies.

        This method recursively identifies all callables (functions, methods, classes) used by the provided function,
        hashes their contents, and combines these hashes with the hashes of all constants and their values identified.
        The resulting combined hash is then returned as the version string. The user provided salt is by `_get_version`
        of the parent CacheFunctionBody.

        Args:
            params (VersionParameters): An object containing the function parameter.

        Returns:
            str: The SHA-256 hash of the combined hashes of the function, its dependencies, and constants.

        Raises:
            ValueError: If the function parameter is None.
        """
        if params.func is None:
            raise ValueError("Function-based cache requires a function parameter")

        # Initialize a list to hold all hash components
        hash_components = [self._get_version(params.func)]  # Start with the hash of the provided function
        # Identify all dependencies of the provided function
        dependencies = self._get_function_dependencies(params.func, set())
        # Hash each dependency and add to the list of hash components
        for dep in dependencies:
            hash_components.append(self._get_version(dep))
        # Add hashes of constants and their values to the list of hash components
        for key, value in self.constants.items():
            hash_components.append(f"{key}={value}")
        # Combine all component hashes into a single version hash
        combined_hash = hashlib.sha256("".join(hash_components).encode("utf-8")).hexdigest()
        return combined_hash

    def _get_alias_name(self, alias: ast.alias) -> str:
        """
        Extracts the alias name from an AST alias node.

        This method takes an AST alias node and returns its alias name. If the alias has an 'asname', it returns the 'asname';
        otherwise, it returns the 'name' of the alias.

        Args:
            alias (ast.alias): The AST alias node from which to extract the alias name.

        Returns:
            str: The alias name or the name of the alias if 'asname' is not provided.
        """
        return alias.asname or alias.name

    def _get_function_dependencies(
        self, func: Callable[..., Any], visited: Set[str], class_attributes: dict = None
    ) -> Set[Callable[..., Any]]:
        """
        Recursively identifies all functions, methods, and classes used within `func` and defined in the user’s package.

        This method traverses the Abstract Syntax Tree (AST) of the given function to identify all imported modules,
        functions, methods, and classes. It then inspects each function call to identify the dependencies. Additionally,
        the method extracts literal constants from the function's global namespace and imported modules.

        Args:
            func (Callable[..., Any]): The function for which to gather dependencies.
            visited (Set[str]): A set to keep track of visited functions to avoid infinite recursion.
            class_attributes (dict, optional): A dictionary of attributes if the func is a method from a class.

        Returns:
            Set[Callable[..., Any]]: A set of all dependencies found.
        """

        dependencies = set()
        source = textwrap.dedent(inspect.getsource(func))
        parsed_ast = ast.parse(source)

        # Initialize a dictionary to mimic the function's global namespace for locally defined imports
        locals_dict = {}
        # Initialize a dictionary to hold constant imports and class attributes
        constant_imports = {}
        # If class attributes are provided, include them in the constant imports
        if class_attributes:
            constant_imports.update(class_attributes)

        # Check each function call in the AST
        for node in ast.walk(parsed_ast):
            if isinstance(node, ast.Import):
                # For each alias in the import statement, we import the module and add it to the locals_dict.
                # This is because the module itself is being imported, not a specific attribute or function.
                for alias in node.names:
                    module = importlib.import_module(alias.name)
                    locals_dict[self._get_alias_name(alias)] = module
                    # We then get all the literal constants defined in the module's __init__.py file.
                    # These constants are later checked for usage within the function.
                    module_constants = self.get_module_literal_constants(module)
                    constant_imports.update(
                        {f"{self._get_alias_name(alias)}.{name}": value for name, value in module_constants.items()}
                    )
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module
                module = importlib.import_module(module_name)
                for alias in node.names:
                    # Attempt to resolve the imported object directly from the module
                    imported_obj = getattr(module, alias.name, None)
                    if imported_obj:
                        # If the object is found directly in the module, add it to the locals_dict
                        locals_dict[self._get_alias_name(alias)] = imported_obj
                        # Check if the imported object is a literal constant and add it to constant_imports if so
                        if self.is_literal_constant(imported_obj):
                            constant_imports.update({f"{self._get_alias_name(alias)}": imported_obj})
                    else:
                        # If the object is not found directly in the module, attempt to import it as a submodule
                        # This is necessary for cases like `from PIL import Image`, where Image is not imported in PIL's __init__.py
                        # PIL and similar packages use different mechanisms to expose their objects, requiring this fallback approach
                        submodule = importlib.import_module(f"{module_name}.{alias.name}")
                        imported_obj = getattr(submodule, alias.name, None)
                        locals_dict[self._get_alias_name(alias)] = imported_obj

            elif isinstance(node, ast.Call):
                # Add callable to the set of dependencies if it's user defined and continue the recursive search within those callables.
                func_name = self._get_callable_name(node.func)
                if func_name and func_name not in visited:
                    visited.add(func_name)
                    try:
                        # Attempt to resolve the callable object using locals first, then globals
                        func_obj = self._resolve_callable(func_name, locals_dict) or self._resolve_callable(
                            func_name, func.__globals__
                        )
                        # If the callable is a class and user-defined, we add and search all method. We also include attributes as potential constants.
                        if inspect.isclass(func_obj) and self._is_user_defined(func_obj):
                            current_class_attributes = {
                                f"class.{func_name}.{name}": value for name, value in func_obj.__dict__.items()
                            }
                            for name, method in inspect.getmembers(func_obj, predicate=inspect.isfunction):
                                if method not in visited:
                                    visited.add(method.__qualname__)
                                    dependencies.add(method)
                                    dependencies.update(
                                        self._get_function_dependencies(method, visited, current_class_attributes)
                                    )
                        # If the callable is a function or method and user-defined, add it as a dependency and search its dependencies
                        elif (inspect.isfunction(func_obj) or inspect.ismethod(func_obj)) and self._is_user_defined(
                            func_obj
                        ):
                            # Add the function or method as a dependency
                            dependencies.add(func_obj)
                            # Recursively search the function or method's dependencies
                            dependencies.update(self._get_function_dependencies(func_obj, visited))
                    except (NameError, AttributeError) as e:
                        click.secho(f"Could not process the callable {func_name} due to error: {str(e)}", fg="yellow")

        # Extract potential constants from the global import context
        global_constants = {}
        for key, value in func.__globals__.items():
            if hasattr(value, "__dict__"):
                module_constants = self.get_module_literal_constants(value)
                global_constants.update({f"{key}.{name}": value for name, value in module_constants.items()})
            elif self.is_literal_constant(value):
                global_constants[key] = value

        # Check for the usage of all potnential constants and update the set of constants to be hashed
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
        Identifies constants that are actually used within the given function.

        Args:
            func: The function to be analyzed for constant references.
            constant_imports: A dictionary containing potential constant imports.
            global_constants: A dictionary of global constants.

        Returns:
            A dictionary containing the constants that are actually referenced within the function.
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

    def _resolve_callable(self, func_name: str, locals_globals_dict: dict) -> Callable[..., Any]:
        """Resolve a callable from its name within the given locals/globals dictionary, handling modules as entry points."""
        parts = func_name.split(".")

        # First, try resolving directly from globals_dict for a straightforward reference
        obj = locals_globals_dict.get(parts[0], None)
        for part in parts[1:]:
            if obj is None:
                break
            obj = getattr(obj, part, None)

        # If not found, iterate through modules in globals_dict and attempt resolution from them
        if not callable(obj):
            for module in locals_globals_dict.values():
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
