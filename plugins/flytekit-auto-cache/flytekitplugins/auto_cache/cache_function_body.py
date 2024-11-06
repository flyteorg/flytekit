import ast
import hashlib
import inspect
from typing import Any, Callable


class CacheFunctionBody:
    """
    A class that implements a versioning mechanism for functions by generating
    a SHA-256 hash of the function's source code combined with a salt.

    Attributes:
        salt (str): A string used to add uniqueness to the generated hash. Default is "salt".

    Methods:
        get_version(func: Callable[..., Any]) -> str:
            Given a function, generates a version hash based on its source code and the salt.
    """

    def __init__(self, salt: str = "salt") -> None:
        """
        Initialize the CacheFunctionBody instance with a salt value.

        Args:
            salt (str): A string to be used as the salt in the hashing process. Defaults to "salt".
        """
        self.salt = salt

    def get_version(self, func: Callable[..., Any]) -> str:
        """
        Generate a version hash for the provided function by parsing its source code
        and adding a salt before applying the SHA-256 hash function.

        Args:
            func (Callable[..., Any]): A callable function whose version hash needs to be generated.

        Returns:
            str: The SHA-256 hash of the function's source code combined with the salt.
        """
        # Get the source code of the function
        source = inspect.getsource(func)

        # Parse the source code into an Abstract Syntax Tree (AST)
        parsed_ast = ast.parse(source)

        # Convert the AST into a string representation (dump it)
        ast_bytes = ast.dump(parsed_ast).encode("utf-8")

        # Combine the AST bytes with the salt (encoded into bytes)
        combined_data = ast_bytes + self.salt.encode("utf-8")

        # Return the SHA-256 hash of the combined data (AST + salt)
        return hashlib.sha256(combined_data).hexdigest()
