from typing import Any, Callable, Protocol, runtime_checkable


@runtime_checkable
class AutoCache(Protocol):
    """
    A protocol that defines the interface for a caching mechanism
    that generates a version hash of a function based on its source code.

    Attributes:
        salt (str): A string used to add uniqueness to the generated hash. Default is "salt".

    Methods:
        get_version(func: Callable[..., Any]) -> str:
            Given a function, generates a version hash based on its source code and the salt.
    """

    def __init__(self, salt: str = "salt") -> None:
        """
        Initialize the AutoCache instance with a salt value.

        Args:
            salt (str): A string to be used as the salt in the hashing process. Defaults to "salt".
        """
        self.salt = salt

    def get_version(self, func: Callable[..., Any]) -> str:
        """
        Generate a version hash for the provided function.

        Args:
            func (Callable[..., Any]): A callable function whose version hash needs to be generated.

        Returns:
            str: The SHA-256 hash of the function's source code combined with the salt.
        """
        ...
