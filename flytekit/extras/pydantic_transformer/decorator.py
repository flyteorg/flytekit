import logging
from typing import Any, Callable, TypeVar, Union

logger = logging.getLogger(__name__)

try:
    # isolate the exception to the pydantic import
    # model_validator and model_serializer are only available in pydantic > 2
    from pydantic import model_serializer, model_validator

except ImportError:
    """
    It's to support the case where pydantic is not installed at all.
    It looks nicer in the real Flyte File/Directory class, but we also want it to not fail.
    """

    logger.debug(
        "Pydantic is not installed.\n" "Please install Pydantic version > 2 to use FlyteTypes in pydantic BaseModel."
    )

    FuncType = TypeVar("FuncType", bound=Callable[..., Any])

    from typing_extensions import Literal as typing_literal

    def model_serializer(
        __f: Union[Callable[..., Any], None] = None,
        *,
        mode: typing_literal["plain", "wrap"] = "plain",
        when_used: typing_literal["always", "unless-none", "json", "json-unless-none"] = "always",
        return_type: Any = None,
    ) -> Callable[[Any], Any]:
        """Placeholder decorator for Pydantic model_serializer."""

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            def wrapper(*args, **kwargs):
                raise Exception(
                    "Pydantic is not installed.\n" "Please install Pydantic version > 2 to use this feature."
                )

            return wrapper

        # If no function (__f) is provided, return the decorator
        if __f is None:
            return decorator
        # If __f is provided, directly decorate the function
        return decorator(__f)

    def model_validator(
        *,
        mode: typing_literal["wrap", "before", "after"],
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Placeholder decorator for Pydantic model_validator."""

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            def wrapper(*args, **kwargs):
                raise Exception(
                    "Pydantic is not installed.\n" "Please install Pydantic version > 2 to use this feature."
                )

            return wrapper

        return decorator
