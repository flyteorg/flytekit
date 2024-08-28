import inspect
import typing

from flytekit._ast.parser import get_function_param_location
from flytekit.core.constants import SOURCE_CODE
from flytekit.exceptions.user import FlyteUserException


def get_source_code_from_fn(fn: typing.Callable, param_name: typing.Optional[str] = None) -> (str, int):
    """
    Get the source code of the function and the column offset of the parameter defined in the input signature.
    """
    lines, start_line = inspect.getsourcelines(fn)
    if param_name is None:
        return "".join(f"{start_line + i} {lines[i]}" for i in range(len(lines))), 0

    target_line_no, column_offset = get_function_param_location(fn, param_name)
    line_index = target_line_no - start_line
    source_code = "".join(f"{start_line + i} {lines[i]}" for i in range(line_index + 1))
    return source_code, column_offset


def annotate_exception_with_code(
    exception: FlyteUserException, fn: typing.Callable, param_name: typing.Optional[str] = None
) -> FlyteUserException:
    """
    Annotate the exception with the source code, and will be printed in the rich panel.
    :param exception: The exception to be annotated.
    :param fn: The function where the parameter is defined.
    :param param_name: The name of the parameter in the function signature.

    For example:
        exception: TypeError, 'a' has no type. Please add a type annotation to the input parameter.
        param_name: a, the position that arrow will point to.
        fn: <function wf at 0x1065227a0>

    ╭─ TypeError ────────────────────────────────────────────────────────────────────────────────────╮
    │ 23 @workflow(on_failure=t2)                                                                    │                                                                                                    │
    │ 24 def wf(b: int = 3, a=4):                                                                    │
    │                     # ^ 'a' has no type. Please add a type annotation to the input parameter.  │
    ╰────────────────────────────────────────────────────────────────────────────────────────────────╯
    """
    try:
        source_code, column_offset = get_source_code_from_fn(fn, param_name)
        exception.__setattr__(SOURCE_CODE, f"{source_code}{' '*column_offset} # ^ {str(exception)}")
    except Exception as e:
        from flytekit.loggers import logger

        logger.error(f"Failed to annotate exception with source code: {e}")
    finally:
        return exception
