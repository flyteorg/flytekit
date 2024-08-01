import ast
import inspect
import typing


def get_function_param_location(func: typing.Callable, param_name: str) -> (int, int):
    """
    Get the line and column number of the parameter in the source code of the function definition.
    """
    # Get source code of the function
    source_lines, start_line = inspect.getsourcelines(func)
    source_code = "".join(source_lines)

    # Parse the source code into an AST
    module = ast.parse(source_code)

    # Traverse the AST to find the function definition
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == func.__name__:
            for i, arg in enumerate(node.args.args):
                if arg.arg == param_name:
                    # Calculate the line and column number of the parameter
                    line_number = start_line + node.lineno - 1
                    column_offset = arg.col_offset
                    return line_number, column_offset
