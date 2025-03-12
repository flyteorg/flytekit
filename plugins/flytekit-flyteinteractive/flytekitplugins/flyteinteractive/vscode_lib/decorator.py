# This file has been moved to flytekit.interactive.vscode_lib.decorator
# Import flytekit.interactive module to keep backwards compatibility
from flytekit.interactive.vscode_lib.decorator import (  # noqa: F401
    VSCODE_TYPE_VALUE,
    download_file,
    download_vscode,
    exit_handler,
    get_code_server_info,
    get_installed_extensions,
    is_extension_installed,
    prepare_interactive_python,
    prepare_launch_json,
    prepare_resume_task_python,
    vscode,
)
