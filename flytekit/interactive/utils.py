import http
import importlib
import os
import socketserver
import subprocess
import sys

from flyteidl.core import literals_pb2 as _literals_pb2

import flytekit
from flytekit.core import utils
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.interactive.constants import EXIT_CODE_SUCCESS
from flytekit.models import literals as _literal_models


def load_module_from_path(module_name, path):
    """
    Imports a Python module from a specified file path.

    Args:
        module_name (str): The name you want to assign to the imported module.
        path (str): The file system path to the Python file (.py) that contains the module you want to import.

    Returns:
        module: The imported module.

    Raises:
        ImportError: If the module cannot be loaded from the provided path, an ImportError is raised.
    """
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is not None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    else:
        raise ImportError(f"Module at {path} could not be loaded")


def get_task_inputs(task_module_name, task_name, context_working_dir):
    """
    Read task input data from inputs.pb for a specific task function and convert it into Python types and structures.

    Args:
        task_module_name (str): The name of the Python module containing the task function.
        task_name (str): The name of the task function within the module.
        context_working_dir (str): The directory path where the input file and module file are located.

    Returns:
        dict: A dictionary containing the task inputs, converted into Python types and structures.
    """
    local_inputs_file = os.path.join(context_working_dir, "inputs.pb")
    input_proto = utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
    idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)

    task_module = load_module_from_path(task_module_name, os.path.join(context_working_dir, f"{task_module_name}.py"))
    task_def = getattr(task_module, task_name)
    native_inputs = TypeEngine.literal_map_to_kwargs(
        FlyteContextManager().current_context(),
        idl_input_literals,
        task_def.python_interface.inputs,
    )
    return native_inputs


def execute_command(cmd):
    """
    Execute a command in the shell.
    """

    logger = flytekit.current_context().logging

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info(f"cmd: {cmd}")
    stdout, stderr = process.communicate()
    if process.returncode != EXIT_CODE_SUCCESS:
        raise RuntimeError(f"Command {cmd} failed with error: {stderr}")
    logger.info(f"stdout: {stdout}")
    logger.info(f"stderr: {stderr}")


def run_dummy_server():
    PORT = 8080
    HTML_CONTENT = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Loading VSCode Server</title>
        <style>
            body { 
                font-family: Arial, sans-serif; 
                display: flex; 
                justify-content: center; 
                align-items: center; 
                height: 100vh;
                background-color: #f3f4f6;
                margin: 0;
            }
            .message {
                text-align: center;
                font-size: 24px;
                color: #333;
            }
        </style>
    </head>
    <body>
        <div class="message">
            Loading VSCode Server...
        </div>
    </body>
    </html>
    """

    class SimpleHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(HTML_CONTENT.encode('utf-8'))

    with socketserver.TCPServer(("", PORT), SimpleHTTPRequestHandler) as httpd:
        def signal_handler(sig, frame):
            print('Terminating server...')
            httpd.shutdown()  # Gracefully shutdown the server
            sys.exit(0)

        import signal

        signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Handle termination signal
        print(f"Serving on port {PORT}")
        httpd.serve_forever()
