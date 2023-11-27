import importlib
import os
import sys

from flytekit.core import utils
from flytekit.core.context_manager import FlyteContextManager
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekit.core.type_engine import TypeEngine
from flytekit.models import literals as _literal_models


def load_module_from_path(module_name, path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is not None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    else:
        raise ImportError(f"Module at {path} could not be loaded")


def get_interactive_debugging_inputs(task_module_name, task_name, working_dir):
    local_inputs_file = os.path.join(working_dir, "inputs.pb")
    input_proto = utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
    idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)
    task_module = load_module_from_path(task_module_name, os.path.join(working_dir, f"{task_module_name}.py"))
    task_def = getattr(task_module, task_name)
    native_inputs = TypeEngine.literal_map_to_kwargs(
        FlyteContextManager(), idl_input_literals, task_def.python_interface.inputs
    )
    return native_inputs
