import os
import typing

from flyteidl.core import literals_pb2, tasks_pb2

from flytekit import FlyteContextManager, logger
from flytekit.core import utils
from flytekit.core.utils import load_proto_from_file
from flytekit.models import literals, task
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


def get_task_template(task_template_path: str) -> TaskTemplate:
    ctx = FlyteContextManager.current_context()
    task_template_local_path = os.path.join(ctx.execution_state.working_dir, "task_template.pb")
    ctx.file_access.get_data(task_template_path, task_template_local_path)
    task_template_proto = load_proto_from_file(tasks_pb2.TaskTemplate, task_template_local_path)
    task_template_model = task.TaskTemplate.from_flyte_idl(task_template_proto)
    print(f"Task Template: {task_template_model}")  # For debug, will remove it
    return task_template_model


def get_task_inputs(inputs_path: str) -> LiteralMap:
    ctx = FlyteContextManager.current_context()
    task_inputs_local_path = os.path.join(ctx.execution_state.working_dir, "inputs.pb")
    ctx.file_access.get_data(inputs_path, task_inputs_local_path)
    input_proto = utils.load_proto_from_file(literals_pb2.LiteralMap, task_inputs_local_path)
    idl_input_literals = literals.LiteralMap.from_flyte_idl(input_proto)
    logger.debug(f"Task inputs: {idl_input_literals}")
    return idl_input_literals


def upload_output_file(output_file_dict: typing.Dict, output_prefix: str):
    ctx = FlyteContextManager.current_context()
    for k, v in output_file_dict.items():
        utils.write_proto_to_file(v.to_flyte_idl(), os.path.join(ctx.execution_state.engine_dir, k))
    ctx.file_access.put_data(ctx.execution_state.engine_dir, output_prefix, is_multipart=True)
