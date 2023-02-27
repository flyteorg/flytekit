import os
import typing

from flytekit import FlyteContextManager
from flytekit.core import utils


def upload_output_file(output_file_dict: typing.Dict, output_prefix: str):
    ctx = FlyteContextManager.current_context()
    for k, v in output_file_dict.items():
        utils.write_proto_to_file(v.to_flyte_idl(), os.path.join(ctx.execution_state.engine_dir, k))
    ctx.file_access.put_data(ctx.execution_state.engine_dir, output_prefix, is_multipart=True)
