import os

from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit import engine as _flyte_engine
from flytekit.annotated import context_manager, stuff as _annotated
from flytekit.common import utils as _utils
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literal_models


class BaseExecutor(object):
    # Should these be class methods?
    def execute(self, ctx: context_manager.FlyteContext, task: _annotated.PythonTask, inputs_path: str):
        # First download the contents of the input file
        local_inputs_file = os.path.join(ctx.workflow_execution_state.working_dir, 'inputs.pb')
        _data_proxy.Data.get_data(inputs_path, local_inputs_file)
        idl_input_literals = _utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)

        # Translate the input literals to Python native
        native_inputs = _flyte_engine.idl_literal_map_to_python_value(ctx, idl_input_literals)

        # TODO: See the flyte/engine.py code to wrap things in try/catches, figure out what type of error things are
        #  Add _exception_scopes.user_entry_point
        # Time to actually run the function
        outputs = task(native_inputs)

        expected_output_names = list(task.interface.outputs.keys())
        if len(expected_output_names) == 1:
            literals = {expected_output_names[0]: outputs}
        else:
            # Question: How do you know you're going to enumerate them in the correct order? Even if autonamed, will
            # output2 come before output100 if there's a hundred outputs? We don't! We'll have to circle back to
            # the Python task instance and inspect annotations again. Or we change the Python model representation
            # of the interface to be an ordered dict and we fill it in correctly to begin with.
            literals = {expected_output_names[i]: outputs[i] for i, _ in enumerate(outputs)}

        # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
        # built into the IDL that all the values of a literal map are of the same type.
        outputs_literal_map = _literal_models.LiteralMap(literals={
            k: _flyte_engine.python_value_to_idl_literal(ctx, v, task.interface.outputs[k].type) for k, v in
            literals.items()
        })
        print("Outputs!")
        print(outputs_literal_map)

        # Write the output back to file and write file to S3.
        # return {
        #     _constants.OUTPUT_FILE_NAME: outputs_literal_map,
        # }


def get_executor(task):
    return BaseExecutor()
