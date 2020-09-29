from flytekit import engine as _flyte_engine
from flytekit.annotated import context_manager, stuff as _annotated
from flytekit.models import literals as _literal_models


class BaseExecutor(object):
    def execute(self, ctx: context_manager.FlyteContext, task: _annotated.PythonTask,
                input_literal_map: _literal_models.LiteralMap) -> _literal_models.LiteralMap:

        # Translate the input literals to Python native
        native_inputs = _flyte_engine.idl_literal_map_to_python_value(ctx, input_literal_map)

        # TODO: See the flyte/engine.py code to wrap things in try/catches, figure out what type of error things are
        #  Add _exception_scopes.user_entry_point
        # Time to actually run the function
        with ctx.new_execution_context(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION) as ctx:
            native_outputs = task(**native_inputs)

        expected_output_names = list(task.interface.outputs.keys())
        if len(expected_output_names) == 1:
            native_outputs_as_map = {expected_output_names[0]: native_outputs}
        else:
            # Question: How do you know you're going to enumerate them in the correct order? Even if autonamed, will
            # output2 come before output100 if there's a hundred outputs? We don't! We'll have to circle back to
            # the Python task instance and inspect annotations again. Or we change the Python model representation
            # of the interface to be an ordered dict and we fill it in correctly to begin with.
            native_outputs_as_map = {expected_output_names[i]: native_outputs[i] for i, _ in enumerate(native_outputs)}

        # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
        # built into the IDL that all the values of a literal map are of the same type.
        outputs_literal_map = _literal_models.LiteralMap(literals={
            k: _flyte_engine.python_value_to_idl_literal(ctx, v, task.interface.outputs[k].type) for k, v in
            native_outputs_as_map.items()
        })
        print("Outputs!")
        print(outputs_literal_map)
        return outputs_literal_map


def get_executor(task):
    return BaseExecutor()
