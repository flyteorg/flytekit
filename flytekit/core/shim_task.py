from __future__ import annotations

from typing import Any, Union, Generic, TypeVar

from flytekit import FlyteContext, ExecutionParameters, logger
from flytekit.core.tracker import TrackedInstance
from flytekit.core.type_engine import TypeEngine
from flytekit.models import task as _task_model, literals as _literal_models, dynamic_job as _dynamic_job


class ExecutableTemplateShimTask(object):
    def __init__(self, tt: _task_model.TaskTemplate, executor: ShimTaskExecutor):
        self._executor = executor
        self._task_template = tt

    @property
    def task_template(self) -> _task_model.TaskTemplate:
        return self._task_template

    @property
    def executor(self) -> ShimTaskExecutor:
        return self._executor

    def execute(self, **kwargs) -> Any:
        """
        This function overrides the default task execute behavior.

        Execution for third-party tasks is different from tasks that run the user workflow container.
        1. Serialize the task out to a TaskTemplate.
        2. Pass the template over to the Executor to run, along with the input arguments.
        3. Executor will reconstruct the Python task class object, before running the e

        When overridden for unit testing using the patch operator, all these steps will be skipped and the mocked code,
        which should just take in and return Python native values, will be run.
        """
        return self.executor.execute_from_model(self.task_template, **kwargs)

    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap
    ) -> Union[_literal_models.LiteralMap, _dynamic_job.DynamicJobSpec]:
        """
        This function overrides the default task execute behavior.
        """
        return self.executor.dispatch_execute(ctx, self.task_template, input_literal_map)


T = TypeVar("T")


class ShimTaskExecutor(TrackedInstance, Generic[T]):
    @classmethod
    def execute_from_model(cls, tt: _task_model.TaskTemplate, **kwargs) -> Any:
        raise NotImplementedError

    @classmethod
    def pre_execute(cls, user_params: ExecutionParameters) -> ExecutionParameters:
        """
        This function is a stub, just here to keep dispatch_execute compatibility between this class and PythonTask.
        """
        return user_params

    @classmethod
    def post_execute(cls, user_params: ExecutionParameters, rval: Any) -> Any:
        """
        This function is a stub, just here to keep dispatch_execute compatibility between this class and PythonTask.
        """
        return rval

    @classmethod
    def dispatch_execute(
        cls, ctx: FlyteContext, tt: _task_model.TaskTemplate, input_literal_map: _literal_models.LiteralMap
    ) -> Union[_literal_models.LiteralMap, _dynamic_job.DynamicJobSpec]:
        """
        This function is copied from PythonTask.dispatch_execute. Will need to make it a mixin and refactor in the
        future.
        """

        # Invoked before the task is executed
        new_user_params = cls.pre_execute(ctx.user_space_params)

        # Create another execution context with the new user params, but let's keep the same working dir
        with ctx.new_execution_context(
            mode=ctx.execution_state.mode,
            execution_params=new_user_params,
            working_dir=ctx.execution_state.working_dir,
        ) as exec_ctx:
            # Added: Have to reverse the Python interface from the task template Flyte interface
            #  This will be moved into the FlyteTask promote logic instead
            guessed_python_input_types = TypeEngine.guess_python_types(tt.interface.inputs)
            native_inputs = TypeEngine.literal_map_to_kwargs(exec_ctx, input_literal_map, guessed_python_input_types)

            logger.info(f"Invoking FlyteTask executor {tt.id.name} with inputs: {native_inputs}")
            try:
                native_outputs = cls.execute_from_model(tt, **native_inputs)
            except Exception as e:
                logger.exception(f"Exception when executing {e}")
                raise e

            logger.info(f"Task executed successfully in user level, outputs: {native_outputs}")
            # Lets run the post_execute method. This may result in a IgnoreOutputs Exception, which is
            # bubbled up to be handled at the callee layer.
            native_outputs = cls.post_execute(new_user_params, native_outputs)

            # Short circuit the translation to literal map because what's returned may be a dj spec (or an
            # already-constructed LiteralMap if the dynamic task was a no-op), not python native values
            if isinstance(native_outputs, _literal_models.LiteralMap) or isinstance(
                native_outputs, _dynamic_job.DynamicJobSpec
            ):
                return native_outputs

            expected_output_names = list(tt.interface.outputs.keys())
            if len(expected_output_names) == 1:
                # Here we have to handle the fact that the task could've been declared with a typing.NamedTuple of
                # length one. That convention is used for naming outputs - and single-length-NamedTuples are
                # particularly troublesome but elegant handling of them is not a high priority
                # Again, we're using the output_tuple_name as a proxy.
                # Deleted some stuff
                native_outputs_as_map = {expected_output_names[0]: native_outputs}
            elif len(expected_output_names) == 0:
                native_outputs_as_map = {}
            else:
                native_outputs_as_map = {
                    expected_output_names[i]: native_outputs[i] for i, _ in enumerate(native_outputs)
                }

            # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
            # built into the IDL that all the values of a literal map are of the same type.
            literals = {}
            for k, v in native_outputs_as_map.items():
                literal_type = tt.interface.outputs[k].type
                py_type = type(v)

                if isinstance(v, tuple):
                    raise AssertionError(f"Output({k}) in task{tt.id.name} received a tuple {v}, instead of {py_type}")
                try:
                    literals[k] = TypeEngine.to_literal(exec_ctx, v, py_type, literal_type)
                except Exception as e:
                    raise AssertionError(f"failed to convert return value for var {k}") from e

            outputs_literal_map = _literal_models.LiteralMap(literals=literals)
            # After the execute has been successfully completed
            return outputs_literal_map