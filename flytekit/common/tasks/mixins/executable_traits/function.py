from __future__ import absolute_import
import abc as _abc
import six as _six

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.core import identifier as _identifier
from flytekit.common.tasks.mixins.executable_traits import common as _common


class WrappedFunctionTask(_six.with_metaclass(_abc.ABCMeta, object)):
    """
    This assumes to be mixed in with a flytekit.common.task.sdk_runnable.SdkRunnableTask
    """
    def __init__(self, task_function=None, **kwargs):
        if not task_function:
            pass
        self._task_function = task_function
        super(WrappedFunctionTask, self).__init__(**kwargs)

    @property
    def task_function(self):
        return self._task_function

    @property
    def task_function_name(self):
        """
        :rtype: Text
        """
        return self.task_function.__name__

    @property
    def task_module(self):
        """
        :rtype: Text
        """
        return self._task_function.__module__

    def _get_container_definition(
            self,
            storage_request=None,
            cpu_request=None,
            gpu_request=None,
            memory_request=None,
            storage_limit=None,
            cpu_limit=None,
            gpu_limit=None,
            memory_limit=None,
            environment=None,
    ):
        # TODO: Make replacing cmd/arg easy
        pass

    # TODO: We can probably put this in SdkRunnable
    # TODO: Stop

    def _execute_user_code(self, context, inputs, outputs):
        """
        :param flytekit.engines.common.EngineContext context:
        :param dict[Text, T] inputs: This variable is a bit of a misnomer, since it's both inputs and outputs. The
            dictionary passed here will be passed to the user-defined function, and will have values that are a
            variety of types.  The T's here are Python std values for inputs.  If there isn't a native Python type for
            something (like Schema or Blob), they are the Flyte classes.  For outputs they are OutputReferences.
            (Note that these are not the same OutputReferences as in BindingData's)
        :rtype: Any: the returned object from user code.
        """
        full_args = inputs.copy()
        full_args.update(outputs)
        return _exception_scopes.user_entry_point(self.task_function)(
            _common.ExecutionParameters(
                execution_date=context.execution_date,
                # TODO: it might be better to consider passing the full struct
                execution_id=_six.text_type(
                    _identifier.WorkflowExecutionIdentifier.promote_from_model(context.execution_id)
                ),
                stats=context.stats,
                logging=context.logging,
                tmp_dir=context.working_directory
            ),
            **full_args,
        )
