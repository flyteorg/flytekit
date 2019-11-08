from __future__ import absolute_import
import abc as _abc
import six as _six

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.core import identifier as _identifier
from flytekit.common.tasks.mixins.executable_traits import common as _common


class NotebookTask(_six.with_metaclass(_abc.ABCMeta, object)):

    def __init__(self, notebook_path=None, **kwargs):
        self._notebook_path = notebook_path
        super(NotebookTask, self).__init__(**kwargs)
        # Get name in thing

    def _execute_user_code(self, context, inputs, outputs):
        # todo: remove
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
        pass
