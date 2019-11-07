from __future__ import absolute_import
import abc as _abc
import six as _six

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.core import identifier as _identifier
from flytekit.common.tasks.mixins.executable_traits import common as _common


class NotebookTask(_six.with_metaclass(_abc.ABCMeta, object)):

    def __init__(self, notebook_path=None, **kwargs):
        if not notebook_path:
            pass
        self._notebook_path = notebook_path
        super(NotebookTask, self).__init__(**kwargs)

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

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        It is not recommended to override this function unless implementing a mixin like in
        flytekit.common.tasks.mixins.executable_traits. This function might be modified by mixins to ensure behavior
        given the execution context. However, the general flow should adhere to the order laid out in this method.

        To modify behavior for a task extension that is being authored, override the methods called from this function.

        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text,flytekit.models.common.FlyteIdlEntity]
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        inputs_dict = self._unpack_inputs(context, inputs)
        outputs_dict = self._unpack_output_references(context)
        user_returned = self._execute_user_code(context, inputs_dict, outputs_dict)
        out_protos = self._handle_user_returns(context, user_returned)
        out_protos.update(self._pack_output_references(context, outputs_dict))
        return out_protos
