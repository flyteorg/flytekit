from typing import Optional, Any, Union, Tuple

from flytekit.core import hash as _hash_mixin
from flytekit.core.context_manager import BranchEvalMode, ExecutionState, FlyteContext
from flytekit.core.interface import Interface
from flytekit.core.promise import (
    Promise,
    VoidPromise,
    create_and_link_node_from_remote,
    extract_obj_name,
)
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions import user as user_exceptions
from flytekit.loggers import remote_logger as logger
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as _identifier_model
from flytekit.models.core.workflow import NodeMetadata
from flytekit.remote import interface as _interfaces


# class RemoteEntity(object):
#     def construct_node_metadata(self) -> NodeMetadata:
#         """
#         Used when constructing the node that encapsulates this task as part of a broader workflow definition.
#         """
#         return NodeMetadata(
#             name=extract_obj_name(self.name),
#             timeout=self.metadata.timeout,
#             retries=self.metadata.retry_strategy,
#             interruptible=self.metadata.interruptible,
#         )


class FlyteTask(_hash_mixin.HashOnReferenceMixin, _task_model.TaskTemplate):
    """A class encapsulating a remote Flyte task."""

    def __init__(self, id, type, metadata, interface, custom, container=None, task_type_version=0, config=None):
        super(FlyteTask, self).__init__(
            id,
            type,
            metadata,
            interface,
            custom,
            container=container,
            task_type_version=task_type_version,
            config=config,
        )
        self._python_interface = None
        # self._reference_entity = None

    # def __call__(self, *args, **kwargs):
    #     if self.reference_entity is None:
    #         logger.warning(
    #             f"FlyteTask {self} is not callable, most likely because flytekit could not "
    #             f"guess the python interface. The workflow calling this task may not behave correctly"
    #         )
    #         return
    #     return self.reference_entity(*args, **kwargs)

    def construct_node_metadata(self) -> NodeMetadata:
        """
        Used when constructing the node that encapsulates this task as part of a broader workflow definition.
        """
        return NodeMetadata(
            name=extract_obj_name(self.id.name),
        )

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        return create_and_link_node_from_remote(ctx, entity=self, **kwargs)

    def __call__(self, *args, **kwargs):
        # When a Task is () aka __called__, there are three things we may do:
        #  a. Plain execution Mode - just run the execute function. If not overridden, we should raise an exception
        #  b. Compilation Mode - this happens when the function is called as part of a workflow (potentially
        #     dynamic task). Produce promise objects and create a node.
        #  c. Workflow Execution Mode - when a workflow is being run locally. Even though workflows are functions
        #     and everything should be able to be passed through naturally, we'll want to wrap output values of the
        #     function into objects, so that potential .with_cpu or other ancillary functions can be attached to do
        #     nothing. Subsequent tasks will have to know how to unwrap these. If by chance a non-Flyte task uses a
        #     task output as an input, things probably will fail pretty obviously.
        #     Since this is a reference entity, it still needs to be mocked otherwise an exception will be raised.
        if len(args) > 0:
            raise user_exceptions.FlyteAssertion(
                f"Cannot call remotely fetched entity with args - detected {len(args)} positional args {args}"
            )

        ctx = FlyteContext.current_context()
        if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
            return self.compile(ctx, *args, **kwargs)
        elif (
            ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
        ):
            if ctx.execution_state.branch_eval_mode == BranchEvalMode.BRANCH_SKIPPED:
                return
            return self.local_execute(ctx, **kwargs)
        else:
            logger.debug("Fetched entity, running raw execute.")
            return self.execute(**kwargs)

    # # TODO: Refactor behind mixin
    # @property
    # def reference_entity(self) -> Optional[ReferenceTask]:
    #     if self._reference_entity is None:
    #         if self.guessed_python_interface is None:
    #             try:
    #                 self.guessed_python_interface = Interface(
    #                     TypeEngine.guess_python_types(self.interface.inputs),
    #                     TypeEngine.guess_python_types(self.interface.outputs),
    #                 )
    #             except Exception as e:
    #                 logger.warning(f"Error backing out interface {e}, Flyte interface {self.interface}")
    #                 return None
    #
    #         self._reference_entity = ReferenceTask(
    #             self.id.project,
    #             self.id.domain,
    #             self.id.name,
    #             self.id.version,
    #             inputs=self.guessed_python_interface.inputs,
    #             outputs=self.guessed_python_interface.outputs,
    #         )
    #     return self._reference_entity

    @property
    def interface(self) -> _interfaces.TypedInterface:
        return super(FlyteTask, self).interface

    def local_execute(self, ctx: FlyteContext, **kwargs) -> Optional[Union[Tuple[Promise], Promise, VoidPromise]]:
        raise Exception("Remotely fetched entities cannot be run locally. You have to mock this out.")

    def execute(self, **kwargs) -> Any:
        raise Exception("Remotely fetched entities cannot be run locally. You have to mock this out.")

    @property
    def name(self) -> str:
        return self.id.name

    @property
    def resource_type(self) -> _identifier_model.ResourceType:
        return _identifier_model.ResourceType.TASK

    @property
    def entity_type_text(self) -> str:
        return "Task"

    @property
    def guessed_python_interface(self) -> Optional[Interface]:
        return self._python_interface

    @guessed_python_interface.setter
    def guessed_python_interface(self, value):
        if self._python_interface is not None:
            return
        self._python_interface = value

    @classmethod
    def promote_from_model(cls, base_model: _task_model.TaskTemplate) -> "FlyteTask":
        t = cls(
            id=base_model.id,
            type=base_model.type,
            metadata=base_model.metadata,
            interface=_interfaces.TypedInterface.promote_from_model(base_model.interface),
            custom=base_model.custom,
            container=base_model.container,
            task_type_version=base_model.task_type_version,
        )
        # Override the newly generated name if one exists in the base model
        if not base_model.id.is_empty:
            t._id = base_model.id

        if t.interface is not None:
            try:
                t.guessed_python_interface = Interface(
                    inputs=TypeEngine.guess_python_types(t.interface.inputs),
                    outputs=TypeEngine.guess_python_types(t.interface.outputs),
                )
            except ValueError:
                logger.warning(f"Could not infer Python types for FlyteTask {base_model.id}")

        return t
