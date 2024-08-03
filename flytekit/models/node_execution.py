import datetime
import typing
from datetime import timezone as _timezone

import flyteidl.admin.node_execution_pb2 as admin_node_execution_pb2
import flyteidl_rust as flyteidl

from flytekit.models import common as _common_models
from flytekit.models import utils
from flytekit.models.core import catalog as catalog_models
from flytekit.models.core import compiler as core_compiler_models
from flytekit.models.core import execution as _core_execution
from flytekit.models.core import identifier as _identifier


class WorkflowNodeMetadata(_common_models.FlyteIdlEntity):
    def __init__(self, execution_id: _identifier.WorkflowExecutionIdentifier):
        self._execution_id = execution_id

    @property
    def execution_id(self) -> _identifier.WorkflowExecutionIdentifier:
        return self._execution_id

    def to_flyte_idl(self) -> flyteidl.admin.WorkflowNodeMetadata:
        return flyteidl.admin.WorkflowNodeMetadata(
            execution_id=self.execution_id.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, p: flyteidl.admin.WorkflowNodeMetadata) -> "WorkflowNodeMetadata":
        return cls(
            execution_id=_identifier.WorkflowExecutionIdentifier.from_flyte_idl(p.execution_id),
        )


class DynamicWorkflowNodeMetadata(_common_models.FlyteIdlEntity):
    def __init__(self, id: _identifier.Identifier, compiled_workflow: core_compiler_models.CompiledWorkflowClosure):
        self._id = id
        self._compiled_workflow = compiled_workflow

    @property
    def id(self) -> _identifier.Identifier:
        return self._id

    @property
    def compiled_workflow(self) -> core_compiler_models.CompiledWorkflowClosure:
        return self._compiled_workflow

    def to_flyte_idl(self) -> admin_node_execution_pb2.DynamicWorkflowNodeMetadata:
        return admin_node_execution_pb2.DynamicWorkflowNodeMetadata(
            id=self.id.to_flyte_idl(),
            compiled_workflow=self.compiled_workflow.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, p: admin_node_execution_pb2.DynamicWorkflowNodeMetadata) -> "DynamicWorkflowNodeMetadata":
        yy = cls(
            id=_identifier.Identifier.from_flyte_idl(p.id),
            compiled_workflow=core_compiler_models.CompiledWorkflowClosure.from_flyte_idl(p.compiled_workflow),
        )
        return yy


class TaskNodeMetadata(_common_models.FlyteIdlEntity):
    def __init__(self, cache_status: int, catalog_key: catalog_models.CatalogMetadata):
        self._cache_status = cache_status
        self._catalog_key = catalog_key

    @property
    def cache_status(self) -> int:
        return self._cache_status

    @property
    def catalog_key(self) -> catalog_models.CatalogMetadata:
        return self._catalog_key

    def to_flyte_idl(self) -> admin_node_execution_pb2.TaskNodeMetadata:
        return admin_node_execution_pb2.TaskNodeMetadata(
            cache_status=self.cache_status,
            catalog_key=self.catalog_key.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, p: admin_node_execution_pb2.TaskNodeMetadata) -> "TaskNodeMetadata":
        return cls(
            cache_status=p.cache_status,
            catalog_key=catalog_models.CatalogMetadata.from_flyte_idl(p.catalog_key) if p.catalog_key else None,
        )


class NodeExecutionClosure(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        phase,
        started_at,
        duration,
        output_uri=None,
        deck_uri=None,
        error=None,
        workflow_node_metadata: typing.Optional[WorkflowNodeMetadata] = None,
        task_node_metadata: typing.Optional[TaskNodeMetadata] = None,
        created_at: typing.Optional[datetime.datetime] = None,
        updated_at: typing.Optional[datetime.datetime] = None,
    ):
        """
        :param int phase:
        :param datetime.datetime started_at:
        :param datetime.timedelta duration:
        :param Text output_uri:
        :param flytekit.models.core.execution.ExecutionError error:
        """
        self._phase = phase
        self._started_at = started_at
        self._duration = duration
        self._output_uri = output_uri
        self._deck_uri = deck_uri
        self._error = error
        self._workflow_node_metadata = workflow_node_metadata
        self._task_node_metadata = task_node_metadata
        # TODO: Add output_data field as well.
        self._created_at = created_at
        self._updated_at = updated_at

    @property
    def phase(self):
        """
        :rtype: int
        """
        return self._phase

    @property
    def started_at(self):
        """
        :rtype: datetime.datetime
        """
        return self._started_at

    @property
    def duration(self):
        """
        :rtype: datetime.timedelta
        """
        return self._duration

    @property
    def created_at(self) -> typing.Optional[datetime.datetime]:
        return self._created_at

    @property
    def updated_at(self) -> typing.Optional[datetime.datetime]:
        return self._updated_at

    @property
    def output_uri(self):
        """
        :rtype: Text
        """
        return self._output_uri

    @property
    def deck_uri(self):
        """
        :rtype: str
        """
        return self._deck_uri

    @property
    def error(self):
        """
        :rtype: flytekit.models.core.execution.ExecutionError
        """
        return self._error

    @property
    def workflow_node_metadata(self) -> typing.Optional[WorkflowNodeMetadata]:
        return self._workflow_node_metadata

    @property
    def task_node_metadata(self) -> typing.Optional[TaskNodeMetadata]:
        return self._task_node_metadata

    @property
    def target_metadata(self) -> typing.Union[WorkflowNodeMetadata, TaskNodeMetadata]:
        return self.workflow_node_metadata or self.task_node_metadata

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecutionClosure
        """
        obj = flyteidl.admin.NodeExecutionClosure(
            phase=flyteidl.task_execution.Phase(self.phase),
            output_result=flyteidl.node_execution_closure.OutputResult.OutputUri(self.output_uri)
            if self.output_uri
            else flyteidl.node_execution_closure.OutputResult.Error(self.error.to_flyte_idl()),
            deck_uri=self.deck_uri,
            # error=self.error.to_flyte_idl() if self.error is not None else None,
            target_metadata=flyteidl.node_execution_closure.TargetMetadata.TaskNodeMetadata(
                self.task_node_metadata.to_flyte_idl()
            )
            if self.task_node_metadata
            else flyteidl.node_execution_closure.TargetMetadata.WorkflowNodeMetadata(
                self.workflow_node_metadata.to_flyte_idl()
            ),
        )
        # obj.started_at.FromDatetime(self.started_at.astimezone(_timezone.utc).replace(tzinfo=None))
        # obj.duration.FromTimedelta(self.duration)
        if self.created_at:
            obj.created_at.FromDatetime(self.created_at.astimezone(_timezone.utc).replace(tzinfo=None))
        if self.updated_at:
            obj.updated_at.FromDatetime(self.updated_at.astimezone(_timezone.utc).replace(tzinfo=None))
        return obj

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecutionClosure p:
        :rtype: NodeExecutionClosure
        """
        return cls(
            phase=p.phase,
            output_uri=p.output_result[0]
            if isinstance(p.output_result, flyteidl.node_execution_closure.OutputResult.OutputUri)
            else "",
            deck_uri=p.deck_uri,
            error=_core_execution.ExecutionError.from_flyte_idl(p.output_result[0])
            if isinstance(p.output_result, flyteidl.node_execution_closure.OutputResult.Error)
            else None,
            started_at=utils.convert_to_datetime(seconds=p.started_at.seconds, nanos=p.started_at.nanos).replace(
                tzinfo=_timezone.utc
            )
            if p.started_at
            else None,
            duration=None,
            # duration=p.duration.ToTimedelta(), #TODO
            workflow_node_metadata=WorkflowNodeMetadata.from_flyte_idl(p.target_metadata[0])
            if isinstance(p.target_metadata, flyteidl.node_execution_closure.TargetMetadata.WorkflowNodeMetadata)
            else None,
            task_node_metadata=TaskNodeMetadata.from_flyte_idl(p.target_metadata[0])
            if isinstance(p.target_metadata, flyteidl.node_execution_closure.TargetMetadata.TaskNodeMetadata)
            else None,
            created_at=utils.convert_to_datetime(seconds=p.created_at.seconds, nanos=p.created_at.nanos).replace(
                tzinfo=_timezone.utc
            )
            if p.created_at
            else None,
            updated_at=utils.convert_to_datetime(seconds=p.updated_at.seconds, nanos=p.updated_at.nanos).replace(
                tzinfo=_timezone.utc
            )
            if p.updated_at
            else None,
        )


class NodeExecution(_common_models.FlyteIdlEntity):
    def __init__(self, id, input_uri, closure, metadata: flyteidl.admin.NodeExecutionMetaData):
        """
        :param flytekit.models.core.identifier.NodeExecutionIdentifier id:
        :param Text input_uri:
        :param NodeExecutionClosure closure:
        :param metadata:
        """
        self._id = id
        self._input_uri = input_uri
        self._closure = closure
        self._metadata = metadata

    @property
    def id(self):
        """
        :rtype: flytekit.models.core.identifier.NodeExecutionIdentifier
        """
        return self._id

    @property
    def input_uri(self):
        """
        :rtype: Text
        """
        return self._input_uri

    @property
    def closure(self):
        """
        :rtype: NodeExecutionClosure
        """
        return self._closure

    @property
    def metadata(self) -> flyteidl.admin.NodeExecutionMetaData:
        return self._metadata

    def to_flyte_idl(self) -> flyteidl.admin.NodeExecution:
        return flyteidl.admin.NodeExecution(
            id=self.id.to_flyte_idl(),
            input_uri=self.input_uri,
            closure=self.closure.to_flyte_idl(),
            metadata=self.metadata,
        )

    @classmethod
    def from_flyte_idl(cls, p: flyteidl.admin.NodeExecution) -> "NodeExecution":
        return cls(
            id=_identifier.NodeExecutionIdentifier.from_flyte_idl(p.id),
            input_uri=p.input_uri,
            closure=NodeExecutionClosure.from_flyte_idl(p.closure),
            metadata=p.metadata,
        )
