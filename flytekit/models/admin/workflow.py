from __future__ import absolute_import
from flytekit.models import common as _common
from flytekit.models.core import compiler as _compiler_models, identifier as _identifier
from flyteidl.admin import workflow_pb2 as _admin_workflow


class WorkflowSpec(_common.FlyteIdlEntity):

    def __init__(self, template):
        """
        This object fully encapsulates the specification of a workflow
        :param flytekit.models.core.workflow.WorkflowTemplate template:
        """
        self._template = template

    @property
    def template(self):
        """
        :rtype: flytekit.models.core.workflow.WorkflowTemplate.WorkflowTemplate
        """
        return self._template

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.workflow_pb2.WorkflowSpec
        """
        return _admin_workflow.WorkflowSpec(
            template=self._template.to_flyte_idl()
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param pb2_object: flyteidl.admin.workflow_pb2.WorkflowSpec
        :rtype: WorkflowSpec
        """
        return cls(WorkflowSpec.from_flyte_idl(pb2_object.template))


class Workflow(_common.FlyteIdlEntity):

    def __init__(
        self,
        id,
        closure
    ):
        """
        :param flytekit.models.core.identifier.Identifier id:
        :param WorkflowClosure closure:
        """
        self._id = id
        self._closure = closure

    @property
    def id(self):
        """
        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self._id

    @property
    def closure(self):
        """
        :rtype: WorkflowClosure
        """
        return self._closure

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.workflow_pb2.Workflow
        """
        return _admin_workflow.Workflow(
            id=self.id.to_flyte_idl(),
            closure=self.closure.to_flyte_idl()
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.workflow_pb2.Workflow pb2_object:
        :return: Workflow
        """
        return cls(
            id=_identifier.Identifier.from_flyte_idl(pb2_object.id),
            closure=WorkflowClosure.from_flyte_idl(pb2_object.closure)
        )


class WorkflowClosure(_common.FlyteIdlEntity):

    def __init__(self, compiled_workflow):
        """
        :param flytekit.models.core.compiler.CompiledWorkflowClosure compiled_workflow:
        """
        self._compiled_workflow = compiled_workflow

    @property
    def compiled_workflow(self):
        """
        :rtype: flytekit.models.core.compiler.CompiledWorkflowClosure
        """
        return self._compiled_workflow

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.workflow_pb2.WorkflowClosure
        """
        return _admin_workflow.WorkflowClosure(
            compiled_workflow=self.compiled_workflow.to_flyte_idl()
        )

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.admin.workflow_pb2.WorkflowClosure p:
        :rtype: WorkflowClosure
        """
        return cls(
            compiled_workflow=_compiler_models.CompiledWorkflowClosure.from_flyte_idl(p.compiled_workflow)
        )
