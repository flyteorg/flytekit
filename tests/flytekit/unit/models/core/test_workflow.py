from __future__ import absolute_import

from datetime import timedelta

from flytekit.models import literals as _literals
from flytekit.models.core import workflow as _workflow, identifier as _identifier, condition as _condition

_generic_id = _identifier.Identifier(_identifier.ResourceType.WORKFLOW, "project", "domain", "name", "version")


def test_node_metadata():
    obj = _workflow.NodeMetadata(name='node1', timeout=timedelta(seconds=10), retries=_literals.RetryStrategy(0))
    assert obj.timeout.seconds == 10
    assert obj.retries.retries == 0
    obj2 = _workflow.NodeMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.timeout.seconds == 10
    assert obj2.retries.retries == 0


def test_alias():
    obj = _workflow.Alias(var='myvar', alias='myalias')
    assert obj.alias == 'myalias'
    assert obj.var == 'myvar'
    obj2 = _workflow.Alias.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.alias == 'myalias'
    assert obj2.var == 'myvar'


def test_workflow_metadata():
    obj = _workflow.WorkflowMetadata(queuing_budget=timedelta(seconds=10))
    obj2 = _workflow.WorkflowMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_task_node():
    obj = _workflow.TaskNode(reference_id=_generic_id)
    assert obj.reference_id == _generic_id

    obj2 = _workflow.TaskNode.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.reference_id == _generic_id


def test_workflow_node_lp():
    obj = _workflow.WorkflowNode(launchplan_ref=_generic_id)
    assert obj.launchplan_ref == _generic_id
    assert obj.reference == _generic_id

    obj2 = _workflow.WorkflowNode.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.reference == _generic_id
    assert obj2.launchplan_ref == _generic_id


def test_workflow_node_sw():
    obj = _workflow.WorkflowNode(sub_workflow_ref=_generic_id)
    assert obj.sub_workflow_ref == _generic_id
    assert obj.reference == _generic_id

    obj2 = _workflow.WorkflowNode.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.reference == _generic_id
    assert obj2.sub_workflow_ref == _generic_id


def _get_sample_node_metadata():
    return _workflow.NodeMetadata(name='node1', timeout=timedelta(seconds=10), retries=_literals.RetryStrategy(0))


def test_node_task_with_no_inputs():
    nm = _get_sample_node_metadata()
    task = _workflow.TaskNode(reference_id=_generic_id)

    obj = _workflow.Node(
        id='some:node:id',
        metadata=nm,
        inputs=[],
        upstream_node_ids=[],
        output_aliases=[],
        task_node=task
    )
    assert obj.target == task
    assert obj.id == 'some:node:id'
    assert obj.metadata == nm

    obj2 = _workflow.Node.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.target == task
    assert obj2.id == 'some:node:id'
    assert obj2.metadata == nm


def test_node_task_with_inputs():
    nm = _get_sample_node_metadata()
    task = _workflow.TaskNode(reference_id=_generic_id)
    bd = _literals.BindingData(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=5)))
    bd2 = _literals.BindingData(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=99)))
    binding = _literals.Binding(var='myvar', binding=bd)
    binding2 = _literals.Binding(var='myothervar', binding=bd2)

    obj = _workflow.Node(
        id='some:node:id',
        metadata=nm,
        inputs=[binding, binding2],
        upstream_node_ids=[],
        output_aliases=[],
        task_node=task
    )
    assert obj.target == task
    assert obj.id == 'some:node:id'
    assert obj.metadata == nm
    assert len(obj.inputs) == 2
    assert obj.inputs[0] == binding

    obj2 = _workflow.Node.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.target == task
    assert obj2.id == 'some:node:id'
    assert obj2.metadata == nm
    assert len(obj2.inputs) == 2
    assert obj2.inputs[1] == binding2


def test_branch_node():
    nm = _get_sample_node_metadata()
    task = _workflow.TaskNode(reference_id=_generic_id)
    bd = _literals.BindingData(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=5)))
    bd2 = _literals.BindingData(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=99)))
    binding = _literals.Binding(var='myvar', binding=bd)
    binding2 = _literals.Binding(var='myothervar', binding=bd2)

    obj = _workflow.Node(
        id='some:node:id',
        metadata=nm,
        inputs=[binding, binding2],
        upstream_node_ids=[],
        output_aliases=[],
        task_node=task
    )

    bn = _workflow.BranchNode(_workflow.IfElseBlock(
        case=_workflow.IfBlock(
            condition=_condition.BooleanExpression(
                comparison=_condition.ComparisonExpression(_condition.ComparisonExpression.Operator.EQ,
                                                           _condition.Operand(primitive=_literals.Primitive(integer=5)),
                                                           _condition.Operand(
                                                               primitive=_literals.Primitive(integer=2)))),
            then_node=obj
        ),
        other=[_workflow.IfBlock(
            condition=_condition.BooleanExpression(
                conjunction=_condition.ConjunctionExpression(_condition.ConjunctionExpression.LogicalOperator.AND,
                                                             _condition.BooleanExpression(
                                                                 comparison=_condition.ComparisonExpression(
                                                                     _condition.ComparisonExpression.Operator.EQ,
                                                                     _condition.Operand(
                                                                         primitive=_literals.Primitive(integer=5)),
                                                                     _condition.Operand(
                                                                         primitive=_literals.Primitive(integer=2)))),
                                                             _condition.BooleanExpression(
                                                                 comparison=_condition.ComparisonExpression(
                                                                     _condition.ComparisonExpression.Operator.EQ,
                                                                     _condition.Operand(
                                                                         primitive=_literals.Primitive(integer=5)),
                                                                     _condition.Operand(
                                                                         primitive=_literals.Primitive(integer=2)))))),
            then_node=obj
        )],
        else_node=obj
    ))

    bn2 = _workflow.BranchNode.from_flyte_idl(bn.to_flyte_idl())
    assert bn == bn2
    assert bn.if_else.case.then_node == obj
