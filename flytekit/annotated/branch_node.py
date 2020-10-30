import typing
from typing import Union

from flytekit.annotated.condition import Case, Condition, ConditionalSection
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.promise import ConjunctionExpression, Promise, ComparisonExpression, ConjunctionOps, \
    ComparisonOps
from flytekit.models.core import condition as _core_cond, workflow as _core_wf
from flytekit.models.literals import Literal
from flytekit.models.types import Error

_logical_ops = {
    ConjunctionOps.AND: _core_cond.ConjunctionExpression.LogicalOperator.AND,
    ConjunctionOps.OR: _core_cond.ConjunctionExpression.LogicalOperator.OR,
}
_comparators = {
    ComparisonOps.EQ: _core_cond.ComparisonExpression.Operator.EQ,
    ComparisonOps.NE: _core_cond.ComparisonExpression.Operator.NEQ,
    ComparisonOps.GT: _core_cond.ComparisonExpression.Operator.GT,
    ComparisonOps.GE: _core_cond.ComparisonExpression.Operator.GTE,
    ComparisonOps.LT: _core_cond.ComparisonExpression.Operator.LT,
    ComparisonOps.LE: _core_cond.ComparisonExpression.Operator.LTE,
}


def transform_to_conj_expr(expr: ConjunctionExpression) -> _core_cond.ConjunctionExpression:
    return _core_cond.ConjunctionExpression(
        left_expression=transform_to_boolexpr(expr.lhs),
        right_expression=transform_to_boolexpr(expr.rhs),
        operator=_logical_ops[expr.op],
    )


def transform_to_operand(v: Union[Promise, Literal]) -> _core_cond.Operand:
    if isinstance(v, Promise):
        return _core_cond.Operand(var=v.var)
    return _core_cond.Operand(primitive=v.scalar.primitive)


def transform_to_comp_expr(expr: ComparisonExpression) -> _core_cond.ComparisonExpression:
    return _core_cond.ComparisonExpression(
        left_value=transform_to_operand(expr.lhs),
        right_value=transform_to_operand(expr.rhs),
        operator=_comparators[expr.op])


def transform_to_boolexpr(expr: Union[ComparisonExpression, ConjunctionExpression]) -> _core_cond.BooleanExpression:
    if isinstance(expr, ConjunctionExpression):
        return _core_cond.BooleanExpression(conjunction=transform_to_conj_expr(expr))
    return _core_cond.BooleanExpression(comparison=transform_to_comp_expr(expr))


def to_case_block(c: Case) -> Union[_core_wf.IfBlock,]:
    return _core_wf.IfBlock(transform_to_boolexpr(c.expr), c.output_promise.ref.sdk_node)


def to_ifelse_block(node_id: str, cs: ConditionalSection) -> _core_wf.IfElseBlock:
    if len(cs.cases) == 0:
        raise AssertionError("Illegal Condition block, with no if-else cases")
    if len(cs.cases) < 2:
        raise AssertionError("Atleast an if/else is required. Dangling If is not allowed")
    first_case = to_case_block(cs.cases[0])
    other_cases: typing.List[_core_wf.IfBlock] = None
    if len(cs.cases) > 2:
        for c in cs.cases[1:-1]:
            other_cases.append(to_case_block(c))
    last_case = cs.cases[-1]
    node = None
    if last_case.output_promise:
        node = last_case.output_promise.ref.sdk_node
    else:
        err = Error(failed_node_id=node_id, message=last_case.err if last_case.err else "Condition failed")
    return _core_wf.IfElseBlock(case=first_case, other=other_cases, else_node=node, error=err)


def to_branch_node(name: str, cs: ConditionalSection) -> _core_wf.BranchNode:
    return _core_wf.BranchNode(if_else=to_ifelse_block(name, cs))


class BranchNode(object):
    def __init__(self, name: str):
        self._name = name
        self._cs = ConditionalSection()
        self._condition = Condition(cs=self._cs)
        self._cs.set_condition(self._condition)

    @property
    def name(self):
        return self._name

    @property
    def condition(self):
        return self._condition

    def if_(self, expr: bool) -> Case:
        ctx = FlyteContext.current_context()
        if ctx.compilation_state:
            # Set some compilation context
            pass
        return self.condition._if(expr)


def conditional(name: str) -> BranchNode:
    return BranchNode(name)
