from __future__ import annotations

import datetime
import typing
from typing import Optional, Tuple, Union

from flytekit.annotated.context_manager import ExecutionState, FlyteContext
from flytekit.annotated.node import Node
from flytekit.annotated.promise import (
    ComparisonExpression,
    ComparisonOps,
    ConjunctionExpression,
    ConjunctionOps,
    NodeOutput,
    Promise,
    VoidPromise,
    create_task_output,
)
from flytekit.models.core import condition as _core_cond
from flytekit.models.core import workflow as _core_wf
from flytekit.models.literals import Binding, BindingData, Literal, RetryStrategy
from flytekit.models.types import Error


class BranchNode(object):
    def __init__(self, name: str, ifelse_block: _core_wf.IfElseBlock):
        self._name = name
        self._registerable_entity = None
        self._ifelse_block = ifelse_block

    @property
    def name(self):
        return self._name


class ConditionalSection(object):
    def __init__(self, name: str):
        self._name = name
        self._cases: typing.List[Case] = []
        self._selected_case = None
        self._last_case = False
        self._condition = Condition(self)

    @property
    def name(self):
        return self._name

    # def __del__(self):
    #     self.validate()
    #
    # def validate(self):
    #     ctx = FlyteContext.current_context()
    #     if ctx.execution_state and ctx.execution_state.branch_eval_mode is not None:
    #         raise AssertionError("Conditional section not completed!")

    def start_branch(self, c: Case, last_case: bool = False) -> Case:
        """
        At the start of an execution of every branch this method should be called.
        :param c: -> the case that represents this branch
        :param last_case: -> a boolean that indicates if this is the last branch in the ifelseblock
        """
        self._last_case = last_case
        self._cases.append(c)
        ctx = FlyteContext.current_context()
        # In case of Local workflow execution, we will actually evaluate the expression and based on the result
        # make the branch to be active using `take_branch` method
        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            # This is a short-circuit for the case when the branch was taken
            # We already have a candidate case selected
            if self._selected_case is None:
                if c.expr is None or c.expr.eval() or last_case:
                    ctx = FlyteContext.current_context().execution_state
                    ctx.take_branch()
                    self._selected_case = self._cases[-1]
        return self._cases[-1]

    def end_branch(self) -> Union[Condition, Promise]:
        """
        This should be invoked after every branch has been visited
        """
        ctx = FlyteContext.current_context()
        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            """
            In case of Local workflow execution, we should first mark the branch as complete, then
            Then we first check for if this is the last case,
            In case this is the last case, we return the output from the selected case - A case should always
            be selected (see start_branch)
            If this is not the last case, we should return the condition so that further chaining can be done
            """
            # Let us mark the execution state as complete
            ctx.execution_state.branch_complete()
            if self._last_case:
                ctx.execution_state.exit_conditional_section()
                if self._selected_case.output_promise is None and self._selected_case.err is None:
                    raise AssertionError("Bad conditional statements, did not resolve in a promise")
                elif self._selected_case.output_promise is not None:
                    return self._selected_case.output_promise
                raise ValueError(self._selected_case.err)
            return self._condition
        elif ctx.compilation_state:
            ########
            # COMPILATION MODE
            """
            In case this is not local workflow execution then, we should check if this is the last case.
            If so then return the promise, else return the condition
            """
            if self._last_case:
                ctx.compilation_state.exit_conditional_section()
                # branch_nodes = ctx.compilation_state.nodes
                node, promises = to_branch_node(self._name, self)
                # Verify branch_nodes == nodes in bn
                bindings: typing.List[Binding] = []
                upstream_nodes = set()
                for p in promises:
                    if not p.is_ready:
                        bindings.append(Binding(var=p.var, binding=BindingData(promise=p.ref)))
                        upstream_nodes.add(p.ref.node)

                n = Node(
                    id=f"{ctx.compilation_state.prefix}node-{len(ctx.compilation_state.nodes)}",
                    metadata=_core_wf.NodeMetadata(self._name, timeout=datetime.timedelta(), retries=RetryStrategy(0)),
                    bindings=sorted(bindings, key=lambda b: b.var),
                    upstream_nodes=list(upstream_nodes),  # type: ignore
                    flyte_entity=node,
                )
                ctx.compilation_state.add_node(n)
                return self._compute_outputs(n)
            return self._condition

        raise AssertionError("Branches can only be invoked within a workflow context!")

    def _compute_outputs(self, n: Node) -> Union[Promise, Tuple[Promise], VoidPromise]:
        output_var_sets: typing.List[typing.Set[str]] = []
        for c in self._cases:
            if c.output_promise is None and c.err is None:
                # One node returns a void output and no error, we will default to a
                # Void output
                return VoidPromise(n.id)
            if c.output_promise is not None:
                if isinstance(c.output_promise, tuple):
                    output_var_sets.append(set([i.var for i in c.output_promise]))
                else:
                    output_var_sets.append(set([c.output_promise.var]))
        curr = output_var_sets[0]
        if len(output_var_sets) > 1:
            for x in output_var_sets[1:]:
                curr = curr.intersection(x)
        promises = [Promise(var=x, val=NodeOutput(node=n, var=x)) for x in curr]
        # TODO: Is there a way to add the Python interface here? Currently, it's an optional arg.
        return create_task_output(promises)

    @property
    def cases(self) -> typing.List[Case]:
        return self._cases

    def if_(self, expr: bool) -> Case:
        ctx = FlyteContext.current_context()
        if ctx.execution_state:
            if ctx.execution_state.branch_eval_mode is not None:
                """
                TODO implement nested branches
                """
                raise NotImplementedError("Nested branches are not yet supported")
            if ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
                """
                In case of local workflow execution we should ensure a conditional section
                is created so that skipped branches result in tasks not being executed
                """
                ctx.execution_state.enter_conditional_section()
        elif ctx.compilation_state:
            if ctx.compilation_state.is_in_a_branch():
                """
                TODO implement nested branches
                """
                raise NotImplementedError("Nested branches are not yet supported")
            ctx.compilation_state.enter_conditional_section()
        return self._condition._if(expr)


class Case(object):
    def __init__(
        self,
        cs: ConditionalSection,
        expr: Optional[Union[ComparisonExpression, ConjunctionExpression]],
        stmt: str = "elif",
    ):
        self._cs = cs
        if expr is not None:
            if isinstance(expr, bool):
                raise AssertionError(
                    "Logical (and/or/is/not) operations are not supported. "
                    "Expressions Comparison (<,<=,>,>=,==,!=) or Conjunction (&/|) are supported."
                    f"Received an evaluated expression with val {expr} in {cs.name}.{stmt}"
                )
            if isinstance(expr, Promise):
                raise AssertionError(
                    "Flytekit does not support unary expressions of the form `if_(x) - where x is an"
                    " input value or output of a previous node."
                    f" Received var {expr} in condition {cs.name}.{stmt}"
                )
            if not (isinstance(expr, ConjunctionExpression) or isinstance(expr, ComparisonExpression)):
                raise AssertionError(
                    "Flytekit only supports Comparison (<,<=,>,>=,==,!=) or Conjunction (&/|) "
                    f"expressions, Received var {expr} in condition {cs.name}.{stmt}"
                )
        self._expr = expr
        self._output_promise: Optional[Union[Tuple[Promise], Promise]] = None
        self._err = None

    @property
    def expr(self) -> Optional[Union[ComparisonExpression, ConjunctionExpression]]:
        return self._expr

    @property
    def output_promise(self) -> Optional[Union[Tuple[Promise], Promise]]:
        return self._output_promise

    @property
    def err(self) -> Optional[str]:
        return self._err

    # TODO this is complicated. We do not want this to run
    def then(self, p: Union[Promise, Tuple[Promise]]) -> Union[Condition, Promise]:
        self._output_promise = p
        # We can always mark branch as completed
        return self._cs.end_branch()

    def fail(self, err: str) -> Promise:
        self._err = err
        return self._cs.end_branch()


class Condition(object):
    def __init__(self, cs: ConditionalSection):
        self._cs = cs

    def _if(self, expr: Union[ComparisonExpression, ConjunctionExpression]) -> Case:
        if expr is None:
            raise AssertionError(f"Required an expression received None for condition:{self._cs.name}.if_(...)")
        return self._cs.start_branch(Case(cs=self._cs, expr=expr, stmt="if_"))

    def elif_(self, expr: Union[ComparisonExpression, ConjunctionExpression]) -> Case:
        if expr is None:
            raise AssertionError(f"Required an expression received None for condition:{self._cs.name}.elif(...)")
        return self._cs.start_branch(Case(cs=self._cs, expr=expr, stmt="elif_"))

    def else_(self) -> Case:
        return self._cs.start_branch(Case(cs=self._cs, expr=None, stmt="else_"), last_case=True)


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


def create_branch_node_promise_var(node_id: str, var: str) -> str:
    """
    Generates a globally (wf-level) unique id for a variable.

    When building bindings for the branch node, the inputs to the conditions (e.g. (x==5)) need to have variable names
    (e.g. x). Because it's currently infeasible to get the name (e.g. x), we resolve to using the referenced node's
    output name (e.g. o0, my_out,... etc.). In order to avoid naming collisions (in cases when, for example, the
    conditions reference two outputs of two different nodes named the same), we build a variable name composed of the
    referenced node name + '.' + the referenced output name. Ideally we use something like
    (https://github.com/pwwang/python-varname) to retrieve the assigned variable name (e.g. x). However, because of
    https://github.com/pwwang/python-varname/issues/28, this is not currently supported for all AST nodes types.

    :param str node_id: the original node_id that produced the variable.
    :param str var: the output variable name from the original node.
    :return: The generated unique id of the variable.
    """
    return f"{node_id}.{var}"


def merge_promises(*args: Promise) -> typing.List[Promise]:
    node_vars: typing.Set[typing.Tuple[str, str]] = set()
    merged_promises: typing.List[Promise] = []
    for p in args:
        if p is not None and p.ref:
            node_var = (p.ref.node_id, p.ref.var)
            if node_var not in node_vars:
                new_p = p.with_var(create_branch_node_promise_var(p.ref.node_id, p.ref.var))
                merged_promises.append(new_p)
                node_vars.add(node_var)
    return merged_promises


def transform_to_conj_expr(
    expr: ConjunctionExpression,
) -> Tuple[_core_cond.ConjunctionExpression, typing.List[Promise]]:
    left, left_promises = transform_to_boolexpr(expr.lhs)
    right, right_promises = transform_to_boolexpr(expr.rhs)
    return (
        _core_cond.ConjunctionExpression(left_expression=left, right_expression=right, operator=_logical_ops[expr.op],),
        merge_promises(*left_promises, *right_promises),
    )


def transform_to_operand(v: Union[Promise, Literal]) -> Tuple[_core_cond.Operand, Optional[Promise]]:
    if isinstance(v, Promise):
        return _core_cond.Operand(var=create_branch_node_promise_var(v.ref.node_id, v.var)), v
    return _core_cond.Operand(primitive=v.scalar.primitive), None


def transform_to_comp_expr(expr: ComparisonExpression) -> (_core_cond.ComparisonExpression, typing.List[Promise]):
    o_lhs, b_lhs = transform_to_operand(expr.lhs)
    o_rhs, b_rhs = transform_to_operand(expr.rhs)
    return (
        _core_cond.ComparisonExpression(left_value=o_lhs, right_value=o_rhs, operator=_comparators[expr.op]),
        merge_promises(b_lhs, b_rhs),
    )


def transform_to_boolexpr(
    expr: Union[ComparisonExpression, ConjunctionExpression]
) -> (_core_cond.BooleanExpression, typing.List[Promise]):
    if isinstance(expr, ConjunctionExpression):
        cexpr, promises = transform_to_conj_expr(expr)
        return _core_cond.BooleanExpression(conjunction=cexpr), promises
    cexpr, promises = transform_to_comp_expr(expr)
    return _core_cond.BooleanExpression(comparison=cexpr), promises


def to_case_block(c: Case) -> (Union[_core_wf.IfBlock], typing.List[Promise]):
    expr, promises = transform_to_boolexpr(c.expr)
    n = c.output_promise.ref.node
    return _core_wf.IfBlock(condition=expr, then_node=n), promises


def to_ifelse_block(node_id: str, cs: ConditionalSection) -> (_core_wf.IfElseBlock, typing.List[Binding]):
    if len(cs.cases) == 0:
        raise AssertionError("Illegal Condition block, with no if-else cases")
    if len(cs.cases) < 2:
        raise AssertionError("Atleast an if/else is required. Dangling If is not allowed")
    all_promises: typing.List[Promise] = []
    first_case, promises = to_case_block(cs.cases[0])
    all_promises.extend(promises)
    other_cases: Optional[typing.List[_core_wf.IfBlock]] = None
    if len(cs.cases) > 2:
        other_cases = []
        for c in cs.cases[1:-1]:
            case, promises = to_case_block(c)
            all_promises.extend(promises)
            other_cases.append(case)
    last_case = cs.cases[-1]
    node = None
    err = None
    if last_case.output_promise is not None:
        node = last_case.output_promise.ref.node
    else:
        err = Error(failed_node_id=node_id, message=last_case.err if last_case.err else "Condition failed")
    return (
        _core_wf.IfElseBlock(case=first_case, other=other_cases, else_node=node, error=err),
        merge_promises(*all_promises),
    )


def to_branch_node(name: str, cs: ConditionalSection) -> (BranchNode, typing.List[Promise]):
    blocks, promises = to_ifelse_block(name, cs)
    return BranchNode(name=name, ifelse_block=blocks), promises


def conditional(name: str) -> ConditionalSection:
    return ConditionalSection(name)
