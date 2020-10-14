from typing import Union, Tuple, Generator

from flytekit.annotated.context_manager import FlyteContext, ExecutionState
from flytekit.annotated.promise import ConjunctionExpression, ComparisonExpression, Promise


class Case(object):
    def __init__(self, branch: 'ConditionalSection', expr: Union[ComparisonExpression, ConjunctionExpression]):
        self._conditional = branch
        self._expr = expr
        self._output_promise = None
        self._err = None

    # TODO this is complicated. We do not want this to run
    def then(self, p: Union[Promise, Tuple[Promise]]) -> Union['ConditionalSection', Promise]:
        self._output_promise = p
        # We can always mark branch as completed
        self._conditional._branch_complete()
        if self._conditional.is_last_condition:
            if self._conditional.selected_case._output_promise is None:
                raise BrokenPipeError("Bad conditional statements, did not resolve in a promise")
            else:
                return self._conditional.selected_case._output_promise
        return self._conditional

    def fail(self, err: str) -> Promise:
        self._err = err
        if self._conditional.is_last_condition:
            if self._conditional.selected_case._output_promise is None:
                raise AssertionError(err)
            return self._conditional.selected_case._output_promise


class ConditionalSection(object):
    def __init__(self):
        self._cases = []
        self._selected_case = None
        self._last_condition = False

        ctx = FlyteContext.current_context()
        self._exec_mode = ctx.execution_state.mode if ctx.execution_state else None

        if self._exec_mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            """
            In case of local workflow execution we should ensure a conditional section
            is created so that skipped branches result in tasks not being executed
            """
            ctx.enter_conditional_section()

    def __del__(self):
        # Its ok to always clean up the conditional section
        ctx = FlyteContext.current_context()
        ctx.exit_conditional_section()

    def _eval_take_branch(self, expr: Union[ComparisonExpression, ConjunctionExpression]):
        if self._exec_mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            if expr is None or expr.eval():
                if self._selected_case:
                    return
                ctx = FlyteContext.current_context()
                ctx.take_branch()
                self._selected_case = self._cases[-1]
        else:
            if self._selected_case is None:
                self._selected_case = self._cases[-1]

    def _branch_complete(self):
        if self._exec_mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            ctx = FlyteContext.current_context()
            return ctx.branch_complete()

    @property
    def is_last_condition(self):
        return self._last_condition

    @property
    def selected_case(self) -> Case:
        return self._selected_case

    def if_(self, expr: Union[ComparisonExpression, ConjunctionExpression]) -> Case:
        # TODO we should probably initialize a branching context here, so that the
        #      code does not get executed
        c = Case(branch=self, expr=expr)
        self._cases.append(c)
        self._eval_take_branch(expr)
        return c

    def elif_(self, expr: Union[ComparisonExpression, ConjunctionExpression]) -> Case:
        c = Case(branch=self, expr=expr)
        self._cases.append(c)
        self._eval_take_branch(expr)
        return c

    def else_(self) -> Case:
        c = Case(branch=self, expr=None)
        self._cases.append(c)
        self._last_condition = True
        self._eval_take_branch(None)
        return c


def conditional() -> ConditionalSection:
    return ConditionalSection()
