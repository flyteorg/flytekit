from typing import Union, Tuple

from flytekit.annotated.promise import ConjunctionExpression, ComparisonExpression, Promise


class Case(object):
    def __init__(self, branch: 'Branch', expr: Union[ComparisonExpression, ConjunctionExpression]):
        self._branch = branch
        self._expr = expr
        self._p = None
        self._err = None

    # TODO this is complicated. We do not want this to run
    def then(self, p: Union[Promise, Tuple[Promise]]) -> 'Branch':
        self._p = p
        return self._branch

    def fail(self, err: str) -> 'Branch':
        self._err = err


class Branch(object):
    def __init__(self):
        self._cases = []

    def _if(self, expr: Union[ComparisonExpression, ConjunctionExpression]) -> Case:
        # TODO we should probably initialize a branching context here, so that the
        #      code does not get executed
        c = Case(branch=self, expr=expr)
        self._cases.append(c)
        return c

    def branch_elif(self, expr: Union[ComparisonExpression, ConjunctionExpression]) -> Case:
        c = Case(branch=self, expr=expr)
        self._cases.append(c)
        return c

    def branch_else(self) -> Case:
        c = Case(branch=self, expr=None)
        self._cases.append(c)
        return c


def branch_if(expr: Union[ComparisonExpression, ConjunctionExpression]) -> Case:
    return Branch()._if(expr)
