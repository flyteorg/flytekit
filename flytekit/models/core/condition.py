import flyteidl_rust as flyteidl

from flytekit.models import common as _common
from flytekit.models import literals as _literals


class ComparisonExpression(_common.FlyteIdlEntity):
    class Operator(object):
        """
        Binary Operator for each expression
        """

        EQ = flyteidl.comparison_expression.Operator.Eq
        NEQ = flyteidl.comparison_expression.Operator.Neq
        GT = flyteidl.comparison_expression.Operator.Gt
        GTE = flyteidl.comparison_expression.Operator.Gte
        LT = flyteidl.comparison_expression.Operator.Lt
        LTE = flyteidl.comparison_expression.Operator.Lte

    def __init__(self, operator, left_value, right_value):
        """
        Defines a 2-level tree where the root is a comparison operator and Operands are primitives or known variables.
        Each expression results in a boolean result.

        :param Operator operator:
        :param Operand left_value:
        :param Operand right_value:
        """
        self._operator = operator
        self._left_value = left_value
        self._right_value = right_value

    @property
    def operator(self):
        """
        Gets the operator representing this comparison expression.
        :rtype: ComparisonExpression.Operator
        """
        return self._operator

    @property
    def left_value(self):
        """
        Gets the left value for the comparison expression.
        :rtype: Operand
        """
        return self._left_value

    @property
    def right_value(self):
        """
        Gets the right value for the comparison expression.
        :rtype: Operand
        """
        return self._right_value

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.condition_pb2.ComparisonExpression
        """
        return flyteidl.core.ComparisonExpression(
            operator=int(self.operator),
            left_value=self.left_value.to_flyte_idl(),
            right_value=self.right_value.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            operator=pb2_object.operator,
            left_value=Operand.from_flyte_idl(pb2_object.left_value),
            right_value=Operand.from_flyte_idl(pb2_object.right_value),
        )


class ConjunctionExpression(_common.FlyteIdlEntity):
    class LogicalOperator(object):
        AND = flyteidl.conjunction_expression.LogicalOperator.And
        OR = flyteidl.conjunction_expression.LogicalOperator.Or

    def __init__(self, operator, left_expression, right_expression):
        """
        Defines a conjunction expression of two boolean expressions.
        :param LogicalOperator operator:
        :param BooleanExpression left_expression:
        :param BooleanExpression right_expression:
        """

        self._operator = operator
        self._left_expression = left_expression
        self._right_expression = right_expression

    @property
    def operator(self):
        """
        Gets the operator representing this conjunction expression.
        :rtype: ConjunctionExpression.LogicalOperator
        """
        return self._operator

    @property
    def left_expression(self):
        """
        Gets the left value for the conjunction expression.
        :rtype: Operand
        """
        return self._left_expression

    @property
    def right_expression(self):
        """
        Gets the right value for the conjunction expression.
        :rtype: Operand
        """
        return self._right_expression

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.condition_pb2.ConjunctionExpression
        """
        return flyteidl.core.ConjunctionExpression(
            operator=self.operator,
            left_expression=self.left_expression.to_flyte_idl(),
            right_expression=self.right_expression.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            operator=pb2_object.operator,
            left_expression=BooleanExpression.from_flyte_idl(pb2_object.left_expression),
            right_expression=BooleanExpression.from_flyte_idl(pb2_object.right_expression),
        )


class Operand(_common.FlyteIdlEntity):
    def __init__(self, primitive=None, var=None, scalar=None):
        """
        Defines an operand to a comparison expression.
        :param flytekit.models.literals.Primitive primitive: A primitive value
        :param Text var: A variable name
        :param flytekit.models.literals.Scalar scalar: A scalar value
        """

        self._primitive = primitive
        self._var = var
        self._scalar = scalar

    @property
    def primitive(self):
        """
        :rtype: flytekit.models.literals.Primitive
        """

        return self._primitive

    @property
    def var(self):
        """
        :rtype: Text
        """

        return self._var

    @property
    def scalar(self):
        """
        :rtype: flytekit.models.literals.Scalar
        """

        return self._scalar

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.condition_pb2.Operand
        """
        val = None
        if self.primitive:
            val = flyteidl.operand.Val.Primitive(self.primitive.to_flyte_idl())
        elif self.var:
            val = flyteidl.operand.Val.Var(self.var)
        elif self.scalar:
            val = flyteidl.operand.Val.Scalar(self.scalar.to_flyte_idl())

        return flyteidl.core.Operand(val=val)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            primitive=_literals.Primitive.from_flyte_idl(pb2_object.primitive)
            if pb2_object.HasField("primitive")
            else None,
            var=pb2_object.var if pb2_object.HasField("var") else None,
            scalar=_literals.Scalar.from_flyte_idl(pb2_object.scalar) if pb2_object.HasField("scalar") else None,
        )


class BooleanExpression(_common.FlyteIdlEntity):
    def __init__(self, conjunction=None, comparison=None):
        """
        Defines a boolean expression tree. It can be a simple or a conjunction expression.
        Multiple expressions can be combined using a conjunction or a disjunction to result in a final boolean result.

        :param ConjunctionExpression conjunction:
        :param ComparisonExpression comparison:
        """

        self._conjunction = conjunction
        self._comparison = comparison

    @property
    def conjunction(self):
        """
        Conjunction expression or None if not set.
        :rtype: ConjunctionExpression
        """
        return self._conjunction

    @property
    def comparison(self):
        """
        Comparison expression or None if not set.
        :rtype: ComparisonExpression
        """
        return self._comparison

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.condition_pb2.BooleanExpression
        """
        expr = None
        if self.conjunction:
            expr = flyteidl.boolean_expression.Expr.Conjunction(self.conjunction.to_flyte_idl())
        elif self.comparison:
            expr = flyteidl.boolean_expression.Expr.Comparison(self.comparison.to_flyte_idl())
        return flyteidl.core.BooleanExpression(expr=expr)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            conjunction=ConjunctionExpression.from_flyte_idl(pb2_object.conjunction)
            if pb2_object.conjunction
            else None,
            comparison=ComparisonExpression.from_flyte_idl(pb2_object.comparison) if pb2_object.comparison else None,
        )
