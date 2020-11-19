import collections
import typing
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from flytekit.annotated import context_manager as _flyte_context
from flytekit.annotated import type_engine
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.type_engine import DictTransformer, ListTransformer, TypeEngine
from flytekit.common.promise import NodeOutput as _NodeOutput
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models import literals as _literals_models
from flytekit.models import types as _type_models
from flytekit.models.literals import Primitive


def translate_inputs_to_literals(
    ctx: FlyteContext,
    input_kwargs: Dict[str, Any],
    interface: _interface_models.TypedInterface,
    native_input_types: Dict[str, type],
) -> Dict[str, _literal_models.Literal]:
    """
    When calling a task inside a workflow, a user might do something like this.

        def my_wf(in1: int) -> int:
            a = task_1(in1=in1)
            b = task_2(in1=5, in2=a)
            return b

    If this is the case, when task_2 is called in local workflow execution, we'll need to translate the Python native
    literal 5 to a Flyte literal.

    More interesting is this:

        def my_wf(in1: int, in2: int) -> int:
            a = task_1(in1=in1)
            b = task_2(in1=5, in2=[a, in2])
            return b

    Here, in task_2, during execution we'd have a list of Promises. We have to make sure to give task2 a Flyte
    LiteralCollection (Flyte's name for list), not a Python list of Flyte literals.
    """

    def extract_value(
        ctx: FlyteContext, input_val: Any, val_type: type, flyte_literal_type: _type_models.LiteralType
    ) -> _literal_models.Literal:
        if isinstance(input_val, list):
            if flyte_literal_type.collection_type is None:
                raise Exception(f"Not a collection type {flyte_literal_type} but got a list {input_val}")
            try:
                sub_type = ListTransformer.get_sub_type(val_type)
            except ValueError:
                if len(input_val) == 0:
                    raise
                sub_type = type(input_val[0])
            literals = [extract_value(ctx, v, sub_type, flyte_literal_type.collection_type) for v in input_val]
            return _literal_models.Literal(collection=_literal_models.LiteralCollection(literals=literals))
        elif isinstance(input_val, dict):
            if (
                flyte_literal_type.map_value_type is None
                and flyte_literal_type.simple != _type_models.SimpleType.STRUCT
            ):
                raise Exception(f"Not a map type {flyte_literal_type} but got a map {input_val}")
            k_type, sub_type = DictTransformer.get_dict_types(val_type)
            if flyte_literal_type.simple == _type_models.SimpleType.STRUCT:
                return TypeEngine.to_literal(ctx, input_val, type(input_val), flyte_literal_type)
            else:
                literals = {
                    k: extract_value(ctx, v, sub_type, flyte_literal_type.map_value_type) for k, v in input_val.items()
                }
                return _literal_models.Literal(map=_literal_models.LiteralMap(literals=literals))
        elif isinstance(input_val, Promise):
            # In the example above, this handles the "in2=a" type of argument
            return input_val.val
        else:
            # This handles native values, the 5 example
            return TypeEngine.to_literal(ctx, input_val, val_type, flyte_literal_type)

    result = {}  # So as to not overwrite the input_kwargs
    for k, v in input_kwargs.items():
        if k not in interface.inputs:
            raise ValueError(f"Received unexpected keyword argument {k}")
        var = interface.inputs[k]
        t = native_input_types[k]
        result[k] = extract_value(ctx, v, t, var.type)

    return result


def get_primitive_val(prim: Primitive) -> Any:
    if prim.integer:
        return prim.integer
    if prim.datetime:
        return prim.datetime
    if prim.boolean:
        return prim.boolean
    if prim.duration:
        return prim.duration
    if prim.string_value:
        return prim.string_value
    return prim.float_value


class ConjunctionOps(Enum):
    AND = "and"
    OR = "or"


class ComparisonOps(Enum):
    EQ = "=="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="


_comparators = {
    ComparisonOps.EQ: lambda x, y: x == y,
    ComparisonOps.NE: lambda x, y: x != y,
    ComparisonOps.GT: lambda x, y: x > y,
    ComparisonOps.GE: lambda x, y: x >= y,
    ComparisonOps.LT: lambda x, y: x < y,
    ComparisonOps.LE: lambda x, y: x <= y,
}


class ComparisonExpression(object):
    def __init__(self, lhs: Union["Promise", Any], op: ComparisonOps, rhs: Union["Promise", Any]):
        self._op = op
        self._lhs = None
        self._rhs = None
        if isinstance(lhs, Promise):
            self._lhs = lhs
            if lhs.is_ready:
                if lhs.val.scalar is None or lhs.val.scalar.primitive is None:
                    raise ValueError("Only primitive values can be used in comparison")
        if isinstance(rhs, Promise):
            self._rhs = rhs
            if rhs.is_ready:
                if rhs.val.scalar is None or rhs.val.scalar.primitive is None:
                    raise ValueError("Only primitive values can be used in comparison")
        if self._lhs is None:
            self._lhs = type_engine.TypeEngine.to_literal(FlyteContext.current_context(), lhs, type(lhs), None)
        if self._rhs is None:
            self._rhs = type_engine.TypeEngine.to_literal(FlyteContext.current_context(), rhs, type(rhs), None)

    @property
    def rhs(self) -> Union["Promise", _literal_models.Literal]:
        return self._rhs

    @property
    def lhs(self) -> Union["Promise", _literal_models.Literal]:
        return self._lhs

    @property
    def op(self) -> ComparisonOps:
        return self._op

    def eval(self) -> bool:
        if isinstance(self.lhs, Promise):
            lhs = self.lhs.eval()
        else:
            lhs = get_primitive_val(self.lhs.scalar.primitive)

        if isinstance(self.rhs, Promise):
            rhs = self.rhs.eval()
        else:
            rhs = get_primitive_val(self.rhs.scalar.primitive)

        return _comparators[self.op](lhs, rhs)

    def __and__(self, other):
        print("Comparison AND called")
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.AND, rhs=other)

    def __or__(self, other):
        print("Comparison OR called")
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.OR, rhs=other)

    def __bool__(self):
        raise ValueError(
            "Cannot perform truth value testing,"
            " This is a limitation in python. For Logical `and\\or` use `&\\|` (bitwise) instead"
        )

    def __repr__(self):
        s = "Comp( "
        s += f"{self._lhs}"
        s += f" {self._op.value} "
        s += f"({self._rhs},{self._rhs})"
        return s


class ConjunctionExpression(object):
    def __init__(
        self,
        lhs: Union[ComparisonExpression, "ConjunctionExpression"],
        op: ConjunctionOps,
        rhs: Union[ComparisonExpression, "ConjunctionExpression"],
    ):
        self._lhs = lhs
        self._rhs = rhs
        self._op = op

    @property
    def rhs(self) -> Union[ComparisonExpression, "ConjunctionExpression"]:
        return self._rhs

    @property
    def lhs(self) -> Union[ComparisonExpression, "ConjunctionExpression"]:
        return self._lhs

    @property
    def op(self) -> ConjunctionOps:
        return self._op

    def eval(self) -> bool:
        l_eval = self.lhs.eval()
        if self.op == ConjunctionOps.AND and l_eval is False:
            return False

        if self.op == ConjunctionOps.OR and l_eval is True:
            return True

        r_eval = self.lhs.eval()
        if self.op == ConjunctionOps.AND:
            return l_eval and r_eval

        return l_eval or r_eval

    def __and__(self, other: Union[ComparisonExpression, "ConjunctionExpression"]):
        print("Conj AND called")
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.AND, rhs=other)

    def __or__(self, other: Union[ComparisonExpression, "ConjunctionExpression"]):
        print("Conj OR called")
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.OR, rhs=other)

    def __bool__(self):
        raise ValueError(
            "Cannot perform truth value testing,"
            " This is a limitation in python. For Logical `and\\or` use `&\\|` (bitwise) instead"
        )

    def __repr__(self):
        return f"( {self._lhs} {self._op} {self._rhs} )"


# TODO: The NodeOutput object, which this Promise wraps, has an sdk_type. Since we're no longer using sdk types,
#  we should consider adding a literal type to this object as well for downstream checking when Bindings are created.
class Promise(object):
    # TODO: Currently, NodeOutput we're creating is the slimmer annotated package Node class, but since only the
    #  id is used, it's okay for now. Let's clean all this up though.
    def __init__(self, var: str, val: Union[_NodeOutput, _literal_models.Literal]):
        self._var = var
        self._promise_ready = True
        self._val = val
        if val and isinstance(val, _NodeOutput):
            self._ref = val
            self._promise_ready = False
            self._val = None

    def with_var(self, new_var: str) -> "Promise":
        if self.is_ready:
            return Promise(var=new_var, val=self.val)
        return Promise(var=new_var, val=self.ref)

    @property
    def is_ready(self) -> bool:
        """
        Returns if the Promise is READY (is not a reference and the val is actually ready)
        Usage:
           p = Promise(...)
           ...
           if p.is_ready():
                print(p.val)
           else:
                print(p.ref)
        """
        return self._promise_ready

    @property
    def val(self) -> _literal_models.Literal:
        """
        If the promise is ready then this holds the actual evaluate value in Flyte's type system
        """
        return self._val

    @property
    def ref(self) -> _NodeOutput:
        """
        If the promise is NOT READY / Incomplete, then it maps to the origin node that owns the promise
        """
        return self._ref

    @property
    def var(self) -> str:
        """
        Name of the variable bound with this promise
        """
        return self._var

    def eval(self) -> Any:
        if not self._promise_ready or self._val is None:
            raise ValueError("Cannot Eval with incomplete promises")
        if self.val.scalar is None or self.val.scalar.primitive is None:
            raise ValueError("Eval can be invoked for primitive types only")
        return get_primitive_val(self.val.scalar.primitive)

    def __eq__(self, other) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.EQ, other)

    def __ne__(self, other) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.NE, other)

    def __gt__(self, other) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.GT, other)

    def __ge__(self, other) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.GE, other)

    def __lt__(self, other) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.LT, other)

    def __le__(self, other) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.LE, other)

    def __bool__(self):
        raise ValueError(
            "Cannot perform truth value testing,"
            " This is a limitation in python. For Logical `and\\or` use `&\\|` (bitwise) instead"
        )

    def __and__(self, other):
        raise ValueError("Cannot perform Logical AND of Promise with other")

    def __or__(self, other):
        raise ValueError("Cannot perform Logical OR of Promise with other")

    def with_overrides(self, *args, **kwargs):
        if not self.is_ready:
            # TODO, this should be forwarded, but right now this results in failure and we want to test this behavior
            # self.ref.sdk_node.with_overrides(*args, **kwargs)
            print(f"Forwarding to node {self.ref.sdk_node.id}")
            self.ref.sdk_node.with_overrides(*args, **kwargs)
        return self

    def __repr__(self):
        if self._promise_ready:
            return f"Var({self._var}={self._val})"
        return f"Promise({self._var},{self.ref.node_id})"

    def __str__(self):
        return str(self.__repr__())


# To create a class that is a named tuple, we might have to create namedtuplemeta and manipulate the tuple
def create_task_output(promises: Optional[Union[List[Promise], Promise]]) -> Optional[Union[Tuple[Promise], Promise]]:
    if promises is None:
        return None

    if isinstance(promises, Promise):
        return promises

    if len(promises) == 0:
        return None

    if len(promises) == 1:
        return promises[0]

    # More than one promises, let us wrap it into a tuple
    variables = [p.var for p in promises]

    class Output(collections.namedtuple("TaskOutput", variables)):
        def with_overrides(self, *args, **kwargs):
            val = self.__getattribute__(self._fields[0])
            val.with_overrides(*args, **kwargs)
            return self

    return Output(*promises)


def binding_data_from_python_std(
    ctx: _flyte_context.FlyteContext,
    expected_literal_type: _type_models.LiteralType,
    t_value: typing.Any,
    t_value_type: type,
) -> _literals_models.BindingData:
    # This handles the case where the given value is the output of another task
    if isinstance(t_value, Promise):
        if not t_value.is_ready:
            return _literals_models.BindingData(promise=t_value.ref)

    elif isinstance(t_value, list):
        if expected_literal_type.collection_type is None:
            raise AssertionError(f"this should be a list and it is not: {type(t_value)} vs {expected_literal_type}")

        sub_type = ListTransformer.get_sub_type(t_value_type)
        collection = _literals_models.BindingDataCollection(
            bindings=[
                binding_data_from_python_std(ctx, expected_literal_type.collection_type, t, sub_type) for t in t_value
            ]
        )

        return _literals_models.BindingData(collection=collection)

    elif isinstance(t_value, dict):
        if (
            expected_literal_type.map_value_type is None
            and expected_literal_type.simple != _type_models.SimpleType.STRUCT
        ):
            raise AssertionError(
                f"this should be a Dictionary type and it is not: {type(t_value)} vs {expected_literal_type}"
            )
        k_type, v_type = DictTransformer.get_dict_types(t_value_type)
        if expected_literal_type.simple == _type_models.SimpleType.STRUCT:
            lit = TypeEngine.to_literal(ctx, t_value, type(t_value), expected_literal_type)
            return _literals_models.BindingData(scalar=lit.scalar)
        else:
            m = _literals_models.BindingDataMap(
                bindings={
                    k: binding_data_from_python_std(ctx, expected_literal_type.map_value_type, v, v_type)
                    for k, v in t_value.items()
                }
            )

        return _literals_models.BindingData(map=m)

    # This is the scalar case - e.g. my_task(in1=5)
    scalar = TypeEngine.to_literal(ctx, t_value, t_value_type, expected_literal_type).scalar
    return _literals_models.BindingData(scalar=scalar)


def binding_from_python_std(
    ctx: _flyte_context.FlyteContext,
    var_name: str,
    expected_literal_type: _type_models.LiteralType,
    t_value: typing.Any,
    t_value_type: type,
) -> _literals_models.Binding:
    binding_data = binding_data_from_python_std(ctx, expected_literal_type, t_value, t_value_type)
    return _literals_models.Binding(var=var_name, binding=binding_data)


def to_binding(p: Promise) -> _literals_models.Binding:
    return _literals_models.Binding(var=p.var, binding=_literals_models.BindingData(promise=p.ref))
