from __future__ import annotations

import collections
import datetime
import typing
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from flytekit.common import constants as _common_constants
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.core import context_manager as _flyte_context
from flytekit.core import interface as flyte_interface
from flytekit.core import type_engine
from flytekit.core.context_manager import FlyteContext
from flytekit.core.interface import Interface
from flytekit.core.node import Node
from flytekit.core.type_engine import DictTransformer, ListTransformer, TypeEngine
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models import literals as _literals_models
from flytekit.models import types as _type_models
from flytekit.models import types as type_models
from flytekit.models.core import workflow as _workflow_model
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
        elif isinstance(input_val, VoidPromise):
            raise AssertionError(
                f"Outputs of a non-output producing task {input_val.task_name} cannot be passed to another task."
            )
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

        r_eval = self.rhs.eval()
        if self.op == ConjunctionOps.AND:
            return l_eval and r_eval

        return l_eval or r_eval

    def __and__(self, other: Union[ComparisonExpression, "ConjunctionExpression"]):
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.AND, rhs=other)

    def __or__(self, other: Union[ComparisonExpression, "ConjunctionExpression"]):
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.OR, rhs=other)

    def __bool__(self):
        raise ValueError(
            "Cannot perform truth value testing,"
            " This is a limitation in python. For Logical `and\\or` use `&\\|` (bitwise) instead. Refer to: PEP-335"
        )

    def __repr__(self):
        return f"( {self._lhs} {self._op} {self._rhs} )"


# TODO: The NodeOutput object, which this Promise wraps, has an sdk_type. Since we're no longer using sdk types,
#  we should consider adding a literal type to this object as well for downstream checking when Bindings are created.
class Promise(object):
    # TODO: Currently, NodeOutput we're creating is the slimmer core package Node class, but since only the
    #  id is used, it's okay for now. Let's clean all this up though.
    def __init__(self, var: str, val: Union[NodeOutput, _literal_models.Literal]):
        self._var = var
        self._promise_ready = True
        self._val = val
        if val and isinstance(val, NodeOutput):
            self._ref = val
            self._promise_ready = False
            self._val = None

    def with_var(self, new_var: str) -> Promise:
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
    def ref(self) -> NodeOutput:
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

    def is_(self, v: bool) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.EQ, v)

    def is_false(self) -> ComparisonExpression:
        return self.is_(False)

    def is_true(self):
        return self.is_(True)

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
            "Flytekit does not support Unary expressions or performing truth value testing,"
            " This is a limitation in python. For Logical `and\\or` use `&\\|` (bitwise) instead"
        )

    def __and__(self, other):
        raise ValueError("Cannot perform Logical AND of Promise with other")

    def __or__(self, other):
        raise ValueError("Cannot perform Logical OR of Promise with other")

    def with_overrides(self, *args, **kwargs):
        if not self.is_ready:
            # TODO, this should be forwarded, but right now this results in failure and we want to test this behavior
            print(f"Forwarding to node {self.ref.node.id}")
            self.ref.node.with_overrides(*args, **kwargs)
        return self

    def __repr__(self):
        if self._promise_ready:
            return f"Var({self._var}={self._val})"
        return f"Promise(node:{self.ref.node_id}.{self._var})"

    def __str__(self):
        return str(self.__repr__())


# To create a class that is a named tuple, we might have to create namedtuplemeta and manipulate the tuple
def create_task_output(
    promises: Optional[Union[List[Promise], Promise]], entity_interface: Optional[Interface] = None
) -> Optional[Union[Tuple[Promise], Promise]]:
    # TODO: Add VoidPromise here to simplify things at call site. Consider returning for [] below as well instead of
    #   raising an exception.
    if promises is None:
        return None

    if isinstance(promises, Promise):
        return promises

    if len(promises) == 0:
        raise Exception(
            "This function should not be called with an empty list. It should have been handled with a"
            "VoidPromise at this function's call-site."
        )

    if len(promises) == 1:
        if not entity_interface:
            return promises[0]
        # See transform_signature_to_interface for more information, we're using the existence of a name as a proxy
        # for the user having specified a one-element typing.NamedTuple, which means we should _not_ extract it. We
        # should still return a tuple but it should be one of ours.
        if not entity_interface.output_tuple_name:
            return promises[0]

    # More than one promise, let us wrap it into a tuple
    # Start with just the var names in the promises
    variables = [p.var for p in promises]

    # These should be OrderedDicts so it should be safe to iterate over the keys.
    if entity_interface:
        variables = [k for k in entity_interface.outputs.keys()]

    named_tuple_name = "DefaultNamedTupleOutput"
    if entity_interface and entity_interface.output_tuple_name:
        named_tuple_name = entity_interface.output_tuple_name

    # Should this class be part of the Interface?
    class Output(collections.namedtuple(named_tuple_name, variables)):
        def with_overrides(self, *args, **kwargs):
            val = self.__getattribute__(self._fields[0])
            val.with_overrides(*args, **kwargs)
            return self

        @property
        def ref(self):
            for p in promises:
                if p.ref:
                    return p.ref
            return None

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

    elif isinstance(t_value, VoidPromise):
        raise AssertionError(
            f"Cannot pass output from task {t_value.task_name} that produces no outputs to a downstream task"
        )

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


class VoidPromise(object):
    """
    This object is returned for tasks that do not return any outputs (declared interface is empty)
    VoidPromise cannot be interacted with and does not allow comparisons or any operations
    """

    def __init__(self, task_name: str):
        self._task_name = task_name

    def runs_before(self, *args, **kwargs):
        """
        This is a placeholder and should do nothing. It is only here to enable local execution of workflows
        where a task returns nothing.
        """

    def __rshift__(self, *args, **kwargs):
        ...  # See runs_before

    @property
    def task_name(self):
        return self._task_name

    def __eq__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __and__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __or__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __le__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __ge__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __gt__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __lt__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __add__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __cmp__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __bool__(self):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __mod__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __xor__(self, other):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __str__(self):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")

    def __repr__(self):
        raise AssertionError(f"Task {self._task_name} returns nothing, NoneType return cannot be used")


class NodeOutput(type_models.OutputReference):
    def __init__(self, node: Node, var: str):
        """
        :param node:
        :param var: The name of the variable this NodeOutput references
        """
        self._node = node
        super(NodeOutput, self).__init__(self._node.id, var)

    @property
    def node_id(self):
        """
        Override the underlying node_id property to refer to SdkNode.
        :rtype: Text
        """
        return self.node.id

    @property
    def node(self) -> Node:
        """
        """
        return self._node

    def __repr__(self) -> str:
        s = f"Node({self.node if self.node.id is not None else None}:{self.var})"
        return s


# TODO we should accept TaskMetadata here and then extract whatever fields we want into NodeMetadata
def create_and_link_node(
    ctx: FlyteContext,
    entity,
    interface: flyte_interface.Interface,
    timeout: Optional[datetime.timedelta] = None,
    retry_strategy: Optional[_literal_models.RetryStrategy] = None,
    **kwargs,
):
    """
    This method is used to generate a node with bindings. This is not used in the execution path.
    """
    if ctx.compilation_state is None:
        raise _user_exceptions.FlyteAssertion("Cannot create node when not compiling...")

    used_inputs = set()
    bindings = []

    typed_interface = flyte_interface.transform_interface_to_typed_interface(interface)

    for k in sorted(interface.inputs):
        var = typed_interface.inputs[k]
        if k not in kwargs:
            raise _user_exceptions.FlyteAssertion("Input was not specified for: {} of type {}".format(k, var.type))
        v = kwargs[k]
        # This check ensures that tuples are not passed into a function, as tuples are not supported by Flyte
        # Usually a Tuple will indicate that multiple outputs from a previous task were accidentally passed
        # into the function.
        if isinstance(v, tuple):
            raise AssertionError(
                f"Variable({k}) for function({entity.name}) cannot receive a multi-valued tuple {v}."
                f" Check if the predecessor function returning more than one value?"
            )
        bindings.append(
            binding_from_python_std(
                ctx, var_name=k, expected_literal_type=var.type, t_value=v, t_value_type=interface.inputs[k]
            )
        )
        used_inputs.add(k)

    extra_inputs = used_inputs ^ set(kwargs.keys())
    if len(extra_inputs) > 0:
        raise _user_exceptions.FlyteAssertion(
            "Too many inputs were specified for the interface.  Extra inputs were: {}".format(extra_inputs)
        )

    # Detect upstream nodes
    # These will be our core Nodes until we can amend the Promise to use NodeOutputs that reference our Nodes
    upstream_nodes = list(
        set(
            [
                input_val.ref.node
                for input_val in kwargs.values()
                if isinstance(input_val, Promise) and input_val.ref.node_id != _common_constants.GLOBAL_INPUT_NODE_ID
            ]
        )
    )

    node_metadata = _workflow_model.NodeMetadata(
        f"{entity.__module__}.{entity.name}",
        timeout or datetime.timedelta(),
        retry_strategy or _literal_models.RetryStrategy(0),
    )

    non_sdk_node = Node(
        # TODO: Better naming, probably a derivative of the function name.
        id=f"{ctx.compilation_state.prefix}n{len(ctx.compilation_state.nodes)}",
        metadata=node_metadata,
        bindings=sorted(bindings, key=lambda b: b.var),
        upstream_nodes=upstream_nodes,
        flyte_entity=entity,
    )
    ctx.compilation_state.add_node(non_sdk_node)

    if len(typed_interface.outputs) == 0:
        return VoidPromise(entity.name)

    # Create a node output object for each output, they should all point to this node of course.
    node_outputs = []
    for output_name, output_var_model in typed_interface.outputs.items():
        # TODO: If node id gets updated later, we have to make sure to update the NodeOutput model's ID, which
        #  is currently just a static str
        node_outputs.append(Promise(output_name, NodeOutput(node=non_sdk_node, var=output_name)))
        # Don't print this, it'll crash cuz sdk_node._upstream_node_ids might be None, but idl code will break

    return create_task_output(node_outputs, interface)
