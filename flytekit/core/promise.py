from __future__ import annotations

import collections
import datetime
import inspect
import typing
from copy import deepcopy
from enum import Enum
from typing import Any, Coroutine, Dict, List, Optional, Set, Tuple, Union, cast, get_args

from google.protobuf import struct_pb2 as _struct
from typing_extensions import Protocol

from flytekit.core import constants as _common_constants
from flytekit.core import context_manager as _flyte_context
from flytekit.core import interface as flyte_interface
from flytekit.core import type_engine
from flytekit.core.context_manager import (
    BranchEvalMode,
    ExecutionParameters,
    ExecutionState,
    FlyteContext,
    FlyteContextManager,
    OutputMetadataTracker,
)
from flytekit.core.interface import Interface
from flytekit.core.node import Node
from flytekit.core.type_engine import (
    DictTransformer,
    ListTransformer,
    TypeEngine,
    TypeTransformerFailedError,
    UnionTransformer,
)
from flytekit.exceptions import user as _user_exceptions
from flytekit.exceptions.user import FlytePromiseAttributeResolveException
from flytekit.extras.accelerators import BaseAccelerator
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literals_models
from flytekit.models import types as _type_models
from flytekit.models import types as type_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.literals import Primitive
from flytekit.models.task import Resources
from flytekit.models.types import SimpleType


def translate_inputs_to_literals(
    ctx: FlyteContext,
    incoming_values: Dict[str, Any],
    flyte_interface_types: Dict[str, _interface_models.Variable],
    native_types: Dict[str, type],
) -> Dict[str, _literals_models.Literal]:
    """
    The point of this function is to extract out Literals from a collection of either Python native values (which would
    be converted into Flyte literals) or Promises (the literals in which would just get extracted).

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

    This helper function is used both when sorting out inputs to a task, as well as outputs of a function.

    :param ctx: Context needed in case a non-primitive literal needs to be translated to a Flyte literal (like a file)
    :param incoming_values: This is a map of your task's input or wf's output kwargs basically
    :param flyte_interface_types: One side of an :py:class:`flytekit.models.interface.TypedInterface` basically.
    :param native_types: Map to native Python type.
    """
    if incoming_values is None:
        raise AssertionError("Incoming values cannot be None, must be a dict")

    result = {}  # So as to not overwrite the input_kwargs
    for k, v in incoming_values.items():
        if k not in flyte_interface_types:
            raise AssertionError(f"Received unexpected keyword argument {k}")
        var = flyte_interface_types[k]
        t = native_types[k]
        try:
            if type(v) is Promise:
                v = resolve_attr_path_in_promise(v)
            result[k] = TypeEngine.to_literal(ctx, v, t, var.type)
        except TypeTransformerFailedError as exc:
            raise TypeTransformerFailedError(f"Failed argument '{k}': {exc}") from None

    return result


def resolve_attr_path_in_promise(p: Promise) -> Promise:
    """
    resolve_attr_path_in_promise resolves the attribute path in a promise and returns a new promise with the resolved value
    This is for local execution only. The remote execution will be resolved in flytepropeller.
    """

    curr_val = p.val

    used = 0

    for attr in p.attr_path:
        # If current value is Flyte literal collection (list) or map (dictionary), use [] to resolve
        if (
            type(curr_val.value) is _literals_models.LiteralMap
            or type(curr_val.value) is _literals_models.LiteralCollection
        ):
            if type(attr) == str and attr not in curr_val.value.literals:
                raise FlytePromiseAttributeResolveException(
                    f"Failed to resolve attribute path {p.attr_path} in promise {p},"
                    f" attribute {attr} not found in {curr_val.value.literals.keys()}"
                )

            if type(attr) == int and attr >= len(curr_val.value.literals):
                raise FlytePromiseAttributeResolveException(
                    f"Failed to resolve attribute path {p.attr_path} in promise {p},"
                    f" index {attr} out of range {len(curr_val.value.literals)}"
                )

            curr_val = curr_val.value.literals[attr]
            used += 1
        # Scalar is always the leaf. There can't be a collection or map in a scalar.
        if type(curr_val.value) is _literals_models.Scalar:
            break

    # If the current value is a dataclass, resolve the dataclass with the remaining path
    if (
        len(p.attr_path) > 0
        and type(curr_val.value) is _literals_models.Scalar
        and type(curr_val.value.value) is _struct.Struct
    ):
        st = curr_val.value.value
        new_st = resolve_attr_path_in_pb_struct(st, attr_path=p.attr_path[used:])
        literal_type = TypeEngine.to_literal_type(type(new_st))
        # Reconstruct the resolved result to flyte literal (because the resolved result might not be struct)
        curr_val = TypeEngine.to_literal(FlyteContextManager.current_context(), new_st, type(new_st), literal_type)

    p._val = curr_val
    return p


def resolve_attr_path_in_pb_struct(st: _struct.Struct, attr_path: List[Union[str, int]]) -> _struct.Struct:
    curr_val = st
    for attr in attr_path:
        if attr not in curr_val:
            raise FlytePromiseAttributeResolveException(
                f"Failed to resolve attribute path {attr_path} in struct {curr_val}, attribute {attr} not found"
            )
        curr_val = curr_val[attr]
    return curr_val


def get_primitive_val(prim: Primitive) -> Any:
    for value in [
        prim.integer,
        prim.float_value,
        prim.string_value,
        prim.boolean,
        prim.datetime,
        prim.duration,
    ]:
        if value is not None:
            return value


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
    """
    ComparisonExpression refers to an expression of the form (lhs operator rhs), where lhs and rhs are operands
    and operator can be any comparison expression like <, >, <=, >=, ==, !=
    """

    def __init__(self, lhs: Union["Promise", Any], op: ComparisonOps, rhs: Union["Promise", Any]):
        self._op = op
        self._lhs = None
        self._rhs = None
        if isinstance(lhs, Promise):
            self._lhs = lhs
            if lhs.is_ready:
                if lhs.val.scalar is None or lhs.val.scalar.primitive is None:
                    union = lhs.val.scalar.union
                    if union and union.value.scalar:
                        if union.value.scalar.primitive or union.value.scalar.none_type:
                            self._lhs = union.value
                        else:
                            raise ValueError("Only primitive values can be used in comparison")
                    else:
                        raise ValueError("Only primitive values can be used in comparison")
        if isinstance(rhs, Promise):
            self._rhs = rhs
            if rhs.is_ready:
                if rhs.val.scalar is None or rhs.val.scalar.primitive is None:
                    union = rhs.val.scalar.union
                    if union and union.value.scalar:
                        if union.value.scalar.primitive or union.value.scalar.none_type:
                            self._rhs = union.value
                        else:
                            raise ValueError("Only primitive values can be used in comparison")
                    else:
                        raise ValueError("Only primitive values can be used in comparison")
        if self._lhs is None:
            self._lhs = type_engine.TypeEngine.to_literal(FlyteContextManager.current_context(), lhs, type(lhs), None)
        if self._rhs is None:
            self._rhs = type_engine.TypeEngine.to_literal(FlyteContextManager.current_context(), rhs, type(rhs), None)

    @property
    def rhs(self) -> Union["Promise", _literals_models.Literal]:
        return self._rhs

    @property
    def lhs(self) -> Union["Promise", _literals_models.Literal]:
        return self._lhs

    @property
    def op(self) -> ComparisonOps:
        return self._op

    def eval(self) -> bool:
        if isinstance(self.lhs, Promise):
            lhs = self.lhs.eval()
        elif self.lhs.scalar.none_type:
            lhs = None
        else:
            lhs = get_primitive_val(self.lhs.scalar.primitive)

        if isinstance(self.rhs, Promise):
            rhs = self.rhs.eval()
        elif self.rhs.scalar.none_type:
            rhs = None
        else:
            rhs = get_primitive_val(self.rhs.scalar.primitive)

        return _comparators[self.op](lhs, rhs)

    def __and__(self, other):
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.AND, rhs=other)

    def __or__(self, other):
        return ConjunctionExpression(lhs=self, op=ConjunctionOps.OR, rhs=other)

    def __bool__(self):
        raise ValueError(
            "Cannot perform truth value testing,"
            " This is a limitation in python. For Logical `and\\or` use `&\\|` (bitwise) instead."
            f" Expr {self}"
        )

    def __repr__(self):
        return f"Comp({self._lhs} {self._op.value} {self._rhs})"

    def __str__(self):
        return self.__repr__()


class ConjunctionExpression(object):
    """
    A Conjunction Expression is an expression of the form either (A and B) or (A or B).
    where A, B are two expressions (comparison or conjunctions) and (and, or) are logical truth operators.

    A conjunctionExpression evaluates to True or False depending on the logical operator and the truth values of
    each of the expressions A & B
    """

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

    def __str__(self):
        return self.__repr__()


# TODO: The NodeOutput object, which this Promise wraps, has an sdk_type. Since we're no longer using sdk types,
#  we should consider adding a literal type to this object as well for downstream checking when Bindings are created.
class Promise(object):
    """
    This object is a wrapper and exists for three main reasons. Let's assume we're dealing with a task like ::

        @task
        def t1() -> (int, str): ...

    #. Handling the duality between compilation and local execution - when the task function is run in a local execution
       mode inside a workflow function, a Python integer and string are produced. When the task is being compiled as
       part of the workflow, the task call creates a Node instead, and the task returns two Promise objects that
       point to that Node.
    #. One needs to be able to call ::

          x = t1().with_overrides(...)

       If the task returns an integer or a ``(int, str)`` tuple like ``t1`` above, calling ``with_overrides`` on the
       result would throw an error. This Promise object adds that.
    #. Assorted handling for conditionals.
    """

    # TODO: Currently, NodeOutput we're creating is the slimmer core package Node class, but since only the
    #  id is used, it's okay for now. Let's clean all this up though.
    def __init__(
        self,
        var: str,
        val: Union[NodeOutput, _literals_models.Literal],
        type: typing.Optional[_type_models.LiteralType] = None,
    ):
        self._var = var
        self._promise_ready = True
        self._val = val
        self._ref = None
        self._attr_path: List[Union[str, int]] = []
        self._type = type
        if val and isinstance(val, NodeOutput):
            self._ref = val
            self._promise_ready = False
            self._val = None

    def __hash__(self):
        return hash(id(self))

    def __rshift__(self, other: Union[Promise, VoidPromise]):
        if not self.is_ready and other.ref:
            self.ref.node.runs_before(other.ref.node)
        return other

    def with_var(self, new_var: str) -> Promise:
        if self.is_ready:
            return Promise(var=new_var, val=self.val)
        return Promise(var=new_var, val=self.ref)

    @property
    def is_ready(self) -> bool:
        """
        Returns if the Promise is READY (is not a reference and the val is actually ready)

        Usage ::

           p = Promise(...)
           ...
           if p.is_ready():
                print(p.val)
           else:
                print(p.ref)
        """
        return self._promise_ready

    @property
    def val(self) -> _literals_models.Literal:
        """
        If the promise is ready then this holds the actual evaluate value in Flyte's type system
        """
        return self._val

    @property
    def ref(self) -> NodeOutput:
        """
        If the promise is NOT READY / Incomplete, then it maps to the origin node that owns the promise
        """
        return self._ref  # type: ignore

    @property
    def var(self) -> str:
        """
        Name of the variable bound with this promise
        """
        return self._var

    @property
    def attr_path(self) -> List[Union[str, int]]:
        """
        The attribute path the promise will be resolved with.
        :rtype: List[Union[str, int]]
        """
        return self._attr_path

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

    def is_true(self) -> ComparisonExpression:
        return self.is_(True)

    def is_none(self) -> ComparisonExpression:
        return ComparisonExpression(self, ComparisonOps.EQ, None)

    def __eq__(self, other) -> ComparisonExpression:  # type: ignore
        return ComparisonExpression(self, ComparisonOps.EQ, other)

    def __ne__(self, other) -> ComparisonExpression:  # type: ignore
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

    def with_overrides(
        self,
        node_name: Optional[str] = None,
        aliases: Optional[Dict[str, str]] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
        retries: Optional[int] = None,
        interruptible: Optional[bool] = None,
        name: Optional[str] = None,
        task_config: Optional[Any] = None,
        container_image: Optional[str] = None,
        accelerator: Optional[BaseAccelerator] = None,
        cache: Optional[bool] = None,
        cache_version: Optional[str] = None,
        cache_serialize: Optional[bool] = None,
        *args,
        **kwargs,
    ):
        if not self.is_ready:
            # TODO, this should be forwarded, but right now this results in failure and we want to test this behavior
            self.ref.node.with_overrides(  # type: ignore
                node_name=node_name,
                aliases=aliases,
                requests=requests,
                limits=limits,
                timeout=timeout,
                retries=retries,
                interruptible=interruptible,
                name=name,
                task_config=task_config,
                container_image=container_image,
                accelerator=accelerator,
                cache=cache,
                cache_version=cache_version,
                cache_serialize=cache_serialize,
                *args,
                **kwargs,
            )
        return self

    def __repr__(self):
        if self._promise_ready:
            return f"Resolved({self._var}={self._val})"
        return f"Promise(node:{self.ref.node_id}.{self._var}.{self.attr_path})"

    def __str__(self):
        return str(self.__repr__())

    def deepcopy(self) -> Promise:
        new_promise = Promise(var=self.var, val=self.val)
        new_promise._promise_ready = self._promise_ready
        new_promise._ref = self._ref
        new_promise._attr_path = deepcopy(self._attr_path)
        return new_promise

    def __getitem__(self, key) -> Promise:
        """
        When we use [] to access the attribute on the promise, for example

        ```
        @workflow
        def wf():
            o = t1()
            t2(x=o["a"][0])
        ```

        The attribute keys are appended on the promise and a new promise is returned with the updated attribute path.
        We don't modify the original promise because it might be used in other places as well.
        """

        if self.ref and self._type:
            if self._type.simple == SimpleType.STRUCT and self._type.metadata is None:
                raise ValueError(f"Trying to index into a unschematized struct type {self.var}[{key}].")
        if isinstance(self.val, _literals_models.Literal):
            if self.val.scalar and self.val.scalar.generic:
                if self._type and self._type.metadata is None:
                    raise ValueError(
                        f"Trying to index into a generic type {self.var}[{key}]."
                        f" It seems the upstream type is not indexable."
                        f" Prefer using `typing.Dict[str, ...]` or `@dataclass`"
                        f" Note: {self.var} is the name of the variable in your workflow function."
                    )
                raise ValueError(
                    f"Trying to index into a struct {self.var}[{key}]. Use {self.var}.{key} instead."
                    f" Note: {self.var} is the name of the variable in your workflow function."
                )
        return self._append_attr(key)

    def __iter__(self):
        """
        Flyte/kit (as of https://github.com/flyteorg/flyte/issues/3864) supports indexing into a list of promises.
        But it still doesn't make sense to
        """
        raise ValueError(
            f" {self.var} is a Promise. Promise objects are not iterable - can't range() over a promise."
            " But you can use [index] or the alpha version of @eager workflows"
        )

    def __getattr__(self, key) -> Promise:
        """
        When we use . to access the attribute on the promise, for example

        ```
        @workflow
        def wf():
            o = t1()
            t2(o.a.b)
        ```

        The attribute keys are appended on the promise and a new promise is returned with the updated attribute path.
        We don't modify the original promise because it might be used in other places as well.
        """
        if isinstance(self.val, _literals_models.Literal):
            if self.val.scalar and self.val.scalar.generic:
                if self._type and self._type.metadata is None:
                    raise ValueError(
                        f"Trying to index into a generic type {self.var}[{key}]."
                        f" It seems the upstream type is not indexable."
                        f" Prefer using `typing.Dict[str, ...]` or `@dataclass`"
                        f" Note: {self.var} is the name of the variable in your workflow function."
                    )
        return self._append_attr(key)

    def _append_attr(self, key) -> Promise:
        new_promise = self.deepcopy()

        # The attr_path on the promise is for local_execute
        new_promise._attr_path.append(key)

        if new_promise.ref is not None:
            # The attr_path on the ref node is for remote execute
            new_promise._ref = new_promise.ref.with_attr(key)

        return new_promise


def create_native_named_tuple(
    ctx: FlyteContext,
    promises: Union[Tuple[Promise], Promise, VoidPromise, None],
    entity_interface: Interface,
) -> Optional[Tuple]:
    """
    Creates and returns a Named tuple with all variables that match the expected named outputs. this makes
    it possible to run things locally and expect a more native behavior, i.e. address elements of a named tuple
    by name.
    """
    if entity_interface is None:
        raise ValueError("Interface of the entity is required to generate named outputs")

    if promises is None:
        return None

    if isinstance(promises, Promise):
        k, v = [(k, v) for k, v in entity_interface.outputs.items()][0]  # get output native type
        # only show the name of output key if it's user-defined (by default Flyte names these as "o<n>")
        key = k if k != "o0" else 0
        try:
            return TypeEngine.to_python_value(ctx, promises.val, v)
        except Exception as e:
            raise TypeError(
                f"Failed to convert output in position {key} of value {promises.val}, expected type {v}."
            ) from e

    if len(cast(Tuple[Promise], promises)) == 0:
        return None

    named_tuple_name = "DefaultNamedTupleOutput"
    if entity_interface.output_tuple_name:
        named_tuple_name = entity_interface.output_tuple_name

    outputs = {}
    for i, p in enumerate(cast(Tuple[Promise], promises)):
        if not isinstance(p, Promise):
            raise AssertionError(
                "Workflow outputs can only be promises that are returned by tasks. Found a value of"
                f"type {type(p)}. Workflows cannot return local variables or constants."
            )
        t = entity_interface.outputs[p.var]
        try:
            outputs[p.var] = TypeEngine.to_python_value(ctx, p.val, t)
        except Exception as e:
            # only show the name of output key if it's user-defined (by default Flyte names these as "o<n>")
            key = p.var if p.var != f"o{i}" else i
            raise TypeError(f"Failed to convert output in position {key} of value {p.val}, expected type {t}.") from e

    # Should this class be part of the Interface?
    nt = collections.namedtuple(named_tuple_name, list(outputs.keys()))  # type: ignore
    return nt(**outputs)


# To create a class that is a named tuple, we might have to create namedtuplemeta and manipulate the tuple
def create_task_output(
    promises: Optional[Union[List[Promise], Promise]],
    entity_interface: Optional[Interface] = None,
) -> Optional[Union[Tuple[Promise], Promise]]:
    # TODO: Add VoidPromise here to simplify things at call site. Consider returning for [] below as well instead of
    #   raising an exception.
    if promises is None:
        return None

    if isinstance(promises, Promise):
        return promises

    if len(promises) == 0:
        raise ValueError(
            "This function should not be called with an empty list. It should have been handled with a"
            "VoidPromise at this function's call-site."
        )

    if len(promises) == 1:
        if not entity_interface:
            return promises[0]
        # See transform_function_to_interface for more information, we're using the existence of a name as a proxy
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
    class Output(collections.namedtuple(named_tuple_name, variables)):  # type: ignore
        def with_overrides(self, *args, **kwargs):
            val = self.__getattribute__(self._fields[0])
            val.with_overrides(*args, **kwargs)
            return self

        def runs_before(self, other: Any):
            """
            This function is just here to allow local workflow execution to run. See the corresponding function in
            flytekit.core.node.Node for more information. Local workflow execution in the manual ``create_node``
            paradigm is already determined by the order in which the nodes were created.
            """
            # TODO: If possible, add a check and raise an Exception if create_node was not called in the correct order.
            return self

        def __rshift__(self, other: Any):
            # See comment for runs_before
            return other

    return Output(*promises)  # type: ignore


def binding_data_from_python_std(
    ctx: _flyte_context.FlyteContext,
    expected_literal_type: _type_models.LiteralType,
    t_value: Any,
    t_value_type: type,
    nodes: List[Node],
) -> _literals_models.BindingData:
    # This handles the case where the given value is the output of another task
    if isinstance(t_value, Promise):
        if not t_value.is_ready:
            nodes.append(t_value.ref.node)  # keeps track of upstream nodes
            return _literals_models.BindingData(promise=t_value.ref)

    elif isinstance(t_value, VoidPromise):
        raise AssertionError(
            f"Cannot pass output from task {t_value.task_name} that produces no outputs to a downstream task"
        )

    elif t_value is not None and expected_literal_type.union_type is not None:
        # If the value is not a container type, then we can directly convert it to a scalar in the Union case.
        # This pushes the handling of the Union types to the type engine.
        if not isinstance(t_value, list) and not isinstance(t_value, dict):
            scalar = TypeEngine.to_literal(ctx, t_value, t_value_type or type(t_value), expected_literal_type).scalar
            return _literals_models.BindingData(scalar=scalar)

        # If it is a container type, then we need to iterate over the variants in the Union type, try each one. This is
        # akin to what the Type Engine does when it finds a Union type (see the UnionTransformer), but we can't rely on
        # that in this case, because of the mix and match of realized values, and Promises.
        for i in range(len(expected_literal_type.union_type.variants)):
            try:
                lt_type = expected_literal_type.union_type.variants[i]
                python_type = get_args(t_value_type)[i] if t_value_type else None
                return binding_data_from_python_std(ctx, lt_type, t_value, python_type, nodes)
            except Exception:
                logger.debug(
                    f"failed to bind data {t_value} with literal type {expected_literal_type.union_type.variants[i]}."
                )
        raise AssertionError(
            f"Failed to bind data {t_value} with literal type {expected_literal_type.union_type.variants}."
        )

    elif isinstance(t_value, list):
        sub_type: Optional[type] = ListTransformer.get_sub_type_or_none(t_value_type)
        collection = _literals_models.BindingDataCollection(
            bindings=[
                binding_data_from_python_std(ctx, expected_literal_type.collection_type, t, sub_type or type(t), nodes)
                for t in t_value
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
        if expected_literal_type.simple == _type_models.SimpleType.STRUCT:
            lit = TypeEngine.to_literal(ctx, t_value, type(t_value), expected_literal_type)
            return _literals_models.BindingData(scalar=lit.scalar)
        else:
            _, v_type = DictTransformer.extract_types_or_metadata(t_value_type)
            m = _literals_models.BindingDataMap(
                bindings={
                    k: binding_data_from_python_std(
                        ctx, expected_literal_type.map_value_type, v, v_type or type(v), nodes
                    )
                    for k, v in t_value.items()
                }
            )
        return _literals_models.BindingData(map=m)

    elif isinstance(t_value, tuple):
        raise AssertionError(
            "Tuples are not a supported type for individual values in Flyte - got a tuple -"
            f" {t_value}. If using named tuple in an inner task, please, de-reference the"
            "actual attribute that you want to use. For example, in NamedTuple('OP', x=int) then"
            "return v.x, instead of v, even if this has a single element"
        )

    # This is the scalar case - e.g. my_task(in1=5)
    scalar = TypeEngine.to_literal(ctx, t_value, t_value_type or type(t_value), expected_literal_type).scalar
    return _literals_models.BindingData(scalar=scalar)


def binding_from_python_std(
    ctx: _flyte_context.FlyteContext,
    var_name: str,
    expected_literal_type: _type_models.LiteralType,
    t_value: Any,
    t_value_type: type,
) -> Tuple[_literals_models.Binding, List[Node]]:
    nodes: List[Node] = []
    binding_data = binding_data_from_python_std(
        ctx,
        expected_literal_type,
        t_value,
        t_value_type,
        nodes,
    )
    return _literals_models.Binding(var=var_name, binding=binding_data), nodes


def to_binding(p: Promise) -> _literals_models.Binding:
    return _literals_models.Binding(var=p.var, binding=_literals_models.BindingData(promise=p.ref))


class VoidPromise(object):
    """
    This object is returned for tasks that do not return any outputs (declared interface is empty)
    VoidPromise cannot be interacted with and does not allow comparisons or any operations
    """

    def __init__(self, task_name: str, ref: Optional[NodeOutput] = None):
        self._task_name = task_name
        self._ref = ref

    def runs_before(self, *args, **kwargs):
        """
        This is a placeholder and should do nothing. It is only here to enable local execution of workflows
        where a task returns nothing.
        """

    @property
    def ref(self) -> Optional[NodeOutput]:
        return self._ref

    def __rshift__(self, other: Union[Promise, VoidPromise]):
        if self.ref and other.ref:
            self.ref.node.runs_before(other.ref.node)
        return other

    def with_overrides(self, *args, **kwargs):
        if self.ref:
            self.ref.node.with_overrides(*args, **kwargs)
        return self

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
    def __init__(self, node: Node, var: str, attr_path: Optional[List[Union[str, int]]] = None):
        """
        :param node:
        :param var: The name of the variable this NodeOutput references
        """
        if attr_path is None:
            attr_path = []

        self._node = node
        super(NodeOutput, self).__init__(self._node.id, var, attr_path)

    @property
    def node_id(self):
        """
        Override the underlying node_id property to refer to the Node's id. This is to make sure that overriding
        node IDs from with_overrides gets serialized correctly.
        :rtype: Text
        """
        return self.node.id

    @property
    def node(self) -> Node:
        """Return Node object."""
        return self._node

    def __repr__(self) -> str:
        s = f"Node({self.node if self.node.id is not None else None}:{self.var})"
        return s

    def deepcopy(self) -> NodeOutput:
        return NodeOutput(node=self.node, var=self.var, attr_path=deepcopy(self._attr_path))

    def with_attr(self, key) -> NodeOutput:
        new_node_output = self.deepcopy()
        new_node_output._attr_path.append(key)
        return new_node_output


class SupportsNodeCreation(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def python_interface(self) -> flyte_interface.Interface: ...

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata: ...


class HasFlyteInterface(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def interface(self) -> _interface_models.TypedInterface: ...

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata: ...


def extract_obj_name(name: str) -> str:
    """
    Generates a shortened name, without the module information. Useful for node-names etc. Only extracts the final
    object information often separated by `.` in the python fully qualified notation
    """
    if name is None:
        return ""
    if "." in name:
        return name.split(".")[-1]
    return name


def create_and_link_node_from_remote(
    ctx: FlyteContext,
    entity: HasFlyteInterface,
    _inputs_not_allowed: Optional[Set[str]] = None,
    _ignorable_inputs: Optional[Set[str]] = None,
    **kwargs,
) -> Optional[Union[Tuple[Promise], Promise, VoidPromise]]:
    """
    This method is used to generate a node with bindings especially when using remote entities, like FlyteWorkflow,
    FlyteTask and FlyteLaunchplan.

    This method is kept separate from the similar named method `create_and_link_node` as remote entities have to be
    handled differently. The major difference arises from the fact that the remote entities do not have a python
    interface, so all comparisons need to happen using the Literals.

    :param ctx: FlyteContext
    :param entity: RemoteEntity
    :param _inputs_not_allowed: Set of all variable names that should not be provided when using this entity.
                     Useful for Launchplans with `fixed` inputs
    :param _ignorable_inputs: Set of all variable names that are optional, but if provided will be overridden. Useful
                     for launchplans with `default` inputs
    :param kwargs: Dict[str, Any] default inputs passed from the user to this entity. Can be promises.
    :return:  Optional[Union[Tuple[Promise], Promise, VoidPromise]]
    """
    if ctx.compilation_state is None:
        raise _user_exceptions.FlyteAssertion("Cannot create node when not compiling...")

    used_inputs = set()
    bindings = []

    typed_interface = entity.interface

    if _inputs_not_allowed:
        inputs_not_allowed_specified = _inputs_not_allowed.intersection(kwargs.keys())
        if inputs_not_allowed_specified:
            raise _user_exceptions.FlyteAssertion(
                f"Fixed inputs cannot be specified. Please remove the following inputs - {inputs_not_allowed_specified}"
            )
    nodes = []
    for k in sorted(typed_interface.inputs):
        var = typed_interface.inputs[k]
        if k not in kwargs:
            if (_ignorable_inputs and k in _ignorable_inputs) or (_inputs_not_allowed and k in _inputs_not_allowed):
                continue
            # TODO to improve the error message, should we show python equivalent types for var.type?
            raise _user_exceptions.FlyteAssertion("Missing input `{}` type `{}`".format(k, var.type))
        v = kwargs[k]
        # This check ensures that tuples are not passed into a function, as tuples are not supported by Flyte
        # Usually a Tuple will indicate that multiple outputs from a previous task were accidentally passed
        # into the function.
        if isinstance(v, tuple):
            raise AssertionError(
                f"Variable({k}) for function({entity.name}) cannot receive a multi-valued tuple {v}."
                f" Check if the predecessor function returning more than one value?"
            )
        try:
            b, n = binding_from_python_std(
                ctx,
                var_name=k,
                expected_literal_type=var.type,
                t_value=v,
                t_value_type=type(v),  # since we don't have the python type available
            )
            bindings.append(b)
            nodes.extend(n)
            used_inputs.add(k)
        except Exception as e:
            raise AssertionError(f"Failed to Bind variable {k} for function {entity.name}.") from e

    extra_inputs = used_inputs ^ set(kwargs.keys())
    if len(extra_inputs) > 0:
        raise _user_exceptions.FlyteAssertion(
            f"Too many inputs for [{entity.name}] Expected inputs: {typed_interface.inputs.keys()} "
            f"- extra inputs: {extra_inputs}"
        )

    # Detect upstream nodes
    # These will be our core Nodes until we can amend the Promise to use NodeOutputs that reference our Nodes
    upstream_nodes = list(set([n for n in nodes if n.id != _common_constants.GLOBAL_INPUT_NODE_ID]))

    flytekit_node = Node(
        id=f"{ctx.compilation_state.prefix}n{len(ctx.compilation_state.nodes)}",
        metadata=entity.construct_node_metadata(),
        bindings=sorted(bindings, key=lambda b: b.var),
        upstream_nodes=upstream_nodes,
        flyte_entity=entity,
    )
    ctx.compilation_state.add_node(flytekit_node)

    if len(typed_interface.outputs) == 0:
        return VoidPromise(entity.name, NodeOutput(node=flytekit_node, var="placeholder"))

    # Create a node output object for each output, they should all point to this node of course.
    node_outputs = []
    for output_name, output_var_model in typed_interface.outputs.items():
        node_outputs.append(
            Promise(output_name, NodeOutput(node=flytekit_node, var=output_name), type=output_var_model.type)
        )

    return create_task_output(node_outputs)


def create_and_link_node(
    ctx: FlyteContext,
    entity: SupportsNodeCreation,
    **kwargs,
) -> Optional[Union[Tuple[Promise], Promise, VoidPromise]]:
    """
    This method is used to generate a node with bindings within a flytekit workflow. this is useful to traverse the
    workflow using regular python interpreter and generate nodes and promises whenever an execution is encountered

    :param ctx: FlyteContext
    :param entity: RemoteEntity
    :param kwargs: Dict[str, Any] default inputs passed from the user to this entity. Can be promises.
    :return:  Optional[Union[Tuple[Promise], Promise, VoidPromise]]
    """
    if ctx.compilation_state is None:
        raise _user_exceptions.FlyteAssertion("Cannot create node when not compiling...")

    used_inputs = set()
    bindings = []
    nodes = []

    interface = entity.python_interface
    typed_interface = flyte_interface.transform_interface_to_typed_interface(
        interface, allow_partial_artifact_id_binding=True
    )
    # Mypy needs some extra help to believe that `typed_interface` will not be `None`
    assert typed_interface is not None

    for k in sorted(interface.inputs):
        var = typed_interface.inputs[k]
        if var.type.simple == SimpleType.NONE:
            raise TypeError("Arguments do not have type annotation")
        if k not in kwargs:
            # interface.inputs_with_defaults[k][0] is the type of the default argument
            # interface.inputs_with_defaults[k][1] is the value of the default argument
            if k in interface.inputs_with_defaults and (
                interface.inputs_with_defaults[k][1] is not None
                or UnionTransformer.is_optional_type(interface.inputs_with_defaults[k][0])
            ):
                default_val = interface.inputs_with_defaults[k][1]
                # Common cases of mutable default arguments, as described in https://www.pullrequest.com/blog/python-pitfalls-the-perils-of-using-lists-and-dicts-as-default-arguments/
                # or https://florimond.dev/en/posts/2018/08/python-mutable-defaults-are-the-source-of-all-evil, are not supported.
                # As of 2024-08-05, Python native sets are not supported in Flytekit. However, they are included here for completeness.
                if isinstance(default_val, list) or isinstance(default_val, dict) or isinstance(default_val, set):
                    raise _user_exceptions.FlyteAssertion(
                        f"Argument {k} for function {entity.name} is a mutable default argument, which is a python anti-pattern and not supported in flytekit tasks"
                    )
                kwargs[k] = default_val
            else:
                error_msg = f"Input {k} of type {interface.inputs[k]} was not specified for function {entity.name}"
                raise _user_exceptions.FlyteAssertion(error_msg)
        v = kwargs[k]
        # This check ensures that tuples are not passed into a function, as tuples are not supported by Flyte
        # Usually a Tuple will indicate that multiple outputs from a previous task were accidentally passed
        # into the function.
        if isinstance(v, tuple):
            raise AssertionError(
                f"Variable({k}) for function({entity.name}) cannot receive a multi-valued tuple {v}."
                f" Check if the predecessor function returning more than one value?"
            )
        try:
            b, n = binding_from_python_std(
                ctx,
                var_name=k,
                expected_literal_type=var.type,
                t_value=v,
                t_value_type=interface.inputs[k],
            )
            bindings.append(b)
            nodes.extend(n)
            used_inputs.add(k)
        except Exception as e:
            raise AssertionError(f"Failed to Bind variable {k} for function {entity.name}.") from e

    extra_inputs = used_inputs ^ set(kwargs.keys())
    if len(extra_inputs) > 0:
        raise _user_exceptions.FlyteAssertion(
            "Too many inputs were specified for the interface.  Extra inputs were: {}".format(extra_inputs)
        )

    # Detect upstream nodes
    # These will be our core Nodes until we can amend the Promise to use NodeOutputs that reference our Nodes
    upstream_nodes = list(set([n for n in nodes if n.id != _common_constants.GLOBAL_INPUT_NODE_ID]))

    flytekit_node = Node(
        # TODO: Better naming, probably a derivative of the function name.
        id=f"{ctx.compilation_state.prefix}n{len(ctx.compilation_state.nodes)}",
        metadata=entity.construct_node_metadata(),
        bindings=sorted(bindings, key=lambda b: b.var),
        upstream_nodes=upstream_nodes,
        flyte_entity=entity,
    )
    ctx.compilation_state.add_node(flytekit_node)

    if len(typed_interface.outputs) == 0:
        return VoidPromise(entity.name, NodeOutput(node=flytekit_node, var="placeholder"))

    # Create a node output object for each output, they should all point to this node of course.
    node_outputs = []
    for output_name, output_var_model in typed_interface.outputs.items():
        node_outputs.append(
            Promise(output_name, NodeOutput(node=flytekit_node, var=output_name), output_var_model.type)
        )
        # Don't print this, it'll crash cuz sdk_node._upstream_node_ids might be None, but idl code will break

    return create_task_output(node_outputs, interface)


class LocallyExecutable(Protocol):
    def local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise, None]: ...

    def local_execution_mode(self) -> ExecutionState.Mode: ...


def flyte_entity_call_handler(
    entity: SupportsNodeCreation, *args, **kwargs
) -> Union[Tuple[Promise], Promise, VoidPromise, Tuple, Coroutine, None]:
    """
    This function is the call handler for tasks, workflows, and launch plans (which redirects to the underlying
    workflow). The logic is the same for all three, but we did not want to create base class, hence this separate
    method. When one of these entities is () aka __called__, there are three things we may do:
    #. Compilation Mode - this happens when the function is called as part of a workflow (potentially
       dynamic task?). Instead of running the user function, produce promise objects and create a node.
    #. Workflow Execution Mode - when a workflow is being run locally. Even though workflows are functions
       and everything should be able to be passed through naturally, we'll want to wrap output values of the
       function into objects, so that potential .with_cpu or other ancillary functions can be attached to do
       nothing. Subsequent tasks will have to know how to unwrap these. If by chance a non-Flyte task uses a
       task output as an input, things probably will fail pretty obviously.
    #. Start a local execution - This means that we're not already in a local workflow execution, which means that
       we should expect inputs to be native Python values and that we should return Python native values.
    """
    # Make sure arguments are part of interface
    for k, v in kwargs.items():
        if k not in entity.python_interface.inputs:
            raise AssertionError(f"Received unexpected keyword argument '{k}' in function '{entity.name}'")

    # Check if we have more arguments than expected
    if len(args) > len(entity.python_interface.inputs):
        raise AssertionError(
            f"Received more arguments than expected in function '{entity.name}'. Expected {len(entity.python_interface.inputs)} but got {len(args)}"
        )

    # Convert args to kwargs
    for arg, input_name in zip(args, entity.python_interface.inputs.keys()):
        if input_name in kwargs:
            raise AssertionError(f"Got multiple values for argument '{input_name}' in function '{entity.name}'")
        kwargs[input_name] = arg

    ctx = FlyteContextManager.current_context()
    if ctx.execution_state and (
        ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION
        or ctx.execution_state.mode == ExecutionState.Mode.LOCAL_TASK_EXECUTION
    ):
        logger.error("You are not supposed to nest @Task/@Workflow inside a @Task!")
    if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
        return create_and_link_node(ctx, entity=entity, **kwargs)
    if ctx.execution_state and ctx.execution_state.is_local_execution():
        mode = cast(LocallyExecutable, entity).local_execution_mode()
        omt = OutputMetadataTracker()
        with FlyteContextManager.with_context(
            ctx.with_execution_state(ctx.execution_state.with_params(mode=mode)).with_output_metadata_tracker(omt)
        ) as child_ctx:
            if (
                child_ctx.execution_state
                and child_ctx.execution_state.branch_eval_mode == BranchEvalMode.BRANCH_SKIPPED
            ):
                if len(entity.python_interface.inputs) > 0 or len(entity.python_interface.outputs) > 0:
                    output_names = list(entity.python_interface.outputs.keys())
                    if len(output_names) == 0:
                        return VoidPromise(entity.name)
                    vals = [Promise(var, None) for var in output_names]
                    return create_task_output(vals, entity.python_interface)
                else:
                    return None
            return cast(LocallyExecutable, entity).local_execute(ctx, **kwargs)
    else:
        mode = cast(LocallyExecutable, entity).local_execution_mode()
        omt = OutputMetadataTracker()
        with FlyteContextManager.with_context(
            ctx.with_execution_state(ctx.new_execution_state().with_params(mode=mode)).with_output_metadata_tracker(omt)
        ) as child_ctx:
            cast(ExecutionParameters, child_ctx.user_space_params)._decks = []
            result = cast(LocallyExecutable, entity).local_execute(child_ctx, **kwargs)

        expected_outputs = len(entity.python_interface.outputs)
        if expected_outputs == 0:
            if result is None or isinstance(result, VoidPromise):
                return None
            else:
                raise ValueError(f"Received an output when workflow local execution expected None. Received: {result}")

        if inspect.iscoroutine(result):
            return result

        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.DYNAMIC_TASK_EXECUTION:
            return result

        if (1 < expected_outputs == len(cast(Tuple[Promise], result))) or (
            result is not None and expected_outputs == 1
        ):
            return create_native_named_tuple(ctx, result, entity.python_interface)

        raise AssertionError(
            f"Expected outputs and actual outputs do not match."
            f"Result {result}. "
            f"Python interface: {entity.python_interface}"
        )
