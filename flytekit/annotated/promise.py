import collections
from typing import Union, Dict, Any, List, Tuple

from flytekit import engine as flytekit_engine
from flytekit.common.promise import NodeOutput as _NodeOutput
from flytekit.models import literals as _literal_models, interface as _interface_models


def translate_inputs_to_literals(ctx, input_kwargs: Dict[str, Any], interface: _interface_models.TypedInterface):
    """
    When calling a task inside a workflow, a user might do something like this.

    def my_wf(in1: int) -> int:
        a = task_1(in1=in1)
        b = task_2(in1=5, in2=a)
        return b

    If this is the case, when task_2 is called in local workflow execution, we'll need to translate the Python native
    literal 5 to a Flyte literal.
    """
    # TODO: Are there any pass-by-reference considerations we need to think about?  I don't think so. Write unit
    #  tests to be sure.
    for k, v in input_kwargs.items():
        if k not in interface.inputs:
            raise ValueError(f"Received unexpected keyword argument {k}")
        # In the example above, this handles the "in2=a" type of argument
        if isinstance(v, Promise):
            input_kwargs[k] = v.val
        # This handles native values, the 5 example
        else:
            val = input_kwargs[k]
            var = interface.inputs[k]
            input_kwargs[k] = flytekit_engine.python_value_to_idl_literal(ctx, val, var.type)

    return input_kwargs


# TODO: The NodeOutput object, which this Promise wraps, has an sdk_type. Since we're no longer using sdk types,
#  we should consider adding a literal type to this object as well for downstream checking when Bindings are created.
class Promise(object):
    def __init__(self, var: str, val: Union[_NodeOutput, _literal_models.Literal]):
        self._var = var
        self._promise_ready = True
        self._val = val
        if isinstance(val, _NodeOutput):
            self._ref = val
            self._promise_ready = False
            self._val = None

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

    def with_overrides(self, *args, **kwargs):
        if not self.is_ready:
            # TODO, this should be forwarded, but right now this results in failure and we want to test this behavior
            # self.ref.sdk_node.with_overrides(*args, **kwargs)
            print(f"Forwarding to node {self.ref.sdk_node.id}")
        return self


# To create a class that is a named tuple, we might have to create namedtuplemeta and manipulate the tuple
def create_task_output(promises: Union[List[Promise], Promise, None]) -> Union[Tuple[Promise], Promise, None]:
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
