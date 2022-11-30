from __future__ import annotations

import datetime
import typing
from typing import Tuple, Union

from flytekit.core import interface as flyte_interface
from flytekit.core.context_manager import FlyteContext
from flytekit.core.promise import NodeOutput, Promise, VoidPromise, flyte_entity_call_handler
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions.user import FlyteValueException
from flytekit.loggers import logger
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.types import LiteralType
from flytekit.core.context_manager import FlyteContextManager


DEFAULT_TIMEOUT = datetime.timedelta(hours=1)


class Gate(object):
    """
    A node type that waits for user input before proceeding with a workflow.
    A gate is a type of node that behaves like a task, but instead of running code, it either needs to wait
    for user input to proceed or wait for a timer to complete running.
    """

    def __init__(
        self,
        name: str,
        input_type: typing.Optional[typing.Type] = None,
        upstream_item: typing.Optional[typing.Any] = None,
        sleep_duration: typing.Optional[datetime.timedelta] = None,
        timeout: typing.Optional[datetime.timedelta] = None,
    ):
        self._name = name
        self._input_type = input_type
        self._sleep_duration = sleep_duration
        self._timeout = timeout or DEFAULT_TIMEOUT
        self._upstream_item = upstream_item
        self._literal_type = TypeEngine.to_literal_type(input_type) if input_type else None

        # Determine the python interface if we can
        if self._sleep_duration:
            # Just a sleep so there is no interface
            self._python_interface = flyte_interface.Interface()
        elif input_type:
            # Waiting for user input, so the output of the node is whatever input the user provides.
            self._python_interface = flyte_interface.Interface(
                outputs={
                    "o0": self.input_type,
                }
            )
        else:
            # We don't know how to find the python interface here, approve() sets it below, See the code.
            self._python_interface = None

    @property
    def name(self) -> str:
        # Part of SupportsNodeCreation interface
        return self._name

    @property
    def input_type(self) -> typing.Optional[typing.Type]:
        return self._input_type

    @property
    def literal_type(self) -> typing.Optional[LiteralType]:
        return self._literal_type

    @property
    def sleep_duration(self) -> typing.Optional[datetime.timedelta]:
        return self._sleep_duration

    @property
    def python_interface(self) -> flyte_interface.Interface:
        """
        This will not be valid during local execution
        Part of SupportsNodeCreation interface
        """
        # If this is just a sleep node, or user input node, then it will have a Python interface upon construction.
        if self._python_interface:
            return self._python_interface

        raise ValueError("You can't check for a Python interface for an approval node outside of compilation")

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata:
        # Part of SupportsNodeCreation interface
        return _workflow_model.NodeMetadata(
            name=self.name,
            timeout=self._timeout,
        )

    # This is to satisfy the LocallyExecutable protocol
    def local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        if self.sleep_duration:
            print(f"Mock sleeping for {self.sleep_duration}")
            return VoidPromise(self.name)

        # Trigger stdin
        if self.input_type:
            if issubclass(self.input_type, bool):
                input(f"Pausing execution for gate {self.name}, press enter to continue...\n")
                l = TypeEngine.to_literal(ctx, True, bool, TypeEngine.to_literal_type(bool))  # noqa
                p = Promise(var="o0", val=l)
                return p
            elif issubclass(self.input_type, int):
                x = input(f"Pausing execution for {self.name}, enter integer: ")
                ii = int(x)
                logger.debug(f"Parsed {x} to {ii} from gate node {self.name}")
                l = TypeEngine.to_literal(ctx, ii, int, TypeEngine.to_literal_type(int))  # noqa
                p = Promise(var="o0", val=l)
                return p
            elif issubclass(self.input_type, float):
                x = input(f"Pausing execution for {self.name}, enter float: ")
                ff = float(x)
                logger.debug(f"Parsed {x} to {ff} from gate node {self.name}")
                l = TypeEngine.to_literal(ctx, ff, float, TypeEngine.to_literal_type(float))  # noqa
                p = Promise(var="o0", val=l)
                return p
            else:
                # Todo: We should implement the rest by way of importing the code in pyflyte run
                #   that parses text from the command line
                raise Exception("only bool or int/float")

        # Assume this is an approval operation
        if self._upstream_item is None:
            # this is true right? need to check this. if nothing is returned it should still be a void promise
            raise ValueError("upstream item should not be none")
        x = input(f"Pausing execution for {self.name}, value is: {self._upstream_item} approve [Y/n]: ")
        if x != "n":
            # Think we need to return a promise here.
            return self._upstream_item
        else:
            raise FlyteValueException(f"User did not approve the transaction for gate node {self.name}")


def wait_for_input(name: str, timeout: datetime.timedelta, expected_type: typing.Type):
    """
    Create a Gate object. This object will function like a task. Note that unlike a task,
    each time this function is called, a new Python object is created. If a workflow
    calls a subworkflow twice, and the subworkflow has a signal, then two Gate
    objects are created. This shouldn't be a problem as long as the objects are identical.

    :param name:
    :param timeout:
    :param expected_type:
    :return:
    """

    g = Gate(name, input_type=expected_type, timeout=timeout)

    return flyte_entity_call_handler(g)


def sleep(duration: datetime.timedelta):
    """
    :param duration:
    :return:
    """
    g = Gate("sleep-gate", sleep_duration=duration)

    return flyte_entity_call_handler(g)


def approve(upstream_item: Union[Tuple[Promise], Promise, VoidPromise], name: str, timeout: datetime.timedelta):
    """
    Create a Gate object. This object will function like a task. Note that unlike a task,
    each time this function is called, a new Python object is created. If a workflow
    calls a subworkflow twice, and the subworkflow has a signal, then two Gate
    objects are created. This shouldn't be a problem as long as the objects are identical.

    :param upstream_item:
    :param name:
    :param timeout:
    :return:
    """
    g = Gate(name, upstream_item=upstream_item, timeout=timeout)

    if upstream_item is None or isinstance(upstream_item, VoidPromise):
        raise ValueError("You can't use approval on a task that doesn't return anything.")

    ctx = FlyteContextManager.current_context()
    if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
        if not upstream_item.ref.node.flyte_entity.python_interface:
            raise ValueError(
                f"Upstream node doesn't have a Python interface. Node entity is: "
                f"{upstream_item.ref.node.flyte_entity}"
            )

        # We have reach back up to the entity that this promise came from, to get the python type, since
        # the approve function itself doesn't have a python interface.
        io_type = upstream_item.ref.node.flyte_entity.python_interface.outputs[upstream_item.var]
        io_var_name = upstream_item.var
    else:
        # We don't know the python type here. in local execution, downstream doesn't really use the type
        # so we should be okay. But use None instead of type() so that errors are more obvious hopefully.
        io_type = None
        io_var_name = "o0"

    # In either case, we need a python interface
    g._python_interface = flyte_interface.Interface(
        inputs={
            io_var_name: io_type,
        },
        outputs={
            io_var_name: io_type,
        },
    )
    kwargs = {io_var_name: upstream_item}

    return flyte_entity_call_handler(g, **kwargs)
