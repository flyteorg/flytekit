from __future__ import annotations

import datetime
import typing
from typing import Tuple, Union

from flytekit.core import interface as flyte_interface
from flytekit.core.context_manager import FlyteContext
from flytekit.core.promise import Promise, VoidPromise, flyte_entity_call_handler
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.types import LiteralType
from flytekit.core.promise import NodeOutput
from flytekit.exceptions.user import FlyteValueException

DEFAULT_TIMEOUT = datetime.timedelta(hours=1)


class Gate(object):
    """
    A gate needs to behave kind of like a task. Instead of running code however, it either needs to wait for user
    input, or run a timer.
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
            self._python_interface = None  # We don't know how to find the python interface

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

        # If this is a signaling node then the output should be of the type requested by the signal.
        if not self._upstream_item or not isinstance(self._upstream_item, NodeOutput):
            raise ValueError("You can't check for a Python interface for an approval node outside of compilation")

        if not self._upstream_item.node.flyte_entity.python_interface:
            raise ValueError(f"Upstream node doesn't have a Python interface. Node entity is: "
                             f"{self._upstream_item.node.flyte_entity}")
        # Saving the interface here is pointless for now, because the approve function is called from scratch each
        # run. Unlike tasks. If you call t1(), no matter how many times you call it, t1 is the same Python object.
        # Gate nodes do not have an associated Python object, unless we one day save them during workflow compile.
        self._python_interface = self._upstream_item.node.flyte_entity.python_interface
        return self._python_interface

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata:
        # Part of SupportsNodeCreation interface
        return _workflow_model.NodeMetadata(
            name=self.name,
            timeout=self._timeout,
        )

    # This is to satisfy the LocallyExecutable protocol
    def local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        if self.sleep_duration:
            print(f"Fake sleeping {self.name}")
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
            raise ValueError("upstreamm item should not be none")  # this is true right? need to check this.
        x = input(f"Pausing execution for {self.name}, value is: {self._upstream_item} approve [Y/n]: ")
        if x != "n":
            return self._upstream_item
        else:
            raise FlyteValueException(f"User did not approve the transaction for gate node {self.name}")


def signal(name: str, timeout: datetime.timedelta, expected_type: typing.Type):
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

def approve(upstream_item: Union[Tuple[Promise], Promise, VoidPromise], name: str, timeout: datetime.timedelta, expected_type: typing.Type):
    """
    Create a Gate object. This object will function like a task. Note that unlike a task,
    each time this function is called, a new Python object is created. If a workflow
    calls a subworkflow twice, and the subworkflow has a signal, then two Gate
    objects are created. This shouldn't be a problem as long as the objects are identical.

    :param upstream_item:
    :param name:
    :param timeout:
    :param expected_type:
    :return:
    """
    g = Gate(name, upstream_item=upstream_item, timeout=timeout)

    return flyte_entity_call_handler(g)
